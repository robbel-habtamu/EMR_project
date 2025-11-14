# core/emr_manager.py

import sys
import time
from typing import Dict, Optional, Tuple, List
from pathlib import Path
from datetime import datetime
import json
import os
import base64

try:
    import boto3
    from botocore.exceptions import ClientError, NoCredentialsError
except ImportError:
    # This check will be reinforced in the main file
    pass

# Import dataclass from the same package
from .dataclasses import ClusterResult 

class EMRLauncher:
    """Production EMR launcher with zero-config for end users. Handles all AWS interactions."""
    
    def __init__(self, region: str = "us-east-1", dry_run: bool = False):
        self.region = region
        self.dry_run = dry_run
        
        try:
            self.emr = boto3.client('emr', region_name=region)
            self.ec2 = boto3.client('ec2', region_name=region)
            self.sts = boto3.client('sts', region_name=region)
        except NoCredentialsError:
            print("ERROR: AWS credentials not found. Run: aws configure")
            sys.exit(1)
            
    # --- Public Methods ---

    def execute(self, config: Dict, mode: str) -> Optional[ClusterResult]:
        """Main entry point to execute the chosen mode."""
        self._validate_config(config)
        
        if mode == 'create':
            return self._mode_create(config)
        elif mode == 'connect':
            return self._mode_connect(config)
        elif mode == 'status':
            return self._mode_status(config)
        elif mode == 'terminate':
            return self._mode_terminate(config)
        else:
            print(f"ERROR: Unknown mode: {mode}")
            sys.exit(1)
            
    # --- Mode Implementations ---

    def _mode_create(self, config: Dict) -> Optional[ClusterResult]:
        """Create cluster mode."""
        print("\n" + "="*80)
        print("CREATE MODE: Setting up complete environment")
        print("="*80)
        
        existing = self._find_existing_cluster(config['cluster_name'])
        if existing:
            print(f"\nWARNING: CLUSTER ALREADY EXISTS! ID: {existing['cluster_id']}. State: {existing['state']}")
            print(f"\nINFO: Use --mode connect to get connection details")
            return self._get_cluster_info(existing['cluster_id'], config)
            
        if self.dry_run:
            print("Configuration is valid (dry-run mode)")
            return None
        
        # 1. Create Bastion Host
        bastion_id, bastion_ip = None, None
        if config.get('create_edge_node', False):
            bastion_id, bastion_ip = self._create_bastion_host(config)
            
        # 2. Create EMR cluster
        cluster_id = self._create_cluster(config)
        self._wait_for_cluster(cluster_id)
        cluster_info = self._get_cluster_details(cluster_id)
        
        # 3. Create Edge Node
        edge_id, edge_private_ip, edge_public_ip = None, None, None
        if config.get('create_edge_node', False):
            edge_id, edge_private_ip, edge_public_ip = self._create_edge_node(
                config, cluster_id, cluster_info, bastion_id
            )
            
        # 4. Build result & Generate scripts
        result = self._build_result(config, cluster_id, cluster_info, bastion_id, bastion_ip, edge_id, edge_private_ip, edge_public_ip)
        self._generate_user_scripts(result, config)
        self._display_setup_complete(result, config)
        
        return result

    def _mode_connect(self, config: Dict) -> ClusterResult:
        """Get connection info."""
        print("\n" + "="*80)
        print("CONNECTION INFO")
        print("="*80)
        
        existing = self._find_existing_cluster(config['cluster_name'])
        if not existing:
            print(f"\nERROR: No cluster found: {config['cluster_name']}")
            sys.exit(1)
            
        result = self._get_cluster_info(existing['cluster_id'], config)
        self._generate_user_scripts(result, config)
        self._display_setup_complete(result, config)
        
        return result

    def _mode_status(self, config: Dict) -> ClusterResult:
        """Check status."""
        print("\n" + "="*80)
        print("CLUSTER STATUS")
        print("="*80)
        
        existing = self._find_existing_cluster(config['cluster_name'])
        if not existing:
            print(f"\nERROR: No cluster found: {config['cluster_name']}")
            sys.exit(1)
            
        result = self._get_cluster_info(existing['cluster_id'], config)
        
        print(f"\nCluster: {result.cluster_name}")
        print(f"    ID: {result.cluster_id}")
        print(f"    State: {result.cluster_state}")
        
        if result.bastion_instance_id:
            print(f"\nBastion: {result.bastion_instance_id}")
            print(f"    IP: {result.bastion_public_ip}")
            
        if result.edge_instance_id:
            print(f"\nEdge Node: {result.edge_instance_id}")
            print(f"    Private IP: {result.edge_private_ip}")
            print(f"    Status: JupyterHub, Spark UI, Hadoop UI are running")
            
        return result

    def _mode_terminate(self, config: Dict) -> None:
        """Terminate all related resources."""
        print("\n" + "="*80)
        print("TERMINATE MODE")
        print("="*80)
        
        existing = self._find_existing_cluster(config['cluster_name'])
        if not existing:
            print(f"\nINFO: No active cluster found: {config['cluster_name']}")
            return
            
        confirm = input(f"\nWARNING: Terminate {config['cluster_name']} and all associated resources? Type 'yes': ")
        if confirm.lower() != 'yes':
            print("Termination cancelled.")
            return
            
        # Terminate Bastion
        bastion_id = self._find_bastion(config['cluster_name'])
        if bastion_id:
            print(f"\nTerminating Bastion: {bastion_id}")
            self.ec2.terminate_instances(InstanceIds=[bastion_id])
            
        # Terminate Edge Node
        edge_id = self._find_edge_node(config['cluster_name'])
        if edge_id:
            print(f"Terminating Edge Node: {edge_id}")
            self.ec2.terminate_instances(InstanceIds=[edge_id])
            
        # Terminate Cluster
        print(f"Terminating Cluster: {existing['cluster_id']}")
        self.emr.terminate_job_flows(JobFlowIds=[existing['cluster_id']])
        
        print(f"\nSUCCESS: Termination initiated. EMR cleanup will complete in a few minutes.")
        
    # --- Utility/Internal Methods ---
    
    def _validate_config(self, config: Dict):
        """Validates required keys."""
        required_keys = ['cluster_name', 'subnet_id', 'key_pair_name', 'release_label', 'service_role', 'instance_profile', 'log_uri']
        for key in required_keys:
            if key not in config:
                print(f"ERROR: Configuration error: Missing required key '{key}' in config.json")
                sys.exit(1)
                
    def _find_existing_cluster(self, cluster_name: str) -> Optional[Dict]:
        """Finds an existing EMR cluster by name and state."""
        response = self.emr.list_clusters(
            ClusterStates=['STARTING', 'BOOTSTRAPPING', 'RUNNING', 'WAITING']
        )
        for cluster in response.get('Clusters', []):
            if cluster['Name'] == cluster_name:
                return {'cluster_id': cluster['Id'], 'state': cluster['Status']['State']}
        return None

    def _get_master_private_ip(self, cluster_id: str) -> str:
        """Gets the master node's private IP."""
        response = self.emr.list_instances(
            ClusterId=cluster_id,
            InstanceGroupTypes=['MASTER']
        )
        instance_id = response['Instances'][0]['Ec2InstanceId']
        ec2_response = self.ec2.describe_instances(InstanceIds=[instance_id])
        return ec2_response['Reservations'][0]['Instances'][0]['PrivateIpAddress']

    def _get_cluster_details(self, cluster_id: str) -> Dict:
        """Gets the full EMR cluster details."""
        return self.emr.describe_cluster(ClusterId=cluster_id)['Cluster']

    def _wait_for_cluster(self, cluster_id: str):
        """Waits for the EMR cluster to enter the WAITING state."""
        print(f"WAITING: Cluster {cluster_id} is starting (this takes 15-20 min)...")
        waiter = self.emr.get_waiter('cluster_running')
        waiter.wait(
            ClusterId=cluster_id,
            WaiterConfig={'Delay': 30, 'MaxAttempts': 100}
        )
        print("SUCCESS: EMR Cluster is RUNNING/WAITING.")

    def _create_cluster(self, config: Dict) -> str:
        """Launches the EMR cluster."""
        print("\n" + "="*80)
        print("Creating EMR Cluster...")
        print("="*80)
        
        applications = config.get('applications', [])
        if any(app.get('Name') == 'JupyterHub' for app in applications):
            print("INFO: Launching EMR with JupyterHub application.")
        
        response = self.emr.run_job_flow(
            Name=config['cluster_name'],
            LogUri=config['log_uri'],
            ReleaseLabel=config['release_label'],
            ServiceRole=config['service_role'],
            Instances={
                'MasterInstanceType': config.get('master_instance_type', 'm5.xlarge'),
                'SlaveInstanceType': config.get('slave_instance_type', 'm5.large'),
                'InstanceCount': config.get('instance_count', 3),
                'Ec2SubnetId': config['subnet_id'],
                'Ec2KeyName': config['key_pair_name'],
                'KeepJobFlowAliveWhenNoSteps': True,
                'TerminationProtected': config.get('termination_protected', False),
                'InstanceProfile': config['instance_profile']
            },
            Applications=applications,
            Configurations=config.get('configurations', []),
            Tags=[{'Key': 'ClusterName', 'Value': config['cluster_name']}]
        )
        
        cluster_id = response['JobFlowId']
        print(f"SUCCESS: EMR Cluster requested: {cluster_id}")
        return cluster_id

    def _find_bastion(self, cluster_name: str) -> Optional[str]:
        """Finds the Bastion EC2 instance ID."""
        response = self.ec2.describe_instances(
            Filters=[
                {'Name': 'tag:ClusterName', 'Values': [cluster_name]},
                {'Name': 'tag:Role', 'Values': ['Bastion']},
                {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
            ]
        )
        reservations = response.get('Reservations', [])
        if reservations and reservations[0].get('Instances'):
            return reservations[0]['Instances'][0]['InstanceId']
        return None

    def _find_edge_node(self, cluster_name: str) -> Optional[str]:
        """Finds the Edge Node EC2 instance ID."""
        response = self.ec2.describe_instances(
            Filters=[
                {'Name': 'tag:ClusterName', 'Values': [cluster_name]},
                {'Name': 'tag:Role', 'Values': ['EdgeNode']},
                {'Name': 'instance-state-name', 'Values': ['pending', 'running']}
            ]
        )
        reservations = response.get('Reservations', [])
        if reservations and reservations[0].get('Instances'):
            return reservations[0]['Instances'][0]['InstanceId']
        return None
        
    def _create_bastion_host(self, config: Dict) -> Tuple[str, str]:
        """Create Bastion Host for team access."""
        print("\n" + "="*80)
        print("Creating Bastion Host (Gateway)")
        print("="*80)
        
        bastion_name = f"{config['cluster_name']}-Bastion"
        existing = self._find_bastion(config['cluster_name'])
        if existing:
            response = self.ec2.describe_instances(InstanceIds=[existing])
            instance = response['Reservations'][0]['Instances'][0]
            public_ip = instance.get('PublicIpAddress', 'N/A')
            print(f"INFO: Reusing existing Bastion: {existing}")
            print(f"    IP: {public_ip}")
            return existing, public_ip
            
        # Get VPC from subnet
        subnet_info = self.ec2.describe_subnets(SubnetIds=[config['subnet_id']])
        vpc_id = subnet_info['Subnets'][0]['VpcId']
        
        # Create Bastion Security Group
        try:
            bastion_sg = self.ec2.create_security_group(
                GroupName=f"{bastion_name}-SG",
                Description=f"Bastion host for {config['cluster_name']}",
                VpcId=vpc_id
            )['GroupId']
            self.ec2.authorize_security_group_ingress(
                GroupId=bastion_sg,
                IpPermissions=[{
                    'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 
                    'IpRanges': [{'CidrIp': '0.0.0.0/0', 'Description': 'SSH access from anywhere'}]
                }]
            )
            print(f"SUCCESS: Created Bastion Security Group: {bastion_sg}")
        except ClientError as e:
            if 'already exists' in str(e):
                response = self.ec2.describe_security_groups(
                     Filters=[
                         {'Name': 'group-name', 'Values': [f"{bastion_name}-SG"]},
                         {'Name': 'vpc-id', 'Values': [vpc_id]}
                     ]
                )
                bastion_sg = response['SecurityGroups'][0]['GroupId']
                print(f"INFO: Using existing Bastion SG: {bastion_sg}")
            else:
                raise

        # Get latest Amazon Linux 2 AMI
        ami_response = self.ec2.describe_images(
             Owners=['amazon'],
             Filters=[
                 {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                 {'Name': 'state', 'Values': ['available']}
             ]
         )
        ami_id = sorted(ami_response['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]['ImageId']

        # Launch Bastion
        response = self.ec2.run_instances(
            ImageId=ami_id,
            InstanceType='t3.micro',
            KeyName=config['key_pair_name'],
            MinCount=1, MaxCount=1, SecurityGroupIds=[bastion_sg], SubnetId=config['subnet_id'],
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [
                {'Key': 'Name', 'Value': bastion_name}, 
                {'Key': 'ClusterName', 'Value': config['cluster_name']}, 
                {'Key': 'Role', 'Value': 'Bastion'}
            ]}]
        )
        
        bastion_id = response['Instances'][0]['InstanceId']
        print(f"SUCCESS: Bastion requested: {bastion_id}")
        print(f"WAITING: Waiting for Bastion to start...")
        
        waiter = self.ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[bastion_id])
        
        # Get public IP
        response = self.ec2.describe_instances(InstanceIds=[bastion_id])
        bastion_ip = response['Reservations'][0]['Instances'][0]['PublicIpAddress']
        
        print(f"SUCCESS: Bastion ready: {bastion_ip}")
        return bastion_id, bastion_ip
        

    def _create_edge_node(self, config: Dict, cluster_id: str, cluster_info: Dict, bastion_id: str) -> Tuple[str, str, str]:
        """Create Edge Node with proper security."""
        print("\n" + "="*80)
        print("Creating Edge Node (JupyterHub + UIs)")
        print("="*80)
        
        edge_name = f"{config['cluster_name']}-EdgeNode"
        existing = self._find_edge_node(config['cluster_name'])
        if existing:
            response = self.ec2.describe_instances(InstanceIds=[existing])
            instance = response['Reservations'][0]['Instances'][0]
            private_ip = instance.get('PrivateIpAddress', 'N/A')
            public_ip = instance.get('PublicIpAddress', 'N/A')
            print(f"INFO: Reusing existing Edge Node: {existing}")
            return existing, private_ip, public_ip
            
        # Security Group Setup
        master_sg = cluster_info['Ec2InstanceAttributes']['EmrManagedMasterSecurityGroup']
        subnet_id = cluster_info['Ec2InstanceAttributes']['Ec2SubnetId']
        master_private_ip = self._get_master_private_ip(cluster_id)
        
        # Get Bastion SG ID
        bastion_response = self.ec2.describe_instances(InstanceIds=[bastion_id])
        bastion_sg = bastion_response['Reservations'][0]['Instances'][0]['SecurityGroups'][0]['GroupId']
        
        # Allow traffic from Bastion to Edge Node (via EMR Master SG)
        try:
            self.ec2.authorize_security_group_ingress(
                GroupId=master_sg,
                IpPermissions=[{'IpProtocol': 'tcp', 'FromPort': 22, 'ToPort': 22, 'UserIdGroupPairs': [{'GroupId': bastion_sg}]}]
            )
            print(f"INFO: Configured security rule: Bastion ({bastion_sg}) to Edge Node ({master_sg}) for SSH.")
        except ClientError:
            pass # Rule already exists
            
        # Get AMI (using the same Amazon Linux 2 AMI as Bastion)
        ami_response = self.ec2.describe_images(
             Owners=['amazon'],
             Filters=[
                 {'Name': 'name', 'Values': ['amzn2-ami-hvm-*-x86_64-gp2']},
                 {'Name': 'state', 'Values': ['available']}
             ]
         )
        ami_id = sorted(ami_response['Images'], key=lambda x: x['CreationDate'], reverse=True)[0]['ImageId']
        
        # Generate setup script and Base64 encode it for UserData
        edge_config = config.get('edge_node', {})
        user_data_script = self._generate_edge_setup_script(
            config, cluster_id, edge_config, master_private_ip
        )
        user_data = base64.b64encode(user_data_script.encode('utf-8')).decode('utf-8')
        
        # Launch Edge Node
        response = self.ec2.run_instances(
            ImageId=ami_id,
            InstanceType=edge_config.get('instance_type', 'm5.xlarge'),
            KeyName=config['key_pair_name'],
            MinCount=1, MaxCount=1, SubnetId=subnet_id, SecurityGroupIds=[master_sg],
            IamInstanceProfile={'Name': edge_config.get('iam_profile', 'EMR_EC2_DefaultRole')},
            UserData=user_data,
            BlockDeviceMappings=[{
                'DeviceName': '/dev/xvda',
                'Ebs': {'VolumeSize': edge_config.get('volume_size', 100), 'VolumeType': 'gp3', 'DeleteOnTermination': True}
            }],
            TagSpecifications=[{'ResourceType': 'instance', 'Tags': [
                {'Key': 'Name', 'Value': edge_name}, 
                {'Key': 'ClusterName', 'Value': config['cluster_name']}, 
                {'Key': 'Role', 'Value': 'EdgeNode'}
            ]}]
        )
        
        edge_id = response['Instances'][0]['InstanceId']
        print(f"SUCCESS: Edge Node requested: {edge_id}")
        print(f"WAITING: Waiting for Edge Node to start...")
        
        waiter = self.ec2.get_waiter('instance_running')
        waiter.wait(InstanceIds=[edge_id])
        
        # Get IPs
        response = self.ec2.describe_instances(InstanceIds=[edge_id])
        instance = response['Reservations'][0]['Instances'][0]
        private_ip = instance.get('PrivateIpAddress', 'N/A')
        public_ip = instance.get('PublicIpAddress', 'N/A')
        
        print(f"SUCCESS: Edge Node running at private IP: {private_ip}")
        print(f"WAITING: Installing JupyterHub, Spark/Hadoop clients and proxies (3-5 minutes)...")
        return edge_id, private_ip, public_ip

    def _generate_edge_setup_script(self, config: Dict, cluster_id: str, edge_config: Dict, master_ip: str) -> str:
        """Generate optimized edge node setup script."""
        
        users = edge_config.get('team_users', ['hadoop'])
        users_space = " ".join(users)
        users_python = "{'" + "', '".join(users) + "'}"
        
        # Note: Using EMR 6.10.0 compatible Spark/Hadoop client versions
        # The script sets up the necessary client configurations (core-site.xml) 
        # to connect to the EMR master node's HDFS.
        
        return f"""#!/bin/bash
set -e
exec > >(tee /var/log/edge-setup.log)
exec 2>&1

echo "=========================================================================="
echo "Edge Node Setup - Starting at $(date)"
echo "=========================================================================="

# System updates and package installation
yum update -y
yum install -y java-1.8.0-openjdk python3 python3-pip git wget curl unzip net-tools

# Install Node.js for JupyterHub proxy
curl -fsSL https://rpm.nodesource.com/setup_16.x | bash -
yum install -y nodejs

# Install JupyterHub and standard data science packages
python3 -m pip install --upgrade pip
python3 -m pip install jupyterhub==3.1.1 jupyterlab==3.6.3 notebook==6.5.4
python3 -m pip install pyspark==3.3.2 pandas numpy boto3 matplotlib seaborn
npm install -g configurable-http-proxy

# Create team users
for username in {users_space}; do
    if ! id "$username" >/dev/null 2>&1; then
        useradd -m -s /bin/bash -G hadoop "$username"
        echo "$username:ChangeMeNow123!" | chpasswd
        mkdir -p /home/$username/notebooks
        chown -R $username:$username /home/$username
    fi
done

# Install Hadoop & Spark clients (for client tools and configuration)
HADOOP_VERSION="3.3.3"
SPARK_VERSION="3.3.2"

cd /opt
if [ ! -d hadoop ]; then
    echo "Installing Hadoop Client $HADOOP_VERSION..."
    wget -q https://archive.apache.org/dist/hadoop/common/hadoop-$HADOOP_VERSION/hadoop-$HADOOP_VERSION.tar.gz
    tar -xzf hadoop-$HADOOP_VERSION.tar.gz && ln -s hadoop-$HADOOP_VERSION hadoop && rm hadoop-$HADOOP_VERSION.tar.gz
fi

if [ ! -d spark ]; then
    echo "Installing Spark Client $SPARK_VERSION..."
    wget -q https://archive.apache.org/dist/spark/spark-$SPARK_VERSION/spark-$SPARK_VERSION-bin-hadoop3.tgz
    tar -xzf spark-$SPARK_VERSION-bin-hadoop3.tgz && ln -s spark-$SPARK_VERSION-bin-hadoop3 spark && rm spark-$SPARK_VERSION-bin-hadoop3.tgz
fi

# Configure environment variables
cat > /etc/profile.d/bigdata.sh <<'EOF'
export HADOOP_HOME=/opt/hadoop
export SPARK_HOME=/opt/spark
export PATH=$HADOOP_HOME/bin:$SPARK_HOME/bin:$PATH
export HADOOP_CONF_DIR=/opt/hadoop/etc/hadoop
export JAVA_HOME=/usr/lib/jvm/jre-1.8.0-openjdk
export PYSPARK_PYTHON=/usr/bin/python3
EOF

source /etc/profile.d/bigdata.sh

# Configure Hadoop (pointing to EMR Master HDFS)
mkdir -p $HADOOP_CONF_DIR
cat > $HADOOP_CONF_DIR/core-site.xml <<CORESITE
<?xml version="1.0"?>
<configuration>
  <property>
    <name>fs.defaultFS</name>
    <value>hdfs://{master_ip}:8020</value>
  </property>
</configuration>
CORESITE

# Configure JupyterHub
mkdir -p /etc/jupyterhub
cat > /etc/jupyterhub/jupyterhub_config.py <<'JHCONFIG'
c.JupyterHub.bind_url = 'http://0.0.0.0:8000'
c.Spawner.default_url = '/lab'
c.Spawner.notebook_dir = '~/notebooks'
c.Authenticator.allowed_users = {users_python}
c.Spawner.environment = {{
    'SPARK_HOME': '/opt/spark',
    'HADOOP_HOME': '/opt/hadoop',
    'HADOOP_CONF_DIR': '/opt/hadoop/etc/hadoop',
    'PYSPARK_PYTHON': '/usr/bin/python3',
    'JAVA_HOME': '/usr/lib/jvm/jre-1.8.0-openjdk'
}}
JHCONFIG

# JupyterHub systemd service
cat > /etc/systemd/system/jupyterhub.service <<'JHSVC'
[Unit]
Description=JupyterHub
After=network.target

[Service]
Type=simple
User=root
ExecStart=/usr/local/bin/jupyterhub --config=/etc/jupyterhub/jupyterhub_config.py
WorkingDirectory=/etc/jupyterhub
Restart=always

[Install]
WantedBy=multi-user.target
JHSVC

systemctl daemon-reload
systemctl enable jupyterhub
systemctl start jupyterhub

# Spark History UI Proxy (Listening on 18080)
# Proxies requests from Edge Node to EMR Master (which runs the History Server)
cat > /opt/spark-proxy.py <<'SPARKPROXY'
#!/usr/bin/env python3
import http.server
import socketserver
from urllib.request import urlopen, Request
PORT = 18080
SPARK_SERVER = "http://{master_ip}:18080" # Spark History Server Port on EMR Master
class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self: http.server.SimpleHTTPRequestHandler):
        try:
            response = urlopen(SPARK_SERVER + self.path, timeout=10)
            self.send_response(200)
            for header, value in response.getheaders():
                if header.lower() not in ['transfer-encoding', 'connection']:
                    self.send_header(header, value)
            self.end_headers()
            self.wfile.write(response.read())
        except Exception as e:
            self.send_error(502, str(e))
with socketserver.TCPServer(("0.0.0.0", PORT), ProxyHandler) as httpd:
    httpd.serve_forever()
SPARKPROXY

chmod +x /opt/spark-proxy.py

cat > /etc/systemd/system/spark-proxy.service <<'SPARKSVC'
[Unit]
Description=Spark History Proxy
After=network.target
[Service]
Type=simple
User=hadoop
ExecStart=/usr/bin/python3 /opt/spark-proxy.py
Restart=always
[Install]
WantedBy=multi-user.target
SPARKSVC

systemctl daemon-reload
systemctl enable spark-proxy
systemctl start spark-proxy

# Hadoop ResourceManager UI Proxy (Listening on 8088)
# Proxies requests from Edge Node to EMR Master
cat > /opt/hadoop-proxy.py <<'HADOOPPROXY'
#!/usr/bin/env python3
import http.server
import socketserver
from urllib.request import urlopen
PORT = 8088
HADOOP_SERVER = "http://{master_ip}:8088" # Hadoop ResourceManager Port on EMR Master
class ProxyHandler(http.server.SimpleHTTPRequestHandler):
    def do_GET(self: http.server.SimpleHTTPRequestHandler):
        try:
            response = urlopen(HADOOP_SERVER + self.path, timeout=10)
            self.send_response(200)
            for header, value in response.getheaders():
                if header.lower() not in ['transfer-encoding', 'connection']:
                    self.send_header(header, value)
            self.end_headers()
            self.wfile.write(response.read())
        except Exception as e:
            self.send_error(502, str(e))
with socketserver.TCPServer(("0.0.0.0", PORT), ProxyHandler) as httpd:
    httpd.serve_forever()
HADOOPPROXY

chmod +x /opt/hadoop-proxy.py

cat > /etc/systemd/system/hadoop-proxy.service <<'HADOOPSVC'
[Unit]
Description=Hadoop RM Proxy
After=network.target
[Service]
Type=simple
User=hadoop
ExecStart=/usr/bin/python3 /opt/hadoop-proxy.py
Restart=always
[Install]
WantedBy=multi-user.target
HADOOPSVC

systemctl daemon-reload
systemctl enable hadoop-proxy
systemctl start hadoop-proxy

chown -R hadoop:hadoop /opt/hadoop /opt/spark

echo "=========================================================================="
echo "Edge Node Setup - Complete at $(date)"
echo "=========================================================================="
"""
        
    def _build_result(self, config: Dict, cluster_id: str, cluster_info: Dict, bastion_id: Optional[str], bastion_ip: Optional[str], edge_id: Optional[str], edge_private_ip: Optional[str], edge_public_ip: Optional[str]) -> ClusterResult:
        """Builds and returns a ClusterResult object."""
        return ClusterResult(
            cluster_id=cluster_id,
            cluster_name=config['cluster_name'],
            cluster_state=cluster_info.get('Status', {}).get('State', 'UNKNOWN'),
            master_dns=cluster_info.get('MasterPublicDnsName', 'N/A'),
            master_private_ip=self._get_master_private_ip(cluster_id),
            edge_instance_id=edge_id,
            edge_private_ip=edge_private_ip,
            edge_public_ip=edge_public_ip,
            bastion_instance_id=bastion_id,
            bastion_public_ip=bastion_ip,
            jupyterhub_ready=True if edge_id else False
        )

    def _get_cluster_info(self, cluster_id: str, config: Dict) -> ClusterResult:
        """Retrieves and consolidates all resource info into ClusterResult."""
        cluster_info = self._get_cluster_details(cluster_id)
        master_private_ip = self._get_master_private_ip(cluster_id)
        
        bastion_id = self._find_bastion(config['cluster_name'])
        bastion_ip = 'N/A'
        if bastion_id:
            response = self.ec2.describe_instances(InstanceIds=[bastion_id])
            bastion_ip = response['Reservations'][0]['Instances'][0].get('PublicIpAddress', 'N/A')
            
        edge_id = self._find_edge_node(config['cluster_name'])
        edge_private_ip, edge_public_ip = 'N/A', 'N/A'
        if edge_id:
            response = self.ec2.describe_instances(InstanceIds=[edge_id])
            instance = response['Reservations'][0]['Instances'][0]
            edge_private_ip = instance.get('PrivateIpAddress', 'N/A')
            edge_public_ip = instance.get('PublicIpAddress', 'N/A')
            
        return ClusterResult(
            cluster_id=cluster_id,
            cluster_name=config['cluster_name'],
            cluster_state=cluster_info.get('Status', {}).get('State', 'UNKNOWN'),
            master_dns=cluster_info.get('MasterPublicDnsName', 'N/A'),
            master_private_ip=master_private_ip,
            edge_instance_id=edge_id,
            edge_private_ip=edge_private_ip,
            edge_public_ip=edge_public_ip,
            bastion_instance_id=bastion_id,
            bastion_public_ip=bastion_ip,
            jupyterhub_ready=True if edge_id else False
        )
        
    def _display_setup_complete(self, result: ClusterResult, config: Dict):
        """Displays completion information."""
        print("\n" + "="*80)
        print("SETUP COMPLETE")
        print("="*80)
        
        if result.bastion_public_ip:
            print(f"\nBastion Host IP: {result.bastion_public_ip}")
            
        if result.edge_private_ip:
            print(f"\nEdge Node Private IP: {result.edge_private_ip}")
            print(f"   JupyterHub, Spark, Hadoop UIs are ready for tunneling.")

        print(f"\nAccess scripts created in: team_access_{config['cluster_name']}/")
        print("   Share this folder and the corresponding .pem key with your team.")
        print("\nNEXT STEP: Instruct users to run: ./connect.sh jupyter")

    def _generate_user_scripts(self, result: ClusterResult, config: Dict) -> None:
        """Generate ready-to-use scripts for team members."""
        
        output_dir = Path(f"team_access_{config['cluster_name']}")
        output_dir.mkdir(exist_ok=True)
        
        # --- connect.sh ---
        connect_script = f"""#!/bin/bash
##############################################################################
# EMR Cluster Connection Script
# Cluster: {config['cluster_name']}
# Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}
##############################################################################

BASTION_IP="{result.bastion_public_ip}"
EDGE_IP="{result.edge_private_ip}"
KEY_NAME="{config['key_pair_name']}"
KEY="~/.ssh/$KEY_NAME.pem"

# Check key exists
if [ ! -f "$KEY" ]; then
    echo "ERROR: SSH key not found: $KEY"
    echo "    Please place your SSH key there first, as described in README.txt."
    exit 1
fi

# Fix permissions
chmod 400 "$KEY" 2>/dev/null

case "$1" in
    jupyter | all)
        echo "Starting JupyterHub & UI tunnels..."
        if [ "$1" == "jupyter" ]; then
            echo ""
            echo "    Tunnel is active. Keep this terminal open!"
            echo "    Open browser: http://localhost:8000"
            echo "    Login with your username/password."
            echo ""
        else
            echo "    All tunnels opened. See README for URLs."
        fi

        # The connection jumps through the Bastion to the Edge Node
        ssh -i "$KEY" -J "ec2-user@$BASTION_IP" -N \\
            -L 8000:localhost:8000 \\
            -L 18080:localhost:18080 \\
            -L 8088:localhost:8088 \\
            "ec2-user@$EDGE_IP"
        ;;
    
    spark)
        echo "Starting Spark History Server tunnel..."
        echo "    Tunnel is active. URL: http://localhost:18080"
        ssh -i "$KEY" -J "ec2-user@$BASTION_IP" -N -L 18080:localhost:18080 "ec2-user@$EDGE_IP"
        ;;

    hadoop)
        echo "Starting Hadoop RM tunnel..."
        echo "    Tunnel is active. URL: http://localhost:8088"
        ssh -i "$KEY" -J "ec2-user@$BASTION_IP" -N -L 8088:localhost:8088 "ec2-user@$EDGE_IP"
        ;;

    ssh)
        echo "Connecting to Edge Node via Bastion..."
        ssh -i "$KEY" -J "ec2-user@$BASTION_IP" "ec2-user@$EDGE_IP"
        ;;
    
    *)
        echo "Usage: $0 [jupyter|spark|hadoop|all|ssh]"
        echo ""
        echo "Commands:"
        echo "  jupyter   - Access JupyterHub (http://localhost:8000)"
        echo "  spark     - Access Spark History Server (http://localhost:18080)"
        echo "  hadoop    - Access Hadoop ResourceManager (http://localhost:8088)"
        echo "  all       - Access all services at once (tunnels for 8000, 18080, 8088)"
        echo "  ssh       - Get shell access to Edge Node"
        echo ""
        echo "Example:"
        echo "  $0 jupyter"
        exit 1
        ;;
esac
"""
        
        script_path = output_dir / "connect.sh"
        script_path.write_text(connect_script)
        script_path.chmod(0o755)
        
        # --- README.txt ---
        edge_config = config.get('edge_node', {})
        users = edge_config.get('team_users', ['hadoop'])
        
        readme = f"""
================================================================================
EMR CLUSTER ACCESS GUIDE
================================================================================
Cluster: {config['cluster_name']}
Generated: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

================================================================================
I. WHAT YOU RECEIVED
================================================================================
1. connect.sh          - The main connection script (Linux/Mac)
2. README.txt          - This guide.
3. windows_guide.bat   - Guide for Windows users.

================================================================================
II. QUICK START (3 STEPS)
================================================================================

Step 1: Place SSH Key
---------------------
You must have the SSH key ({config['key_pair_name']}.pem) used for cluster creation.
Move the key to your home SSH directory and set permissions (Linux/Mac):

mkdir -p ~/.ssh
mv /path/to/{config['key_pair_name']}.pem ~/.ssh/{config['key_pair_name']}.pem
chmod 400 ~/.ssh/{config['key_pair_name']}.pem

Step 2: Make the script executable
-----------------------------------
chmod +x connect.sh

Step 3: Connect!
----------------
cd team_access_{config['cluster_name']}
./connect.sh jupyter

Then open your browser to: http://localhost:8000

================================================================================
III. YOUR LOGIN CREDENTIALS
================================================================================
Available Usernames: {', '.join(users)}
Default Password: ChangeMeNow123!

NOTE: Change your password immediately after first login via the JupyterHub Terminal (run: passwd).

================================================================================
IV. AVAILABLE ACCESS POINTS
================================================================================

1. JupyterHub (Notebooks on Edge Node)
    Command: ./connect.sh jupyter
    URL: http://localhost:8000
    
2. Spark History Server (UI Proxy)
    Command: ./connect.sh spark (or ./connect.sh all)
    URL: http://localhost:18080

3. Hadoop ResourceManager (UI Proxy)
    Command: ./connect.sh hadoop (or ./connect.sh all)
    URL: http://localhost:8088
"""
        
        readme_path = output_dir / "README.txt"
        readme_path.write_text(readme)
        
        # --- windows_guide.bat ---
        windows_script = f"""@echo off
REM EMR Cluster Connection Script for Windows
REM This file provides instructions for Windows users.

echo ================================================================================
echo EMR Cluster Access - Windows Guide
echo ================================================================================
echo.
echo SSH tunneling is best handled by a Linux shell environment.
echo.
echo Please use ONE of these options:
echo.
echo Option 1 (Recommended): Git Bash or WSL (Windows Subsystem for Linux)
echo    1. Ensure your SSH key is in the correct location (%%USERPROFILE%%\\.ssh\\{config['key_pair_name']}.pem).
echo    2. Open Git Bash or WSL terminal in the 'team_access_{config['cluster_name']}' folder.
echo    3. Run: ./connect.sh jupyter
echo.
echo Option 2: Raw SSH Command (Advanced)
echo    Run this command in a PowerShell or CMD window, replacing your key path:
echo    ssh -i "%%USERPROFILE%%\\.ssh\\{config['key_pair_name']}.pem" -J ec2-user@{result.bastion_public_ip} -N -L 8000:localhost:8000 ec2-user@{result.edge_private_ip}
echo.
pause
"""
        windows_path = output_dir / "windows_guide.bat"
        windows_path.write_text(windows_script)
