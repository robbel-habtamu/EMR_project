#!/usr/bin/env python3
"""
Production EMR Cluster Launcher - ZERO CONFIGURATION FOR USERS
===============================================================
This tool automates the creation of an EMR cluster, a Bastion Host, and an 
Edge Node with JupyterHub pre-configured for secure team access.

Features:
- Automatic Bastion Host creation (SSH gateway)
- Automatic Edge Node setup with JupyterHub, Spark, and Hadoop clients
- Generates ready-to-use SSH tunneling scripts for team members
- Enforces modern EMR release labels and best practices

Usage:
    # Create the complete environment
    python emr_launcher.py --config config.json --mode create
    
    # Get connection info and regenerate scripts
    python emr_launcher.py --config config.json --mode connect
    
    # Check the status of the cluster and nodes
    python emr_launcher.py --config config.json --mode status
    
    # Terminate all AWS resources created (EMR, Bastion, Edge Node)
    python emr_launcher.py --config config.json --mode terminate
"""

import argparse
import json
import sys
import os
from typing import Dict

# Check for required external dependencies first
try:
    import boto3
    from botocore.exceptions import NoCredentialsError
except ImportError:
    print("ERROR: boto3 is required. Install with: pip install boto3")
    sys.exit(1)

# Import the core logic from the separate file
try:
    from core.emr_manager import EMRLauncher
except ImportError:
    print("ERROR: Could not find 'core/emr_manager.py'. Ensure the file structure is correct.")
    sys.exit(1)

def load_config(config_path: str) -> Dict:
    """Load the configuration JSON file."""
    try:
        with open(config_path, 'r') as f:
            return json.load(f)
    except FileNotFoundError:
        print(f"ERROR: Configuration file not found at {config_path}")
        sys.exit(1)
    except json.JSONDecodeError:
        print(f"ERROR: Invalid JSON format in {config_path}")
        sys.exit(1)

def main():
    """Parses arguments and executes the EMR Launcher."""
    parser = argparse.ArgumentParser(
        description="Production EMR Cluster Launcher for secure team access."
    )
    parser.add_argument(
        '--config',
        type=str,
        default='config.json',
        help='Path to the configuration JSON file (default: config.json).'
    )
    parser.add_argument(
        '--mode',
        type=str,
        required=True,
        choices=['create', 'connect', 'status', 'terminate'],
        help='Operation mode: create, connect, status, or terminate.'
    )
    parser.add_argument(
        '--region',
        type=str,
        default=os.environ.get('AWS_REGION', 'us-east-1'),
        help='AWS region (default: us-east-1 or AWS_REGION env var).'
    )
    parser.add_argument(
        '--dry-run',
        action='store_true',
        help='Only validate configuration, do not make any AWS changes (only applicable to create mode).'
    )
    
    args = parser.parse_args()
    
    print(f"Running in AWS Region: {args.region}")
    
    # Load Configuration
    config = load_config(args.config)
    
    # Instantiate and Execute Core Logic
    launcher = EMRLauncher(region=args.region, dry_run=args.dry_run)
    
    try:
        launcher.execute(config, args.mode)
    except NoCredentialsError:
        print("\nFATAL ERROR: AWS credentials not found. Ensure you have run 'aws configure'.")
        sys.exit(1)
    except Exception as e:
        print(f"\nFATAL ERROR: A critical error occurred during execution: {type(e).__name__}")
        print(f"   Error details: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main()
