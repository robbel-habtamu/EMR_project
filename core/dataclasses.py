# core/dataclasses.py

from dataclasses import dataclass
from typing import Optional

@dataclass
class ClusterResult:
    """Container for EMR cluster and supporting resource results."""
    cluster_id: str
    cluster_name: str
    cluster_state: str
    master_dns: str
    master_private_ip: str
    edge_instance_id: Optional[str] = None
    edge_private_ip: Optional[str] = None
    edge_public_ip: Optional[str] = None
    bastion_instance_id: Optional[str] = None
    bastion_public_ip: Optional[str] = None
    jupyterhub_ready: bool = False
