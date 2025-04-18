"""
src/exapyte/consensus/protocol_factory.py
=========================================

Factory for creating different consensus protocol implementations.
This demonstrates how different consensus algorithms can be swapped
while maintaining the same system architecture.
"""

import logging
from enum import Enum

# Import consensus implementations (will be implemented later)
from exapyte.consensus.raft_node import RaftNode
from exapyte.consensus.paxos_node import PaxosNode
from exapyte.consensus.zab_node import ZabNode


class ConsensusType(Enum):
    """Types of consensus protocols supported by the platform"""
    RAFT = "raft"
    PAXOS = "paxos"
    ZAB = "zab"


class ProtocolFactory:
    """Factory for creating consensus protocol instances"""
    
    @staticmethod
    def create_consensus_node(protocol_type, node_id, cluster_config, **kwargs):
        """
        Create a consensus protocol node of the specified type
        
        Args:
            protocol_type: Type of consensus protocol to create
            node_id: Unique identifier for this node
            cluster_config: Configuration for the cluster
            **kwargs: Additional protocol-specific parameters
            
        Returns:
            A consensus protocol node implementation
        """
        # Handle both enum and string inputs
        if isinstance(protocol_type, str):
            try:
                protocol_type = ConsensusType(protocol_type)
            except ValueError:
                raise ValueError(f"Unsupported consensus protocol: {protocol_type}")
                
        logging.info(f"Creating {protocol_type.value} consensus node with ID {node_id}")
        
        if protocol_type == ConsensusType.RAFT:
            from src.exapyte.consensus.raft_node import RaftNode
            return RaftNode(node_id, cluster_config, **kwargs)
        elif protocol_type == ConsensusType.PAXOS:
            from src.exapyte.consensus.paxos_node import PaxosNode
            return PaxosNode(node_id, cluster_config, **kwargs)
        elif protocol_type == ConsensusType.ZAB:
            from src.exapyte.consensus.zab_node import ZabNode
            return ZabNode(node_id, cluster_config, **kwargs)
        else:
            raise ValueError(f"Unsupported consensus protocol: {protocol_type}")
    
    @staticmethod
    def get_protocol_characteristics(protocol_type):
        """
        Returns key characteristics of each consensus protocol
        to demonstrate knowledge of their differences
        
        Args:
            protocol_type: Type of consensus protocol
            
        Returns:
            Dict containing protocol characteristics
        """
        if protocol_type == ConsensusType.RAFT:
            return {
                "name": "Raft",
                "leadership": "Strong leader model",
                "election": "Randomized timeouts",
                "log_replication": "Leader-driven, sequential",
                "safety": "Never commits entries from previous terms unless explicitly committed",
                "membership_changes": "Joint consensus approach",
                "best_for": "Understandability and systems where leader crashes are infrequent",
                "trade_offs": "Simplicity over absolute performance optimization"
            }
        elif protocol_type == ConsensusType.PAXOS:
            return {
                "name": "Paxos",
                "leadership": "Weak leader model (Multi-Paxos)",
                "election": "Leader 'acquisition' through prepare phase",
                "log_replication": "Two-phase commit per value",
                "safety": "Guaranteed through quorums and proposal numbers",
                "membership_changes": "Complex, typically requires reconfiguration protocols",
                "best_for": "Theoretical completeness and performance in stable environments",
                "trade_offs": "Conceptual complexity for optimized message patterns"
            }
        elif protocol_type == ConsensusType.ZAB:
            return {
                "name": "ZooKeeper Atomic Broadcast (ZAB)",
                "leadership": "Primary-backup model",
                "election": "Fast leader election with epoch numbers",
                "log_replication": "Two-phase commit with bulk transfers",
                "safety": "Guaranteed through epochs and version numbers",
                "membership_changes": "Managed through separate configuration mechanism",
                "best_for": "High-throughput broadcast scenarios with large fan-out",
                "trade_offs": "Optimized for ZooKeeper's specific requirements"
            }
        else:
            raise ValueError(f"Unsupported consensus protocol: {protocol_type}")
