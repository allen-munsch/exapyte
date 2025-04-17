"""
src/exapyte/replication/replicator_factory.py
============================================

Factory for creating different replication strategies.
This enables swapping replication implementations to demonstrate
trade-offs in distributed data systems.
"""

import logging
from enum import Enum
from typing import Dict, Any

# Import replication implementations
from exapyte.replication.async_replicator import AsyncReplicator
from exapyte.replication.sync_replicator import SyncReplicator
from exapyte.replication.optimized_replicator import OptimizedReplicator


class ReplicationType(Enum):
    """Types of replication strategies supported by the platform"""
    ASYNC = "async"
    SYNC = "sync"
    OPTIMIZED = "optimized"


class ReplicatorFactory:
    """Factory for creating replication strategies"""
    
    @staticmethod
    def create_replicator(replication_type: ReplicationType, node_id: str, 
                         network_manager: Any, storage_engine: Any,
                         config: Dict[str, Any] = None) -> Any:
        """
        Create a replication strategy of the specified type
        
        Args:
            replication_type: Type of replication to create
            node_id: ID of the node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration for the replication strategy
            
        Returns:
            A replication strategy implementation
        """
        config = config or {}
        logger = logging.getLogger("replication.factory")
        logger.info(f"Creating {replication_type.value} replication strategy")
        
        if replication_type == ReplicationType.ASYNC:
            return AsyncReplicator(node_id, network_manager, storage_engine, config)
        elif replication_type == ReplicationType.SYNC:
            return SyncReplicator(node_id, network_manager, storage_engine, config)
        elif replication_type == ReplicationType.OPTIMIZED:
            return OptimizedReplicator(node_id, network_manager, storage_engine, config)
        else:
            raise ValueError(f"Unsupported replication type: {replication_type}")
    
    @staticmethod
    def get_replication_characteristics(replication_type: ReplicationType) -> Dict[str, str]:
        """
        Returns key characteristics of each replication type
        to demonstrate knowledge of their differences
        
        Args:
            replication_type: Type of replication
            
        Returns:
            Dict containing replication characteristics
        """
        if replication_type == ReplicationType.ASYNC:
            return {
                "name": "Asynchronous Replication",
                "consistency": "Eventual consistency",
                "latency": "Low write latency",
                "availability": "High availability during network partitions",
                "throughput": "High throughput",
                "recovery": "May require conflict resolution on recovery",
                "best_for": "High-throughput, latency-sensitive applications",
                "trade_offs": "Availability and latency over consistency"
            }
        elif replication_type == ReplicationType.SYNC:
            return {
                "name": "Synchronous Replication",
                "consistency": "Strong consistency",
                "latency": "Higher write latency",
                "availability": "Reduced availability during partitions",
                "throughput": "Lower throughput",
                "recovery": "Automatic consistency during recovery",
                "best_for": "Systems requiring strong consistency guarantees",
                "trade_offs": "Consistency over availability and latency"
            }
        elif replication_type == ReplicationType.OPTIMIZED:
            return {
                "name": "Optimized Replication",
                "consistency": "Tunable consistency levels",
                "latency": "Optimized based on data patterns",
                "availability": "Adaptive to network conditions",
                "throughput": "High throughput with batching and compression",
                "recovery": "Smart conflict resolution",
                "best_for": "Large-scale systems with mixed workloads",
                "trade_offs": "Complex implementation for better resource utilization"
            }
        else:
            raise ValueError(f"Unsupported replication type: {replication_type}")