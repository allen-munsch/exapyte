"""
src/exapyte/distribution/strategy_factory.py
===========================================

Factory for creating different data distribution strategies.
This enables swapping distribution implementations to demonstrate
trade-offs in distributed data systems.
"""

import logging
from enum import Enum
from typing import Dict, Any

# Import distribution implementations
from exapyte.distribution.centralized import CentralizedDistribution
from exapyte.distribution.sharded import ShardedDistribution
from exapyte.distribution.hierarchical import HierarchicalDistribution


class DistributionType(Enum):
    """Types of distribution strategies supported by the platform"""
    CENTRALIZED = "centralized"
    SHARDED = "sharded"
    HIERARCHICAL = "hierarchical"


class StrategyFactory:
    """Factory for creating distribution strategies"""
    
    @staticmethod
    def create_strategy(strategy_type: DistributionType, node_id: str,
                       network_manager: Any, storage_engine: Any,
                       config: Dict[str, Any] = None) -> Any:
        """
        Create a distribution strategy of the specified type
        
        Args:
            strategy_type: Type of distribution strategy to create
            node_id: ID of the node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration for the distribution strategy
            
        Returns:
            A distribution strategy implementation
        """
        config = config or {}
        logger = logging.getLogger("distribution.factory")
        logger.info(f"Creating {strategy_type.value} distribution strategy")
        
        if strategy_type == DistributionType.CENTRALIZED:
            return CentralizedDistribution(node_id, network_manager, storage_engine, config)
        elif strategy_type == DistributionType.SHARDED:
            return ShardedDistribution(node_id, network_manager, storage_engine, config)
        elif strategy_type == DistributionType.HIERARCHICAL:
            return HierarchicalDistribution(node_id, network_manager, storage_engine, config)
        else:
            raise ValueError(f"Unsupported distribution strategy: {strategy_type}")
    
    @staticmethod
    def get_strategy_characteristics(strategy_type: DistributionType) -> Dict[str, str]:
        """
        Returns key characteristics of each distribution strategy
        to demonstrate knowledge of their differences
        
        Args:
            strategy_type: Type of distribution strategy
            
        Returns:
            Dict containing strategy characteristics
        """
        if strategy_type == DistributionType.CENTRALIZED:
            return {
                "name": "Centralized Distribution",
                "data_locality": "All data in central location",
                "scalability": "Limited by central node capacity",
                "query_routing": "Simple - all queries go to central node",
                "coordination": "Minimal coordination overhead",
                "failure_handling": "Single point of failure",
                "best_for": "Small datasets or simple architectures",
                "trade_offs": "Simplicity vs. scalability"
            }
        elif strategy_type == DistributionType.SHARDED:
            return {
                "name": "Sharded Distribution",
                "data_locality": "Data partitioned across nodes by key",
                "scalability": "Horizontal scalability for capacity",
                "query_routing": "Based on shard mapping",
                "coordination": "Moderate for shard management",
                "failure_handling": "Can lose subset of data on node failure",
                "best_for": "High-volume data with clear partitioning scheme",
                "trade_offs": "Scalability vs. query flexibility"
            }
        elif strategy_type == DistributionType.HIERARCHICAL:
            return {
                "name": "Hierarchical Distribution",
                "data_locality": "Multi-tier with global and local data",
                "scalability": "Global coordination with local scaling",
                "query_routing": "Context-aware with local optimization",
                "coordination": "Complex but efficient",
                "failure_handling": "Resilient with redundancy across tiers",
                "best_for": "Global systems with regional access patterns",
                "trade_offs": "Complexity vs. optimized access patterns"
            }
        else:
            raise ValueError(f"Unsupported distribution strategy: {strategy_type}")