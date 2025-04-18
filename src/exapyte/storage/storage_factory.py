"""
src/exapyte/storage/storage_factory.py
======================================

Factory for creating different storage backends.
This enables swapping storage implementations to demonstrate
trade-offs in distributed data systems.
"""

import logging
from enum import Enum
from typing import Dict, Any, Optional

# Import storage implementations
from src.exapyte.storage.memory_store import MemoryStore
from src.exapyte.storage.disk_store import DiskStore
from src.exapyte.storage.tiered_store import TieredStore


class StorageType(Enum):
    """Types of storage backends supported by the platform"""
    MEMORY = "memory"
    DISK = "disk"
    TIERED = "tiered"


class StorageFactory:
    """Factory for creating storage backends"""
    
    @staticmethod
    def create_storage(storage_type: StorageType, config: Dict[str, Any] = None) -> Any:
        """
        Create a storage backend of the specified type
        
        Args:
            storage_type: Type of storage to create
            config: Configuration for the storage backend
            
        Returns:
            A storage backend implementation
        """
        config = config or {}
        logger = logging.getLogger("storage.factory")
        logger.info(f"Creating {storage_type.value} storage backend")
        
        if storage_type == StorageType.MEMORY:
            return MemoryStore(config)
        elif storage_type == StorageType.DISK:
            return DiskStore(config)
        elif storage_type == StorageType.TIERED:
            return TieredStore(config)
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
    
    @staticmethod
    def get_storage_characteristics(storage_type: StorageType) -> Dict[str, str]:
        """
        Returns key characteristics of each storage type
        to demonstrate knowledge of their differences
        
        Args:
            storage_type: Type of storage
            
        Returns:
            Dict containing storage characteristics
        """
        if storage_type == StorageType.MEMORY:
            return {
                "name": "In-Memory Storage",
                "persistence": "None (data lost on restart)",
                "performance": "Highest read/write performance",
                "scalability": "Limited by available memory",
                "replication": "Requires custom synchronization",
                "best_for": "High-throughput, low-latency temporary data",
                "trade_offs": "Data durability vs. performance"
            }
        elif storage_type == StorageType.DISK:
            return {
                "name": "Disk-Based Storage",
                "persistence": "Full persistence with transaction log",
                "performance": "I/O bound, optimized for sequential access",
                "scalability": "Limited by disk space and I/O throughput",
                "replication": "Supports log shipping and snapshots",
                "best_for": "Durable data with moderate access patterns",
                "trade_offs": "Durability vs. latency"
            }
        elif storage_type == StorageType.TIERED:
            return {
                "name": "Tiered Storage",
                "persistence": "Configurable per tier",
                "performance": "Hot data in memory, cold data on disk",
                "scalability": "Dynamic capacity management across tiers",
                "replication": "Tier-aware replication strategies",
                "best_for": "Mixed workloads with varied access patterns",
                "trade_offs": "Complexity vs. optimized resource usage"
            }
        else:
            raise ValueError(f"Unsupported storage type: {storage_type}")
