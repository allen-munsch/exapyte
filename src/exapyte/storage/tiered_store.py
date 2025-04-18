"""
src/exapyte/storage/tiered_store.py
==================================

Tiered storage implementation for exapyte.
Combines memory and disk storage for optimal performance.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Set, Tuple


class TieredStore:
    """
    Tiered storage implementation
    
    Features:
    - Hot data in memory, cold data on disk
    - Automatic promotion/demotion between tiers
    - Configurable tier policies
    - Optimized for access patterns
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the tiered store
        
        Args:
            config: Configuration parameters
                - memory_tier_size: Maximum size of memory tier
                - promotion_threshold: Access count to promote to memory
                - demotion_threshold: Time without access to demote to disk
                - memory_tier_config: Configuration for memory tier
                - disk_tier_config: Configuration for disk tier
        """
        self.config = config or {}
        self.logger = logging.getLogger("storage.tiered")
        
        # Configuration
        self.memory_tier_size = self.config.get("memory_tier_size", 1000)
        self.promotion_threshold = self.config.get("promotion_threshold", 3)
        self.demotion_threshold = self.config.get("demotion_threshold", 3600)  # 1 hour
        
        # Import storage implementations here to avoid circular imports
        from exapyte.storage.memory_store import MemoryStore
        from exapyte.storage.disk_store import DiskStore
        
        # Initialize tiers
        memory_config = self.config.get("memory_tier_config", {})
        disk_config = self.config.get("disk_tier_config", {})
        
        self.memory_tier = MemoryStore(memory_config)
        self.disk_tier = DiskStore(disk_config)
        
        # Tier management
        self.access_counts = {}  # key -> access count
        self.last_access_time = {}  # key -> last access time
        self.tier_location = {}  # key -> "memory" or "disk"
        
        # Background tasks
        self.tier_management_task = None
        self.running = False
        
        # Metrics
        self.metrics = {
            "gets": 0,
            "sets": 0,
            "deletes": 0,
            "memory_hits": 0,
            "disk_hits": 0,
            "promotions": 0,
            "demotions": 0
        }
        
        self.logger.info(f"Initialized TieredStore with config: {self.config}")
    
    async def start(self):
        """Start the tiered store and background tasks"""
        self.logger.info("Starting TieredStore")
        self.running = True
        
        # Start tiers
        await self.memory_tier.start()
        await self.disk_tier.start()
        
        # Start tier management task
        self.tier_management_task = asyncio.create_task(self._tier_management_loop())
    
    async def stop(self):
        """Stop the tiered store and background tasks"""
        self.logger.info("Stopping TieredStore")
        self.running = False
        
        # Cancel background tasks
        if self.tier_management_task:
            self.tier_management_task.cancel()
            try:
                await self.tier_management_task
            except asyncio.CancelledError:
                pass
        
        # Stop tiers
        await self.memory_tier.stop()
        await self.disk_tier.stop()
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found
        """
        self.metrics["gets"] += 1
        
        # Update access tracking
        self._track_access(key)
        
        # Check memory tier first
        value = await self.memory_tier.get(key)
        if value is not None:
            self.metrics["memory_hits"] += 1
            return value
        
        # Check disk tier
        value = await self.disk_tier.get(key)
        if value is not None:
            self.metrics["disk_hits"] += 1
            
            # Consider promotion to memory tier
            await self._consider_promotion(key, value)
            
            return value
        
        return None
    
    async def set(self, key: str, value: Any, ttl: Optional[int] = None) -> bool:
        """
        Set a value by key
        
        Args:
            key: The key to set
            value: The value to store
            ttl: Time-to-live in seconds (None for default)
            
        Returns:
            True if set successfully
        """
        self.metrics["sets"] += 1
        
        # Update access tracking
        self._track_access(key)
        
        # Determine which tier to use
        if await self._should_use_memory_tier(key):
            # Store in memory tier
            result = await self.memory_tier.set(key, value, ttl)
            if result:
                self.tier_location[key] = "memory"
            return result
        else:
            # Store in disk tier
            result = await self.disk_tier.set(key, value, ttl)
            if result:
                self.tier_location[key] = "disk"
            return result
    
    async def delete(self, key: str) -> bool:
        """
        Delete a value by key
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted, False if not found
        """
        self.metrics["deletes"] += 1
        
        # Delete from both tiers to ensure consistency
        memory_result = await self.memory_tier.delete(key)
        disk_result = await self.disk_tier.delete(key)
        
        # Clean up tracking
        if key in self.access_counts:
            del self.access_counts[key]
        if key in self.last_access_time:
            del self.last_access_time[key]
        if key in self.tier_location:
            del self.tier_location[key]
        
        return memory_result or disk_result
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists
        
        Args:
            key: The key to check
            
        Returns:
            True if the key exists
        """
        # Check memory tier first
        if await self.memory_tier.exists(key):
            return True
        
        # Check disk tier
        return await self.disk_tier.exists(key)
    
    async def get_many(self, keys: List[str]) -> Dict[str, Any]:
        """
        Get multiple values by keys
        
        Args:
            keys: List of keys to retrieve
            
        Returns:
            Dict mapping keys to values (only existing keys)
        """
        result = {}
        
        for key in keys:
            value = await self.get(key)
            if value is not None:
                result[key] = value
        
        return result
    
    async def set_many(self, items: Dict[str, Any], ttl: Optional[int] = None) -> bool:
        """
        Set multiple values at once
        
        Args:
            items: Dict mapping keys to values
            ttl: Time-to-live in seconds (None for default)
            
        Returns:
            True if all set successfully
        """
        for key, value in items.items():
            if not await self.set(key, value, ttl):
                return False
        
        return True
    
    async def delete_many(self, keys: List[str]) -> int:
        """
        Delete multiple keys at once
        
        Args:
            keys: List of keys to delete
            
        Returns:
            Number of keys deleted
        """
        deleted = 0
        
        for key in keys:
            if await self.delete(key):
                deleted += 1
        
        return deleted
    
    async def clear(self) -> bool:
        """
        Clear all data
        
        Returns:
            True if successful
        """
        # Clear both tiers
        memory_result = await self.memory_tier.clear()
        disk_result = await self.disk_tier.clear()
        
        # Clear tracking
        self.access_counts.clear()
        self.last_access_time.clear()
        self.tier_location.clear()
        
        return memory_result and disk_result
    
    async def count(self) -> int:
        """
        Get the total number of items
        
        Returns:
            Number of items
        """
        # Count items in both tiers
        # Note: This might count some items twice if they exist in both tiers
        memory_count = await self.memory_tier.count()
        disk_count = await self.disk_tier.count()
        
        return memory_count + disk_count
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get storage metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        
        # Add tier-specific metrics
        memory_metrics = await self.memory_tier.get_metrics()
        disk_metrics = await self.disk_tier.get_metrics()
        
        metrics["memory_tier"] = memory_metrics
        metrics["disk_tier"] = disk_metrics
        metrics["memory_tier_size"] = memory_metrics.get("size", 0)
        metrics["disk_tier_size"] = disk_metrics.get("size", 0)
        
        return metrics
    
    def _track_access(self, key: str) -> None:
        """
        Track access to a key
        
        Args:
            key: The key being accessed
        """
        # Update access count
        self.access_counts[key] = self.access_counts.get(key, 0) + 1
        
        # Update last access time
        self.last_access_time[key] = time.time()
    
    async def _consider_promotion(self, key: str, value: Any) -> None:
        """
        Consider promoting a key from disk to memory
        
        Args:
            key: The key to consider
            value: The value to promote
        """
        # Check if key meets promotion criteria
        if (self.access_counts.get(key, 0) >= self.promotion_threshold and
            self.tier_location.get(key) == "disk"):
            
            # Promote to memory tier
            await self.memory_tier.set(key, value)
            self.tier_location[key] = "memory"
            self.metrics["promotions"] += 1
            
            self.logger.debug(f"Promoted key {key} to memory tier")
    
    async def _should_use_memory_tier(self, key: str) -> bool:
        """
        Determine if a key should use the memory tier
        
        Args:
            key: The key to check
            
        Returns:
            True if should use memory tier
        """
        # Check if already in memory tier
        if self.tier_location.get(key) == "memory":
            return True
        
        # Check if memory tier is full
        if await self.memory_tier.count() >= self.memory_tier_size:
            # Memory tier is full, only use for frequently accessed keys
            return self.access_counts.get(key, 0) >= self.promotion_threshold
        
        # Memory tier has space
        return True
    
    async def _tier_management_loop(self) -> None:
        """Background task for tier management"""
        self.logger.info("Starting tier management loop")
        
        while self.running:
            try:
                # Perform tier management
                await self._demote_cold_keys()
                
                # Wait before next cycle
                await asyncio.sleep(60.0)  # Check every minute
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in tier management loop: {e}")
                await asyncio.sleep(300.0)  # Longer sleep on error
    
    async def _demote_cold_keys(self) -> None:
        """Demote cold keys from memory to disk"""
        now = time.time()
        keys_to_demote = []
        
        # Find keys that haven't been accessed recently
        for key, last_access in self.last_access_time.items():
            if (self.tier_location.get(key) == "memory" and
                now - last_access > self.demotion_threshold):
                keys_to_demote.append(key)
        
        # Demote keys
        for key in keys_to_demote:
            try:
                # Get value from memory
                value = await self.memory_tier.get(key)
                if value is not None:
                    # Store in disk tier
                    await self.disk_tier.set(key, value)
                    
                    # Remove from memory tier
                    await self.memory_tier.delete(key)
                    
                    # Update tracking
                    self.tier_location[key] = "disk"
                    self.metrics["demotions"] += 1
                    
                    self.logger.debug(f"Demoted key {key} to disk tier")
            except Exception as e:
                self.logger.error(f"Error demoting key {key}: {e}")
"""
src/exapyte/storage/tiered_store.py
==================================

Tiered storage implementation for exapyte.
Combines memory and disk storage for optimal performance.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Set, Tuple


class TieredStore:
    """
    Tiered storage implementation
    
    Features:
    - Hot data in memory, cold data on disk
    - Automatic promotion/demotion between tiers
    - Configurable tier policies
    - Optimized for access patterns
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the tiered store
        
        Args:
            config: Configuration parameters
                - memory_tier_size: Maximum size of memory tier
                - promotion_threshold: Access count to promote to memory
                - demotion_threshold: Time without access to demote to disk
                - memory_tier_config: Configuration for memory tier
                - disk_tier_config: Configuration for disk tier
        """
        self.config = config or {}
        self.logger = logging.getLogger("storage.tiered")
        
        # This is a placeholder implementation
        # The full implementation would initialize memory and disk tiers
        self.logger.info("TieredStore initialized (placeholder)")
    
    async def start(self):
        """Start the tiered store"""
        pass
    
    async def stop(self):
        """Stop the tiered store"""
        pass
    
    async def get(self, key: str) -> Optional[Any]:
        """Get a value by key"""
        return None
    
    async def set(self, key: str, value: Any) -> bool:
        """Set a value by key"""
        return True
    
    async def delete(self, key: str) -> bool:
        """Delete a value by key"""
        return True
    
    async def exists(self, key: str) -> bool:
        """Check if a key exists"""
        return False
