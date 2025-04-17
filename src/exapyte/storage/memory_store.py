"""
src/exapyte/storage/memory_store.py
==================================

In-memory storage implementation for exapyte.
Optimized for high performance but with no persistence.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Tuple, Set
import json


class MemoryStore:
    """
    In-memory storage implementation
    
    Features:
    - High performance read/write
    - Optional TTL (time-to-live) support
    - No persistence (data is lost on restart)
    - Optional size-based eviction
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the memory store
        
        Args:
            config: Configuration parameters
                - max_size: Maximum number of items (optional)
                - eviction_policy: Strategy for eviction (LRU, LFU)
                - default_ttl: Default TTL in seconds (optional)
        """
        self.config = config or {}
        self.logger = logging.getLogger("storage.memory")
        
        # Main data store
        self.data: Dict[str, Dict[str, Any]] = {}
        
        # Metadata
        self.access_time: Dict[str, float] = {}  # Last access time
        self.expiry_time: Dict[str, float] = {}  # Expiration timestamp
        self.access_count: Dict[str, int] = {}   # Number of accesses (for LFU)
        
        # Configuration
        self.max_size = self.config.get("max_size")
        self.eviction_policy = self.config.get("eviction_policy", "LRU")
        self.default_ttl = self.config.get("default_ttl")  # in seconds
        
        # Metrics
        self.metrics = {
            "gets": 0,
            "sets": 0,
            "deletes": 0,
            "hits": 0,
            "misses": 0,
            "evictions": 0
        }
        
        # Background tasks
        self.clean_task = None
        self.running = False
        
        self.logger.info(f"Initialized MemoryStore with config: {self.config}")
    
    async def start(self):
        """Start the memory store and background tasks"""
        self.running = True
        
        # Start expiry cleanup task if TTL is used
        if self.default_ttl:
            self.clean_task = asyncio.create_task(self._cleanup_expired())
    
    async def stop(self):
        """Stop the memory store and background tasks"""
        self.running = False
        
        if self.clean_task:
            self.clean_task.cancel()
            try:
                await self.clean_task
            except asyncio.CancelledError:
                pass
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found
        """
        self.metrics["gets"] += 1
        
        if key not in self.data:
            self.metrics["misses"] += 1
            return None
        
        # Check if expired
        if key in self.expiry_time and time.time() > self.expiry_time[key]:
            await self.delete(key)
            self.metrics["misses"] += 1
            return None
        
        # Update access metadata
        self.access_time[key] = time.time()
        self.access_count[key] = self.access_count.get(key, 0) + 1
        
        self.metrics["hits"] += 1
        return self.data[key]
    
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
        
        # Check if we need to evict
        if self.max_size and len(self.data) >= self.max_size and key not in self.data:
            await self._evict()
        
        # Store the value
        self.data[key] = value
        
        # Update metadata
        self.access_time[key] = time.time()
        self.access_count[key] = self.access_count.get(key, 0) + 1
        
        # Set expiry if TTL provided
        if ttl is not None or self.default_ttl:
            ttl_value = ttl if ttl is not None else self.default_ttl
            self.expiry_time[key] = time.time() + ttl_value
        
        return True
    
    async def delete(self, key: str) -> bool:
        """
        Delete a value by key
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted, False if not found
        """
        self.metrics["deletes"] += 1
        
        if key not in self.data:
            return False
        
        del self.data[key]
        
        # Clean up metadata
        if key in self.access_time:
            del self.access_time[key]
        
        if key in self.expiry_time:
            del self.expiry_time[key]
        
        if key in self.access_count:
            del self.access_count[key]
        
        return True
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists and is not expired
        
        Args:
            key: The key to check
            
        Returns:
            True if the key exists and is not expired
        """
        if key not in self.data:
            return False
        
        # Check if expired
        if key in self.expiry_time and time.time() > self.expiry_time[key]:
            await self.delete(key)
            return False
        
        return True
    
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
            await self.set(key, value, ttl)
        
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
        self.data.clear()
        self.access_time.clear()
        self.expiry_time.clear()
        self.access_count.clear()
        
        return True
    
    async def count(self) -> int:
        """
        Get the total number of items
        
        Returns:
            Number of items
        """
        return len(self.data)
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get storage metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["size"] = len(self.data)
        metrics["hit_ratio"] = (
            metrics["hits"] / (metrics["hits"] + metrics["misses"])
            if (metrics["hits"] + metrics["misses"]) > 0
            else 0
        )
        
        return metrics
    
    async def _evict(self) -> bool:
        """
        Evict an item based on the configured policy
        
        Returns:
            True if an item was evicted
        """
        if not self.data:
            return False
        
        key_to_evict = None
        
        if self.eviction_policy == "LRU":
            # Least Recently Used
            key_to_evict = min(self.access_time.items(), key=lambda x: x[1])[0]
        elif self.eviction_policy == "LFU":
            # Least Frequently Used
            key_to_evict = min(self.access_count.items(), key=lambda x: x[1])[0]
        else:
            # Random (fallback)
            import random
            key_to_evict = random.choice(list(self.data.keys()))
        
        if key_to_evict:
            await self.delete(key_to_evict)
            self.metrics["evictions"] += 1
            return True
        
        return False
    
    async def _cleanup_expired(self):
        """Background task to clean up expired items"""
        while self.running:
            try:
                now = time.time()
                expired_keys = [
                    key for key, expiry in self.expiry_time.items()
                    if expiry <= now
                ]
                
                for key in expired_keys:
                    await self.delete(key)
                
                # Sleep for a while before next cleanup
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cleanup task: {e}")
                await asyncio.sleep(5.0)  # Longer sleep on error
    
    # Methods specific to Raft consensus storage
    
    async def save_raft_state(self, state: Dict[str, Any]) -> bool:
        """
        Save Raft persistent state
        
        Args:
            state: Raft state to save
            
        Returns:
            True if successful
        """
        return await self.set("raft_state", state)
    
    async def load_raft_state(self) -> Optional[Dict[str, Any]]:
        """
        Load Raft persistent state
        
        Returns:
            Raft state or None if not found
        """
        return await self.get("raft_state")
    
    async def save_log_entries(self, entries: List[Dict[str, Any]]) -> bool:
        """
        Save Raft log entries
        
        Args:
            entries: List of log entries to save
            
        Returns:
            True if successful
        """
        return await self.set("raft_log", entries)
    
    async def load_log_entries(self) -> List[Dict[str, Any]]:
        """
        Load Raft log entries
        
        Returns:
            List of log entries or empty list if not found
        """
        entries = await self.get("raft_log")
        return entries if entries else []