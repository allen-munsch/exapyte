"""
src/exapyte/storage/disk_store.py
================================

Disk-based storage implementation for exapyte.
Provides persistence with transaction logging.
"""

import asyncio
import logging
import os
import json
import time
import aiofiles
import shutil
from typing import Dict, Any, List, Optional, Set
import hashlib


class DiskStore:
    """
    Disk-based storage implementation
    
    Features:
    - Persistent storage
    - Transaction logging
    - Periodic compaction
    - File-based organization
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the disk store
        
        Args:
            config: Configuration parameters
                - data_dir: Directory for data files
                - sync_writes: If True, sync writes to disk immediately
                - compaction_interval: Seconds between compactions
                - max_transaction_log: Max size of transaction log before compaction
        """
        self.config = config or {}
        self.logger = logging.getLogger("storage.disk")
        
        # Configuration
        self.data_dir = self.config.get("data_dir", "data")
        self.sync_writes = self.config.get("sync_writes", True)
        self.compaction_interval = self.config.get("compaction_interval", 300)  # 5 minutes
        self.max_transaction_log = self.config.get("max_transaction_log", 1000)  # entries
        
        # Sub-directories
        self.data_files_dir = os.path.join(self.data_dir, "data")
        self.transaction_log_dir = os.path.join(self.data_dir, "transaction_log")
        self.temp_dir = os.path.join(self.data_dir, "temp")
        
        # State
        self.data_cache: Dict[str, Any] = {}  # In-memory cache
        self.transaction_count = 0
        self.running = False
        self.compaction_task = None
        self.compaction_lock = asyncio.Lock()
        
        # Metrics
        self.metrics = {
            "gets": 0,
            "sets": 0,
            "deletes": 0,
            "cache_hits": 0,
            "disk_reads": 0,
            "compactions": 0
        }
        
        self.logger.info(f"Initialized DiskStore with config: {self.config}")
    
    async def start(self):
        """Start the disk store and initialize directories"""
        self.running = True
        
        # Create directories if they don't exist
        os.makedirs(self.data_files_dir, exist_ok=True)
        os.makedirs(self.transaction_log_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Load transaction log to recover state
        await self._recover_from_transaction_log()
        
        # Start compaction task
        self.compaction_task = asyncio.create_task(self._periodic_compaction())
        
        self.logger.info("DiskStore started successfully")
    
    async def stop(self):
        """Stop the disk store and background tasks"""
        self.running = False
        
        if self.compaction_task:
            self.compaction_task.cancel()
            try:
                await self.compaction_task
            except asyncio.CancelledError:
                pass
        
        # Final compaction to ensure all data is persisted
        await self._compact()
        
        self.logger.info("DiskStore stopped successfully")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found
        """
        self.metrics["gets"] += 1
        
        # Check in-memory cache first
        if key in self.data_cache:
            self.metrics["cache_hits"] += 1
            return self.data_cache[key]
        
        # Not in cache, try to load from disk
        self.metrics["disk_reads"] += 1
        file_path = self._get_data_file_path(key)
        
        if not os.path.exists(file_path):
            return None
        
        try:
            async with aiofiles.open(file_path, "r") as f:
                data = await f.read()
                value = json.loads(data)
                
                # Update cache
                self.data_cache[key] = value
                return value
        except (IOError, json.JSONDecodeError) as e:
            self.logger.error(f"Error reading data for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any) -> bool:
        """
        Set a value by key
        
        Args:
            key: The key to set
            value: The value to store
            
        Returns:
            True if set successfully
        """
        self.metrics["sets"] += 1
        
        # Update in-memory cache
        self.data_cache[key] = value
        
        # Write to disk
        file_path = self._get_data_file_path(key)
        
        try:
            # Prepare directory if needed
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Atomic write using temporary file
            temp_path = os.path.join(self.temp_dir, f"{key}_{time.time()}.tmp")
            
            async with aiofiles.open(temp_path, "w") as f:
                await f.write(json.dumps(value))
                if self.sync_writes:
                    await f.flush()
                    os.fsync(f.fileno())
            
            # Atomic rename
            shutil.move(temp_path, file_path)
            
            # Record transaction
            await self._append_transaction("set", key)
            
            return True
        except Exception as e:
            self.logger.error(f"Error writing data for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete a value by key
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted, False if not found
        """
        self.metrics["deletes"] += 1
        
        # Remove from cache
        if key in self.data_cache:
            del self.data_cache[key]
        
        # Remove from disk
        file_path = self._get_data_file_path(key)
        
        if not os.path.exists(file_path):
            return False
        
        try:
            os.remove(file_path)
            
            # Record transaction
            await self._append_transaction("delete", key)
            
            return True
        except OSError as e:
            self.logger.error(f"Error deleting data for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists
        
        Args:
            key: The key to check
            
        Returns:
            True if the key exists
        """
        # Check cache first
        if key in self.data_cache:
            return True
        
        # Check disk
        file_path = self._get_data_file_path(key)
        return os.path.exists(file_path)
    
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
    
    async def set_many(self, items: Dict[str, Any]) -> bool:
        """
        Set multiple values at once
        
        Args:
            items: Dict mapping keys to values
            
        Returns:
            True if all set successfully
        """
        for key, value in items.items():
            if not await self.set(key, value):
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
        try:
            # Clear in-memory cache
            self.data_cache.clear()
            
            # Remove all data files
            shutil.rmtree(self.data_files_dir)
            os.makedirs(self.data_files_dir, exist_ok=True)
            
            # Clear transaction log
            shutil.rmtree(self.transaction_log_dir)
            os.makedirs(self.transaction_log_dir, exist_ok=True)
            
            self.transaction_count = 0
            
            return True
        except Exception as e:
            self.logger.error(f"Error clearing data: {e}")
            return False
    
    async def count(self) -> int:
        """
        Get the total number of items
        
        Returns:
            Number of items
        """
        # This is an expensive operation as we need to scan the directory
        count = 0
        
        for root, _, files in os.walk(self.data_files_dir):
            for file in files:
                if not file.endswith(".tmp"):
                    count += 1
        
        return count
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get storage metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["size"] = await self.count()
        metrics["cache_size"] = len(self.data_cache)
        metrics["transaction_count"] = self.transaction_count
        
        return metrics
    
    def _get_data_file_path(self, key: str) -> str:
        """
        Get the file path for a key
        
        Args:
            key: The key
            
        Returns:
            File path
        """
        # Create a deterministic path with subdirectories to avoid too many files in one directory
        h = hashlib.md5(key.encode()).hexdigest()
        subdir = h[:2]
        return os.path.join(self.data_files_dir, subdir, h[2:] + ".json")
    
    async def _append_transaction(self, op: str, key: str) -> bool:
        """
        Append an operation to the transaction log
        
        Args:
            op: Operation type (set, delete)
            key: The key
            
        Returns:
            True if successful
        """
        transaction = {
            "op": op,
            "key": key,
            "timestamp": time.time()
        }
        
        log_file = os.path.join(
            self.transaction_log_dir, 
            f"txn_{int(time.time() * 1000)}.json"
        )
        
        try:
            async with aiofiles.open(log_file, "w") as f:
                await f.write(json.dumps(transaction))
                if self.sync_writes:
                    await f.flush()
                    os.fsync(f.fileno())
            
            self.transaction_count += 1
            
            # Check if compaction is needed
            if self.transaction_count >= self.max_transaction_log:
                asyncio.create_task(self._compact())
            
            return True
        except Exception as e:
            self.logger.error(f"Error writing transaction log: {e}")
            return False
    
    async def _recover_from_transaction_log(self) -> bool:
        """
        Recover state from transaction log
        
        Returns:
            True if successful
        """
        self.logger.info("Recovering from transaction log...")
        
        try:
            # Get all transaction log files, sorted by name (which includes timestamp)
            log_files = sorted([
                os.path.join(self.transaction_log_dir, f)
                for f in os.listdir(self.transaction_log_dir)
                if f.startswith("txn_") and f.endswith(".json")
            ])
            
            # Apply transactions in order
            for log_file in log_files:
                try:
                    async with aiofiles.open(log_file, "r") as f:
                        data = await f.read()
                        transaction = json.loads(data)
                        
                        op = transaction.get("op")
                        key = transaction.get("key")
                        
                        if op == "set":
                            # Make sure we have the data file
                            file_path = self._get_data_file_path(key)
                            if os.path.exists(file_path):
                                # Load into cache
                                async with aiofiles.open(file_path, "r") as df:
                                    value = json.loads(await df.read())
                                    self.data_cache[key] = value
                        elif op == "delete":
                            # Ensure it's removed
                            file_path = self._get_data_file_path(key)
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            
                            # Remove from cache
                            if key in self.data_cache:
                                del self.data_cache[key]
                    
                    self.transaction_count += 1
                except Exception as e:
                    self.logger.error(f"Error processing transaction log {log_file}: {e}")
            
            self.logger.info(f"Recovery complete: processed {len(log_files)} transaction logs")
            return True
        except Exception as e:
            self.logger.error(f"Error during recovery: {e}")
            return False
    
    async def _compact(self) -> bool:
        """
        Compact the transaction log
        
        Returns:
            True if successful
        """
        # Use a lock to prevent multiple compactions at once
        async with self.compaction_lock:
            self.logger.info("Starting compaction...")
            
            try:
                # Clear transaction logs
                for f in os.listdir(self.transaction_log_dir):
                    if f.startswith("txn_") and f.endswith(".json"):
                        os.remove(os.path.join(self.transaction_log_dir, f))
                
                # Reset transaction count
                self.transaction_count = 0
                
                self.metrics["compactions"] += 1
                self.logger.info("Compaction completed successfully")
                return True
            except Exception as e:
                self.logger.error(f"Error during compaction: {e}")
                return False
    
    async def _periodic_compaction(self):
        """Background task for periodic compaction"""
        while self.running:
            try:
                await asyncio.sleep(self.compaction_interval)
                
                if self.transaction_count > 0:
                    await self._compact()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in compaction task: {e}")
                await asyncio.sleep(60)  # Longer sleep on error
    
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
        return entries if entries else []"""
src/exapyte/storage/disk_store.py
================================

Disk-based storage implementation for exapyte.
Provides persistence with transaction logging.
"""

import asyncio
import logging
import os
import json
import time
import aiofiles
import shutil
from typing import Dict, Any, List, Optional, Set
import hashlib


class DiskStore:
    """
    Disk-based storage implementation
    
    Features:
    - Persistent storage
    - Transaction logging
    - Periodic compaction
    - File-based organization
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the disk store
        
        Args:
            config: Configuration parameters
                - data_dir: Directory for data files
                - sync_writes: If True, sync writes to disk immediately
                - compaction_interval: Seconds between compactions
                - max_transaction_log: Max size of transaction log before compaction
        """
        self.config = config or {}
        self.logger = logging.getLogger("storage.disk")
        
        # Configuration
        self.data_dir = self.config.get("data_dir", "data")
        self.sync_writes = self.config.get("sync_writes", True)
        self.compaction_interval = self.config.get("compaction_interval", 300)  # 5 minutes
        self.max_transaction_log = self.config.get("max_transaction_log", 1000)  # entries
        
        # Sub-directories
        self.data_files_dir = os.path.join(self.data_dir, "data")
        self.transaction_log_dir = os.path.join(self.data_dir, "transaction_log")
        self.temp_dir = os.path.join(self.data_dir, "temp")
        
        # State
        self.data_cache: Dict[str, Any] = {}  # In-memory cache
        self.transaction_count = 0
        self.running = False
        self.compaction_task = None
        self.compaction_lock = asyncio.Lock()
        
        # Metrics
        self.metrics = {
            "gets": 0,
            "sets": 0,
            "deletes": 0,
            "cache_hits": 0,
            "disk_reads": 0,
            "compactions": 0
        }
        
        self.logger.info(f"Initialized DiskStore with config: {self.config}")
    
    async def start(self):
        """Start the disk store and initialize directories"""
        self.running = True
        
        # Create directories if they don't exist
        os.makedirs(self.data_files_dir, exist_ok=True)
        os.makedirs(self.transaction_log_dir, exist_ok=True)
        os.makedirs(self.temp_dir, exist_ok=True)
        
        # Load transaction log to recover state
        await self._recover_from_transaction_log()
        
        # Start compaction task
        self.compaction_task = asyncio.create_task(self._periodic_compaction())
        
        self.logger.info("DiskStore started successfully")
    
    async def stop(self):
        """Stop the disk store and background tasks"""
        self.running = False
        
        if self.compaction_task:
            self.compaction_task.cancel()
            try:
                await self.compaction_task
            except asyncio.CancelledError:
                pass
        
        # Final compaction to ensure all data is persisted
        await self._compact()
        
        self.logger.info("DiskStore stopped successfully")
    
    async def get(self, key: str) -> Optional[Any]:
        """
        Get a value by key
        
        Args:
            key: The key to retrieve
            
        Returns:
            The value or None if not found
        """
        self.metrics["gets"] += 1
        
        # Check in-memory cache first
        if key in self.data_cache:
            self.metrics["cache_hits"] += 1
            return self.data_cache[key]
        
        # Not in cache, try to load from disk
        self.metrics["disk_reads"] += 1
        file_path = self._get_data_file_path(key)
        
        if not os.path.exists(file_path):
            return None
        
        try:
            async with aiofiles.open(file_path, "r") as f:
                data = await f.read()
                value = json.loads(data)
                
                # Update cache
                self.data_cache[key] = value
                return value
        except (IOError, json.JSONDecodeError) as e:
            self.logger.error(f"Error reading data for key {key}: {e}")
            return None
    
    async def set(self, key: str, value: Any) -> bool:
        """
        Set a value by key
        
        Args:
            key: The key to set
            value: The value to store
            
        Returns:
            True if set successfully
        """
        self.metrics["sets"] += 1
        
        # Update in-memory cache
        self.data_cache[key] = value
        
        # Write to disk
        file_path = self._get_data_file_path(key)
        
        try:
            # Prepare directory if needed
            os.makedirs(os.path.dirname(file_path), exist_ok=True)
            
            # Atomic write using temporary file
            temp_path = os.path.join(self.temp_dir, f"{key}_{time.time()}.tmp")
            
            async with aiofiles.open(temp_path, "w") as f:
                await f.write(json.dumps(value))
                if self.sync_writes:
                    await f.flush()
                    os.fsync(f.fileno())
            
            # Atomic rename
            shutil.move(temp_path, file_path)
            
            # Record transaction
            await self._append_transaction("set", key)
            
            return True
        except Exception as e:
            self.logger.error(f"Error writing data for key {key}: {e}")
            return False
    
    async def delete(self, key: str) -> bool:
        """
        Delete a value by key
        
        Args:
            key: The key to delete
            
        Returns:
            True if deleted, False if not found
        """
        self.metrics["deletes"] += 1
        
        # Remove from cache
        if key in self.data_cache:
            del self.data_cache[key]
        
        # Remove from disk
        file_path = self._get_data_file_path(key)
        
        if not os.path.exists(file_path):
            return False
        
        try:
            os.remove(file_path)
            
            # Record transaction
            await self._append_transaction("delete", key)
            
            return True
        except OSError as e:
            self.logger.error(f"Error deleting data for key {key}: {e}")
            return False
    
    async def exists(self, key: str) -> bool:
        """
        Check if a key exists
        
        Args:
            key: The key to check
            
        Returns:
            True if the key exists
        """
        # Check cache first
        if key in self.data_cache:
            return True
        
        # Check disk
        file_path = self._get_data_file_path(key)
        return os.path.exists(file_path)
    
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
    
    async def set_many(self, items: Dict[str, Any]) -> bool:
        """
        Set multiple values at once
        
        Args:
            items: Dict mapping keys to values
            
        Returns:
            True if all set successfully
        """
        for key, value in items.items():
            if not await self.set(key, value):
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
        try:
            # Clear in-memory cache
            self.data_cache.clear()
            
            # Remove all data files
            shutil.rmtree(self.data_files_dir)
            os.makedirs(self.data_files_dir, exist_ok=True)
            
            # Clear transaction log
            shutil.rmtree(self.transaction_log_dir)
            os.makedirs(self.transaction_log_dir, exist_ok=True)
            
            self.transaction_count = 0
            
            return True
        except Exception as e:
            self.logger.error(f"Error clearing data: {e}")
            return False
    
    async def count(self) -> int:
        """
        Get the total number of items
        
        Returns:
            Number of items
        """
        # This is an expensive operation as we need to scan the directory
        count = 0
        
        for root, _, files in os.walk(self.data_files_dir):
            for file in files:
                if not file.endswith(".tmp"):
                    count += 1
        
        return count
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get storage metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["size"] = await self.count()
        metrics["cache_size"] = len(self.data_cache)
        metrics["transaction_count"] = self.transaction_count
        
        return metrics
    
    def _get_data_file_path(self, key: str) -> str:
        """
        Get the file path for a key
        
        Args:
            key: The key
            
        Returns:
            File path
        """
        # Create a deterministic path with subdirectories to avoid too many files in one directory
        h = hashlib.md5(key.encode()).hexdigest()
        subdir = h[:2]
        return os.path.join(self.data_files_dir, subdir, h[2:] + ".json")
    
    async def _append_transaction(self, op: str, key: str) -> bool:
        """
        Append an operation to the transaction log
        
        Args:
            op: Operation type (set, delete)
            key: The key
            
        Returns:
            True if successful
        """
        transaction = {
            "op": op,
            "key": key,
            "timestamp": time.time()
        }
        
        log_file = os.path.join(
            self.transaction_log_dir, 
            f"txn_{int(time.time() * 1000)}.json"
        )
        
        try:
            async with aiofiles.open(log_file, "w") as f:
                await f.write(json.dumps(transaction))
                if self.sync_writes:
                    await f.flush()
                    os.fsync(f.fileno())
            
            self.transaction_count += 1
            
            # Check if compaction is needed
            if self.transaction_count >= self.max_transaction_log:
                asyncio.create_task(self._compact())
            
            return True
        except Exception as e:
            self.logger.error(f"Error writing transaction log: {e}")
            return False
    
    async def _recover_from_transaction_log(self) -> bool:
        """
        Recover state from transaction log
        
        Returns:
            True if successful
        """
        self.logger.info("Recovering from transaction log...")
        
        try:
            # Get all transaction log files, sorted by name (which includes timestamp)
            log_files = sorted([
                os.path.join(self.transaction_log_dir, f)
                for f in os.listdir(self.transaction_log_dir)
                if f.startswith("txn_") and f.endswith(".json")
            ])
            
            # Apply transactions in order
            for log_file in log_files:
                try:
                    async with aiofiles.open(log_file, "r") as f:
                        data = await f.read()
                        transaction = json.loads(data)
                        
                        op = transaction.get("op")
                        key = transaction.get("key")
                        
                        if op == "set":
                            # Make sure we have the data file
                            file_path = self._get_data_file_path(key)
                            if os.path.exists(file_path):
                                # Load into cache
                                async with aiofiles.open(file_path, "r") as df:
                                    value = json.loads(await df.read())
                                    self.data_cache[key] = value
                        elif op == "delete":
                            # Ensure it's removed
                            file_path = self._get_data_file_path(key)
                            if os.path.exists(file_path):
                                os.remove(file_path)
                            
                            # Remove from cache
                            if key in self.data_cache:
                                del self.data_cache[key]
                    
                    self.transaction_count += 1
                except Exception as e:
                    self.logger.error(f"Error processing transaction log {log_file}: {e}")
            
            self.logger.info(f"Recovery complete: processed {len(log_files)} transaction logs")
            return True
        except Exception as e:
            self.logger.error(f"Error during recovery: {e}")
            return False
    
    async def _compact(self) -> bool:
        """
        Compact the transaction log
        
        Returns:
            True if successful
        """
        # Use a lock to prevent multiple compactions at once
        async with self.compaction_lock:
            self.logger.info("Starting compaction...")
            
            try:
                # Clear transaction logs
                for f in os.listdir(self.transaction_log_dir):
                    if f.startswith("txn_") and f.endswith(".json"):
                        os.remove(os.path.join(self.transaction_log_dir, f))
                
                # Reset transaction count
                self.transaction_count = 0
                
                self.metrics["compactions"] += 1
                self.logger.info("Compaction completed successfully")
                return True
            except Exception as e:
                self.logger.error(f"Error during compaction: {e}")
                return False
    
    async def _periodic_compaction(self):
        """Background task for periodic compaction"""
        while self.running:
            try:
                await asyncio.sleep(self.compaction_interval)
                
                if self.transaction_count > 0:
                    await self._compact()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in compaction task: {e}")
                await asyncio.sleep(60)  # Longer sleep on error
    
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
