"""
src/exapyte/replication/optimized_replicator.py
=============================================

Optimized replication implementation for exapyte.
Combines the best aspects of synchronous and asynchronous replication
with advanced optimizations for performance and reliability.
"""

import asyncio
import logging
import time
import random
import json
import zlib
from typing import Dict, Any, List, Optional, Set, Tuple
from enum import Enum


class ReplicationMode(Enum):
    """Replication modes supported by the optimized replicator"""
    SYNC = "sync"
    ASYNC = "async"
    ADAPTIVE = "adaptive"


class OptimizedReplicator:
    """
    Optimized replication implementation
    
    Features:
    - Adaptive replication modes based on workload
    - Intelligent batching and compression
    - Delta-based replication for large objects
    - Prioritization of critical updates
    - Automatic failure recovery
    """
    
    def __init__(self, node_id: str, network_manager: Any, storage_engine: Any, 
                config: Dict[str, Any] = None):
        """
        Initialize the optimized replicator
        
        Args:
            node_id: ID of this node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration parameters
                - replication_nodes: List of nodes to replicate to
                - default_mode: Default replication mode
                - batch_size: Maximum batch size for replication
                - compression_threshold: Size threshold for compression
                - delta_threshold: Size threshold for delta encoding
                - sync_timeout_ms: Timeout for synchronous operations
                - async_interval_ms: Interval for asynchronous replication
        """
        self.node_id = node_id
        self.network_manager = network_manager
        self.storage_engine = storage_engine
        self.config = config or {}
        
        # Set up logging
        self.logger = logging.getLogger(f"replication.optimized.{node_id}")
        
        # Configuration
        self.replication_nodes = self.config.get("replication_nodes", [])
        self.default_mode = ReplicationMode(self.config.get("default_mode", "adaptive"))
        self.batch_size = self.config.get("batch_size", 100)
        self.compression_threshold = self.config.get("compression_threshold", 1024)  # bytes
        self.delta_threshold = self.config.get("delta_threshold", 4096)  # bytes
        self.sync_timeout_ms = self.config.get("sync_timeout_ms", 5000)  # 5 seconds
        self.async_interval_ms = self.config.get("async_interval_ms", 100)  # 100ms
        
        # Add self to replication nodes if not present
        if self.node_id not in self.replication_nodes:
            self.replication_nodes.append(self.node_id)
        
        # Replication state
        self.replication_queue = asyncio.PriorityQueue()  # (priority, (key, timestamp))
        self.last_replicated = {}  # node_id -> {key: timestamp}
        self.node_versions = {}  # node_id -> {key: version}
        self.replication_failures = {}  # node_id -> count
        
        # Baseline data for delta encoding
        self.baselines = {}  # key -> baseline_value
        
        # Background tasks
        self.replication_task = None
        self.running = False
        
        # Metrics
        self.metrics = {
            "replication_attempts": 0,
            "replication_successes": 0,
            "replication_failures": 0,
            "sync_operations": 0,
            "async_operations": 0,
            "adaptive_operations": 0,
            "keys_replicated": 0,
            "bytes_sent": 0,
            "bytes_saved": 0,
            "compression_ratio": 1.0
        }
        
        self.logger.info(f"Initialized OptimizedReplicator with config: {self.config}")
    
    async def start(self):
        """Start the replicator and background tasks"""
        self.logger.info(f"Starting OptimizedReplicator for node {self.node_id}")
        self.running = True
        
        # Register replication handlers
        self.network_manager.register_handlers({
            "replicate_data": self.handle_replicate_data,
            "replicate_delta": self.handle_replicate_delta,
            "replicate_synchronously": self.handle_replicate_synchronously,
            "get_baseline": self.handle_get_baseline
        })
        
        # Start background replication task
        self.replication_task = asyncio.create_task(self._replication_loop())
    
    async def stop(self):
        """Stop the replicator and background tasks"""
        self.logger.info(f"Stopping OptimizedReplicator for node {self.node_id}")
        self.running = False
        
        # Cancel background tasks
        if self.replication_task:
            self.replication_task.cancel()
            try:
                await self.replication_task
            except asyncio.CancelledError:
                pass
    
    async def replicate_key(self, key: str, priority: int = 0, mode: Optional[ReplicationMode] = None) -> bool:
        """
        Schedule a key for replication
        
        Args:
            key: The key to replicate
            priority: Priority (lower values = higher priority)
            mode: Replication mode (None for default)
            
        Returns:
            True if scheduled successfully
        """
        timestamp = time.time()
        
        try:
            # Add to replication queue
            await self.replication_queue.put((priority, (key, timestamp, mode)))
            return True
        except Exception as e:
            self.logger.error(f"Error scheduling key {key} for replication: {e}")
            return False
    
    async def replicate_synchronously(self, keyspace: str, key: str, value: Any) -> Dict[str, Any]:
        """
        Replicate a key-value pair synchronously to all nodes
        
        Args:
            keyspace: Keyspace name
            key: The key to replicate
            value: The value to replicate
            
        Returns:
            Dict with replication status
        """
        self.metrics["replication_attempts"] += 1
        self.metrics["sync_operations"] += 1
        
        # Prepare replication data
        data = {
            key: {
                "value": value,
                "timestamp": time.time(),
                "version": self._generate_version()
            }
        }
        
        # Replicate to all nodes
        success_nodes = []
        failed_nodes = {}
        
        # Create tasks for each node
        replication_tasks = []
        
        for node_id in self.replication_nodes:
            if node_id != self.node_id:  # Skip self
                task = self._replicate_to_node(node_id, keyspace, data, True)
                replication_tasks.append((node_id, task))
        
        # Wait for all tasks to complete or timeout
        timeout = self.sync_timeout_ms / 1000  # Convert to seconds
        
        for node_id, task in replication_tasks:
            try:
                result = await asyncio.wait_for(task, timeout=timeout)
                
                if result and result.get("success", False):
                    success_nodes.append(node_id)
                    
                    # Update node version tracking
                    if node_id not in self.node_versions:
                        self.node_versions[node_id] = {}
                    
                    for k, v in data.items():
                        self.node_versions[node_id][k] = v.get("version")
                else:
                    error = result.get("error", "Unknown error") if result else "Timeout"
                    failed_nodes[node_id] = error
                    
                    # Track failure
                    self.replication_failures[node_id] = self.replication_failures.get(node_id, 0) + 1
                    
            except asyncio.TimeoutError:
                failed_nodes[node_id] = "Timeout"
                self.replication_failures[node_id] = self.replication_failures.get(node_id, 0) + 1
                
            except Exception as e:
                failed_nodes[node_id] = str(e)
                self.replication_failures[node_id] = self.replication_failures.get(node_id, 0) + 1
        
        # Count self as successful
        success_nodes.append(self.node_id)
        
        # Check if we have majority
        quorum_size = (len(self.replication_nodes) // 2) + 1
        quorum_achieved = len(success_nodes) >= quorum_size
        
        if quorum_achieved:
            self.metrics["replication_successes"] += 1
            self.metrics["keys_replicated"] += len(data)
        else:
            self.metrics["replication_failures"] += 1
        
        return {
            "success": quorum_achieved,
            "quorum_size": quorum_size,
            "success_nodes": success_nodes,
            "failed_nodes": failed_nodes,
            "errors": failed_nodes
        }
    
    async def handle_replicate_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle incoming replication data from another node
        
        Args:
            payload: Replication data
                - sender_id: ID of the sender
                - keyspace: Keyspace name
                - data: Dict of key-value pairs
                - compressed: Whether data is compressed
                - timestamp: Timestamp of the replication
                
        Returns:
            Response with success status
        """
        sender_id = payload.get("sender_id")
        keyspace = payload.get("keyspace", "default")
        compressed = payload.get("compressed", False)
        timestamp = payload.get("timestamp", 0)
        
        # Get data, decompress if needed
        if compressed:
            compressed_data = payload.get("data", "")
            try:
                data_bytes = zlib.decompress(compressed_data)
                data = json.loads(data_bytes.decode('utf-8'))
            except Exception as e:
                self.logger.error(f"Error decompressing data from {sender_id}: {e}")
                return {
                    "success": False,
                    "error": f"Decompression error: {str(e)}"
                }
        else:
            data = payload.get("data", {})
        
        self.logger.debug(f"Received replication data from {sender_id} with {len(data)} keys")
        
        # Process the replicated data
        success_keys = []
        failed_keys = []
        
        for key, value_data in data.items():
            try:
                # Parse value data
                value = value_data.get("value")
                value_timestamp = value_data.get("timestamp", 0)
                value_version = value_data.get("version")
                
                # Check if we already have a newer version
                existing_value = await self.storage_engine.get(key)
                if existing_value is not None:
                    existing_timestamp = existing_value.get("_timestamp", 0)
                    if existing_timestamp > value_timestamp:
                        # Our version is newer, keep it
                        success_keys.append(key)
                        continue
                
                # Store the value with metadata
                if isinstance(value, dict):
                    if "_timestamp" not in value:
                        value["_timestamp"] = value_timestamp
                    if "_source_node" not in value:
                        value["_source_node"] = sender_id
                    if "_version" not in value and value_version:
                        value["_version"] = value_version
                
                # Store in storage engine
                await self.storage_engine.set(key, value)
                
                # Update baseline for delta encoding
                self.baselines[key] = value
                
                success_keys.append(key)
                
            except Exception as e:
                self.logger.error(f"Error processing replicated key {key}: {e}")
                failed_keys.append(key)
        
        # Update tracking of what this node has
        if sender_id not in self.last_replicated:
            self.last_replicated[sender_id] = {}
        
        for key in success_keys:
            self.last_replicated[sender_id][key] = timestamp
        
        return {
            "success": len(failed_keys) == 0,
            "keyspace": keyspace,
            "received_keys": len(data),
            "success_keys": len(success_keys),
            "failed_keys": len(failed_keys),
            "timestamp": time.time()
        }
    
    async def handle_replicate_delta(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle delta-based replication data
        
        Args:
            payload: Delta replication data
                - sender_id: ID of the sender
                - keyspace: Keyspace name
                - key: The key being replicated
                - baseline_version: Version of the baseline
                - delta: Delta changes to apply
                - timestamp: Timestamp of the replication
                
        Returns:
            Response with success status
        """
        sender_id = payload.get("sender_id")
        keyspace = payload.get("keyspace", "default")
        key = payload.get("key")
        baseline_version = payload.get("baseline_version")
        delta = payload.get("delta", {})
        timestamp = payload.get("timestamp", 0)
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        self.logger.debug(f"Received delta replication for key {key} from {sender_id}")
        
        try:
            # Get current value
            current_value = await self.storage_engine.get(key)
            
            # If we don't have the key or baseline version mismatch, request full value
            if current_value is None or current_value.get("_version") != baseline_version:
                return {
                    "success": False,
                    "error": "baseline_mismatch",
                    "need_full_value": True
                }
            
            # Apply delta to current value
            new_value = self._apply_delta(current_value, delta)
            
            # Update metadata
            if isinstance(new_value, dict):
                new_value["_timestamp"] = timestamp
                new_value["_source_node"] = sender_id
                new_value["_version"] = self._generate_version()
            
            # Store updated value
            await self.storage_engine.set(key, new_value)
            
            # Update baseline
            self.baselines[key] = new_value
            
            # Update tracking
            if sender_id not in self.last_replicated:
                self.last_replicated[sender_id] = {}
            
            self.last_replicated[sender_id][key] = timestamp
            
            return {
                "success": True,
                "keyspace": keyspace,
                "key": key,
                "timestamp": time.time()
            }
            
        except Exception as e:
            self.logger.error(f"Error handling delta replication for key {key}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def handle_replicate_synchronously(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle synchronous replication request
        
        Args:
            payload: Replication request
                - sender_id: ID of the sender
                - keyspace: Keyspace name
                - key: Key to replicate
                - value: Value to replicate
                - version: Version of the value
                
        Returns:
            Response with success status
        """
        sender_id = payload.get("sender_id")
        keyspace = payload.get("keyspace", "default")
        key = payload.get("key")
        value = payload.get("value")
        version = payload.get("version")
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        self.logger.debug(f"Received synchronous replication request from {sender_id} for key {key}")
        
        try:
            # Update metadata
            if isinstance(value, dict):
                value["_timestamp"] = time.time()
                value["_source_node"] = sender_id
                if version and "_version" not in value:
                    value["_version"] = version
            
            # Store the value
            result = await self.storage_engine.set(key, value)
            
            # Update baseline
            self.baselines[key] = value
            
            # Update tracking
            if sender_id not in self.last_replicated:
                self.last_replicated[sender_id] = {}
            
            self.last_replicated[sender_id][key] = time.time()
            
            if sender_id not in self.node_versions:
                self.node_versions[sender_id] = {}
            
            if version:
                self.node_versions[sender_id][key] = version
            
            return {
                "success": result,
                "keyspace": keyspace,
                "key": key,
                "timestamp": time.time()
            }
            
        except Exception as e:
            self.logger.error(f"Error handling synchronous replication for key {key}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def handle_get_baseline(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle request for baseline value
        
        Args:
            payload: Request details
                - sender_id: ID of the sender
                - key: Key to get baseline for
                
        Returns:
            Response with baseline value
        """
        sender_id = payload.get("sender_id")
        key = payload.get("key")
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        self.logger.debug(f"Received baseline request from {sender_id} for key {key}")
        
        try:
            # Get current value
            value = await self.storage_engine.get(key)
            
            if value is None:
                return {
                    "success": False,
                    "error": "key_not_found"
                }
            
            # Get version
            version = value.get("_version") if isinstance(value, dict) else None
            
            return {
                "success": True,
                "key": key,
                "value": value,
                "version": version,
                "timestamp": time.time()
            }
            
        except Exception as e:
            self.logger.error(f"Error handling baseline request for key {key}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _replication_loop(self):
        """Background task for replication"""
        self.logger.info("Starting replication loop")
        
        while self.running:
            try:
                # Process replication queue in batches
                batch = []
                batch_size = 0
                
                # Try to fill batch up to the maximum size
                while batch_size < self.batch_size:
                    try:
                        # Get next item with a short timeout
                        item = await asyncio.wait_for(
                            self.replication_queue.get(), 
                            timeout=0.1
                        )
                        
                        # Add to batch
                        batch.append(item)
                        batch_size += 1
                        
                    except asyncio.TimeoutError:
                        # No more items in queue
                        break
                
                if not batch:
                    # No items to process, wait before checking again
                    await asyncio.sleep(self.async_interval_ms / 1000)
                    continue
                
                # Process the batch
                await self._replicate_batch(batch)
                
                # Small sleep to avoid tight loop
                await asyncio.sleep(0.01)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in replication loop: {e}")
                await asyncio.sleep(1.0)  # Longer sleep on error
    
    async def _replicate_batch(self, batch: List[Tuple[int, Tuple[str, float, Optional[ReplicationMode]]]]):
        """
        Replicate a batch of keys to all replication nodes
        
        Args:
            batch: List of (priority, (key, timestamp, mode)) tuples
        """
        if not batch or not self.replication_nodes:
            return
        
        self.logger.debug(f"Replicating batch of {len(batch)} keys")
        self.metrics["replication_attempts"] += 1
        
        # Group by replication mode
        sync_keys = []
        async_keys = []
        adaptive_keys = []
        
        for _, (key, timestamp, mode) in batch:
            if mode == ReplicationMode.SYNC:
                sync_keys.append(key)
            elif mode == ReplicationMode.ASYNC:
                async_keys.append(key)
            else:
                adaptive_keys.append(key)
        
        # Update metrics
        if sync_keys:
            self.metrics["sync_operations"] += 1
        if async_keys:
            self.metrics["async_operations"] += 1
        if adaptive_keys:
            self.metrics["adaptive_operations"] += 1
        
        # Get data for all keys
        all_keys = sync_keys + async_keys + adaptive_keys
        data = {}
        
        for key in all_keys:
            try:
                value = await self.storage_engine.get(key)
                if value is not None:
                    # Generate version if needed
                    version = value.get("_version") if isinstance(value, dict) else None
                    if not version:
                        version = self._generate_version()
                        if isinstance(value, dict):
                            value["_version"] = version
                    
                    data[key] = {
                        "value": value,
                        "timestamp": time.time(),
                        "version": version
                    }
                    
                    # Update baseline
                    self.baselines[key] = value
            except Exception as e:
                self.logger.error(f"Error getting data for key {key}: {e}")
        
        if not data:
            return
        
        # Replicate to all nodes
        replication_tasks = []
        
        for node_id in self.replication_nodes:
            if node_id != self.node_id:  # Skip self
                # Determine which keys to replicate synchronously to this node
                node_sync_keys = sync_keys.copy()
                
                # For adaptive keys, decide based on node health and key importance
                for key in adaptive_keys:
                    if self._should_replicate_synchronously(node_id, key):
                        node_sync_keys.append(key)
                
                # Get node-specific data
                node_data = {k: v for k, v in data.items() if k in all_keys}
                
                # Create replication task
                task = self._replicate_to_node_optimized(
                    node_id=node_id,
                    keyspace="default",
                    data=node_data,
                    sync_keys=set(node_sync_keys)
                )
                replication_tasks.append((node_id, task))
        
        # Wait for all replication tasks to complete
        for node_id, task in replication_tasks:
            try:
                result = await task
                
                # Process result
                if result and result.get("success", False):
                    # Reset failure count on success
                    if node_id in self.replication_failures:
                        self.replication_failures[node_id] = 0
                else:
                    # Increment failure count
                    self.replication_failures[node_id] = self.replication_failures.get(node_id, 0) + 1
                    
            except Exception as e:
                self.logger.error(f"Error replicating to node {node_id}: {e}")
                self.replication_failures[node_id] = self.replication_failures.get(node_id, 0) + 1
        
        # Update metrics
        self.metrics["replication_successes"] += 1
        self.metrics["keys_replicated"] += len(data)
    
    async def _replicate_to_node_optimized(self, node_id: str, keyspace: str, 
                                         data: Dict[str, Dict[str, Any]],
                                         sync_keys: Set[str]) -> Dict[str, Any]:
        """
        Replicate data to a node with optimizations
        
        Args:
            node_id: ID of the target node
            keyspace: Keyspace name
            data: Dict of key-value pairs to replicate
            sync_keys: Set of keys to replicate synchronously
            
        Returns:
            Replication result
        """
        if not data:
            return {"success": True, "message": "No data to replicate"}
        
        # Split data into sync and async
        sync_data = {k: v for k, v in data.items() if k in sync_keys}
        async_data = {k: v for k, v in data.items() if k not in sync_keys}
        
        # Track results
        results = {
            "success": True,
            "sync_success": True,
            "async_success": True,
            "sync_keys": len(sync_data),
            "async_keys": len(async_data)
        }
        
        # Handle synchronous replication first
        if sync_data:
            sync_results = []
            
            for key, value_data in sync_data.items():
                value = value_data.get("value")
                version = value_data.get("version")
                
                # Replicate synchronously
                payload = {
                    "sender_id": self.node_id,
                    "keyspace": keyspace,
                    "key": key,
                    "value": value,
                    "version": version
                }
                
                try:
                    result = await self.network_manager.send_rpc(
                        node_id,
                        "replicate_synchronously",
                        payload,
                        timeout=self.sync_timeout_ms / 1000
                    )
                    
                    sync_results.append(result and result.get("success", False))
                    
                    # Update tracking on success
                    if result and result.get("success", False):
                        if node_id not in self.last_replicated:
                            self.last_replicated[node_id] = {}
                        
                        self.last_replicated[node_id][key] = time.time()
                        
                        if node_id not in self.node_versions:
                            self.node_versions[node_id] = {}
                        
                        self.node_versions[node_id][key] = version
                        
                except Exception as e:
                    self.logger.error(f"Error in synchronous replication to {node_id}: {e}")
                    sync_results.append(False)
            
            # Update result
            results["sync_success"] = all(sync_results) if sync_results else True
        
        # Handle asynchronous replication
        if async_data:
            try:
                # Optimize data for transmission
                optimized_data, bytes_saved = await self._optimize_data_for_node(node_id, async_data)
                
                # Update metrics
                self.metrics["bytes_saved"] += bytes_saved
                
                # Send data
                result = await self._send_optimized_data(node_id, keyspace, optimized_data)
                
                # Update result
                results["async_success"] = result.get("success", False)
                
                # Update tracking on success
                if result and result.get("success", False):
                    if node_id not in self.last_replicated:
                        self.last_replicated[node_id] = {}
                    
                    for key in async_data:
                        self.last_replicated[node_id][key] = time.time()
                        
                        # Update version tracking
                        if node_id not in self.node_versions:
                            self.node_versions[node_id] = {}
                        
                        version = async_data[key].get("version")
                        if version:
                            self.node_versions[node_id][key] = version
                
            except Exception as e:
                self.logger.error(f"Error in asynchronous replication to {node_id}: {e}")
                results["async_success"] = False
        
        # Overall success is both sync and async success
        results["success"] = results["sync_success"] and results["async_success"]
        
        return results
    
    async def _optimize_data_for_node(self, node_id: str, 
                                    data: Dict[str, Dict[str, Any]]) -> Tuple[Dict[str, Any], int]:
        """
        Optimize data for transmission to a specific node
        
        Args:
            node_id: ID of the target node
            data: Dict of key-value pairs to optimize
            
        Returns:
            Tuple of (optimized_data, bytes_saved)
        """
        # Start with full data
        optimized_data = {
            "type": "full",
            "data": data,
            "compressed": False
        }
        
        # Calculate original size
        original_size = len(json.dumps(data).encode('utf-8'))
        bytes_saved = 0
        
        # Try delta encoding for large values
        if node_id in self.node_versions:
            delta_encoded = {}
            full_data = {}
            
            for key, value_data in data.items():
                value = value_data.get("value")
                current_version = value_data.get("version")
                
                # Check if node has a previous version
                if key in self.node_versions[node_id]:
                    prev_version = self.node_versions[node_id][key]
                    
                    # Only use delta for large values
                    value_size = len(json.dumps(value).encode('utf-8'))
                    
                    if value_size > self.delta_threshold:
                        # Use delta encoding
                        delta = await self._compute_delta(node_id, key, value)
                        
                        if delta:
                            delta_encoded[key] = {
                                "baseline_version": prev_version,
                                "delta": delta,
                                "timestamp": value_data.get("timestamp"),
                                "version": current_version
                            }
                            continue
                
                # Fall back to full data
                full_data[key] = value_data
            
            # If we have delta encoded data, use mixed approach
            if delta_encoded:
                optimized_data = {
                    "type": "mixed",
                    "delta_encoded": delta_encoded,
                    "full_data": full_data
                }
                
                # Calculate new size
                new_size = len(json.dumps(optimized_data).encode('utf-8'))
                if new_size < original_size:
                    bytes_saved = original_size - new_size
                else:
                    # If not smaller, revert to full data
                    optimized_data = {
                        "type": "full",
                        "data": data,
                        "compressed": False
                    }
        
        # Try compression for large data
        data_size = len(json.dumps(optimized_data).encode('utf-8'))
        
        if data_size > self.compression_threshold:
            # Compress the data
            if optimized_data["type"] == "full":
                # Compress full data
                data_json = json.dumps(data).encode('utf-8')
                compressed_data = zlib.compress(data_json)
                
                compressed_size = len(compressed_data)
                
                if compressed_size < data_size:
                    optimized_data = {
                        "type": "full",
                        "data": compressed_data,
                        "compressed": True
                    }
                    
                    bytes_saved += (data_size - compressed_size)
            else:
                # Compress mixed data
                mixed_json = json.dumps({
                    "delta_encoded": optimized_data["delta_encoded"],
                    "full_data": optimized_data["full_data"]
                }).encode('utf-8')
                
                compressed_data = zlib.compress(mixed_json)
                compressed_size = len(compressed_data)
                
                if compressed_size < data_size:
                    optimized_data = {
                        "type": "mixed",
                        "data": compressed_data,
                        "compressed": True
                    }
                    
                    bytes_saved += (data_size - compressed_size)
        
        return optimized_data, bytes_saved
    
    async def _send_optimized_data(self, node_id: str, keyspace: str, 
                                 optimized_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Send optimized data to a node
        
        Args:
            node_id: ID of the target node
            keyspace: Keyspace name
            optimized_data: Optimized data to send
            
        Returns:
            Response from the node
        """
        data_type = optimized_data.get("type")
        
        if data_type == "full":
            # Send full data
            payload = {
                "sender_id": self.node_id,
                "keyspace": keyspace,
                "data": optimized_data.get("data"),
                "compressed": optimized_data.get("compressed", False),
                "timestamp": time.time()
            }
            
            return await self.network_manager.send_rpc(
                node_id,
                "replicate_data",
                payload
            )
            
        elif data_type == "mixed":
            # Handle mixed data (delta + full)
            if optimized_data.get("compressed", False):
                # Send compressed mixed data
                payload = {
                    "sender_id": self.node_id,
                    "keyspace": keyspace,
                    "data": optimized_data.get("data"),
                    "compressed": True,
                    "mixed": True,
                    "timestamp": time.time()
                }
                
                return await self.network_manager.send_rpc(
                    node_id,
                    "replicate_data",
                    payload
                )
            else:
                # Send delta encoded and full data separately
                results = {
                    "success": True,
                    "delta_results": {},
                    "full_results": {}
                }
                
                # Send delta encoded data
                for key, delta_data in optimized_data.get("delta_encoded", {}).items():
                    payload = {
                        "sender_id": self.node_id,
                        "keyspace": keyspace,
                        "key": key,
                        "baseline_version": delta_data.get("baseline_version"),
                        "delta": delta_data.get("delta"),
                        "timestamp": delta_data.get("timestamp", time.time())
                    }
                    
                    result = await self.network_manager.send_rpc(
                        node_id,
                        "replicate_delta",
                        payload
                    )
                    
                    results["delta_results"][key] = result
                    
                    # If delta failed due to baseline mismatch, send full data
                    if (not result or not result.get("success", False)) and result.get("need_full_value", False):
                        # Get full data for this key
                        full_data = optimized_data.get("full_data", {})
                        if key not in full_data:
                            # Need to get it from original data
                            value = await self.storage_engine.get(key)
                            if value is not None:
                                version = value.get("_version") if isinstance(value, dict) else None
                                if not version:
                                    version = self._generate_version()
                                
                                full_data[key] = {
                                    "value": value,
                                    "timestamp": time.time(),
                                    "version": version
                                }
                        
                        # Send full value
                        if key in full_data:
                            value_data = full_data[key]
                            payload = {
                                "sender_id": self.node_id,
                                "keyspace": keyspace,
                                "key": key,
                                "value": value_data.get("value"),
                                "version": value_data.get("version"),
                                "timestamp": value_data.get("timestamp", time.time())
                            }
                            
                            result = await self.network_manager.send_rpc(
                                node_id,
                                "replicate_synchronously",
                                payload
                            )
                            
                            results["full_results"][key] = result
                
                # Send full data
                if optimized_data.get("full_data"):
                    payload = {
                        "sender_id": self.node_id,
                        "keyspace": keyspace,
                        "data": optimized_data.get("full_data"),
                        "compressed": False,
                        "timestamp": time.time()
                    }
                    
                    full_result = await self.network_manager.send_rpc(
                        node_id,
                        "replicate_data",
                        payload
                    )
                    
                    results["full_success"] = full_result.get("success", False)
                
                # Overall success
                delta_success = all(r.get("success", False) for r in results["delta_results"].values()) if results["delta_results"] else True
                full_success = results.get("full_success", True)
                results["success"] = delta_success and full_success
                
                return results
        
        # Fallback for unknown type
        return {
            "success": False,
            "error": f"Unknown data type: {data_type}"
        }
    
    async def _compute_delta(self, node_id: str, key: str, new_value: Any) -> Optional[Dict[str, Any]]:
        """
        Compute delta between baseline and new value
        
        Args:
            node_id: ID of the target node
            key: The key
            new_value: New value
            
        Returns:
            Delta changes or None if delta can't be computed
        """
        # For demonstration, implement a simple delta computation
        # In a real implementation, would use a more sophisticated algorithm
        
        # First, get baseline from node
        try:
            payload = {
                "sender_id": self.node_id,
                "key": key
            }
            
            result = await self.network_manager.send_rpc(
                node_id,
                "get_baseline",
                payload
            )
            
            if not result or not result.get("success", False):
                return None
            
            baseline = result.get("value")
            
            # Compute delta (simple implementation)
            if isinstance(baseline, dict) and isinstance(new_value, dict):
                delta = {}
                
                # Find changed or added fields
                for k, v in new_value.items():
                    if k not in baseline or baseline[k] != v:
                        delta[k] = v
                
                # Find removed fields
                for k in baseline:
                    if k not in new_value:
                        delta[k] = None  # Mark as deleted
                
                # Only return delta if it's smaller than full value
                if delta and len(json.dumps(delta)) < len(json.dumps(new_value)):
                    return delta
            
            return None
            
        except Exception as e:
            self.logger.error(f"Error computing delta for key {key}: {e}")
            return None
    
    def _apply_delta(self, base_value: Any, delta: Dict[str, Any]) -> Any:
        """
        Apply delta changes to a base value
        
        Args:
            base_value: Base value
            delta: Delta changes
            
        Returns:
            Updated value
        """
        if not isinstance(base_value, dict) or not isinstance(delta, dict):
            return delta if delta is not None else base_value
        
        # Create a copy of the base value
        result = dict(base_value)
        
        # Apply changes
        for k, v in delta.items():
            if v is None:
                # Remove field
                if k in result:
                    del result[k]
            else:
                # Add or update field
                result[k] = v
        
        return result
    
    def _should_replicate_synchronously(self, node_id: str, key: str) -> bool:
        """
        Determine if a key should be replicated synchronously to a node
        
        Args:
            node_id: ID of the target node
            key: The key to replicate
            
        Returns:
            True if should replicate synchronously
        """
        # Check node health
        failure_count = self.replication_failures.get(node_id, 0)
        if failure_count > 3:
            # Node is unhealthy, use async to avoid blocking
            return False
        
        # Check key importance (for demonstration, use simple heuristic)
        # In a real implementation, would use more sophisticated criteria
        if key.startswith("critical_") or key.startswith("config_"):
            return True
        
        # For other keys, use random selection with bias
        # This demonstrates adaptive behavior
        return random.random() < 0.3  # 30% chance of sync replication
    
    def _generate_version(self) -> str:
        """
        Generate a unique version identifier
        
        Returns:
            Version string
        """
        import uuid
        return str(uuid.uuid4())
    
    async def _replicate_to_node(self, node_id: str, keyspace: str, 
                               data: Dict[str, Dict[str, Any]], 
                               synchronous: bool = False) -> Dict[str, Any]:
        """
        Replicate data to a specific node
        
        Args:
            node_id: ID of the target node
            keyspace: Keyspace name
            data: Dict of key-value pairs to replicate
            synchronous: Whether to wait for response
            
        Returns:
            Response from the node
        """
        # For single key replication, use direct method
        if len(data) == 1:
            key, value_data = next(iter(data.items()))
            value = value_data.get("value")
            version = value_data.get("version")
            
            payload = {
                "sender_id": self.node_id,
                "keyspace": keyspace,
                "key": key,
                "value": value,
                "version": version
            }
            
            try:
                return await self.network_manager.send_rpc(
                    node_id,
                    "replicate_synchronously" if synchronous else "replicate_data",
                    payload
                )
            except Exception as e:
                self.logger.error(f"Error replicating to node {node_id}: {e}")
                return {
                    "success": False,
                    "error": str(e)
                }
        
        # For multiple keys, use batch replication
        payload = {
            "sender_id": self.node_id,
            "keyspace": keyspace,
            "data": data,
            "compressed": False,
            "timestamp": time.time()
        }
        
        # Check if compression would help
        data_json = json.dumps(data).encode('utf-8')
        data_size = len(data_json)
        
        if data_size > self.compression_threshold:
            compressed_data = zlib.compress(data_json)
            compressed_size = len(compressed_data)
            
            if compressed_size < data_size:
                payload["data"] = compressed_data
                payload["compressed"] = True
                
                # Update metrics
                self.metrics["bytes_sent"] += compressed_size
                self.metrics["bytes_saved"] += (data_size - compressed_size)
                self.metrics["compression_ratio"] = data_size / compressed_size if compressed_size > 0 else 1.0
        else:
            self.metrics["bytes_sent"] += data_size
        
        try:
            return await self.network_manager.send_rpc(
                node_id,
                "replicate_data",
                payload
            )
        except Exception as e:
            self.logger.error(f"Error replicating to node {node_id}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get replication metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["queue_size"] = self.replication_queue.qsize()
        metrics["replication_nodes"] = len(self.replication_nodes)
        metrics["failed_nodes"] = len(self.replication_failures)
        metrics["baseline_keys"] = len(self.baselines)
        
        return metrics
