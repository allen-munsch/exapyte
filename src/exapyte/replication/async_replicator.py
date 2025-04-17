"""
src/exapyte/replication/async_replicator.py
=========================================

Asynchronous replication implementation for exapyte.
Provides eventual consistency with high availability.
"""

import asyncio
import logging
import time
import random
from typing import Dict, Any, List, Optional, Set, Tuple
import json


class AsyncReplicator:
    """
    Asynchronous replication implementation
    
    Features:
    - Asynchronous multi-region replication
    - Propagation of updates without waiting
    - High availability during network partitions
    - Background reconciliation process
    """
    
    def __init__(self, node_id: str, network_manager: Any, storage_engine: Any, 
                config: Dict[str, Any] = None):
        """
        Initialize the async replicator
        
        Args:
            node_id: ID of this node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration parameters
                - replication_nodes: List of nodes to replicate to
                - replication_interval_ms: Time between replication cycles
                - replication_batch_size: Maximum batch size for replication
                - reconciliation_interval_ms: Time between reconciliation cycles
        """
        self.node_id = node_id
        self.network_manager = network_manager
        self.storage_engine = storage_engine
        self.config = config or {}
        
        # Set up logging
        self.logger = logging.getLogger(f"replication.async.{node_id}")
        
        # Configuration
        self.replication_nodes = self.config.get("replication_nodes", [])
        self.replication_interval_ms = self.config.get("replication_interval_ms", 100)
        self.replication_batch_size = self.config.get("replication_batch_size", 100)
        self.reconciliation_interval_ms = self.config.get("reconciliation_interval_ms", 30000)  # 30s
        
        # Replication state
        self.replication_queue = asyncio.PriorityQueue()  # (priority, (key, timestamp))
        self.last_replicated_time = {}  # node_id -> timestamp
        self.replication_failures = {}  # node_id -> count
        
        # Background tasks
        self.replication_task = None
        self.reconciliation_task = None
        self.running = False
        
        # Metrics
        self.metrics = {
            "replication_attempts": 0,
            "replication_successes": 0,
            "replication_failures": 0,
            "keys_replicated": 0,
            "reconciliations": 0
        }
        
        self.logger.info(f"Initialized AsyncReplicator with config: {self.config}")
    
    async def start(self):
        """Start the replicator and background tasks"""
        self.logger.info(f"Starting AsyncReplicator for node {self.node_id}")
        self.running = True
        
        # Register replication handlers
        self.network_manager.register_handlers({
            "replicate_data": self.handle_replicate_data,
            "reconciliation_request": self.handle_reconciliation_request
        })
        
        # Start background tasks
        self.replication_task = asyncio.create_task(self._replication_loop())
        self.reconciliation_task = asyncio.create_task(self._reconciliation_loop())
    
    async def stop(self):
        """Stop the replicator and background tasks"""
        self.logger.info(f"Stopping AsyncReplicator for node {self.node_id}")
        self.running = False
        
        # Cancel background tasks
        if self.replication_task:
            self.replication_task.cancel()
            try:
                await self.replication_task
            except asyncio.CancelledError:
                pass
        
        if self.reconciliation_task:
            self.reconciliation_task.cancel()
            try:
                await self.reconciliation_task
            except asyncio.CancelledError:
                pass
    
    async def replicate_key(self, key: str, priority: int = 0) -> bool:
        """
        Schedule a key for replication
        
        Args:
            key: The key to replicate
            priority: Priority (lower values = higher priority)
            
        Returns:
            True if scheduled successfully
        """
        timestamp = time.time()
        
        try:
            # Add to replication queue
            await self.replication_queue.put((priority, (key, timestamp)))
            return True
        except Exception as e:
            self.logger.error(f"Error scheduling key {key} for replication: {e}")
            return False
    
    async def replicate_keys(self, keys: List[str], priority: int = 0) -> bool:
        """
        Schedule multiple keys for replication
        
        Args:
            keys: The keys to replicate
            priority: Priority (lower values = higher priority)
            
        Returns:
            True if all scheduled successfully
        """
        timestamp = time.time()
        
        try:
            # Add all keys to replication queue
            for key in keys:
                await self.replication_queue.put((priority, (key, timestamp)))
            return True
        except Exception as e:
            self.logger.error(f"Error scheduling keys for replication: {e}")
            return False
    
    async def replicate_all(self, priority: int = 0) -> bool:
        """
        Schedule all keys for replication
        
        Args:
            priority: Priority (lower values = higher priority)
            
        Returns:
            True if scheduled successfully
        """
        try:
            # This could be optimized by having the storage engine support listing all keys
            # For now, we'll just trigger a reconciliation
            for node_id in self.replication_nodes:
                await self._request_reconciliation(node_id)
            return True
        except Exception as e:
            self.logger.error(f"Error scheduling all keys for replication: {e}")
            return False
    
    async def handle_replicate_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle incoming replication data from another node
        
        Args:
            payload: Replication data
                - sender_id: ID of the sender
                - timestamp: Timestamp of the replication
                - data: Dict of key-value pairs
                
        Returns:
            Response with success status
        """
        sender_id = payload.get("sender_id")
        timestamp = payload.get("timestamp", 0)
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
                
                # Check if we already have a newer version
                existing_value = await self.storage_engine.get(key)
                if existing_value is not None:
                    existing_timestamp = existing_value.get("_timestamp", 0)
                    if existing_timestamp > value_timestamp:
                        # Our version is newer, keep it
                        success_keys.append(key)
                        continue
                
                # Store the value with metadata
                value["_timestamp"] = value_timestamp
                value["_source_node"] = sender_id
                await self.storage_engine.set(key, value)
                success_keys.append(key)
                
            except Exception as e:
                self.logger.error(f"Error processing replicated key {key}: {e}")
                failed_keys.append(key)
        
        return {
            "success": len(failed_keys) == 0,
            "received_keys": len(data),
            "success_keys": len(success_keys),
            "failed_keys": len(failed_keys),
            "timestamp": time.time()
        }
    
    async def handle_reconciliation_request(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a reconciliation request from another node
        
        Args:
            payload: Reconciliation request
                - sender_id: ID of the sender
                - since_timestamp: Only include keys updated since this time
                
        Returns:
            Response with keys that need replication
        """
        sender_id = payload.get("sender_id")
        since_timestamp = payload.get("since_timestamp", 0)
        
        self.logger.debug(f"Received reconciliation request from {sender_id}, since {since_timestamp}")
        
        # This would be optimized in a real implementation
        # For demo, we'll just scan a limited set of keys
        keys_to_reconcile = {}
        
        # In a real implementation, we would have an efficient way to list recently updated keys
        # For this demo, we'll use a simple approach
        sample_keys = ["key1", "key2", "key3", "key4", "key5"]
        
        for key in sample_keys:
            try:
                value = await self.storage_engine.get(key)
                if value is not None:
                    value_timestamp = value.get("_timestamp", 0)
                    if value_timestamp > since_timestamp:
                        keys_to_reconcile[key] = {
                            "timestamp": value_timestamp
                        }
            except Exception as e:
                self.logger.error(f"Error checking key {key} for reconciliation: {e}")
        
        return {
            "success": True,
            "keys_to_reconcile": keys_to_reconcile,
            "node_id": self.node_id,
            "timestamp": time.time()
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
                while batch_size < self.replication_batch_size:
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
                    await asyncio.sleep(self.replication_interval_ms / 1000)
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
    
    async def _replicate_batch(self, batch: List[Tuple[int, Tuple[str, float]]]):
        """
        Replicate a batch of keys to all replication nodes
        
        Args:
            batch: List of (priority, (key, timestamp)) tuples
        """
        if not batch or not self.replication_nodes:
            return
        
        self.logger.debug(f"Replicating batch of {len(batch)} keys")
        self.metrics["replication_attempts"] += 1
        
        # Extract keys from batch
        keys = [item[1][0] for item in batch]
        
        # Get data for all keys
        data = {}
        for key in keys:
            try:
                value = await self.storage_engine.get(key)
                if value is not None:
                    data[key] = {
                        "value": value,
                        "timestamp": time.time()
                    }
            except Exception as e:
                self.logger.error(f"Error getting data for key {key}: {e}")
        
        if not data:
            return
        
        # Prepare replication payload
        payload = {
            "sender_id": self.node_id,
            "timestamp": time.time(),
            "data": data
        }
        
        # Replicate to all nodes
        replication_tasks = []
        
        for node_id in self.replication_nodes:
            task = self._replicate_to_node(node_id, payload)
            replication_tasks.append(task)
        
        # Wait for all replication tasks to complete
        results = await asyncio.gather(*replication_tasks, return_exceptions=True)
        
        # Process results
        success_count = 0
        for i, result in enumerate(results):
            node_id = self.replication_nodes[i]
            
            if isinstance(result, Exception):
                self.logger.warning(f"Error replicating to node {node_id}: {result}")
                self.metrics["replication_failures"] += 1
                self.replication_failures[node_id] = self.replication_failures.get(node_id, 0) + 1
            else:
                success_count += 1
                self.last_replicated_time[node_id] = time.time()
                
                # Reset failure count on success
                if node_id in self.replication_failures:
                    self.replication_failures[node_id] = 0
        
        if success_count > 0:
            self.metrics["replication_successes"] += 1
            self.metrics["keys_replicated"] += len(data)
        
        self.logger.debug(f"Replicated batch to {success_count}/{len(self.replication_nodes)} nodes")
    
    async def _replicate_to_node(self, node_id: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Replicate data to a specific node
        
        Args:
            node_id: ID of the target node
            payload: Replication payload
            
        Returns:
            Response from the node
        """
        try:
            # Send replication data
            result = await self.network_manager.send_rpc(
                node_id, 
                "replicate_data", 
                payload
            )
            
            return result or {}
        except Exception as e:
            self.logger.warning(f"Failed to replicate to node {node_id}: {e}")
            raise
    
    async def _reconciliation_loop(self):
        """Background task for periodic reconciliation"""
        self.logger.info("Starting reconciliation loop")
        
        while self.running:
            try:
                # Wait for reconciliation interval
                await asyncio.sleep(self.reconciliation_interval_ms / 1000)
                
                # Perform reconciliation with all nodes
                for node_id in self.replication_nodes:
                    await self._reconcile_with_node(node_id)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in reconciliation loop: {e}")
                await asyncio.sleep(5.0)  # Longer sleep on error
    
    async def _reconcile_with_node(self, node_id: str):
        """
        Perform reconciliation with a specific node
        
        Args:
            node_id: ID of the node to reconcile with
        """
        self.logger.debug(f"Starting reconciliation with node {node_id}")
        
        try:
            # Get timestamp of last reconciliation
            since_timestamp = self.last_replicated_time.get(node_id, 0)
            
            # Send reconciliation request
            result = await self._request_reconciliation(node_id, since_timestamp)
            
            if not result or not result.get("success", False):
                self.logger.warning(f"Reconciliation request to node {node_id} failed")
                return
            
            # Process keys that need reconciliation
            keys_to_reconcile = result.get("keys_to_reconcile", {})
            
            if keys_to_reconcile:
                self.logger.debug(f"Node {node_id} has {len(keys_to_reconcile)} keys to reconcile")
                
                # For each key, get the data and replicate
                for key, info in keys_to_reconcile.items():
                    # Schedule key for high-priority replication
                    await self.replicate_key(key, priority=0)
                
                self.metrics["reconciliations"] += 1
                
        except Exception as e:
            self.logger.error(f"Error during reconciliation with node {node_id}: {e}")
    
    async def _request_reconciliation(self, node_id: str, since_timestamp: float = 0) -> Dict[str, Any]:
        """
        Send a reconciliation request to a node
        
        Args:
            node_id: ID of the target node
            since_timestamp: Only include keys updated since this time
            
        Returns:
            Response from the node
        """
        try:
            payload = {
                "sender_id": self.node_id,
                "since_timestamp": since_timestamp
            }
            
            result = await self.network_manager.send_rpc(
                node_id,
                "reconciliation_request",
                payload
            )
            
            return result or {}
        except Exception as e:
            self.logger.warning(f"Failed to send reconciliation request to node {node_id}: {e}")
            return {}
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get replication metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["queue_size"] = self.replication_queue.qsize()
        metrics["replication_nodes"] = len(self.replication_nodes)
        
        return metrics