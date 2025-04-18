"""
src/exapyte/replication/sync_replicator.py
========================================

Synchronous replication implementation for exapyte.
Provides strong consistency with synchronous updates across nodes.
"""

import asyncio
import logging
import time
import json
from typing import Dict, Any, List, Optional, Set, Tuple


class SyncReplicator:
    """
    Synchronous replication implementation
    
    Features:
    - Strong consistency guarantees
    - Synchronous multi-node replication
    - Quorum-based acknowledgment
    - Automatic failure handling
    """
    
    def __init__(self, node_id: str, network_manager: Any, storage_engine: Any, 
                config: Dict[str, Any] = None):
        """
        Initialize the sync replicator
        
        Args:
            node_id: ID of this node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration parameters
                - replication_nodes: List of nodes to replicate to
                - quorum_size: Number of nodes required for quorum (default: majority)
                - timeout_ms: Timeout for replication operations
                - retry_count: Number of retries for failed operations
        """
        self.node_id = node_id
        self.network_manager = network_manager
        self.storage_engine = storage_engine
        self.config = config or {}
        
        # Set up logging
        self.logger = logging.getLogger(f"replication.sync.{node_id}")
        
        # Configuration
        self.replication_nodes = self.config.get("replication_nodes", [])
        self.quorum_size = self.config.get("quorum_size")
        self.timeout_ms = self.config.get("timeout_ms", 5000)  # 5 seconds
        self.retry_count = self.config.get("retry_count", 3)
        
        # If quorum size not specified, use majority
        if not self.quorum_size:
            self.quorum_size = (len(self.replication_nodes) // 2) + 1
        
        # Add self to replication nodes if not present
        if self.node_id not in self.replication_nodes:
            self.replication_nodes.append(self.node_id)
        
        # Replication state
        self.replication_failures = {}  # node_id -> count
        
        # Background tasks
        self.running = False
        
        # Metrics
        self.metrics = {
            "replication_attempts": 0,
            "replication_successes": 0,
            "replication_failures": 0,
            "quorum_failures": 0,
            "keys_replicated": 0
        }
        
        self.logger.info(f"Initialized SyncReplicator with config: {self.config}")
    
    async def start(self):
        """Start the replicator"""
        self.logger.info(f"Starting SyncReplicator for node {self.node_id}")
        self.running = True
        
        # Register replication handlers
        self.network_manager.register_handlers({
            "replicate_data": self.handle_replicate_data,
            "replicate_synchronously": self.handle_replicate_synchronously
        })
    
    async def stop(self):
        """Stop the replicator"""
        self.logger.info(f"Stopping SyncReplicator for node {self.node_id}")
        self.running = False
    
    async def replicate_key(self, key: str) -> bool:
        """
        Schedule a key for replication
        
        Args:
            key: The key to replicate
            
        Returns:
            True if scheduled successfully
        """
        # For synchronous replication, this is a no-op
        # Actual replication happens during the set/delete operation
        return True
    
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
        
        # Prepare replication data
        data = {
            key: {
                "value": value,
                "timestamp": time.time()
            }
        }
        
        # Replicate to all nodes
        success_nodes = []
        failed_nodes = {}
        
        # Create tasks for each node
        replication_tasks = []
        
        for node_id in self.replication_nodes:
            if node_id != self.node_id:  # Skip self
                task = self._replicate_to_node(node_id, keyspace, data)
                replication_tasks.append((node_id, task))
        
        # Wait for all tasks to complete or timeout
        timeout = self.timeout_ms / 1000  # Convert to seconds
        
        for node_id, task in replication_tasks:
            try:
                result = await asyncio.wait_for(task, timeout=timeout)
                
                if result and result.get("success", False):
                    success_nodes.append(node_id)
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
        
        # Check if we have quorum
        quorum_achieved = len(success_nodes) >= self.quorum_size
        
        if quorum_achieved:
            self.metrics["replication_successes"] += 1
            self.metrics["keys_replicated"] += len(data)
        else:
            self.metrics["quorum_failures"] += 1
        
        return {
            "success": quorum_achieved,
            "quorum_size": self.quorum_size,
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
                - timestamp: Timestamp of the replication
                
        Returns:
            Response with success status
        """
        sender_id = payload.get("sender_id")
        keyspace = payload.get("keyspace", "default")
        data = payload.get("data", {})
        timestamp = payload.get("timestamp", 0)
        
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
                if isinstance(value, dict) and "_timestamp" not in value:
                    value["_timestamp"] = value_timestamp
                    value["_source_node"] = sender_id
                
                await self.storage_engine.set(key, value)
                success_keys.append(key)
                
            except Exception as e:
                self.logger.error(f"Error processing replicated key {key}: {e}")
                failed_keys.append(key)
        
        return {
            "success": len(failed_keys) == 0,
            "keyspace": keyspace,
            "received_keys": len(data),
            "success_keys": len(success_keys),
            "failed_keys": len(failed_keys),
            "timestamp": time.time()
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
                
        Returns:
            Response with success status
        """
        sender_id = payload.get("sender_id")
        keyspace = payload.get("keyspace", "default")
        key = payload.get("key")
        value = payload.get("value")
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        self.logger.debug(f"Received synchronous replication request from {sender_id} for key {key}")
        
        try:
            # Store the value
            if isinstance(value, dict) and "_timestamp" not in value:
                value["_timestamp"] = time.time()
                value["_source_node"] = sender_id
            
            result = await self.storage_engine.set(key, value)
            
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
    
    async def _replicate_to_node(self, node_id: str, keyspace: str, data: Dict[str, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Replicate data to a specific node
        
        Args:
            node_id: ID of the target node
            keyspace: Keyspace name
            data: Dict of key-value pairs to replicate
            
        Returns:
            Response from the node
        """
        # For single key replication, use direct synchronous replication
        if len(data) == 1:
            key, value_data = next(iter(data.items()))
            value = value_data.get("value")
            
            payload = {
                "sender_id": self.node_id,
                "keyspace": keyspace,
                "key": key,
                "value": value
            }
            
            try:
                return await self.network_manager.send_rpc(
                    node_id,
                    "replicate_synchronously",
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
            "timestamp": time.time()
        }
        
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
        metrics["quorum_size"] = self.quorum_size
        metrics["replication_nodes"] = len(self.replication_nodes)
        metrics["failed_nodes"] = len(self.replication_failures)
        
        return metrics
