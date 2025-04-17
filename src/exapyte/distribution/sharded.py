"""
src/exapyte/distribution/sharded.py
==================================

Sharded data distribution implementation for exapyte.
Distributes data across multiple nodes based on key ranges or hash functions.
"""

import asyncio
import logging
import time
import hashlib
import json
from typing import Dict, Any, List, Optional, Set, Tuple
import random


class ShardedDistribution:
    """
    Sharded distribution strategy
    
    Features:
    - Horizontal partitioning of data across nodes
    - Consistent hashing for shard assignment
    - Automatic rebalancing on node changes
    - Range-based or hash-based sharding
    """
    
    def __init__(self, node_id: str, network_manager: Any, storage_engine: Any,
                config: Dict[str, Any] = None):
        """
        Initialize the sharded distribution
        
        Args:
            node_id: ID of this node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration parameters
                - sharding_method: "hash" or "range"
                - num_virtual_nodes: Number of virtual nodes for consistent hashing
                - rebalance_interval_ms: Time between rebalancing cycles
                - shard_nodes: List of nodes participating in sharding
                - is_coordinator: Whether this node is the shard coordinator
        """
        self.node_id = node_id
        self.network_manager = network_manager
        self.storage_engine = storage_engine
        self.config = config or {}
        
        # Set up logging
        self.logger = logging.getLogger(f"distribution.sharded.{node_id}")
        
        # Configuration
        self.sharding_method = self.config.get("sharding_method", "hash")
        self.num_virtual_nodes = self.config.get("num_virtual_nodes", 100)
        self.rebalance_interval_ms = self.config.get("rebalance_interval_ms", 60000)  # 1m
        self.shard_nodes = self.config.get("shard_nodes", [])
        self.is_coordinator = self.config.get("is_coordinator", False)
        
        # Add self to shard nodes if not present
        if self.node_id not in self.shard_nodes:
            self.shard_nodes.append(self.node_id)
        
        # Sharding state
        self.virtual_nodes = {}  # virtual_node_id -> node_id
        self.node_versions = {}  # node_id -> version
        self.ring = []  # Sorted list of (hash_value, virtual_node_id) for consistent hashing
        self.range_map = {}  # key_range -> node_id for range-based sharding
        self.shard_version = 0  # Incremented on shard configuration changes
        
        # For range-based sharding
        self.ranges = []  # List of range boundaries
        
        # Background tasks
        self.rebalance_task = None
        self.running = False
        
        # Transfer state
        self.transfers_in_progress = set()
        
        # Metrics
        self.metrics = {
            "keys_routed": 0,
            "local_keys_processed": 0,
            "remote_keys_processed": 0,
            "rebalances": 0,
            "keys_transferred": 0
        }
        
        self.logger.info(f"Initialized ShardedDistribution with config: {self.config}")
    
    async def start(self):
        """Start the distribution strategy and background tasks"""
        self.logger.info(f"Starting ShardedDistribution for node {self.node_id}")
        self.running = True
        
        # Register handlers
        self.network_manager.register_handlers({
            "get_shard_config": self.handle_get_shard_config,
            "update_shard_config": self.handle_update_shard_config,
            "transfer_keys": self.handle_transfer_keys,
            "route_operation": self.handle_route_operation
        })
        
        # Initialize sharding
        await self._initialize_sharding()
        
        # Start rebalance task if coordinator
        if self.is_coordinator:
            self.rebalance_task = asyncio.create_task(self._rebalance_loop())
    
    async def stop(self):
        """Stop the distribution strategy and background tasks"""
        self.logger.info(f"Stopping ShardedDistribution for node {self.node_id}")
        self.running = False
        
        # Cancel background tasks
        if self.rebalance_task:
            self.rebalance_task.cancel()
            try:
                await self.rebalance_task
            except asyncio.CancelledError:
                pass
    
    async def route_key(self, key: str) -> str:
        """
        Determine which node should handle a key
        
        Args:
            key: The key to route
            
        Returns:
            Node ID that should handle the key
        """
        self.metrics["keys_routed"] += 1
        
        if self.sharding_method == "hash":
            return self._route_by_hash(key)
        else:
            return self._route_by_range(key)
    
    async def process_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an operation (get, set, delete)
        
        Args:
            operation: Operation details
                - type: Operation type ("get", "set", "delete")
                - key: The key to operate on
                - value: The value (for set operations)
                
        Returns:
            Operation result
        """
        op_type = operation.get("type")
        key = operation.get("key")
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        # Determine which node should handle this key
        target_node = await self.route_key(key)
        
        # If local, process directly
        if target_node == self.node_id:
            self.metrics["local_keys_processed"] += 1
            return await self._process_local_operation(operation)
        
        # If remote, forward to appropriate node
        self.metrics["remote_keys_processed"] += 1
        return await self._forward_operation(target_node, operation)
    
    async def handle_get_shard_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle request for shard configuration
        
        Args:
            payload: Request details
                - requester_id: ID of the requesting node
                
        Returns:
            Current shard configuration
        """
        requester_id = payload.get("requester_id")
        
        self.logger.debug(f"Sending shard config to node {requester_id}")
        
        if self.sharding_method == "hash":
            config = {
                "sharding_method": "hash",
                "virtual_nodes": self.virtual_nodes,
                "ring": self.ring,
                "version": self.shard_version
            }
        else:
            config = {
                "sharding_method": "range",
                "range_map": self.range_map,
                "ranges": self.ranges,
                "version": self.shard_version
            }
        
        return {
            "success": True,
            "config": config
        }
    
    async def handle_update_shard_config(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle update to shard configuration
        
        Args:
            payload: Update details
                - config: New shard configuration
                - coordinator_id: ID of the coordinator node
                
        Returns:
            Update status
        """
        config = payload.get("config", {})
        coordinator_id = payload.get("coordinator_id")
        
        # Only accept updates from the coordinator
        if self.is_coordinator and coordinator_id != self.node_id:
            return {
                "success": False,
                "error": "not_from_coordinator"
            }
        
        # Check version
        new_version = config.get("version", 0)
        if new_version <= self.shard_version:
            return {
                "success": False,
                "error": "outdated_version"
            }
        
        # Update configuration
        if config.get("sharding_method") == "hash":
            self.virtual_nodes = config.get("virtual_nodes", {})
            self.ring = config.get("ring", [])
        else:
            self.range_map = config.get("range_map", {})
            self.ranges = config.get("ranges", [])
        
        self.shard_version = new_version
        
        self.logger.info(f"Updated shard configuration to version {self.shard_version}")
        
        return {
            "success": True,
            "version": self.shard_version
        }
    
    async def handle_transfer_keys(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle transfer of keys from another node
        
        Args:
            payload: Transfer details
                - keys: Dict of key-value pairs
                - source_node: ID of the source node
                - transfer_id: Unique ID for this transfer
                
        Returns:
            Transfer status
        """
        keys = payload.get("keys", {})
        source_node = payload.get("source_node")
        transfer_id = payload.get("transfer_id")
        
        self.logger.info(f"Receiving {len(keys)} keys from node {source_node}")
        
        # Store the keys
        stored_count = 0
        for key, value in keys.items():
            await self.storage_engine.set(key, value)
            stored_count += 1
        
        self.metrics["keys_transferred"] += stored_count
        
        return {
            "success": True,
            "stored_keys": stored_count,
            "transfer_id": transfer_id
        }
    
    async def handle_route_operation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle an operation routed from another node
        
        Args:
            payload: Operation details
                - operation: The operation to perform
                - source_node: ID of the source node
                
        Returns:
            Operation result
        """
        operation = payload.get("operation", {})
        source_node = payload.get("source_node")
        
        key = operation.get("key")
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        # Verify this node should handle the key
        target_node = await self.route_key(key)
        if target_node != self.node_id:
            return {
                "success": False,
                "error": "wrong_node",
                "correct_node": target_node
            }
        
        # Process the operation locally
        result = await self._process_local_operation(operation)
        
        # Add routing info to result
        result["routed_by"] = source_node
        result["handled_by"] = self.node_id
        
        return result
    
    async def _process_local_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process an operation locally
        
        Args:
            operation: Operation details
                
        Returns:
            Operation result
        """
        op_type = operation.get("type")
        key = operation.get("key")
        
        if op_type == "get":
            value = await self.storage_engine.get(key)
            return {
                "success": True,
                "key": key,
                "value": value,
                "exists": value is not None
            }
            
        elif op_type == "set":
            value = operation.get("value")
            result = await self.storage_engine.set(key, value)
            return {
                "success": result,
                "key": key
            }
            
        elif op_type == "delete":
            result = await self.storage_engine.delete(key)
            return {
                "success": result,
                "key": key
            }
            
        else:
            return {
                "success": False,
                "error": "unknown_operation_type"
            }
    
    async def _forward_operation(self, target_node: str, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Forward an operation to another node
        
        Args:
            target_node: ID of the target node
            operation: Operation to forward
                
        Returns:
            Operation result from target node
        """
        payload = {
            "operation": operation,
            "source_node": self.node_id
        }
        
        try:
            result = await self.network_manager.send_rpc(
                target_node, 
                "route_operation", 
                payload
            )
            
            return result or {
                "success": False,
                "error": "no_response_from_target"
            }
            
        except Exception as e:
            self.logger.error(f"Error forwarding operation to node {target_node}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _initialize_sharding(self):
        """Initialize the sharding configuration"""
        if self.is_coordinator:
            # Coordinator initializes the sharding configuration
            await self._create_initial_sharding()
        else:
            # Non-coordinator fetches configuration from coordinator
            await self._fetch_shard_config()
    
    async def _create_initial_sharding(self):
        """Create initial sharding configuration (coordinator only)"""
        if not self.is_coordinator:
            return
        
        self.logger.info("Creating initial sharding configuration")
        
        # Increment version
        self.shard_version += 1
        
        if self.sharding_method == "hash":
            # Create virtual nodes for consistent hashing
            self.virtual_nodes = {}
            self.ring = []
            
            # Assign virtual nodes evenly across physical nodes
            for node_id in self.shard_nodes:
                for i in range(self.num_virtual_nodes // len(self.shard_nodes)):
                    virtual_node_id = f"{node_id}_{i}"
                    self.virtual_nodes[virtual_node_id] = node_id
                    
                    # Calculate hash of virtual node ID
                    hash_value = self._calculate_hash(virtual_node_id)
                    self.ring.append((hash_value, virtual_node_id))
            
            # Sort the ring by hash value
            self.ring.sort(key=lambda x: x[0])
            
        else:
            # Create range-based sharding
            # For simplicity, divide keyspace evenly
            # In practice, would analyze data distribution
            self.range_map = {}
            self.ranges = []
            
            # Create simple boundaries (e.g., a-g, h-n, o-u, v-z)
            range_boundaries = []
            alphabet = "abcdefghijklmnopqrstuvwxyz"
            chunk_size = len(alphabet) // len(self.shard_nodes)
            
            for i in range(0, len(alphabet), chunk_size):
                if i > 0:
                    boundary = alphabet[i]
                    range_boundaries.append(boundary)
            
            # Create ranges
            self.ranges = [""] + range_boundaries + [None]  # None represents end of range
            
            # Assign ranges to nodes
            for i in range(len(self.ranges) - 1):
                start = self.ranges[i]
                end = self.ranges[i+1]
                range_key = f"{start}:{end}"
                node_index = i % len(self.shard_nodes)
                self.range_map[range_key] = self.shard_nodes[node_index]
        
        # Distribute the configuration to all nodes
        await self._distribute_shard_config()
    
    async def _fetch_shard_config(self):
        """Fetch shard configuration from coordinator"""
        # Find coordinator
        coordinator_id = None
        for node_id in self.shard_nodes:
            if node_id != self.node_id:
                config = self.config.get("nodes", {}).get(node_id, {})
                if config.get("is_coordinator", False):
                    coordinator_id = node_id
                    break
        
        if not coordinator_id:
            self.logger.warning("No coordinator found, using default configuration")
            return
        
        try:
            payload = {
                "requester_id": self.node_id
            }
            
            result = await self.network_manager.send_rpc(
                coordinator_id,
                "get_shard_config",
                payload
            )
            
            if result and result.get("success", False):
                config = result.get("config", {})
                
                # Update local configuration
                self.sharding_method = config.get("sharding_method", self.sharding_method)
                self.shard_version = config.get("version", 0)
                
                if self.sharding_method == "hash":
                    self.virtual_nodes = config.get("virtual_nodes", {})
                    self.ring = config.get("ring", [])
                else:
                    self.range_map = config.get("range_map", {})
                    self.ranges = config.get("ranges", [])
                
                self.logger.info(f"Fetched shard configuration from coordinator (version {self.shard_version})")
            else:
                self.logger.warning("Failed to fetch shard configuration")
                
        except Exception as e:
            self.logger.error(f"Error fetching shard configuration: {e}")
    
    async def _distribute_shard_config(self):
        """Distribute shard configuration to all nodes (coordinator only)"""
        if not self.is_coordinator:
            return
        
        self.logger.info(f"Distributing shard configuration version {self.shard_version}")
        
        # Prepare configuration
        if self.sharding_method == "hash":
            config = {
                "sharding_method": "hash",
                "virtual_nodes": self.virtual_nodes,
                "ring": self.ring,
                "version": self.shard_version
            }
        else:
            config = {
                "sharding_method": "range",
                "range_map": self.range_map,
                "ranges": self.ranges,
                "version": self.shard_version
            }
        
        # Send to all nodes except self
        for node_id in self.shard_nodes:
            if node_id != self.node_id:
                try:
                    payload = {
                        "config": config,
                        "coordinator_id": self.node_id
                    }
                    
                    result = await self.network_manager.send_rpc(
                        node_id,
                        "update_shard_config",
                        payload
                    )
                    
                    if not result or not result.get("success", False):
                        self.logger.warning(f"Failed to update shard configuration on node {node_id}")
                        
                except Exception as e:
                    self.logger.error(f"Error distributing shard configuration to node {node_id}: {e}")
    
    async def _rebalance_loop(self):
        """Background task for periodic rebalancing (coordinator only)"""
        if not self.is_coordinator:
            return
        
        self.logger.info("Starting rebalance loop")
        
        while self.running:
            try:
                # Wait for rebalance interval
                await asyncio.sleep(self.rebalance_interval_ms / 1000)
                
                # Check if rebalance is needed
                if await self._check_rebalance_needed():
                    await self._perform_rebalance()
                    self.metrics["rebalances"] += 1
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in rebalance loop: {e}")
                await asyncio.sleep(5.0)  # Longer sleep on error
    
    async def _check_rebalance_needed(self) -> bool:
        """
        Check if rebalancing is needed
        
        Returns:
            True if rebalance is needed
        """
        # In a real implementation, would check:
        # 1. Node health and capacity
        # 2. Data distribution imbalance
        # 3. Hot spots
        
        # For demonstration, occasionally rebalance
        return random.random() < 0.1  # 10% chance of rebalance
    
    async def _perform_rebalance(self):
        """Perform rebalancing of data across nodes"""
        self.logger.info("Performing shard rebalance")
        
        # 1. Calculate new shard configuration
        if self.sharding_method == "hash":
            await self._rebalance_hash_sharding()
        else:
            await self._rebalance_range_sharding()
        
        # 2. Distribute new configuration
        await self._distribute_shard_config()
        
        # 3. Trigger data migration
        await self._migrate_data_after_rebalance()
    
    async def _rebalance_hash_sharding(self):
        """Rebalance hash-based sharding"""
        # Increment version
        self.shard_version += 1
        
        # For demonstration, add some randomness to virtual node assignment
        new_virtual_nodes = dict(self.virtual_nodes)
        
        # Randomly reassign some virtual nodes
        num_to_reassign = max(1, len(new_virtual_nodes) // 10)  # Reassign 10%
        
        nodes_to_reassign = random.sample(list(new_virtual_nodes.keys()), num_to_reassign)
        
        for virtual_node_id in nodes_to_reassign:
            # Assign to a different physical node
            current_node = new_virtual_nodes[virtual_node_id]
            available_nodes = [n for n in self.shard_nodes if n != current_node]
            
            if available_nodes:
                new_node = random.choice(available_nodes)
                new_virtual_nodes[virtual_node_id] = new_node
        
        # Update configuration
        self.virtual_nodes = new_virtual_nodes
        
        # Rebuild ring
        self.ring = []
        for virtual_node_id, node_id in self.virtual_nodes.items():
            hash_value = self._calculate_hash(virtual_node_id)
            self.ring.append((hash_value, virtual_node_id))
        
        # Sort the ring
        self.ring.sort(key=lambda x: x[0])
    
    async def _rebalance_range_sharding(self):
        """Rebalance range-based sharding"""
        # Increment version
        self.shard_version += 1
        
        # For demonstration, adjust range boundaries slightly
        # In a real implementation, would analyze data distribution
        
        # Randomly adjust one boundary
        if len(self.ranges) > 2:  # Need at least one adjustable boundary
            # Pick a boundary to adjust (not first or last)
            boundary_index = random.randint(1, len(self.ranges) - 2)
            
            # Current boundary
            current_boundary = self.ranges[boundary_index]
            
            # Adjust boundary
            alphabet = "abcdefghijklmnopqrstuvwxyz"
            current_idx = alphabet.index(current_boundary)
            
            # Move up or down by 1
            new_idx = max(0, min(len(alphabet) - 1, current_idx + random.choice([-1, 1])))
            new_boundary = alphabet[new_idx]
            
            # Update ranges
            self.ranges[boundary_index] = new_boundary
            
            # Rebuild range map
            new_range_map = {}
            for i in range(len(self.ranges) - 1):
                start = self.ranges[i]
                end = self.ranges[i+1]
                range_key = f"{start}:{end}"
                node_index = i % len(self.shard_nodes)
                new_range_map[range_key] = self.shard_nodes[node_index]
            
            self.range_map = new_range_map
    
    async def _migrate_data_after_rebalance(self):
        """Migrate data after rebalance"""
        self.logger.info("Starting data migration after rebalance")
        
        # 1. Scan local data
        # For demonstration, we'll use a small sample
        sample_keys = ["key1", "key2", "key3", "key4", "key5"]
        
        # 2. Determine which keys need to move
        keys_to_move = {}  # target_node -> {key: value}
        
        for key in sample_keys:
            try:
                # Check if key exists locally
                value = await self.storage_engine.get(key)
                if value is not None:
                    # Determine target node
                    target_node = await self.route_key(key)
                    
                    # If not local, mark for movement
                    if target_node != self.node_id:
                        if target_node not in keys_to_move:
                            keys_to_move[target_node] = {}
                        keys_to_move[target_node][key] = value
                        
            except Exception as e:
                self.logger.error(f"Error checking key {key} for migration: {e}")
        
        # 3. Transfer keys to new owners
        for target_node, keys in keys_to_move.items():
            if keys:
                await self._transfer_keys_to_node(target_node, keys)
    
    async def _transfer_keys_to_node(self, target_node: str, keys: Dict[str, Any]) -> bool:
        """
        Transfer keys to another node
        
        Args:
            target_node: ID of the target node
            keys: Dict of key-value pairs to transfer
            
        Returns:
            True if transfer successful
        """
        if not keys:
            return True
        
        self.logger.info(f"Transferring {len(keys)} keys to node {target_node}")
        
        try:
            # Generate transfer ID
            transfer_id = f"{self.node_id}_{int(time.time())}_{random.randint(0, 1000)}"
            
            # Add to in-progress transfers
            self.transfers_in_progress.add(transfer_id)
            
            # Send keys to target node
            payload = {
                "keys": keys,
                "source_node": self.node_id,
                "transfer_id": transfer_id
            }
            
            result = await self.network_manager.send_rpc(
                target_node,
                "transfer_keys",
                payload
            )
            
            # Process result
            if result and result.get("success", False):
                # Keys transferred successfully
                # In a real implementation, would delete local copies after confirmation
                self.metrics["keys_transferred"] += len(keys)
                
                # Remove from in-progress transfers
                self.transfers_in_progress.discard(transfer_id)
                
                return True
            else:
                self.logger.warning(f"Failed to transfer keys to node {target_node}")
                return False
                
        except Exception as e:
            self.logger.error(f"Error transferring keys to node {target_node}: {e}")
            return False
    
    def _route_by_hash(self, key: str) -> str:
        """
        Route a key using consistent hashing
        
        Args:
            key: The key to route
            
        Returns:
            Node ID that should handle the key
        """
        if not self.ring:
            # No ring configuration, use self
            return self.node_id
        
        # Calculate hash of the key
        key_hash = self._calculate_hash(key)
        
        # Find the first node on the ring with hash >= key_hash
        for hash_value, virtual_node_id in self.ring:
            if hash_value >= key_hash:
                return self.virtual_nodes.get(virtual_node_id, self.node_id)
        
        # Wrap around to the first node if no match found
        return self.virtual_nodes.get(self.ring[0][1], self.node_id)
    
    def _route_by_range(self, key: str) -> str:
        """
        Route a key using range-based sharding
        
        Args:
            key: The key to route
            
        Returns:
            Node ID that should handle the key
        """
        if not self.ranges or not self.range_map:
            # No range configuration, use self
            return self.node_id
        
        # Find the range containing the key
        for i in range(len(self.ranges) - 1):
            start = self.ranges[i]
            end = self.ranges[i+1]
            
            # Check if key is in this range
            in_range = True
            
            # Check lower bound
            if start and key < start:
                in_range = False
                
            # Check upper bound
            if end is not None and key >= end:
                in_range = False
                
            if in_range:
                range_key = f"{start}:{end}"
                return self.range_map.get(range_key, self.node_id)
        
        # Default to self if no range match found
        return self.node_id
    
    def _calculate_hash(self, value: str) -> int:
        """
        Calculate hash of a value
        
        Args:
            value: The value to hash
            
        Returns:
            Hash value as integer
        """
        hash_bytes = hashlib.md5(value.encode()).digest()
        return int.from_bytes(hash_bytes[:4], byteorder='big')
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get distribution metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        
        # Add sharding info
        metrics["sharding_method"] = self.sharding_method
        metrics["shard_version"] = self.shard_version
        metrics["num_nodes"] = len(self.shard_nodes)
        
        if self.sharding_method == "hash":
            metrics["num_virtual_nodes"] = len(self.virtual_nodes)
        else:
            metrics["num_ranges"] = len(self.range_map)
        
        return metrics