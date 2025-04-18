"""
src/exapyte/distribution/hierarchical.py
=======================================

Hierarchical data distribution implementation for exapyte.
Distributes data across multiple regions and tiers for optimal locality.
"""

import asyncio
import logging
import time
import json
from typing import Dict, Any, List, Optional, Set, Tuple


class HierarchicalDistribution:
    """
    Hierarchical distribution strategy
    
    Features:
    - Multi-tier distribution (global, regional, node)
    - Topology-aware request routing
    - Locality optimization for access patterns
    - Cross-region coordination
    """
    
    def __init__(self, node_id: str, network_manager: Any, storage_engine: Any,
                config: Dict[str, Any] = None):
        """
        Initialize the hierarchical distribution
        
        Args:
            node_id: ID of this node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration parameters
                - region_id: ID of this node's region
                - is_global_coordinator: Whether this node is a global coordinator
                - is_region_coordinator: Whether this node is a region coordinator
                - topology: Hierarchical topology configuration
        """
        self.node_id = node_id
        self.network_manager = network_manager
        self.storage_engine = storage_engine
        self.config = config or {}
        
        # Set up logging
        self.logger = logging.getLogger(f"distribution.hierarchical.{node_id}")
        
        # Configuration
        self.region_id = self.config.get("region_id", "default")
        self.is_global_coordinator = self.config.get("is_global_coordinator", False)
        self.is_region_coordinator = self.config.get("is_region_coordinator", False)
        self.topology = self.config.get("topology", {})
        
        # Hierarchical state
        self.regions = {}  # region_id -> {nodes: [], coordinator: node_id}
        self.global_coordinator = None
        self.region_coordinators = {}  # region_id -> node_id
        self.node_regions = {}  # node_id -> region_id
        
        # Routing state
        self.routing_cache = {}  # key_prefix -> region_id
        self.routing_cache_ttl = self.config.get("routing_cache_ttl", 60)  # seconds
        self.routing_cache_expiry = {}  # key_prefix -> expiry timestamp
        
        # Background tasks
        self.topology_sync_task = None
        self.running = False
        
        # Metrics
        self.metrics = {
            "keys_routed": 0,
            "local_keys_processed": 0,
            "remote_keys_processed": 0,
            "cross_region_operations": 0,
            "routing_cache_hits": 0,
            "routing_cache_misses": 0
        }
        
        self.logger.info(f"Initialized HierarchicalDistribution with config: {self.config}")
    
    async def start(self):
        """Start the distribution strategy and background tasks"""
        self.logger.info(f"Starting HierarchicalDistribution for node {self.node_id}")
        self.running = True
        
        # Register handlers
        self.network_manager.register_handlers({
            "get_topology": self.handle_get_topology,
            "update_topology": self.handle_update_topology,
            "route_key": self.handle_route_key,
            "route_operation": self.handle_route_operation
        })
        
        # Initialize topology
        await self._initialize_topology()
        
        # Start topology sync task
        self.topology_sync_task = asyncio.create_task(self._topology_sync_loop())
    
    async def stop(self):
        """Stop the distribution strategy and background tasks"""
        self.logger.info(f"Stopping HierarchicalDistribution for node {self.node_id}")
        self.running = False
        
        # Cancel background tasks
        if self.topology_sync_task:
            self.topology_sync_task.cancel()
            try:
                await self.topology_sync_task
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
        
        # First, determine the region for this key
        target_region = await self._get_key_region(key)
        
        # If key belongs to our region, route within region
        if target_region == self.region_id:
            return await self._route_within_region(key)
        
        # If key belongs to another region, route to that region's coordinator
        return self.region_coordinators.get(target_region, self.global_coordinator)
    
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
        
        # If remote but in same region, forward directly
        target_region = self.node_regions.get(target_node)
        if target_region == self.region_id:
            self.metrics["remote_keys_processed"] += 1
            return await self._forward_operation(target_node, operation)
        
        # If in different region, route through region coordinator
        self.metrics["cross_region_operations"] += 1
        return await self._forward_cross_region(target_node, operation)
    
    async def handle_get_topology(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle request for topology information
        
        Args:
            payload: Request details
                - requester_id: ID of the requesting node
                
        Returns:
            Current topology information
        """
        requester_id = payload.get("requester_id")
        
        self.logger.debug(f"Sending topology info to node {requester_id}")
        
        return {
            "success": True,
            "topology": {
                "regions": self.regions,
                "global_coordinator": self.global_coordinator,
                "region_coordinators": self.region_coordinators,
                "node_regions": self.node_regions
            }
        }
    
    async def handle_update_topology(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle update to topology information
        
        Args:
            payload: Update details
                - topology: New topology information
                - source_node: ID of the source node
                
        Returns:
            Update status
        """
        topology = payload.get("topology", {})
        source_node = payload.get("source_node")
        
        # Validate source
        if self.is_global_coordinator and source_node != self.node_id:
            # Only global coordinator can update topology
            return {
                "success": False,
                "error": "unauthorized_update"
            }
        
        # Update topology
        if "regions" in topology:
            self.regions = topology["regions"]
        
        if "global_coordinator" in topology:
            self.global_coordinator = topology["global_coordinator"]
        
        if "region_coordinators" in topology:
            self.region_coordinators = topology["region_coordinators"]
        
        if "node_regions" in topology:
            self.node_regions = topology["node_regions"]
        
        self.logger.info(f"Updated topology from node {source_node}")
        
        return {
            "success": True
        }
    
    async def handle_route_key(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle request to route a key
        
        Args:
            payload: Request details
                - key: The key to route
                - requester_id: ID of the requesting node
                
        Returns:
            Routing information
        """
        key = payload.get("key")
        requester_id = payload.get("requester_id")
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
        # Determine target node
        target_node = await self.route_key(key)
        target_region = self.node_regions.get(target_node)
        
        return {
            "success": True,
            "key": key,
            "target_node": target_node,
            "target_region": target_region
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
        
        # Process the operation
        result = await self.process_operation(operation)
        
        # Add routing info to result
        result["routed_by"] = source_node
        result["handled_by"] = self.node_id
        
        return result
    
    async def _initialize_topology(self):
        """Initialize the topology information"""
        if self.is_global_coordinator:
            # Global coordinator initializes the topology
            await self._create_initial_topology()
        else:
            # Non-coordinator fetches topology from coordinator
            await self._fetch_topology()
    
    async def _create_initial_topology(self):
        """Create initial topology (global coordinator only)"""
        if not self.is_global_coordinator:
            return
        
        self.logger.info("Creating initial topology")
        
        # Set self as global coordinator
        self.global_coordinator = self.node_id
        
        # Initialize regions from config
        regions_config = self.config.get("regions", {})
        
        for region_id, region_config in regions_config.items():
            nodes = region_config.get("nodes", [])
            coordinator = region_config.get("coordinator")
            
            self.regions[region_id] = {
                "nodes": nodes,
                "coordinator": coordinator
            }
            
            # Set region coordinator
            if coordinator:
                self.region_coordinators[region_id] = coordinator
            
            # Set node regions
            for node_id in nodes:
                self.node_regions[node_id] = region_id
        
        # Add self to topology if not already included
        if self.node_id not in self.node_regions:
            self.node_regions[self.node_id] = self.region_id
            
            if self.region_id not in self.regions:
                self.regions[self.region_id] = {
                    "nodes": [self.node_id],
                    "coordinator": self.node_id if self.is_region_coordinator else None
                }
            else:
                if self.node_id not in self.regions[self.region_id]["nodes"]:
                    self.regions[self.region_id]["nodes"].append(self.node_id)
                
                if self.is_region_coordinator and not self.regions[self.region_id]["coordinator"]:
                    self.regions[self.region_id]["coordinator"] = self.node_id
                    self.region_coordinators[self.region_id] = self.node_id
        
        # Distribute the topology to all nodes
        await self._distribute_topology()
    
    async def _fetch_topology(self):
        """Fetch topology from coordinator"""
        # First try global coordinator
        if self.global_coordinator and self.global_coordinator != self.node_id:
            success = await self._fetch_from_node(self.global_coordinator)
            if success:
                return
        
        # Then try region coordinator
        region_coordinator = self.region_coordinators.get(self.region_id)
        if region_coordinator and region_coordinator != self.node_id:
            success = await self._fetch_from_node(region_coordinator)
            if success:
                return
        
        # Try any known node
        for node_id in self.node_regions:
            if node_id != self.node_id:
                success = await self._fetch_from_node(node_id)
                if success:
                    return
        
        self.logger.warning("Could not fetch topology from any node")
    
    async def _fetch_from_node(self, node_id: str) -> bool:
        """
        Fetch topology from a specific node
        
        Args:
            node_id: ID of the node to fetch from
            
        Returns:
            True if successful
        """
        try:
            payload = {
                "requester_id": self.node_id
            }
            
            result = await self.network_manager.send_rpc(
                node_id,
                "get_topology",
                payload
            )
            
            if result and result.get("success", False):
                topology = result.get("topology", {})
                
                # Update local topology
                if "regions" in topology:
                    self.regions = topology["regions"]
                
                if "global_coordinator" in topology:
                    self.global_coordinator = topology["global_coordinator"]
                
                if "region_coordinators" in topology:
                    self.region_coordinators = topology["region_coordinators"]
                
                if "node_regions" in topology:
                    self.node_regions = topology["node_regions"]
                
                self.logger.info(f"Fetched topology from node {node_id}")
                return True
            
            return False
            
        except Exception as e:
            self.logger.error(f"Error fetching topology from node {node_id}: {e}")
            return False
    
    async def _distribute_topology(self):
        """Distribute topology to all nodes (coordinator only)"""
        if not self.is_global_coordinator and not self.is_region_coordinator:
            return
        
        self.logger.info("Distributing topology to nodes")
        
        # Prepare topology
        topology = {
            "regions": self.regions,
            "global_coordinator": self.global_coordinator,
            "region_coordinators": self.region_coordinators,
            "node_regions": self.node_regions
        }
        
        # If global coordinator, distribute to all regions
        if self.is_global_coordinator:
            target_nodes = []
            
            # Include all region coordinators
            for region_id, coordinator_id in self.region_coordinators.items():
                if coordinator_id and coordinator_id != self.node_id:
                    target_nodes.append(coordinator_id)
            
            # Include all nodes in our region
            if self.region_id in self.regions:
                for node_id in self.regions[self.region_id]["nodes"]:
                    if node_id != self.node_id and node_id not in target_nodes:
                        target_nodes.append(node_id)
        
        # If region coordinator, distribute to nodes in our region
        elif self.is_region_coordinator and self.region_id in self.regions:
            target_nodes = [
                node_id for node_id in self.regions[self.region_id]["nodes"]
                if node_id != self.node_id
            ]
        else:
            target_nodes = []
        
        # Send to all target nodes
        for node_id in target_nodes:
            try:
                payload = {
                    "topology": topology,
                    "source_node": self.node_id
                }
                
                result = await self.network_manager.send_rpc(
                    node_id,
                    "update_topology",
                    payload
                )
                
                if not result or not result.get("success", False):
                    self.logger.warning(f"Failed to update topology on node {node_id}")
                    
            except Exception as e:
                self.logger.error(f"Error distributing topology to node {node_id}: {e}")
    
    async def _topology_sync_loop(self):
        """Background task for periodic topology synchronization"""
        while self.running:
            try:
                # Wait for a while
                await asyncio.sleep(60.0)  # Sync every minute
                
                # If coordinator, distribute topology
                if self.is_global_coordinator or self.is_region_coordinator:
                    await self._distribute_topology()
                else:
                    # Otherwise, fetch topology
                    await self._fetch_topology()
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in topology sync loop: {e}")
                await asyncio.sleep(300.0)  # Longer sleep on error
    
    async def _get_key_region(self, key: str) -> str:
        """
        Determine which region should handle a key
        
        Args:
            key: The key to route
            
        Returns:
            Region ID that should handle the key
        """
        # Check routing cache first
        prefix = self._get_key_prefix(key)
        
        if prefix in self.routing_cache:
            # Check if cache entry is still valid
            if time.time() < self.routing_cache_expiry.get(prefix, 0):
                self.metrics["routing_cache_hits"] += 1
                return self.routing_cache[prefix]
        
        self.metrics["routing_cache_misses"] += 1
        
        # Determine region based on key characteristics
        # For demonstration, use a simple approach based on key prefix
        # In a real implementation, would use more sophisticated routing
        
        # If key starts with region ID, route to that region
        for region_id in self.regions:
            if key.startswith(f"{region_id}_"):
                target_region = region_id
                break
        else:
            # Otherwise, use a hash-based approach
            target_region = self._hash_key_to_region(key)
        
        # Update routing cache
        self.routing_cache[prefix] = target_region
        self.routing_cache_expiry[prefix] = time.time() + self.routing_cache_ttl
        
        return target_region
    
    def _get_key_prefix(self, key: str) -> str:
        """
        Extract prefix from key for routing cache
        
        Args:
            key: The key
            
        Returns:
            Prefix for routing cache
        """
        # For demonstration, use first component of key
        parts = key.split("_", 1)
        return parts[0] if len(parts) > 1 else key
    
    def _hash_key_to_region(self, key: str) -> str:
        """
        Map a key to a region using hashing
        
        Args:
            key: The key to map
            
        Returns:
            Region ID
        """
        if not self.regions:
            return self.region_id
        
        # Simple hash-based mapping
        region_ids = list(self.regions.keys())
        hash_value = hash(key) % len(region_ids)
        return region_ids[hash_value]
    
    async def _route_within_region(self, key: str) -> str:
        """
        Route a key within the current region
        
        Args:
            key: The key to route
            
        Returns:
            Node ID that should handle the key
        """
        # Get nodes in this region
        region_nodes = self.regions.get(self.region_id, {}).get("nodes", [])
        
        if not region_nodes:
            return self.node_id
        
        # For demonstration, use simple hash-based routing
        hash_value = hash(key) % len(region_nodes)
        return region_nodes[hash_value]
    
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
    
    async def _forward_cross_region(self, target_node: str, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Forward an operation to another region
        
        Args:
            target_node: ID of the target node
            operation: Operation to forward
                
        Returns:
            Operation result from target node
        """
        # Get target region
        target_region = self.node_regions.get(target_node)
        
        if not target_region:
            return {
                "success": False,
                "error": "unknown_target_region"
            }
        
        # Get region coordinator
        region_coordinator = self.region_coordinators.get(target_region)
        
        if not region_coordinator:
            # If no region coordinator, try global coordinator
            if self.global_coordinator:
                return await self._forward_operation(self.global_coordinator, operation)
            else:
                return {
                    "success": False,
                    "error": "no_coordinator_available"
                }
        
        # Forward to region coordinator
        return await self._forward_operation(region_coordinator, operation)
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get distribution metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        
        # Add topology info
        metrics["region_id"] = self.region_id
        metrics["is_global_coordinator"] = self.is_global_coordinator
        metrics["is_region_coordinator"] = self.is_region_coordinator
        metrics["num_regions"] = len(self.regions)
        metrics["routing_cache_size"] = len(self.routing_cache)
        
        return metrics
