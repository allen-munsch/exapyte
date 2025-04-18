"""
src/exapyte/distribution/centralized.py
======================================

Centralized data distribution implementation for exapyte.
All data is stored on a central node for simplicity.
"""

import asyncio
import logging
import time
from typing import Dict, Any, List, Optional, Set, Tuple


class CentralizedDistribution:
    """
    Centralized distribution strategy
    
    Features:
    - All data stored on a central node
    - Simple routing (all operations go to central node)
    - No sharding or partitioning
    - Useful for small deployments or testing
    """
    
    def __init__(self, node_id: str, network_manager: Any, storage_engine: Any,
                config: Dict[str, Any] = None):
        """
        Initialize the centralized distribution
        
        Args:
            node_id: ID of this node
            network_manager: Network manager instance
            storage_engine: Storage engine instance
            config: Configuration parameters
                - central_node: ID of the central node
        """
        self.node_id = node_id
        self.network_manager = network_manager
        self.storage_engine = storage_engine
        self.config = config or {}
        
        # Set up logging
        self.logger = logging.getLogger(f"distribution.centralized.{node_id}")
        
        # Configuration
        self.central_node = self.config.get("central_node")
        
        # If central node not specified, use first node in config or self
        if not self.central_node:
            nodes = self.config.get("nodes", {})
            if nodes:
                self.central_node = next(iter(nodes))
            else:
                self.central_node = self.node_id
        
        # State
        self.is_central = (self.node_id == self.central_node)
        
        # Metrics
        self.metrics = {
            "operations_processed": 0,
            "operations_forwarded": 0,
            "operations_received": 0
        }
        
        self.logger.info(f"Initialized CentralizedDistribution with central node: {self.central_node}")
    
    async def start(self):
        """Start the distribution strategy"""
        self.logger.info(f"Starting CentralizedDistribution for node {self.node_id}")
        
        # Register handlers
        self.network_manager.register_handlers({
            "route_operation": self.handle_route_operation
        })
    
    async def stop(self):
        """Stop the distribution strategy"""
        self.logger.info(f"Stopping CentralizedDistribution for node {self.node_id}")
    
    async def route_key(self, key: str) -> str:
        """
        Determine which node should handle a key
        
        Args:
            key: The key to route
            
        Returns:
            Node ID that should handle the key (always central node)
        """
        # In centralized distribution, all keys go to the central node
        return self.central_node
    
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
        self.metrics["operations_processed"] += 1
        
        # If we're the central node, process locally
        if self.is_central:
            return await self._process_local_operation(operation)
        
        # Otherwise, forward to central node
        self.metrics["operations_forwarded"] += 1
        return await self._forward_operation(operation)
    
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
        
        self.logger.debug(f"Received operation from {source_node}")
        self.metrics["operations_received"] += 1
        
        # Only central node should receive routed operations
        if not self.is_central:
            return {
                "success": False,
                "error": "not_central_node",
                "central_node": self.central_node
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
        
        if not key:
            return {
                "success": False,
                "error": "missing_key"
            }
        
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
    
    async def _forward_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Forward an operation to the central node
        
        Args:
            operation: Operation to forward
                
        Returns:
            Operation result from central node
        """
        payload = {
            "operation": operation,
            "source_node": self.node_id
        }
        
        try:
            result = await self.network_manager.send_rpc(
                self.central_node, 
                "route_operation", 
                payload
            )
            
            return result or {
                "success": False,
                "error": "no_response_from_central_node"
            }
            
        except Exception as e:
            self.logger.error(f"Error forwarding operation to central node: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get distribution metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["is_central"] = self.is_central
        metrics["central_node"] = self.central_node
        
        return metrics
