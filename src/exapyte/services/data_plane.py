"""
src/exapyte/services/data_plane.py
==================================

Regional data plane service for exapyte.
Handles request routing, processing, and storage management.
"""

import asyncio
import logging
import time
import json
import uuid
from typing import Dict, Any, List, Optional, Set, Tuple
from enum import Enum


class OperationType(Enum):
    """Types of operations that can be performed by the data plane"""
    GET = "get"
    SET = "set"
    DELETE = "delete"
    SCAN = "scan"
    QUERY = "query"


class ConsistencyLevel(Enum):
    """Consistency levels supported by the system"""
    EVENTUAL = "eventual"
    CONSISTENT_PREFIX = "consistent_prefix"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    STRONG = "strong"


class DataPlane:
    """
    Regional data plane for the distributed system
    
    Responsibilities:
    - Request routing
    - Query processing
    - Storage management
    - Replication handling
    """
    
    def __init__(self, node_id: str, region_id: str, config: Dict[str, Any] = None,
                storage_engine: Any = None, replication_manager: Any = None, 
                distribution_strategy: Any = None, network_manager: Any = None):
        """
        Initialize the data plane
        
        Args:
            node_id: ID of this node
            region_id: ID of this region
            config: Configuration parameters
            storage_engine: Storage engine instance
            replication_manager: Replication manager instance
            distribution_strategy: Distribution strategy instance
            network_manager: Network manager for communication
        """
        self.node_id = node_id
        self.region_id = region_id
        self.config = config or {}
        self.storage_engine = storage_engine
        self.replication_manager = replication_manager
        self.distribution_strategy = distribution_strategy
        self.network_manager = network_manager
        
        # Set up logging
        self.logger = logging.getLogger(f"services.data_plane.{node_id}")
        
        # Configuration
        self.default_consistency = self.config.get("default_consistency", "eventual")
        self.request_timeout = self.config.get("request_timeout", 5.0)
        self.enable_caching = self.config.get("enable_caching", True)
        
        # Cache (simple in-memory cache for demonstration)
        self.cache = {}
        self.cache_ttl = self.config.get("cache_ttl", 60)  # seconds
        self.cache_expiry = {}  # key -> expiry timestamp
        
        # Operation tracking
        self.pending_operations = {}
        self.operation_counter = 0
        
        # Background tasks
        self.cache_cleanup_task = None
        self.running = False
        
        # Metrics
        self.metrics = {
            "gets": 0,
            "sets": 0,
            "deletes": 0,
            "scans": 0,
            "queries": 0,
            "cache_hits": 0,
            "cache_misses": 0,
            "forwarded_requests": 0,
            "local_requests": 0
        }
        
        self.logger.info(f"Initialized DataPlane with config: {self.config}")
    
    async def start(self):
        """Start the data plane service"""
        self.logger.info(f"Starting DataPlane on node {self.node_id}")
        self.running = True
        
        # Register handlers
        if self.network_manager:
            self.network_manager.register_handlers({
                "data_operation": self.handle_data_operation,
                "replicate_data": self.handle_replicate_data,
                "invalidate_cache": self.handle_invalidate_cache
            })
        
        # Start background tasks
        if self.enable_caching:
            self.cache_cleanup_task = asyncio.create_task(self._cache_cleanup_loop())
    
    async def stop(self):
        """Stop the data plane service"""
        self.logger.info(f"Stopping DataPlane on node {self.node_id}")
        self.running = False
        
        # Cancel background tasks
        if self.cache_cleanup_task:
            self.cache_cleanup_task.cancel()
            try:
                await self.cache_cleanup_task
            except asyncio.CancelledError:
                pass
    
    async def process_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Process a data operation
        
        Args:
            operation: Operation details
                - type: Operation type
                - key: Key to operate on
                - value: Value for set operations
                - consistency: Desired consistency level
                - keyspace: Keyspace to use
                
        Returns:
            Operation result
        """
        op_type_str = operation.get("type")
        key = operation.get("key")
        keyspace = operation.get("keyspace", "default")
        consistency_str = operation.get("consistency", self.default_consistency)
        
        self.logger.debug(f"Processing operation: {op_type_str} on key {key}")
        
        try:
            # Convert strings to enums
            op_type = OperationType(op_type_str)
            consistency = ConsistencyLevel(consistency_str)
            
            # Update metrics
            if op_type == OperationType.GET:
                self.metrics["gets"] += 1
            elif op_type == OperationType.SET:
                self.metrics["sets"] += 1
            elif op_type == OperationType.DELETE:
                self.metrics["deletes"] += 1
            elif op_type == OperationType.SCAN:
                self.metrics["scans"] += 1
            elif op_type == OperationType.QUERY:
                self.metrics["queries"] += 1
            
            # Check if we need to route this operation to another node
            if self.distribution_strategy and key:
                target_node = await self.distribution_strategy.route_key(key)
                
                if target_node != self.node_id:
                    # Forward to correct node
                    self.logger.debug(f"Forwarding operation to node {target_node}")
                    self.metrics["forwarded_requests"] += 1
                    return await self._forward_operation(target_node, operation)
            
            # Process locally
            self.metrics["local_requests"] += 1
            
            if op_type == OperationType.GET:
                return await self._handle_get(key, keyspace, consistency)
            elif op_type == OperationType.SET:
                value = operation.get("value")
                return await self._handle_set(key, value, keyspace, consistency)
            elif op_type == OperationType.DELETE:
                return await self._handle_delete(key, keyspace, consistency)
            elif op_type == OperationType.SCAN:
                prefix = operation.get("prefix")
                limit = operation.get("limit", 100)
                return await self._handle_scan(prefix, limit, keyspace, consistency)
            elif op_type == OperationType.QUERY:
                query = operation.get("query", {})
                return await self._handle_query(query, keyspace, consistency)
            else:
                return {
                    "success": False,
                    "error": f"Unsupported operation type: {op_type_str}"
                }
                
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid operation type or consistency level"
            }
        except Exception as e:
            self.logger.error(f"Error processing operation: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def handle_data_operation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a data operation request from another node
        
        Args:
            payload: Operation details
                - operation: The operation to perform
                - source_node: ID of the source node
                - operation_id: Unique operation ID
                
        Returns:
            Operation result
        """
        operation = payload.get("operation", {})
        source_node = payload.get("source_node")
        operation_id = payload.get("operation_id")
        
        try:
            # Process the operation
            result = await self.process_operation(operation)
            
            # Add tracking info
            result["operation_id"] = operation_id
            result["processed_by"] = self.node_id
            
            return result
            
        except Exception as e:
            self.logger.error(f"Error handling data operation: {e}")
            return {
                "success": False,
                "error": str(e),
                "operation_id": operation_id
            }
    
    async def handle_replicate_data(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle data replication from another node
        
        Args:
            payload: Replication details
                - keyspace: Keyspace name
                - data: Dict of key-value pairs
                - source_node: ID of the source node
                - timestamp: Replication timestamp
                
        Returns:
            Replication result
        """
        keyspace = payload.get("keyspace", "default")
        data = payload.get("data", {})
        source_node = payload.get("source_node")
        timestamp = payload.get("timestamp", time.time())
        
        self.logger.debug(f"Handling replication from {source_node} with {len(data)} keys")
        
        try:
            # Apply the replicated data
            success_count = 0
            failed_keys = []
            
            for key, value_info in data.items():
                try:
                    value = value_info.get("value")
                    
                    # Store the value (bypassing normal set operation flow)
                    if self.storage_engine:
                        await self.storage_engine.set(key, value)
                        
                        # Invalidate cache if present
                        self._invalidate_cache_key(key, keyspace)
                        
                        success_count += 1
                except Exception as e:
                    self.logger.error(f"Error replicating key {key}: {e}")
                    failed_keys.append(key)
            
            return {
                "success": len(failed_keys) == 0,
                "processed_keys": success_count,
                "failed_keys": failed_keys,
                "timestamp": time.time()
            }
                
        except Exception as e:
            self.logger.error(f"Error handling data replication: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def handle_invalidate_cache(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle cache invalidation request
        
        Args:
            payload: Invalidation details
                - keyspace: Keyspace name
                - keys: List of keys to invalidate
                - source_node: ID of the source node
                
        Returns:
            Invalidation result
        """
        keyspace = payload.get("keyspace", "default")
        keys = payload.get("keys", [])
        source_node = payload.get("source_node")
        
        self.logger.debug(f"Handling cache invalidation from {source_node} for {len(keys)} keys")
        
        invalidated_count = 0
        
        for key in keys:
            # Invalidate in local cache
            if self._invalidate_cache_key(key, keyspace):
                invalidated_count += 1
        
        return {
            "success": True,
            "invalidated_keys": invalidated_count
        }
    
    async def _handle_get(self, key: str, keyspace: str, 
                         consistency: ConsistencyLevel) -> Dict[str, Any]:
        """
        Handle a get operation
        
        Args:
            key: Key to get
            keyspace: Keyspace to use
            consistency: Desired consistency level
            
        Returns:
            Get result
        """
        # Check cache first (for non-strong consistency)
        if self.enable_caching and consistency != ConsistencyLevel.STRONG:
            cache_key = f"{keyspace}:{key}"
            if cache_key in self.cache and time.time() < self.cache_expiry.get(cache_key, 0):
                self.metrics["cache_hits"] += 1
                return {
                    "success": True,
                    "key": key,
                    "value": self.cache[cache_key],
                    "keyspace": keyspace,
                    "from_cache": True
                }
            
            self.metrics["cache_misses"] += 1
        
        # Get from storage
        if not self.storage_engine:
            return {
                "success": False,
                "error": "Storage engine not available"
            }
        
        value = await self.storage_engine.get(key)
        
        # For strong consistency, ensure we have latest value
        if consistency == ConsistencyLevel.STRONG and self.replication_manager:
            # Fetch latest value from other nodes
            updated_value = await self._ensure_strong_consistency(key, keyspace)
            if updated_value is not None:
                value = updated_value
        
        # Update cache
        if self.enable_caching and value is not None:
            cache_key = f"{keyspace}:{key}"
            self.cache[cache_key] = value
            self.cache_expiry[cache_key] = time.time() + self.cache_ttl
        
        return {
            "success": True,
            "key": key,
            "value": value,
            "exists": value is not None,
            "keyspace": keyspace,
            "from_cache": False
        }
    
    async def _handle_set(self, key: str, value: Any, keyspace: str, 
                         consistency: ConsistencyLevel) -> Dict[str, Any]:
        """
        Handle a set operation
        
        Args:
            key: Key to set
            value: Value to set
            keyspace: Keyspace to use
            consistency: Desired consistency level
            
        Returns:
            Set result
        """
        if not self.storage_engine:
            return {
                "success": False,
                "error": "Storage engine not available"
            }
        
        # Add metadata to value
        metadata = {
            "timestamp": time.time(),
            "node_id": self.node_id,
            "region_id": self.region_id,
            "version": str(uuid.uuid4())
        }
        
        # Store value with metadata
        value_with_metadata = {
            "value": value,
            "metadata": metadata
        }
        
        # Set in storage
        result = await self.storage_engine.set(key, value_with_metadata)
        
        # Update cache
        if self.enable_caching:
            cache_key = f"{keyspace}:{key}"
            self.cache[cache_key] = value_with_metadata
            self.cache_expiry[cache_key] = time.time() + self.cache_ttl
        
        # Schedule replication
        if self.replication_manager:
            if consistency == ConsistencyLevel.STRONG:
                # Synchronous replication
                replication_result = await self.replication_manager.replicate_synchronously(
                    keyspace, key, value_with_metadata
                )
                
                # If replication failed, the operation fails
                if not replication_result.get("success", False):
                    return {
                        "success": False,
                        "error": "Replication failed",
                        "replication_errors": replication_result.get("errors", {})
                    }
            else:
                # Asynchronous replication
                await self.replication_manager.replicate_key(key)
                
                # Invalidate cache in other nodes
                asyncio.create_task(self._broadcast_cache_invalidation(keyspace, [key]))
        
        return {
            "success": result,
            "key": key,
            "keyspace": keyspace,
            "version": metadata["version"]
        }
    
    async def _handle_delete(self, key: str, keyspace: str, 
                           consistency: ConsistencyLevel) -> Dict[str, Any]:
        """
        Handle a delete operation
        
        Args:
            key: Key to delete
            keyspace: Keyspace to use
            consistency: Desired consistency level
            
        Returns:
            Delete result
        """
        if not self.storage_engine:
            return {
                "success": False,
                "error": "Storage engine not available"
            }
        
        # Delete from storage
        result = await self.storage_engine.delete(key)
        
        # Invalidate cache
        if self.enable_caching:
            self._invalidate_cache_key(key, keyspace)
        
        # Schedule replication if successful
        if result and self.replication_manager:
            # Create a tombstone marker
            tombstone = {
                "deleted": True,
                "metadata": {
                    "timestamp": time.time(),
                    "node_id": self.node_id,
                    "region_id": self.region_id,
                    "version": str(uuid.uuid4())
                }
            }
            
            if consistency == ConsistencyLevel.STRONG:
                # Synchronous replication
                replication_result = await self.replication_manager.replicate_synchronously(
                    keyspace, key, tombstone
                )
                
                # If replication failed, the operation still succeeds locally
                # but we report the replication status
                if not replication_result.get("success", False):
                    result = {
                        "success": True,
                        "key": key,
                        "keyspace": keyspace,
                        "local_success": True,
                        "replication_failed": True,
                        "replication_errors": replication_result.get("errors", {})
                    }
            else:
                # Asynchronous replication with tombstone
                await self.replication_manager.replicate_key(key)
                
                # Invalidate cache in other nodes
                asyncio.create_task(self._broadcast_cache_invalidation(keyspace, [key]))
        
        return {
            "success": result,
            "key": key,
            "keyspace": keyspace
        }
    
    async def _handle_scan(self, prefix: str, limit: int, keyspace: str, 
                          consistency: ConsistencyLevel) -> Dict[str, Any]:
        """
        Handle a scan operation (prefix search)
        
        Args:
            prefix: Key prefix to scan
            limit: Maximum number of results
            keyspace: Keyspace to use
            consistency: Desired consistency level
            
        Returns:
            Scan result
        """
        if not self.storage_engine:
            return {
                "success": False,
                "error": "Storage engine not available"
            }
        
        # For strong consistency, scan may need to query all nodes
        # For simplicity, we'll just scan local storage
        
        # In a real implementation, would use storage engine's scan capability
        # For demonstration, we'll simulate scanning
        results = {}
        count = 0
        
        # Simulated scan - in a real implementation would use storage engine's scan API
        # Would scan keys in lexicographical order starting with prefix
        simulated_keys = [
            f"{prefix}1", f"{prefix}2", f"{prefix}3", 
            f"{prefix}4", f"{prefix}5"
        ]
        
        for key in simulated_keys:
            if count >= limit:
                break
                
            value = await self.storage_engine.get(key)
            if value is not None:
                results[key] = value
                count += 1
        
        return {
            "success": True,
            "prefix": prefix,
            "results": results,
            "count": count,
            "keyspace": keyspace
        }
    
    async def _handle_query(self, query: Dict[str, Any], keyspace: str, 
                          consistency: ConsistencyLevel) -> Dict[str, Any]:
        """
        Handle a query operation
        
        Args:
            query: Query parameters
            keyspace: Keyspace to use
            consistency: Desired consistency level
            
        Returns:
            Query result
        """
        # In a real implementation, would support rich querying capabilities
        # For demonstration, we'll support a simple equality query
        
        if not self.storage_engine:
            return {
                "success": False,
                "error": "Storage engine not available"
            }
        
        field = query.get("field")
        value = query.get("value")
        limit = query.get("limit", 100)
        
        if not field or value is None:
            return {
                "success": False,
                "error": "Invalid query: missing field or value"
            }
        
        # For demonstration, simulate matching against data
        # In a real implementation, would use storage engine's query capability
        # would need a secondary index for this kind of query
        
        results = {}
        count = 0
        
        # Simulated query results
        # In a real implementation, would scan indexed values
        simulated_keys = ["key1", "key2", "key3", "key4", "key5"]
        
        for key in simulated_keys:
            if count >= limit:
                break
                
            data = await self.storage_engine.get(key)
            if data is not None:
                # Check if the field matches
                if (isinstance(data, dict) and 
                    data.get("value", {}).get(field) == value):
                    results[key] = data
                    count += 1
        
        return {
            "success": True,
            "query": query,
            "results": results,
            "count": count,
            "keyspace": keyspace
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
        if not self.network_manager:
            return {
                "success": False,
                "error": "Network manager not available"
            }
        
        try:
            # Generate operation ID
            operation_id = f"{self.node_id}_{int(time.time())}_{self.operation_counter}"
            self.operation_counter += 1
            
            # Prepare payload
            payload = {
                "operation": operation,
                "source_node": self.node_id,
                "operation_id": operation_id
            }
            
            # Send to target node
            result = await self.network_manager.send_rpc(
                target_node,
                "data_operation",
                payload,
                timeout=self.request_timeout
            )
            
            return result or {
                "success": False,
                "error": "No response from target node"
            }
            
        except Exception as e:
            self.logger.error(f"Error forwarding operation to node {target_node}: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _ensure_strong_consistency(self, key: str, keyspace: str) -> Optional[Any]:
        """
        Ensure strong consistency by querying all nodes for latest value
        
        Args:
            key: Key to check
            keyspace: Keyspace to use
            
        Returns:
            Latest value or None
        """
        if not self.replication_manager or not self.network_manager:
            return None
        
        try:
            # Get all nodes that might have this key
            # In a real implementation, would use knowledge about the topology
            # For demonstration, we'll query all known nodes
            
            # Query operation
            operation = {
                "type": "get",
                "key": key,
                "keyspace": keyspace,
                "consistency": "eventual"  # Use eventual to avoid circular strong reads
            }
            
            # Track responses
            values = {}
            latest_timestamp = 0
            latest_value = None
            
            # Send to all nodes in parallel
            # In a real implementation, would optimize this based on topology
            query_tasks = []
            
            # Get nodes from replication manager
            target_nodes = getattr(self.replication_manager, "replication_nodes", [])
            if not target_nodes:
                return None
            
            for node_id in target_nodes:
                if node_id != self.node_id:
                    task = self._forward_operation(node_id, operation)
                    query_tasks.append((node_id, task))
            
            # Wait for all queries to complete or timeout
            for node_id, task in query_tasks:
                try:
                    result = await asyncio.wait_for(task, timeout=self.request_timeout)
                    
                    if result and result.get("success", False) and result.get("exists", False):
                        value_with_metadata = result.get("value")
                        if isinstance(value_with_metadata, dict):
                            metadata = value_with_metadata.get("metadata", {})
                            timestamp = metadata.get("timestamp", 0)
                            
                            values[node_id] = {
                                "value": value_with_metadata,
                                "timestamp": timestamp
                            }
                            
                            if timestamp > latest_timestamp:
                                latest_timestamp = timestamp
                                latest_value = value_with_metadata
                                
                except asyncio.TimeoutError:
                    self.logger.warning(f"Timeout querying node {node_id} for strong consistency")
                except Exception as e:
                    self.logger.error(f"Error querying node {node_id} for strong consistency: {e}")
            
            # If we found a newer value, return it
            return latest_value
            
        except Exception as e:
            self.logger.error(f"Error ensuring strong consistency for key {key}: {e}")
            return None
    
    async def _broadcast_cache_invalidation(self, keyspace: str, keys: List[str]) -> None:
        """
        Broadcast cache invalidation to all nodes
        
        Args:
            keyspace: Keyspace name
            keys: List of keys to invalidate
        """
        if not self.network_manager or not self.replication_manager:
            return
        
        try:
            # Prepare payload
            payload = {
                "keyspace": keyspace,
                "keys": keys,
                "source_node": self.node_id,
                "timestamp": time.time()
            }
            
            # Send to all nodes
            target_nodes = getattr(self.replication_manager, "replication_nodes", [])
            
            for node_id in target_nodes:
                if node_id != self.node_id:
                    try:
                        await self.network_manager.send_rpc(
                            node_id,
                            "invalidate_cache",
                            payload,
                            timeout=1.0  # Short timeout for cache invalidation
                        )
                    except Exception as e:
                        self.logger.warning(f"Failed to invalidate cache on node {node_id}: {e}")
                        
        except Exception as e:
            self.logger.error(f"Error broadcasting cache invalidation: {e}")
    
    def _invalidate_cache_key(self, key: str, keyspace: str) -> bool:
        """
        Invalidate a key in the local cache
        
        Args:
            key: Key to invalidate
            keyspace: Keyspace name
            
        Returns:
            True if key was in cache and invalidated
        """
        if not self.enable_caching:
            return False
        
        cache_key = f"{keyspace}:{key}"
        
        if cache_key in self.cache:
            del self.cache[cache_key]
            
            if cache_key in self.cache_expiry:
                del self.cache_expiry[cache_key]
                
            return True
            
        return False
    
    async def _cache_cleanup_loop(self):
        """Background task for cleaning up expired cache entries"""
        if not self.enable_caching:
            return
        
        self.logger.info("Starting cache cleanup loop")
        
        while self.running:
            try:
                # Find expired entries
                now = time.time()
                expired_keys = []
                
                for cache_key, expiry in self.cache_expiry.items():
                    if now > expiry:
                        expired_keys.append(cache_key)
                
                # Remove expired entries
                for cache_key in expired_keys:
                    if cache_key in self.cache:
                        del self.cache[cache_key]
                    
                    if cache_key in self.cache_expiry:
                        del self.cache_expiry[cache_key]
                
                # Wait before next cleanup
                await asyncio.sleep(10.0)  # Check every 10 seconds
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in cache cleanup loop: {e}")
                await asyncio.sleep(30.0)  # Longer sleep on error
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get data plane metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        
        # Cache metrics
        if self.enable_caching:
            metrics["cache_size"] = len(self.cache)
            metrics["cache_hit_ratio"] = (
                metrics["cache_hits"] / (metrics["cache_hits"] + metrics["cache_misses"])
                if (metrics["cache_hits"] + metrics["cache_misses"]) > 0
                else 0
            )
        
        return metrics