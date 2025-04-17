"""
src/exapyte/services/client.py
==============================

Client service for interacting with the exapyte distributed data platform.
Provides a simple, high-level interface to perform operations against the platform.
"""

import asyncio
import logging
import time
import uuid
import json
from typing import Dict, Any, List, Optional, Set, Tuple, Union
from enum import Enum

class ConsistencyLevel(Enum):
    """Consistency levels supported by the client"""
    EVENTUAL = "eventual"
    CONSISTENT_PREFIX = "consistent_prefix"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    STRONG = "strong"


class ExabyteClient:
    """
    Client interface for the exapyte distributed platform
    
    Provides a high-level API for:
    - Key-value operations (get, set, delete)
    - Range queries and scans
    - Cluster management
    - Monitoring and metrics
    """
    
    def __init__(self, config: Dict[str, Any] = None, control_plane=None, data_plane=None):
        """
        Initialize the client
        
        Args:
            config: Client configuration
            control_plane: Control plane instance (optional, for direct access)
            data_plane: Data plane instance (optional, for direct access)
        """
        self.config = config or {}
        self.control_plane = control_plane
        self.data_plane = data_plane
        
        # Set up logging
        client_id = self.config.get("client_id", str(uuid.uuid4())[:8])
        self.logger = logging.getLogger(f"services.client.{client_id}")
        
        # Configuration
        self.default_keyspace = self.config.get("default_keyspace", "default")
        self.default_consistency = self.config.get("default_consistency", "eventual")
        self.request_timeout = self.config.get("request_timeout", 10.0)
        self.auto_reconnect = self.config.get("auto_reconnect", True)
        self.max_retries = self.config.get("max_retries", 3)
        self.retry_delay = self.config.get("retry_delay", 1.0)
        
        # Connection state
        self.endpoints = self.config.get("endpoints", [])
        self.preferred_region = self.config.get("preferred_region")
        self.session_id = str(uuid.uuid4())
        self.connected = False
        self.current_endpoint = None
        self.network_client = None
        
        # Session state
        self.session_keys = set()  # Keys modified in this session
        
        # Metrics
        self.metrics = {
            "requests": 0,
            "successes": 0,
            "failures": 0,
            "retries": 0,
            "connection_failures": 0,
            "reconnections": 0
        }
        
        self.logger.info(f"Initialized ExabyteClient with config: {self.config}")
    
    async def connect(self) -> bool:
        """
        Connect to the exapyte cluster
        
        Returns:
            True if connected successfully
        """
        if self.connected:
            return True
        
        self.logger.info("Connecting to exapyte cluster")
        
        # If we have direct access to control/data planes, use them
        if self.control_plane or self.data_plane:
            self.connected = True
            return True
        
        # Otherwise, use endpoints from configuration
        if not self.endpoints:
            self.logger.error("No endpoints configured")
            return False
        
        # Initialize network client
        self.network_client = self._create_network_client()
        
        # Try each endpoint until one works
        for endpoint in self.endpoints:
            try:
                self.logger.debug(f"Trying endpoint: {endpoint}")
                self.current_endpoint = endpoint
                
                # Test connection by fetching cluster info
                result = await self._request(
                    "cluster_info", 
                    {}, 
                    timeout=5.0
                )
                
                if result and result.get("success", False):
                    self.connected = True
                    self.logger.info(f"Connected to exapyte cluster at {endpoint}")
                    return True
                    
            except Exception as e:
                self.logger.warning(f"Failed to connect to endpoint {endpoint}: {e}")
                self.metrics["connection_failures"] += 1
        
        self.logger.error("Failed to connect to any endpoint")
        return False
    
    async def disconnect(self) -> None:
        """Disconnect from the cluster"""
        if not self.connected:
            return
        
        self.logger.info("Disconnecting from exapyte cluster")
        
        # Close network client if needed
        if self.network_client:
            await self.network_client.close()
            self.network_client = None
        
        self.connected = False
        self.current_endpoint = None
    
    async def get(self, key: str, keyspace: str = None, 
                 consistency: Union[ConsistencyLevel, str] = None) -> Dict[str, Any]:
        """
        Get a value by key
        
        Args:
            key: Key to retrieve
            keyspace: Keyspace to use (defaults to client default)
            consistency: Consistency level (defaults to client default)
            
        Returns:
            Dict with operation result
                - success: True if operation succeeded
                - value: The value if found
                - exists: True if key exists
        """
        keyspace = keyspace or self.default_keyspace
        consistency = consistency or self.default_consistency
        
        # Convert enum to string if needed
        if isinstance(consistency, ConsistencyLevel):
            consistency = consistency.value
        
        # Build operation
        operation = {
            "type": "get",
            "key": key,
            "keyspace": keyspace,
            "consistency": consistency,
            "session_id": self.session_id
        }
        
        # Perform operation
        result = await self._perform_operation(operation)
        
        # Update session state
        if result.get("success", False) and result.get("exists", False):
            # Add to session keys if consistency includes session
            if consistency == "session":
                self.session_keys.add(f"{keyspace}:{key}")
        
        return result
    
    async def set(self, key: str, value: Any, keyspace: str = None,
                 consistency: Union[ConsistencyLevel, str] = None,
                 ttl: Optional[int] = None) -> Dict[str, Any]:
        """
        Set a value by key
        
        Args:
            key: Key to set
            value: Value to store
            keyspace: Keyspace to use (defaults to client default)
            consistency: Consistency level (defaults to client default)
            ttl: Time-to-live in seconds (optional)
            
        Returns:
            Dict with operation result
                - success: True if operation succeeded
                - version: Version identifier of the stored value
        """
        keyspace = keyspace or self.default_keyspace
        consistency = consistency or self.default_consistency
        
        # Convert enum to string if needed
        if isinstance(consistency, ConsistencyLevel):
            consistency = consistency.value
        
        # Build operation
        operation = {
            "type": "set",
            "key": key,
            "value": value,
            "keyspace": keyspace,
            "consistency": consistency,
            "session_id": self.session_id
        }
        
        if ttl is not None:
            operation["ttl"] = ttl
        
        # Perform operation
        result = await self._perform_operation(operation)
        
        # Update session state
        if result.get("success", False):
            # Add to session keys
            self.session_keys.add(f"{keyspace}:{key}")
        
        return result
    
    async def delete(self, key: str, keyspace: str = None,
                    consistency: Union[ConsistencyLevel, str] = None) -> Dict[str, Any]:
        """
        Delete a value by key
        
        Args:
            key: Key to delete
            keyspace: Keyspace to use (defaults to client default)
            consistency: Consistency level (defaults to client default)
            
        Returns:
            Dict with operation result
                - success: True if operation succeeded
        """
        keyspace = keyspace or self.default_keyspace
        consistency = consistency or self.default_consistency
        
        # Convert enum to string if needed
        if isinstance(consistency, ConsistencyLevel):
            consistency = consistency.value
        
        # Build operation
        operation = {
            "type": "delete",
            "key": key,
            "keyspace": keyspace,
            "consistency": consistency,
            "session_id": self.session_id
        }
        
        # Perform operation
        result = await self._perform_operation(operation)
        
        # Update session state
        if result.get("success", False):
            # Remove from session keys
            session_key = f"{keyspace}:{key}"
            if session_key in self.session_keys:
                self.session_keys.remove(session_key)
        
        return result
    
    async def scan(self, prefix: str, limit: int = 100, keyspace: str = None,
                  consistency: Union[ConsistencyLevel, str] = None) -> Dict[str, Any]:
        """
        Scan keys by prefix
        
        Args:
            prefix: Key prefix to scan
            limit: Maximum number of results
            keyspace: Keyspace to use (defaults to client default)
            consistency: Consistency level (defaults to client default)
            
        Returns:
            Dict with operation result
                - success: True if operation succeeded
                - results: Dict of key-value pairs matching the scan
                - count: Number of results returned
        """
        keyspace = keyspace or self.default_keyspace
        consistency = consistency or self.default_consistency
        
        # Convert enum to string if needed
        if isinstance(consistency, ConsistencyLevel):
            consistency = consistency.value
        
        # Build operation
        operation = {
            "type": "scan",
            "prefix": prefix,
            "limit": limit,
            "keyspace": keyspace,
            "consistency": consistency,
            "session_id": self.session_id
        }
        
        # Perform operation
        return await self._perform_operation(operation)
    
    async def query(self, query: Dict[str, Any], keyspace: str = None,
                   consistency: Union[ConsistencyLevel, str] = None) -> Dict[str, Any]:
        """
        Perform a query operation
        
        Args:
            query: Query parameters
            keyspace: Keyspace to use (defaults to client default)
            consistency: Consistency level (defaults to client default)
            
        Returns:
            Dict with operation result
                - success: True if operation succeeded
                - results: Dict of key-value pairs matching the query
                - count: Number of results returned
        """
        keyspace = keyspace or self.default_keyspace
        consistency = consistency or self.default_consistency
        
        # Convert enum to string if needed
        if isinstance(consistency, ConsistencyLevel):
            consistency = consistency.value
        
        # Build operation
        operation = {
            "type": "query",
            "query": query,
            "keyspace": keyspace,
            "consistency": consistency,
            "session_id": self.session_id
        }
        
        # Perform operation
        return await self._perform_operation(operation)
    
    async def batch(self, operations: List[Dict[str, Any]], 
                   atomic: bool = False) -> List[Dict[str, Any]]:
        """
        Perform a batch of operations
        
        Args:
            operations: List of operations to perform
            atomic: If True, all operations succeed or fail together
            
        Returns:
            List of operation results
        """
        if not operations:
            return []
        
        # Prepare batch request
        batch_request = {
            "operations": operations,
            "atomic": atomic,
            "session_id": self.session_id
        }
        
        # Perform batch operation
        result = await self._request("batch", batch_request)
        
        if not result or not result.get("success", False):
            # Handle batch failure
            error = result.get("error") if result else "Unknown error"
            self.logger.error(f"Batch operation failed: {error}")
            
            # Return failure result for each operation
            return [{"success": False, "error": error} for _ in operations]
        
        # Return individual operation results
        return result.get("results", [])
    
    async def get_cluster_info(self) -> Dict[str, Any]:
        """
        Get information about the cluster
        
        Returns:
            Dict with cluster information
        """
        return await self._request("cluster_info", {})
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get metrics for the cluster and client
        
        Returns:
            Dict with metrics
        """
        # Get cluster metrics
        cluster_metrics = await self._request("metrics", {})
        
        # Combine with client metrics
        combined_metrics = {
            "client": dict(self.metrics),
            "cluster": cluster_metrics.get("metrics", {}) if cluster_metrics else {}
        }
        
        return combined_metrics
    
    async def create_keyspace(self, keyspace: str, config: Dict[str, Any]) -> Dict[str, Any]:
        """
        Create a new keyspace
        
        Args:
            keyspace: Name of the keyspace
            config: Keyspace configuration
            
        Returns:
            Dict with operation result
        """
        # Build request
        request = {
            "keyspace": keyspace,
            "config": config
        }
        
        # Perform operation
        return await self._request("create_keyspace", request)
    
    async def _perform_operation(self, operation: Dict[str, Any]) -> Dict[str, Any]:
        """
        Perform a data operation with retries
        
        Args:
            operation: Operation details
            
        Returns:
            Operation result
        """
        # Ensure connection
        if not self.connected and not await self.connect():
            return {
                "success": False,
                "error": "Not connected to cluster"
            }
        
        # Update metrics
        self.metrics["requests"] += 1
        
        # If we have direct access to data plane, use it
        if self.data_plane:
            try:
                result = await self.data_plane.process_operation(operation)
                
                if result.get("success", False):
                    self.metrics["successes"] += 1
                else:
                    self.metrics["failures"] += 1
                
                return result
            except Exception as e:
                self.logger.error(f"Error performing operation via data plane: {e}")
                self.metrics["failures"] += 1
                
                return {
                    "success": False,
                    "error": str(e)
                }
        
        # Otherwise, use network client with retries
        retries = 0
        last_error = None
        
        while retries <= self.max_retries:
            try:
                # Send request
                result = await self._request("data_operation", operation)
                
                # Check result
                if result and result.get("success", False):
                    self.metrics["successes"] += 1
                    return result
                
                # If redirected to another node, follow the redirect
                if result and "redirect_node" in result:
                    redirect_node = result["redirect_node"]
                    self.logger.debug(f"Following redirect to node {redirect_node}")
                    
                    # Update operation with redirection info
                    operation["redirect_from"] = self.current_endpoint
                    
                    # Try to connect to redirect node
                    if await self._connect_to_node(redirect_node):
                        # Retry immediately with new endpoint
                        continue
                
                # Operation failed
                error = result.get("error") if result else "Unknown error"
                self.logger.warning(f"Operation failed: {error}")
                self.metrics["failures"] += 1
                
                # Some errors shouldn't be retried
                if result and not result.get("retryable", True):
                    return result
                
                # Otherwise, proceed with retry
                last_error = error
                
            except Exception as e:
                self.logger.warning(f"Error performing operation: {e}")
                last_error = str(e)
                
                # Check if we should reconnect
                if self.auto_reconnect:
                    if await self._reconnect():
                        # Successfully reconnected, retry immediately
                        continue
            
            # Increment retry counter
            retries += 1
            
            if retries <= self.max_retries:
                # Apply backoff delay before retry
                delay = self.retry_delay * (2 ** (retries - 1))  # Exponential backoff
                self.logger.debug(f"Retrying operation after {delay:.2f}s (attempt {retries}/{self.max_retries})")
                self.metrics["retries"] += 1
                
                await asyncio.sleep(delay)
        
        # All retries failed
        self.metrics["failures"] += 1
        
        return {
            "success": False,
            "error": f"Operation failed after {retries} retries: {last_error}"
        }
    
    async def _request(self, method: str, params: Dict[str, Any], 
                      timeout: Optional[float] = None) -> Dict[str, Any]:
        """
        Send a request to the cluster
        
        Args:
            method: Method name
            params: Method parameters
            timeout: Request timeout in seconds (None for default)
            
        Returns:
            Response data
        """
        if not self.connected and method != "cluster_info":
            raise RuntimeError("Not connected to cluster")
        
        # Use specified timeout or default
        timeout = timeout or self.request_timeout
        
        # If we have direct access to control/data planes, use them
        if method == "data_operation" and self.data_plane:
            return await self.data_plane.process_operation(params)
        elif method in ["cluster_info", "create_keyspace"] and self.control_plane:
            # Map method to control plane operation
            if method == "cluster_info":
                return await self.control_plane.get_topology_info()
            elif method == "create_keyspace":
                # Map to control plane operation
                from exapyte.services.control_plane import OperationType
                return await self.control_plane.perform_operation(
                    OperationType.CONFIGURE_KEYSPACE, 
                    params
                )
        
        # Otherwise, use network client
        if not self.network_client:
            raise RuntimeError("Network client not initialized")
        
        # Prepare request
        request = {
            "method": method,
            "params": params,
            "id": str(uuid.uuid4()),
            "client_id": self.session_id,
            "timestamp": time.time()
        }
        
        # Send request
        return await self.network_client.send_request(
            self.current_endpoint, 
            request, 
            timeout
        )
    
    async def _reconnect(self) -> bool:
        """
        Attempt to reconnect to the cluster
        
        Returns:
            True if reconnected successfully
        """
        self.logger.info("Attempting to reconnect")
        
        # Disconnect first
        await self.disconnect()
        
        # Try to connect again
        result = await self.connect()
        
        if result:
            self.metrics["reconnections"] += 1
            self.logger.info("Reconnected successfully")
        else:
            self.logger.warning("Reconnection failed")
        
        return result
    
    async def _connect_to_node(self, node_id: str) -> bool:
        """
        Connect to a specific node
        
        Args:
            node_id: ID of the node to connect to
            
        Returns:
            True if connected successfully
        """
        # This would require knowledge of the node's endpoint
        # In a real implementation, would look up the endpoint in the topology
        self.logger.warning(f"Direct connection to node {node_id} not implemented")
        return False
    
    def _create_network_client(self) -> Any:
        """
        Create a network client for communication with the cluster
        
        Returns:
            Network client instance
        """
        # In a real implementation, would create real HTTP or gRPC client
        # For demonstration, return a simulated client
        return SimulatedNetworkClient()


class SimulatedNetworkClient:
    """Simulated network client for demonstration"""
    
    async def send_request(self, endpoint: str, request: Dict[str, Any], 
                          timeout: float) -> Dict[str, Any]:
        """
        Send a request to an endpoint
        
        Args:
            endpoint: Target endpoint
            request: Request data
            timeout: Request timeout in seconds
            
        Returns:
            Response data
        """
        # Simulate network delay
        await asyncio.sleep(0.05)
        
        # Simulate basic responses
        method = request.get("method")
        params = request.get("params", {})
        
        if method == "cluster_info":
            return {
                "success": True,
                "cluster_name": "exapyte-demo",
                "version": "1.0.0",
                "nodes": 3,
                "regions": 1
            }
        
        elif method == "data_operation":
            op_type = params.get("type")
            key = params.get("key")
            
            if op_type == "get":
                # Simulate get response
                return {
                    "success": True,
                    "key": key,
                    "value": f"simulated-value-for-{key}",
                    "exists": True
                }
            
            elif op_type == "set":
                # Simulate set response
                return {
                    "success": True,
                    "key": key,
                    "version": str(uuid.uuid4())
                }
            
            elif op_type == "delete":
                # Simulate delete response
                return {
                    "success": True,
                    "key": key
                }
            
            elif op_type == "scan":
                # Simulate scan response
                prefix = params.get("prefix", "")
                limit = params.get("limit", 10)
                
                results = {}
                for i in range(min(5, limit)):
                    key = f"{prefix}{i}"
                    results[key] = f"simulated-value-for-{key}"
                
                return {
                    "success": True,
                    "prefix": prefix,
                    "results": results,
                    "count": len(results)
                }
        
        elif method == "metrics":
            # Simulate metrics response
            return {
                "success": True,
                "metrics": {
                    "operations": {
                        "gets": 1000,
                        "sets": 500,
                        "deletes": 100
                    },
                    "performance": {
                        "p99_latency_ms": 15,
                        "p95_latency_ms": 8,
                        "p50_latency_ms": 3
                    }
                }
            }
        
        # Default response for unhandled methods
        return {
            "success": False,
            "error": f"Unhandled method: {method}"
        }
    
    async def close(self) -> None:
        """Close the client connection"""
        pass


async def create_client(config: Dict[str, Any] = None) -> ExabyteClient:
    """
    Factory function to create and initialize an ExabyteClient
    
    Args:
        config: Client configuration
        
    Returns:
        Initialized ExabyteClient instance
    """
    config = config or {}
    
    # Create client
    client = ExabyteClient(config)
    
    # Connect to cluster
    await client.connect()
    
    return client