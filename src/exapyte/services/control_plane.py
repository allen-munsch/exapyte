"""
src/exapyte/services/control_plane.py
====================================

Global control plane service for exapyte.
Manages metadata, topology, and orchestrates cross-region operations.
"""

import asyncio
import logging
import time
import json
import uuid
from typing import Dict, Any, List, Optional, Set, Tuple
from enum import Enum

class OperationType(Enum):
    """Types of operations that can be performed by the control plane"""
    REGISTER_NODE = "register_node"
    UNREGISTER_NODE = "unregister_node"
    UPDATE_TOPOLOGY = "update_topology"
    CONFIGURE_KEYSPACE = "configure_keyspace"
    INITIATE_REBALANCE = "initiate_rebalance"


class ConsistencyLevel(Enum):
    """Consistency levels supported by the system"""
    EVENTUAL = "eventual"
    CONSISTENT_PREFIX = "consistent_prefix"
    BOUNDED_STALENESS = "bounded_staleness"
    SESSION = "session"
    STRONG = "strong"


class ControlPlane:
    """
    Global control plane for the distributed system
    
    Responsibilities:
    - Metadata management
    - Topology configuration
    - Cross-region orchestration
    - Consensus protocol coordination
    """
    
    def __init__(self, node_id: str, config: Dict[str, Any] = None,
                consensus_node: Any = None, network_manager: Any = None):
        """
        Initialize the control plane
        
        Args:
            node_id: ID of this node
            config: Configuration parameters
            consensus_node: Consensus protocol node (for strong consistency)
            network_manager: Network manager for communication
        """
        self.node_id = node_id
        self.config = config or {}
        self.consensus_node = consensus_node
        self.network_manager = network_manager
        
        # Set up logging
        self.logger = logging.getLogger(f"services.control_plane.{node_id}")
        
        # Configuration
        self.is_leader = self.config.get("is_leader", False)
        self.leadership_check_interval = self.config.get("leadership_check_interval", 5.0)
        self.metadata_sync_interval = self.config.get("metadata_sync_interval", 30.0)
        
        # System state
        self.topology = {
            "regions": {},
            "nodes": {},
            "keyspaces": {}
        }
        
        # Leadership state
        self.current_leader = None
        self.leadership_term = 0
        self.last_heartbeat = 0
        
        # Operation tracking
        self.pending_operations = {}
        self.operation_counter = 0
        
        # Background tasks
        self.leadership_task = None
        self.metadata_sync_task = None
        self.running = False
        
        # Metrics
        self.metrics = {
            "operations_processed": 0,
            "metadata_syncs": 0,
            "leadership_changes": 0,
            "nodes_registered": 0,
            "nodes_unregistered": 0
        }
        
        self.logger.info(f"Initialized ControlPlane with config: {self.config}")
    
    async def start(self):
        """Start the control plane service"""
        self.logger.info(f"Starting ControlPlane on node {self.node_id}")
        self.running = True
        
        # Register handlers
        if self.network_manager:
            self.network_manager.register_handlers({
                "control_operation": self.handle_control_operation,
                "get_topology": self.handle_get_topology,
                "leadership_heartbeat": self.handle_leadership_heartbeat
            })
        
        # Start leadership management if using consensus
        if self.consensus_node:
            self.leadership_task = asyncio.create_task(self._leadership_loop())
        
        # Start metadata sync task
        self.metadata_sync_task = asyncio.create_task(self._metadata_sync_loop())
        
        # Load initial topology (from storage in real implementation)
        await self._load_initial_topology()
    
    async def stop(self):
        """Stop the control plane service"""
        self.logger.info(f"Stopping ControlPlane on node {self.node_id}")
        self.running = False
        
        # Cancel background tasks
        if self.leadership_task:
            self.leadership_task.cancel()
            try:
                await self.leadership_task
            except asyncio.CancelledError:
                pass
        
        if self.metadata_sync_task:
            self.metadata_sync_task.cancel()
            try:
                await self.metadata_sync_task
            except asyncio.CancelledError:
                pass
        
        # Save topology (to storage in real implementation)
        await self._save_topology()
    
    async def perform_operation(self, operation_type: OperationType, 
                              params: Dict[str, Any], 
                              consistency: ConsistencyLevel = ConsistencyLevel.STRONG) -> Dict[str, Any]:
        """
        Perform a control plane operation
        
        Args:
            operation_type: Type of operation to perform
            params: Operation parameters
            consistency: Desired consistency level
            
        Returns:
            Operation result
        """
        self.logger.info(f"Performing operation: {operation_type.value}")
        
        # For strong consistency, use consensus protocol
        if (consistency == ConsistencyLevel.STRONG and 
            self.consensus_node and not self.is_leader):
            
            # Forward to leader if not the leader
            return await self._forward_to_leader(operation_type, params)
        
        # Handle the operation
        if operation_type == OperationType.REGISTER_NODE:
            return await self._register_node(params)
        elif operation_type == OperationType.UNREGISTER_NODE:
            return await self._unregister_node(params)
        elif operation_type == OperationType.UPDATE_TOPOLOGY:
            return await self._update_topology(params)
        elif operation_type == OperationType.CONFIGURE_KEYSPACE:
            return await self._configure_keyspace(params)
        elif operation_type == OperationType.INITIATE_REBALANCE:
            return await self._initiate_rebalance(params)
        else:
            return {
                "success": False,
                "error": f"Unknown operation type: {operation_type.value}"
            }
    
    async def handle_control_operation(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle a control operation from another node
        
        Args:
            payload: Operation details
                - operation_type: Type of operation
                - params: Operation parameters
                - source_node: ID of the source node
                - operation_id: Unique operation ID
                
        Returns:
            Operation result
        """
        operation_type_str = payload.get("operation_type")
        params = payload.get("params", {})
        source_node = payload.get("source_node")
        operation_id = payload.get("operation_id")
        
        try:
            # Convert string to enum
            operation_type = OperationType(operation_type_str)
            
            # Perform the operation
            result = await self.perform_operation(
                operation_type, 
                params, 
                ConsistencyLevel.STRONG
            )
            
            # Add operation tracking info
            result["operation_id"] = operation_id
            result["processed_by"] = self.node_id
            
            return result
            
        except ValueError:
            return {
                "success": False,
                "error": f"Invalid operation type: {operation_type_str}",
                "operation_id": operation_id
            }
        except Exception as e:
            self.logger.error(f"Error handling control operation: {e}")
            return {
                "success": False,
                "error": str(e),
                "operation_id": operation_id
            }
    
    async def handle_get_topology(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle request for topology information
        
        Args:
            payload: Request details
                - requester_id: ID of the requesting node
                - include_keyspaces: Whether to include keyspace information
                
        Returns:
            Current topology information
        """
        requester_id = payload.get("requester_id")
        include_keyspaces = payload.get("include_keyspaces", True)
        
        # Create response with the requested information
        topology_info = {
            "regions": self.topology["regions"],
            "nodes": self.topology["nodes"]
        }
        
        if include_keyspaces:
            topology_info["keyspaces"] = self.topology["keyspaces"]
        
        # Add metadata
        response = {
            "success": True,
            "topology": topology_info,
            "version": int(time.time()),
            "leader_id": self.current_leader
        }
        
        self.logger.debug(f"Sending topology info to node {requester_id}")
        return response
    
    async def handle_leadership_heartbeat(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Handle leadership heartbeat from leader
        
        Args:
            payload: Heartbeat details
                - leader_id: ID of the current leader
                - term: Leadership term
                - timestamp: Heartbeat timestamp
                
        Returns:
            Acknowledgment
        """
        leader_id = payload.get("leader_id")
        term = payload.get("term", 0)
        timestamp = payload.get("timestamp", 0)
        
        # Check if this is a newer term
        if term > self.leadership_term:
            self.leadership_term = term
            self.current_leader = leader_id
            self.last_heartbeat = time.time()
            
            self.logger.info(f"Recognized new leader: {leader_id} (term {term})")
            self.metrics["leadership_changes"] += 1
        
        # Update last heartbeat time if from current leader
        elif leader_id == self.current_leader and term == self.leadership_term:
            self.last_heartbeat = time.time()
            
        return {
            "success": True,
            "node_id": self.node_id,
            "term_accepted": term == self.leadership_term
        }
    
    async def _register_node(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Register a new node in the topology
        
        Args:
            params: Registration parameters
                - node_id: ID of the node to register
                - region_id: ID of the node's region
                - node_config: Node configuration
                
        Returns:
            Registration result
        """
        node_id = params.get("node_id")
        region_id = params.get("region_id")
        node_config = params.get("node_config", {})
        
        if not node_id or not region_id:
            return {
                "success": False,
                "error": "Missing required parameters"
            }
        
        # Check if region exists
        if region_id not in self.topology["regions"]:
            # Auto-create region if specified
            if params.get("auto_create_region", False):
                self.topology["regions"][region_id] = {
                    "id": region_id,
                    "name": params.get("region_name", region_id),
                    "created_at": time.time()
                }
            else:
                return {
                    "success": False,
                    "error": f"Region {region_id} does not exist"
                }
        
        # Add or update node
        self.topology["nodes"][node_id] = {
            "id": node_id,
            "region_id": region_id,
            "endpoint": node_config.get("endpoint"),
            "capabilities": node_config.get("capabilities", []),
            "status": "active",
            "last_seen": time.time(),
            "registered_at": time.time()
        }
        
        self.metrics["nodes_registered"] += 1
        self.logger.info(f"Registered node {node_id} in region {region_id}")
        
        # Notify other nodes about the topology change
        if self.is_leader:
            asyncio.create_task(self._notify_topology_change())
        
        return {
            "success": True,
            "node_id": node_id,
            "region_id": region_id,
            "topology_version": int(time.time())
        }
    
    async def _unregister_node(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Unregister a node from the topology
        
        Args:
            params: Unregistration parameters
                - node_id: ID of the node to unregister
                
        Returns:
            Unregistration result
        """
        node_id = params.get("node_id")
        
        if not node_id:
            return {
                "success": False,
                "error": "Missing required parameters"
            }
        
        # Check if node exists
        if node_id not in self.topology["nodes"]:
            return {
                "success": False,
                "error": f"Node {node_id} does not exist"
            }
        
        # Update node status to inactive
        self.topology["nodes"][node_id]["status"] = "inactive"
        self.topology["nodes"][node_id]["last_seen"] = time.time()
        
        self.metrics["nodes_unregistered"] += 1
        self.logger.info(f"Unregistered node {node_id}")
        
        # Notify other nodes about the topology change
        if self.is_leader:
            asyncio.create_task(self._notify_topology_change())
            
            # Initiate rebalancing if needed
            if params.get("rebalance", True):
                asyncio.create_task(self._handle_node_failure(node_id))
        
        return {
            "success": True,
            "node_id": node_id,
            "topology_version": int(time.time())
        }
    
    async def _update_topology(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Update the topology configuration
        
        Args:
            params: Update parameters
                - regions: Region updates
                - nodes: Node updates
                
        Returns:
            Update result
        """
        regions = params.get("regions", {})
        nodes = params.get("nodes", {})
        
        # Update regions
        for region_id, region_info in regions.items():
            if region_id in self.topology["regions"]:
                # Update existing region
                self.topology["regions"][region_id].update(region_info)
            else:
                # Add new region
                self.topology["regions"][region_id] = region_info
        
        # Update nodes
        for node_id, node_info in nodes.items():
            if node_id in self.topology["nodes"]:
                # Update existing node
                self.topology["nodes"][node_id].update(node_info)
            else:
                # Add new node
                self.topology["nodes"][node_id] = node_info
        
        self.logger.info(f"Updated topology with {len(regions)} regions and {len(nodes)} nodes")
        
        # Notify other nodes about the topology change
        if self.is_leader:
            asyncio.create_task(self._notify_topology_change())
        
        return {
            "success": True,
            "topology_version": int(time.time())
        }
    
    async def _configure_keyspace(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Configure a keyspace
        
        Args:
            params: Keyspace configuration
                - keyspace: Keyspace name
                - partition_strategy: How data is partitioned
                - replication_strategy: How data is replicated
                - consistency_level: Default consistency level
                
        Returns:
            Configuration result
        """
        keyspace = params.get("keyspace")
        
        if not keyspace:
            return {
                "success": False,
                "error": "Missing required parameters"
            }
        
        # Add or update keyspace configuration
        self.topology["keyspaces"][keyspace] = {
            "name": keyspace,
            "partition_strategy": params.get("partition_strategy", "hash"),
            "replication_strategy": params.get("replication_strategy", {}),
            "consistency_level": params.get("consistency_level", "eventual"),
            "created_at": self.topology["keyspaces"].get(keyspace, {}).get("created_at", time.time()),
            "updated_at": time.time()
        }
        
        self.logger.info(f"Configured keyspace {keyspace}")
        
        # Notify other nodes about the configuration change
        if self.is_leader:
            asyncio.create_task(self._notify_topology_change())
            
            # Initiate rebalancing if requested
            if params.get("rebalance", False):
                asyncio.create_task(self._initiate_rebalance({"keyspace": keyspace}))
        
        return {
            "success": True,
            "keyspace": keyspace,
            "topology_version": int(time.time())
        }
    
    async def _initiate_rebalance(self, params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Initiate data rebalancing
        
        Args:
            params: Rebalance parameters
                - keyspace: Keyspace to rebalance (optional)
                - regions: Regions to include (optional)
                
        Returns:
            Rebalance initiation result
        """
        keyspace = params.get("keyspace")
        regions = params.get("regions", [])
        
        # Validate parameters
        if keyspace and keyspace not in self.topology["keyspaces"]:
            return {
                "success": False,
                "error": f"Keyspace {keyspace} does not exist"
            }
        
        # If specific regions specified, validate them
        valid_regions = list(self.topology["regions"].keys())
        if regions:
            for region_id in regions:
                if region_id not in valid_regions:
                    return {
                        "success": False,
                        "error": f"Region {region_id} does not exist"
                    }
        else:
            # Use all regions if none specified
            regions = valid_regions
        
        # Determine which keyspaces to rebalance
        if keyspace:
            keyspaces = [keyspace]
        else:
            keyspaces = list(self.topology["keyspaces"].keys())
        
        self.logger.info(f"Initiating rebalance for keyspaces {keyspaces} in regions {regions}")
        
        # Trigger rebalancing on all relevant nodes
        # In a real implementation, would use a more sophisticated approach
        # For demonstration, we'll just send rebalance notifications
        
        # Get all active nodes in the specified regions
        target_nodes = []
        for node_id, node_info in self.topology["nodes"].items():
            if (node_info.get("status") == "active" and 
                node_info.get("region_id") in regions):
                target_nodes.append(node_id)
        
        # Create rebalance operation
        rebalance_id = str(uuid.uuid4())
        rebalance_operation = {
            "id": rebalance_id,
            "type": "rebalance",
            "keyspaces": keyspaces,
            "regions": regions,
            "initiated_by": self.node_id,
            "initiated_at": time.time(),
            "target_nodes": target_nodes,
            "status": "initiated"
        }
        
        # Store operation (would persist in real implementation)
        self.pending_operations[rebalance_id] = rebalance_operation
        
        # Notify nodes (in a real implementation, would coordinate this properly)
        notification_tasks = []
        for node_id in target_nodes:
            task = self._notify_node_rebalance(node_id, rebalance_operation)
            notification_tasks.append(task)
        
        # Wait for notifications to complete
        await asyncio.gather(*notification_tasks, return_exceptions=True)
        
        return {
            "success": True,
            "rebalance_id": rebalance_id,
            "keyspaces": keyspaces,
            "regions": regions,
            "target_nodes": target_nodes
        }
    
    async def _notify_node_rebalance(self, node_id: str, rebalance_operation: Dict[str, Any]) -> bool:
        """
        Notify a node about rebalancing
        
        Args:
            node_id: ID of the node to notify
            rebalance_operation: Rebalance operation details
            
        Returns:
            True if notification successful
        """
        try:
            if not self.network_manager:
                return False
            
            payload = {
                "operation": "rebalance",
                "rebalance_id": rebalance_operation["id"],
                "keyspaces": rebalance_operation["keyspaces"],
                "coordinator_id": self.node_id
            }
            
            result = await self.network_manager.send_rpc(
                node_id,
                "control_notification",
                payload
            )
            
            return result and result.get("success", False)
            
        except Exception as e:
            self.logger.error(f"Error notifying node {node_id} about rebalance: {e}")
            return False
    
    async def _handle_node_failure(self, node_id: str) -> None:
        """
        Handle a node failure by initiating recovery
        
        Args:
            node_id: ID of the failed node
        """
        self.logger.info(f"Handling failure of node {node_id}")
        
        # Get the node's region
        failed_node = self.topology["nodes"].get(node_id)
        if not failed_node:
            self.logger.warning(f"Failed node {node_id} not found in topology")
            return
        
        region_id = failed_node.get("region_id")
        
        # Find other active nodes in the same region
        active_nodes = []
        for nid, node_info in self.topology["nodes"].items():
            if (nid != node_id and 
                node_info.get("status") == "active" and
                node_info.get("region_id") == region_id):
                active_nodes.append(nid)
        
        if not active_nodes:
            self.logger.warning(f"No active nodes in region {region_id} to handle recovery")
            return
        
        # Initiate recovery process
        # In a real implementation, would coordinate recovery properly
        # For demonstration, we'll just broadcast to active nodes
        
        recovery_id = str(uuid.uuid4())
        recovery_operation = {
            "id": recovery_id,
            "type": "recovery",
            "failed_node": node_id,
            "region_id": region_id,
            "recovery_nodes": active_nodes,
            "initiated_at": time.time(),
            "status": "initiated"
        }
        
        # Store operation
        self.pending_operations[recovery_id] = recovery_operation
        
        # Notify recovery nodes
        for recovery_node in active_nodes:
            try:
                if self.network_manager:
                    payload = {
                        "operation": "recover_node",
                        "recovery_id": recovery_id,
                        "failed_node": node_id,
                        "coordinator_id": self.node_id
                    }
                    
                    await self.network_manager.send_rpc(
                        recovery_node,
                        "control_notification",
                        payload
                    )
                    
            except Exception as e:
                self.logger.error(f"Error notifying node {recovery_node} about recovery: {e}")
    
    async def _forward_to_leader(self, operation_type: OperationType, 
                               params: Dict[str, Any]) -> Dict[str, Any]:
        """
        Forward an operation to the leader node
        
        Args:
            operation_type: Type of operation
            params: Operation parameters
            
        Returns:
            Operation result
        """
        if not self.current_leader or not self.network_manager:
            return {
                "success": False,
                "error": "No leader available"
            }
        
        try:
            # Generate operation ID
            operation_id = f"{self.node_id}_{int(time.time())}_{self.operation_counter}"
            self.operation_counter += 1
            
            # Prepare payload
            payload = {
                "operation_type": operation_type.value,
                "params": params,
                "source_node": self.node_id,
                "operation_id": operation_id
            }
            
            # Send to leader
            result = await self.network_manager.send_rpc(
                self.current_leader,
                "control_operation",
                payload
            )
            
            return result or {
                "success": False,
                "error": "No response from leader"
            }
            
        except Exception as e:
            self.logger.error(f"Error forwarding operation to leader: {e}")
            return {
                "success": False,
                "error": str(e)
            }
    
    async def _leadership_loop(self):
        """Background task for leadership management"""
        if not self.consensus_node:
            return
        
        self.logger.info("Starting leadership management loop")
        
        while self.running:
            try:
                # Check if we are the leader
                is_leader = await self.consensus_node.is_leader()
                
                if is_leader and not self.is_leader:
                    # Became leader
                    self.is_leader = True
                    self.current_leader = self.node_id
                    self.leadership_term += 1
                    
                    self.logger.info(f"Became leader for term {self.leadership_term}")
                    self.metrics["leadership_changes"] += 1
                    
                    # Notify other nodes about leadership change
                    asyncio.create_task(self._broadcast_leadership_heartbeat())
                    
                elif not is_leader and self.is_leader:
                    # Lost leadership
                    self.is_leader = False
                    
                    self.logger.info("Lost leadership")
                    
                # If leader, send heartbeats
                if self.is_leader:
                    await self._broadcast_leadership_heartbeat()
                
                # Check leader timeout
                elif self.current_leader and self.current_leader != self.node_id:
                    # Check if leader has timed out
                    if time.time() - self.last_heartbeat > self.leadership_check_interval * 3:
                        self.logger.warning(f"Leader {self.current_leader} timed out")
                        self.current_leader = None
                        
                        # In a real implementation, would trigger new leader election
                
                # Wait before checking again
                await asyncio.sleep(self.leadership_check_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in leadership loop: {e}")
                await asyncio.sleep(1.0)  # Longer sleep on error
    
    async def _broadcast_leadership_heartbeat(self) -> None:
        """Broadcast leadership heartbeat to all nodes"""
        if not self.network_manager or not self.is_leader:
            return
        
        heartbeat_payload = {
            "leader_id": self.node_id,
            "term": self.leadership_term,
            "timestamp": time.time()
        }
        
        # Send to all nodes except self
        for node_id, node_info in self.topology["nodes"].items():
            if node_id != self.node_id and node_info.get("status") == "active":
                try:
                    await self.network_manager.send_rpc(
                        node_id,
                        "leadership_heartbeat",
                        heartbeat_payload
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to send heartbeat to node {node_id}: {e}")
    
    async def _metadata_sync_loop(self):
        """Background task for metadata synchronization"""
        self.logger.info("Starting metadata synchronization loop")
        
        while self.running:
            try:
                # Only non-leaders need to sync from the leader
                if not self.is_leader and self.current_leader and self.network_manager:
                    await self._sync_metadata_from_leader()
                    self.metrics["metadata_syncs"] += 1
                
                # Wait before syncing again
                await asyncio.sleep(self.metadata_sync_interval)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in metadata sync loop: {e}")
                await asyncio.sleep(5.0)  # Longer sleep on error
    
    async def _sync_metadata_from_leader(self) -> None:
        """Sync metadata from the current leader"""
        if not self.current_leader or not self.network_manager:
            return
        
        try:
            payload = {
                "requester_id": self.node_id,
                "include_keyspaces": True
            }
            
            result = await self.network_manager.send_rpc(
                self.current_leader,
                "get_topology",
                payload
            )
            
            if result and result.get("success", False):
                # Update local topology
                topology = result.get("topology", {})
                
                if "regions" in topology:
                    self.topology["regions"] = topology["regions"]
                
                if "nodes" in topology:
                    self.topology["nodes"] = topology["nodes"]
                
                if "keyspaces" in topology:
                    self.topology["keyspaces"] = topology["keyspaces"]
                
                self.logger.info("Successfully synced metadata from leader")
            else:
                self.logger.warning("Failed to sync metadata from leader")
                
        except Exception as e:
            self.logger.error(f"Error syncing metadata from leader: {e}")
    
    async def _notify_topology_change(self) -> None:
        """Notify all nodes about a topology change"""
        if not self.network_manager or not self.is_leader:
            return
        
        notification = {
            "operation": "topology_changed",
            "coordinator_id": self.node_id,
            "timestamp": time.time(),
            "version": int(time.time())
        }
        
        # Send to all nodes except self
        for node_id, node_info in self.topology["nodes"].items():
            if node_id != self.node_id and node_info.get("status") == "active":
                try:
                    await self.network_manager.send_rpc(
                        node_id,
                        "control_notification",
                        notification
                    )
                except Exception as e:
                    self.logger.warning(f"Failed to notify node {node_id} about topology change: {e}")
    
    async def _load_initial_topology(self) -> None:
        """Load initial topology (would load from storage in real implementation)"""
        # For demonstration, we'll initialize with basic topology
        # In a real implementation, would load from persistent storage
        
        # Add self to nodes
        self.topology["nodes"][self.node_id] = {
            "id": self.node_id,
            "region_id": self.config.get("region_id", "default"),
            "endpoint": self.config.get("endpoint", "localhost:8000"),
            "capabilities": self.config.get("capabilities", []),
            "status": "active",
            "last_seen": time.time(),
            "registered_at": time.time()
        }

        # Add region if not exists
        region_id = self.config.get("region_id", "default")
        if region_id not in self.topology["regions"]:
            self.topology["regions"][region_id] = {
                "id": region_id,
                "name": self.config.get("region_name", region_id),
                "created_at": time.time()
            }
        
        # Add default keyspace if none exists
        if not self.topology["keyspaces"]:
            self.topology["keyspaces"]["default"] = {
                "name": "default",
                "partition_strategy": "hash",
                "replication_strategy": {
                    "class": "SimpleStrategy",
                    "replication_factor": 3
                },
                "consistency_level": "eventual",
                "created_at": time.time(),
                "updated_at": time.time()
            }
        
        self.logger.info(f"Loaded initial topology with {len(self.topology['regions'])} regions, "
                       f"{len(self.topology['nodes'])} nodes, and {len(self.topology['keyspaces'])} keyspaces")
    
    async def _save_topology(self) -> None:
        """Save topology (would save to storage in real implementation)"""
        # In a real implementation, would persist to storage
        self.logger.info("Saving topology (simulated)")
    
    async def get_topology_info(self) -> Dict[str, Any]:
        """
        Get current topology information
        
        Returns:
            Dict with topology details
        """
        return {
            "regions": self.topology["regions"],
            "nodes": self.topology["nodes"],
            "keyspaces": self.topology["keyspaces"],
            "leader_id": self.current_leader,
            "version": int(time.time())
        }
    
    async def get_metrics(self) -> Dict[str, Any]:
        """
        Get control plane metrics
        
        Returns:
            Dict of metrics
        """
        metrics = dict(self.metrics)
        metrics["is_leader"] = self.is_leader
        metrics["current_leader"] = self.current_leader
        metrics["leadership_term"] = self.leadership_term
        metrics["pending_operations"] = len(self.pending_operations)
        
        return metrics