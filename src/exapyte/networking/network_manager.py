"""
src/exapyte/networking/network_manager.py
========================================

Network manager for handling communication between distributed nodes.
This component abstracts away the network details and provides
reliable message delivery with timeouts and retries.
"""

import asyncio
import logging
import random
import time
import json
import aiohttp
from typing import Dict, Callable, Any, List, Optional, Union


class NetworkManager:
    """
    Network manager for distributed systems communication
    
    Handles:
    - Node-to-node communication
    - RPC request/response patterns
    - Timeouts and retries
    - Network failure simulation (for testing)
    """
    
    def __init__(self, node_id: str, node_address: str, cluster_config: Dict[str, Any],
                 simulation_mode: bool = False):
        """
        Initialize the network manager
        
        Args:
            node_id: Unique identifier for this node
            node_address: Network address (host:port) of this node
            cluster_config: Configuration containing address information for all nodes
            simulation_mode: If True, operate in simulation mode (no actual network)
        """
        self.node_id = node_id
        self.node_address = node_address
        self.cluster_config = cluster_config
        self.simulation_mode = simulation_mode
        
        # Set up logging
        self.logger = logging.getLogger(f"network.{node_id}")
        
        # RPC handlers
        self.rpc_handlers: Dict[str, Callable] = {}
        
        # HTTP server and client
        self.server = None
        self.client_session = None
        
        # For simulation mode
        self.simulated_network = None
        
        # Network status
        self.connected_nodes = set()
        self.network_latency = {}  # node_id -> latency in ms
        
        # RPC statistics
        self.rpc_stats = {
            "sent": 0,
            "received": 0,
            "failures": 0,
            "retries": 0,
            "timeouts": 0
        }
        
        # Configuration
        self.max_retries = 3
        self.retry_backoff = 1.5  # Exponential backoff multiplier
        self.base_timeout = 0.5   # Base timeout in seconds
        
        # Message compression threshold in bytes
        self.compression_threshold = 1024
    
    async def start(self):
        """Start the network manager"""
        self.logger.info(f"Starting network manager for node {self.node_id}")
        
        if self.simulation_mode:
            # In simulation mode, create simulated network
            from exapyte.simulation.network_sim import SimulatedNetwork
            self.simulated_network = SimulatedNetwork()
            self.simulated_network.register_node(self.node_id, self)
            self.logger.info("Running in network simulation mode")
        else:
            # Create HTTP client session
            self.client_session = aiohttp.ClientSession(
                json_serialize=json.dumps,
                timeout=aiohttp.ClientTimeout(total=30)
            )
            
            # Start HTTP server
            await self._start_server()
            
            # Test connectivity to all nodes in cluster
            await self._test_connectivity()
    
    async def stop(self):
        """Stop the network manager"""
        self.logger.info(f"Stopping network manager for node {self.node_id}")
        
        if not self.simulation_mode:
            # Close HTTP client session
            if self.client_session:
                await self.client_session.close()
            
            # Stop HTTP server
            if self.server:
                self.server.close()
                await self.server.wait_closed()
    
    async def _start_server(self):
        """Start the HTTP server for handling incoming RPCs"""
        from aiohttp import web
        
        # Define route handlers
        async def handle_rpc(request):
            """Handle incoming RPC requests"""
            try:
                data = await request.json()
                rpc_type = data.get("rpc_type")
                rpc_id = data.get("rpc_id")
                sender_id = data.get("sender_id")
                payload = data.get("payload", {})
                
                self.logger.debug(f"Received RPC {rpc_type} from {sender_id}")
                self.rpc_stats["received"] += 1
                
                if rpc_type not in self.rpc_handlers:
                    return web.json_response({
                        "success": False,
                        "error": "unknown_rpc_type"
                    }, status=400)
                
                # Call the appropriate handler
                handler = self.rpc_handlers[rpc_type]
                result = await handler(payload)
                
                # Send response
                return web.json_response({
                    "success": True,
                    "rpc_id": rpc_id,
                    "result": result
                })
            except Exception as e:
                self.logger.error(f"Error handling RPC: {e}")
                return web.json_response({
                    "success": False,
                    "error": str(e)
                }, status=500)
        
        # Create application
        app = web.Application()
        app.add_routes([
            web.post('/rpc', handle_rpc)
        ])
        
        # Parse host and port from node address
        host, port_str = self.node_address.split(":")
        port = int(port_str)
        
        # Start server
        runner = web.AppRunner(app)
        await runner.setup()
        self.server = web.TCPSite(runner, host, port)
        await self.server.start()
        
        self.logger.info(f"RPC server started on {self.node_address}")
    
    async def _test_connectivity(self):
        """Test connectivity to all nodes in the cluster"""
        for node_id, node_info in self.cluster_config.get("nodes", {}).items():
            if node_id == self.node_id:
                continue  # Skip self
            
            address = node_info.get("address")
            if not address:
                self.logger.warning(f"No address for node {node_id}, skipping connectivity test")
                continue
            
            try:
                # Send ping message
                start_time = time.time()
                response = await self.send_rpc(node_id, "ping", {}, timeout=1.0)
                latency = (time.time() - start_time) * 1000  # Convert to ms
                
                if response and response.get("success"):
                    self.connected_nodes.add(node_id)
                    self.network_latency[node_id] = latency
                    self.logger.info(f"Connected to node {node_id}, latency: {latency:.2f}ms")
                else:
                    self.logger.warning(f"Failed to connect to node {node_id}: {response}")
            except Exception as e:
                self.logger.warning(f"Failed to connect to node {node_id}: {e}")
    
    def register_handlers(self, handlers: Dict[str, Callable]):
        """
        Register RPC handlers
        
        Args:
            handlers: Dict mapping RPC types to handler functions
        """
        self.rpc_handlers.update(handlers)
        
        # Add built-in handlers if not overridden
        if "ping" not in self.rpc_handlers:
            self.rpc_handlers["ping"] = self._handle_ping
    
    async def _handle_ping(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        """
        Built-in handler for ping messages
        
        Args:
            payload: The RPC payload
            
        Returns:
            Response payload
        """
        return {
            "status": "ok",
            "node_id": self.node_id,
            "timestamp": time.time()
        }
    
    async def send_rpc(self, target_node_id: str, rpc_type: str, payload: Dict[str, Any],
                      timeout: float = None, retries: int = None) -> Optional[Dict[str, Any]]:
        """
        Send an RPC to a target node
        
        Args:
            target_node_id: ID of the target node
            rpc_type: Type of RPC
            payload: RPC payload data
            timeout: Operation timeout in seconds (None for default)
            retries: Number of retries (None for default)
            
        Returns:
            Response payload or None if failed
        """
        if self.simulation_mode:
            return await self._send_simulated_rpc(target_node_id, rpc_type, payload, timeout)
        
        # Use default values if not specified
        timeout = timeout or self.base_timeout
        retries = retries if retries is not None else self.max_retries
        
        # Get target address
        target_address = self._get_node_address(target_node_id)
        if not target_address:
            self.logger.error(f"Unknown node ID: {target_node_id}")
            return None
        
        # Generate unique RPC ID
        rpc_id = f"{self.node_id}-{int(time.time() * 1000)}-{random.randint(0, 10000)}"
        
        # Prepare request data
        request_data = {
            "rpc_type": rpc_type,
            "rpc_id": rpc_id,
            "sender_id": self.node_id,
            "payload": payload
        }
        
        # Apply compression if needed
        request_size = len(json.dumps(request_data))
        if request_size > self.compression_threshold:
            # In a real implementation, we would compress the data here
            pass
        
        # Track statistics
        self.rpc_stats["sent"] += 1
        
        # Try to send the RPC with retries
        current_retry = 0
        current_timeout = timeout
        
        while current_retry <= retries:
            try:
                self.logger.debug(f"Sending RPC {rpc_type} to {target_node_id} (attempt {current_retry+1})")
                
                # Send HTTP request
                url = f"http://{target_address}/rpc"
                
                # Use client timeout
                timeout_obj = aiohttp.ClientTimeout(total=current_timeout)
                
                async with self.client_session.post(
                    url, json=request_data, timeout=timeout_obj
                ) as response:
                    # Check response status
                    if response.status == 200:
                        # Parse response
                        response_data = await response.json()
                        return response_data.get("result")
                    else:
                        error_text = await response.text()
                        self.logger.warning(f"RPC failed with status {response.status}: {error_text}")
                
            except asyncio.TimeoutError:
                self.logger.warning(f"RPC to {target_node_id} timed out after {current_timeout}s")
                self.rpc_stats["timeouts"] += 1
                
            except Exception as e:
                self.logger.warning(f"Error sending RPC to {target_node_id}: {e}")
                self.rpc_stats["failures"] += 1
            
            # Retry with exponential backoff
            current_retry += 1
            if current_retry <= retries:
                self.rpc_stats["retries"] += 1
                current_timeout *= self.retry_backoff
                await asyncio.sleep(0.1 * current_retry)  # Small delay before retry
        
        self.logger.error(f"RPC {rpc_type} to {target_node_id} failed after {retries} retries")
        return None
    
    async def _send_simulated_rpc(self, target_node_id: str, rpc_type: str, 
                                 payload: Dict[str, Any], timeout: float) -> Optional[Dict[str, Any]]:
        """
        Send an RPC in simulation mode
        
        Args:
            target_node_id: ID of the target node
            rpc_type: Type of RPC
            payload: RPC payload data
            timeout: Operation timeout in seconds
            
        Returns:
            Response payload or None if failed
        """
        if not self.simulated_network:
            self.logger.error("Simulated network not initialized")
            return None
        
        # Prepare request
        request = {
            "rpc_type": rpc_type,
            "sender_id": self.node_id,
            "payload": payload
        }
        
        # Send via simulated network
        try:
            result = await asyncio.wait_for(
                self.simulated_network.deliver_message(self.node_id, target_node_id, request),
                timeout=timeout
            )
            return result
        except asyncio.TimeoutError:
            self.logger.warning(f"Simulated RPC to {target_node_id} timed out")
            self.rpc_stats["timeouts"] += 1
            return None
        except Exception as e:
            self.logger.error(f"Error in simulated RPC: {e}")
            self.rpc_stats["failures"] += 1
            return None
    
    def _get_node_address(self, node_id: str) -> Optional[str]:
        """
        Get the network address for a node
        
        Args:
            node_id: ID of the node
            
        Returns:
            Network address or None if not found
        """
        node_info = self.cluster_config.get("nodes", {}).get(node_id)
        if not node_info:
            return None
        
        return node_info.get("address")
    
    def handle_incoming_message(self, sender_id: str, message: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Handle incoming message in simulation mode
        
        Args:
            sender_id: ID of the sender node
            message: Message data
            
        Returns:
            Response data or None
        """
        if not self.simulation_mode:
            self.logger.error("Called handle_incoming_message but not in simulation mode")
            return None
        
        # Extract message details
        rpc_type = message.get("rpc_type")
        payload = message.get("payload", {})
        
        # Check if we have a handler
        if rpc_type not in self.rpc_handlers:
            self.logger.warning(f"No handler for RPC type: {rpc_type}")
            return None
        
        # Call handler
        handler = self.rpc_handlers[rpc_type]
        
        # Need to use asyncio.run_coroutine_threadsafe or similar in real implementation
        # For demo, we'll use a simple approach
        loop = asyncio.get_event_loop()
        future = asyncio.run_coroutine_threadsafe(handler(payload), loop)
        return future.result(timeout=5.0)
    
    def get_network_status(self) -> Dict[str, Any]:
        """
        Get current network status information
        
        Returns:
            Dict with network status details
        """
        return {
            "node_id": self.node_id,
            "connected_nodes": list(self.connected_nodes),
            "network_latency": self.network_latency,
            "rpc_stats": self.rpc_stats
        }
    
    def simulate_network_partition(self, partition_nodes: List[str], duration_sec: float = 30.0):
        """
        Simulate a network partition in simulation mode
        
        Args:
            partition_nodes: List of nodes to partition from this node
            duration_sec: Duration of partition in seconds
        """
        if not self.simulation_mode or not self.simulated_network:
            self.logger.error("Network partition simulation only available in simulation mode")
            return
        
        self.logger.info(f"Simulating network partition from nodes: {partition_nodes}")
        self.simulated_network.create_partition(self.node_id, partition_nodes, duration_sec)
    
    def simulate_message_delay(self, target_node_id: str, min_delay_ms: float, max_delay_ms: float):
        """
        Simulate network delay to a specific node in simulation mode
        
        Args:
            target_node_id: Target node ID
            min_delay_ms: Minimum delay in milliseconds
            max_delay_ms: Maximum delay in milliseconds
        """
        if not self.simulation_mode or not self.simulated_network:
            self.logger.error("Message delay simulation only available in simulation mode")
            return
        
        self.logger.info(f"Simulating message delay to node {target_node_id}: "
                       f"{min_delay_ms}ms-{max_delay_ms}ms")
        self.simulated_network.set_link_delay(self.node_id, target_node_id, min_delay_ms, max_delay_ms)
    
    def simulate_message_loss(self, target_node_id: str, loss_probability: float):
        """
        Simulate message loss to a specific node in simulation mode
        
        Args:
            target_node_id: Target node ID
            loss_probability: Probability of message loss (0.0-1.0)
        """
        if not self.simulation_mode or not self.simulated_network:
            self.logger.error("Message loss simulation only available in simulation mode")
            return
        
        self.logger.info(f"Simulating message loss to node {target_node_id}: {loss_probability:.2f}")
        self.simulated_network.set_link_loss(self.node_id, target_node_id, loss_probability)