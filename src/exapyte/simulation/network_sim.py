"""
src/exapyte/simulation/network_sim.py
====================================

Network simulation for exapyte distributed system.
Simulates network conditions like latency, packet loss, and partitions.
"""

import asyncio
import logging
import random
import time
from typing import Dict, Any, List, Optional, Set, Tuple, Callable


class SimulatedNetwork:
    """
    Simulated network for distributed systems testing
    
    Features:
    - Simulated message delivery
    - Configurable latency and jitter
    - Packet loss simulation
    - Network partition simulation
    - Bandwidth limitations
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the simulated network
        
        Args:
            config: Configuration parameters
                - seed: Random seed for reproducibility
                - default_latency_ms: Default latency in milliseconds
                - default_jitter_ms: Default jitter in milliseconds
                - default_loss_rate: Default packet loss rate (0.0-1.0)
                - default_bandwidth: Default bandwidth in KB/s
        """
        self.config = config or {}
        self.logger = logging.getLogger("simulation.network")
        
        # Configuration
        self.seed = self.config.get("seed")
        self.default_latency_ms = self.config.get("default_latency_ms", 10)
        self.default_jitter_ms = self.config.get("default_jitter_ms", 5)
        self.default_loss_rate = self.config.get("default_loss_rate", 0.0)
        self.default_bandwidth = self.config.get("default_bandwidth", 1024)  # KB/s
        
        # Initialize random generator
        if self.seed is not None:
            random.seed(self.seed)
        
        # Network state
        self.nodes = {}  # node_id -> node_handler
        self.links = {}  # (source, target) -> link_properties
        self.partitions = {}  # partition_id -> {nodes, end_time}
        
        # Bandwidth tracking
        self.bandwidth_usage = {}  # (source, target) -> {last_time, bytes_sent}
        
        # Background tasks
        self.partition_task = None
        self.running = True
        
        self.logger.info(f"Initialized SimulatedNetwork with config: {self.config}")
        
        # Start partition management task
        self.partition_task = asyncio.create_task(self._manage_partitions())
    
    def register_node(self, node_id: str, node_handler: Any) -> None:
        """
        Register a node with the simulated network
        
        Args:
            node_id: ID of the node
            node_handler: Node handler object (must implement handle_incoming_message)
        """
        self.nodes[node_id] = node_handler
        self.logger.debug(f"Registered node {node_id}")
    
    def unregister_node(self, node_id: str) -> None:
        """
        Unregister a node from the simulated network
        
        Args:
            node_id: ID of the node
        """
        if node_id in self.nodes:
            del self.nodes[node_id]
            self.logger.debug(f"Unregistered node {node_id}")
    
    def set_link_properties(self, source: str, target: str, 
                          latency_ms: Optional[float] = None,
                          jitter_ms: Optional[float] = None,
                          loss_rate: Optional[float] = None,
                          bandwidth: Optional[float] = None) -> None:
        """
        Set properties for a network link
        
        Args:
            source: Source node ID
            target: Target node ID
            latency_ms: Latency in milliseconds
            jitter_ms: Jitter in milliseconds
            loss_rate: Packet loss rate (0.0-1.0)
            bandwidth: Bandwidth in KB/s
        """
        link_key = (source, target)
        
        if link_key not in self.links:
            self.links[link_key] = {
                "latency_ms": self.default_latency_ms,
                "jitter_ms": self.default_jitter_ms,
                "loss_rate": self.default_loss_rate,
                "bandwidth": self.default_bandwidth
            }
        
        # Update properties
        if latency_ms is not None:
            self.links[link_key]["latency_ms"] = latency_ms
        
        if jitter_ms is not None:
            self.links[link_key]["jitter_ms"] = jitter_ms
        
        if loss_rate is not None:
            self.links[link_key]["loss_rate"] = loss_rate
        
        if bandwidth is not None:
            self.links[link_key]["bandwidth"] = bandwidth
        
        self.logger.debug(f"Set link properties for {source} -> {target}: {self.links[link_key]}")
    
    def set_link_delay(self, source: str, target: str, 
                     min_delay_ms: float, max_delay_ms: float) -> None:
        """
        Set delay for a network link
        
        Args:
            source: Source node ID
            target: Target node ID
            min_delay_ms: Minimum delay in milliseconds
            max_delay_ms: Maximum delay in milliseconds
        """
        latency_ms = (min_delay_ms + max_delay_ms) / 2
        jitter_ms = (max_delay_ms - min_delay_ms) / 2
        
        self.set_link_properties(source, target, latency_ms=latency_ms, jitter_ms=jitter_ms)
    
    def set_link_loss(self, source: str, target: str, loss_rate: float) -> None:
        """
        Set packet loss rate for a network link
        
        Args:
            source: Source node ID
            target: Target node ID
            loss_rate: Packet loss rate (0.0-1.0)
        """
        self.set_link_properties(source, target, loss_rate=loss_rate)
    
    def set_link_bandwidth(self, source: str, target: str, bandwidth: float) -> None:
        """
        Set bandwidth for a network link
        
        Args:
            source: Source node ID
            target: Target node ID
            bandwidth: Bandwidth in KB/s
        """
        self.set_link_properties(source, target, bandwidth=bandwidth)
    
    def create_partition(self, node_id: str, isolated_nodes: List[str], 
                       duration_sec: float) -> str:
        """
        Create a network partition
        
        Args:
            node_id: ID of the node to partition
            isolated_nodes: List of nodes to isolate from
            duration_sec: Duration of partition in seconds
            
        Returns:
            Partition ID
        """
        partition_id = f"partition_{int(time.time())}_{random.randint(1000, 9999)}"
        end_time = time.time() + duration_sec
        
        self.partitions[partition_id] = {
            "node": node_id,
            "isolated_nodes": isolated_nodes,
            "end_time": end_time
        }
        
        self.logger.info(f"Created partition {partition_id}: {node_id} isolated from {isolated_nodes} for {duration_sec}s")
        
        return partition_id
    
    def remove_partition(self, partition_id: str) -> bool:
        """
        Remove a network partition
        
        Args:
            partition_id: ID of the partition to remove
            
        Returns:
            True if partition was removed
        """
        if partition_id in self.partitions:
            del self.partitions[partition_id]
            self.logger.info(f"Removed partition {partition_id}")
            return True
        
        return False
    
    async def deliver_message(self, source: str, target: str, message: Any) -> Any:
        """
        Deliver a message from source to target
        
        Args:
            source: Source node ID
            target: Target node ID
            message: Message to deliver
            
        Returns:
            Response from target node
        """
        # Check if nodes exist
        if source not in self.nodes:
            raise ValueError(f"Source node {source} not registered")
        
        if target not in self.nodes:
            raise ValueError(f"Target node {target} not registered")
        
        # Check for partitions
        if self._is_partitioned(source, target):
            self.logger.debug(f"Message from {source} to {target} dropped due to partition")
            raise TimeoutError("Network partition")
        
        # Get link properties
        link_key = (source, target)
        link_props = self.links.get(link_key, {
            "latency_ms": self.default_latency_ms,
            "jitter_ms": self.default_jitter_ms,
            "loss_rate": self.default_loss_rate,
            "bandwidth": self.default_bandwidth
        })
        
        # Check for packet loss
        if random.random() < link_props["loss_rate"]:
            self.logger.debug(f"Message from {source} to {target} dropped due to packet loss")
            raise TimeoutError("Packet loss")
        
        # Calculate delay
        latency = link_props["latency_ms"]
        jitter = link_props["jitter_ms"]
        delay = (latency + random.uniform(-jitter, jitter)) / 1000.0  # Convert to seconds
        
        # Apply bandwidth limitation
        message_size = self._estimate_message_size(message)
        bandwidth_delay = self._apply_bandwidth_limit(source, target, message_size, link_props["bandwidth"])
        total_delay = delay + bandwidth_delay
        
        # Simulate network delay
        if total_delay > 0:
            await asyncio.sleep(total_delay)
        
        # Deliver message to target
        target_node = self.nodes[target]
        response = target_node.handle_incoming_message(source, message)
        
        # Apply same delay and loss for response
        if random.random() < link_props["loss_rate"]:
            self.logger.debug(f"Response from {target} to {source} dropped due to packet loss")
            raise TimeoutError("Packet loss")
        
        # Calculate response delay
        response_size = self._estimate_message_size(response)
        response_bandwidth_delay = self._apply_bandwidth_limit(target, source, response_size, link_props["bandwidth"])
        response_total_delay = delay + response_bandwidth_delay
        
        # Simulate response delay
        if response_total_delay > 0:
            await asyncio.sleep(response_total_delay)
        
        return response
    
    def _is_partitioned(self, source: str, target: str) -> bool:
        """
        Check if two nodes are partitioned
        
        Args:
            source: Source node ID
            target: Target node ID
            
        Returns:
            True if nodes are partitioned
        """
        for partition in self.partitions.values():
            node = partition["node"]
            isolated_nodes = partition["isolated_nodes"]
            
            if (source == node and target in isolated_nodes) or (target == node and source in isolated_nodes):
                return True
        
        return False
    
    def _estimate_message_size(self, message: Any) -> int:
        """
        Estimate size of a message in bytes
        
        Args:
            message: Message to estimate size of
            
        Returns:
            Estimated size in bytes
        """
        # For demonstration, use a simple heuristic
        # In a real implementation, would serialize the message
        
        if isinstance(message, dict):
            # Rough estimate based on number of keys and values
            return sum(
                len(str(k)) + self._estimate_message_size(v)
                for k, v in message.items()
            )
        elif isinstance(message, list):
            # Sum of sizes of elements
            return sum(self._estimate_message_size(item) for item in message)
        elif isinstance(message, str):
            return len(message)
        elif isinstance(message, (int, float, bool)):
            return 8
        else:
            # Default size for unknown types
            return 64
    
    def _apply_bandwidth_limit(self, source: str, target: str, 
                             message_size: int, bandwidth: float) -> float:
        """
        Apply bandwidth limitation and calculate delay
        
        Args:
            source: Source node ID
            target: Target node ID
            message_size: Size of message in bytes
            bandwidth: Bandwidth in KB/s
            
        Returns:
            Delay in seconds
        """
        link_key = (source, target)
        
        # Initialize bandwidth tracking if needed
        if link_key not in self.bandwidth_usage:
            self.bandwidth_usage[link_key] = {
                "last_time": time.time(),
                "bytes_sent": 0
            }
        
        # Get current time
        current_time = time.time()
        
        # Get time since last message
        last_time = self.bandwidth_usage[link_key]["last_time"]
        time_diff = current_time - last_time
        
        # Calculate bytes that could have been sent in this time
        bytes_capacity = bandwidth * 1024 * time_diff  # Convert KB/s to B/s
        
        # Get bytes already sent
        bytes_sent = self.bandwidth_usage[link_key]["bytes_sent"]
        
        # Reset if enough time has passed
        if time_diff > 1.0:  # Reset after 1 second
            bytes_sent = 0
        
        # Calculate remaining capacity
        remaining_capacity = max(0, bytes_capacity - bytes_sent)
        
        # Calculate delay
        if remaining_capacity >= message_size:
            # Enough capacity, no delay
            delay = 0
        else:
            # Not enough capacity, calculate delay
            needed_capacity = message_size - remaining_capacity
            delay = needed_capacity / (bandwidth * 1024)  # Convert KB/s to B/s
        
        # Update bandwidth usage
        self.bandwidth_usage[link_key] = {
            "last_time": current_time,
            "bytes_sent": bytes_sent + message_size
        }
        
        return delay
    
    async def _manage_partitions(self):
        """Background task for managing network partitions"""
        while self.running:
            try:
                current_time = time.time()
                
                # Check for expired partitions
                expired_partitions = []
                
                for partition_id, partition in self.partitions.items():
                    if current_time >= partition["end_time"]:
                        expired_partitions.append(partition_id)
                
                # Remove expired partitions
                for partition_id in expired_partitions:
                    self.remove_partition(partition_id)
                
                # Wait before next check
                await asyncio.sleep(1.0)
                
            except asyncio.CancelledError:
                self.running = False
                break
            except Exception as e:
                self.logger.error(f"Error in partition management: {e}")
                await asyncio.sleep(5.0)  # Longer sleep on error
    
    def get_network_status(self) -> Dict[str, Any]:
        """
        Get current network status
        
        Returns:
            Dict with network status
        """
        return {
            "nodes": list(self.nodes.keys()),
            "links": {f"{s}->{t}": props for (s, t), props in self.links.items()},
            "partitions": {
                pid: {
                    "node": p["node"],
                    "isolated_nodes": p["isolated_nodes"],
                    "remaining_sec": max(0, p["end_time"] - time.time())
                }
                for pid, p in self.partitions.items()
            }
        }
