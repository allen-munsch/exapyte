"""
src/exapyte/simulation/failure_sim.py
====================================

Failure simulation for exapyte distributed system.
Allows controlled injection of various failure scenarios for testing.
"""

import asyncio
import logging
import random
import time
from typing import Dict, Any, List, Optional, Set, Callable


class FailureSimulator:
    """
    Failure simulator for distributed systems
    
    Features:
    - Node failure simulation
    - Partial failure simulation
    - Cascading failure simulation
    - Recovery simulation
    - Scheduled failure scenarios
    """
    
    def __init__(self, config: Dict[str, Any] = None):
        """
        Initialize the failure simulator
        
        Args:
            config: Configuration parameters
                - seed: Random seed for reproducibility
                - log_failures: Whether to log failures
        """
        self.config = config or {}
        self.logger = logging.getLogger("simulation.failure")
        
        # Configuration
        self.seed = self.config.get("seed")
        self.log_failures = self.config.get("log_failures", True)
        
        # Initialize random generator
        if self.seed is not None:
            random.seed(self.seed)
        
        # State
        self.failed_nodes = set()
        self.partially_failed_nodes = {}  # node_id -> {component: failure_info}
        self.scheduled_failures = []  # List of (time, failure_func, args, kwargs)
        self.scheduled_recoveries = []  # List of (time, recovery_func, args, kwargs)
        
        # Callbacks
        self.failure_callbacks = {}  # node_id -> callback
        self.recovery_callbacks = {}  # node_id -> callback
        
        # Background tasks
        self.scheduler_task = None
        self.running = False
        
        self.logger.info(f"Initialized FailureSimulator with config: {self.config}")
    
    async def start(self):
        """Start the failure simulator"""
        self.logger.info("Starting failure simulator")
        self.running = True
        
        # Start scheduler task
        self.scheduler_task = asyncio.create_task(self._scheduler_loop())
    
    async def stop(self):
        """Stop the failure simulator"""
        self.logger.info("Stopping failure simulator")
        self.running = False
        
        # Cancel scheduler task
        if self.scheduler_task:
            self.scheduler_task.cancel()
            try:
                await self.scheduler_task
            except asyncio.CancelledError:
                pass
        
        # Recover all failed nodes
        for node_id in list(self.failed_nodes):
            await self.recover_node(node_id)
        
        # Recover all partially failed nodes
        for node_id in list(self.partially_failed_nodes.keys()):
            await self.recover_node_component(node_id, "all")
    
    def register_failure_callback(self, node_id: str, callback: Callable):
        """
        Register a callback for node failure
        
        Args:
            node_id: ID of the node
            callback: Callback function to call on failure
        """
        self.failure_callbacks[node_id] = callback
    
    def register_recovery_callback(self, node_id: str, callback: Callable):
        """
        Register a callback for node recovery
        
        Args:
            node_id: ID of the node
            callback: Callback function to call on recovery
        """
        self.recovery_callbacks[node_id] = callback
    
    async def fail_node(self, node_id: str, duration: Optional[float] = None) -> bool:
        """
        Simulate complete failure of a node
        
        Args:
            node_id: ID of the node to fail
            duration: Duration of failure in seconds (None for indefinite)
            
        Returns:
            True if node was failed
        """
        if node_id in self.failed_nodes:
            return False  # Already failed
        
        self.logger.info(f"Simulating failure of node {node_id}")
        self.failed_nodes.add(node_id)
        
        # Call failure callback if registered
        if node_id in self.failure_callbacks:
            try:
                self.failure_callbacks[node_id](node_id, "complete")
            except Exception as e:
                self.logger.error(f"Error in failure callback for node {node_id}: {e}")
        
        # Schedule recovery if duration specified
        if duration is not None:
            await self.schedule_recovery(duration, self.recover_node, node_id)
        
        return True
    
    async def fail_node_component(self, node_id: str, component: str, 
                                duration: Optional[float] = None) -> bool:
        """
        Simulate partial failure of a node component
        
        Args:
            node_id: ID of the node
            component: Component to fail (storage, network, cpu)
            duration: Duration of failure in seconds (None for indefinite)
            
        Returns:
            True if component was failed
        """
        if node_id in self.failed_nodes:
            return False  # Node already completely failed
        
        if node_id not in self.partially_failed_nodes:
            self.partially_failed_nodes[node_id] = {}
        
        if component in self.partially_failed_nodes[node_id]:
            return False  # Component already failed
        
        self.logger.info(f"Simulating failure of {component} on node {node_id}")
        
        # Record failure with timestamp
        self.partially_failed_nodes[node_id][component] = {
            "failed_at": time.time(),
            "duration": duration
        }
        
        # Call failure callback if registered
        if node_id in self.failure_callbacks:
            try:
                self.failure_callbacks[node_id](node_id, component)
            except Exception as e:
                self.logger.error(f"Error in failure callback for node {node_id}: {e}")
        
        # Schedule recovery if duration specified
        if duration is not None:
            await self.schedule_recovery(duration, self.recover_node_component, node_id, component)
        
        return True
    
    async def recover_node(self, node_id: str) -> bool:
        """
        Recover a failed node
        
        Args:
            node_id: ID of the node to recover
            
        Returns:
            True if node was recovered
        """
        if node_id not in self.failed_nodes:
            return False  # Not failed
        
        self.logger.info(f"Simulating recovery of node {node_id}")
        self.failed_nodes.remove(node_id)
        
        # Call recovery callback if registered
        if node_id in self.recovery_callbacks:
            try:
                self.recovery_callbacks[node_id](node_id, "complete")
            except Exception as e:
                self.logger.error(f"Error in recovery callback for node {node_id}: {e}")
        
        return True
    
    async def recover_node_component(self, node_id: str, component: str) -> bool:
        """
        Recover a failed node component
        
        Args:
            node_id: ID of the node
            component: Component to recover (storage, network, cpu, or "all")
            
        Returns:
            True if component was recovered
        """
        if node_id not in self.partially_failed_nodes:
            return False  # No failed components
        
        if component == "all":
            # Recover all components
            components = list(self.partially_failed_nodes[node_id].keys())
            recovered = False
            
            for comp in components:
                if await self.recover_node_component(node_id, comp):
                    recovered = True
            
            # Clean up if all components recovered
            if not self.partially_failed_nodes[node_id]:
                del self.partially_failed_nodes[node_id]
            
            return recovered
        
        if component not in self.partially_failed_nodes[node_id]:
            return False  # Component not failed
        
        self.logger.info(f"Simulating recovery of {component} on node {node_id}")
        del self.partially_failed_nodes[node_id][component]
        
        # Clean up if no more failed components
        if not self.partially_failed_nodes[node_id]:
            del self.partially_failed_nodes[node_id]
        
        # Call recovery callback if registered
        if node_id in self.recovery_callbacks:
            try:
                self.recovery_callbacks[node_id](node_id, component)
            except Exception as e:
                self.logger.error(f"Error in recovery callback for node {node_id}: {e}")
        
        return True
    
    async def schedule_failure(self, delay: float, failure_func: Callable, *args, **kwargs) -> int:
        """
        Schedule a failure to occur after a delay
        
        Args:
            delay: Delay in seconds
            failure_func: Failure function to call
            *args: Arguments for failure function
            **kwargs: Keyword arguments for failure function
            
        Returns:
            ID of the scheduled failure
        """
        scheduled_time = time.time() + delay
        failure_id = len(self.scheduled_failures)
        
        self.scheduled_failures.append((scheduled_time, failure_func, args, kwargs))
        self.logger.debug(f"Scheduled failure {failure_id} at {scheduled_time}")
        
        return failure_id
    
    async def schedule_recovery(self, delay: float, recovery_func: Callable, *args, **kwargs) -> int:
        """
        Schedule a recovery to occur after a delay
        
        Args:
            delay: Delay in seconds
            recovery_func: Recovery function to call
            *args: Arguments for recovery function
            **kwargs: Keyword arguments for recovery function
            
        Returns:
            ID of the scheduled recovery
        """
        scheduled_time = time.time() + delay
        recovery_id = len(self.scheduled_recoveries)
        
        self.scheduled_recoveries.append((scheduled_time, recovery_func, args, kwargs))
        self.logger.debug(f"Scheduled recovery {recovery_id} at {scheduled_time}")
        
        return recovery_id
    
    async def simulate_cascading_failure(self, start_node: str, 
                                       cascade_probability: float = 0.5,
                                       max_affected: int = 3,
                                       recovery_time: Optional[float] = 60.0) -> List[str]:
        """
        Simulate a cascading failure starting from one node
        
        Args:
            start_node: ID of the node to start failure from
            cascade_probability: Probability of failure cascading to each node
            max_affected: Maximum number of nodes to affect
            recovery_time: Time until recovery in seconds (None for indefinite)
            
        Returns:
            List of affected node IDs
        """
        self.logger.info(f"Simulating cascading failure starting from node {start_node}")
        
        # Start with the initial node
        affected_nodes = [start_node]
        await self.fail_node(start_node, recovery_time)
        
        # Get potential cascade targets (all nodes with callbacks)
        potential_targets = [
            node_id for node_id in self.failure_callbacks.keys()
            if node_id != start_node and node_id not in self.failed_nodes
        ]
        
        # Simulate cascade
        cascade_delay = 2.0  # seconds between cascading failures
        current_delay = cascade_delay
        
        while potential_targets and len(affected_nodes) < max_affected:
            # Pick a random target
            target = random.choice(potential_targets)
            potential_targets.remove(target)
            
            # Check if failure cascades to this node
            if random.random() < cascade_probability:
                # Schedule failure with increasing delay
                await self.schedule_failure(current_delay, self.fail_node, target, recovery_time)
                affected_nodes.append(target)
                current_delay += cascade_delay
        
        return affected_nodes
    
    async def simulate_network_partition(self, node_groups: List[List[str]], 
                                       duration: Optional[float] = 30.0) -> bool:
        """
        Simulate a network partition between groups of nodes
        
        Args:
            node_groups: List of node groups (each group is a list of node IDs)
            duration: Duration of partition in seconds (None for indefinite)
            
        Returns:
            True if partition was created
        """
        self.logger.info(f"Simulating network partition between {len(node_groups)} groups")
        
        # For each node in each group, fail network connections to nodes in other groups
        for i, group in enumerate(node_groups):
            for node_id in group:
                # Collect nodes from other groups
                other_nodes = []
                for j, other_group in enumerate(node_groups):
                    if i != j:
                        other_nodes.extend(other_group)
                
                # Fail network component with list of isolated nodes
                if node_id not in self.partially_failed_nodes:
                    self.partially_failed_nodes[node_id] = {}
                
                self.partially_failed_nodes[node_id]["network"] = {
                    "failed_at": time.time(),
                    "duration": duration,
                    "isolated_nodes": other_nodes
                }
                
                # Call failure callback if registered
                if node_id in self.failure_callbacks:
                    try:
                        self.failure_callbacks[node_id](node_id, "network", {"isolated_nodes": other_nodes})
                    except Exception as e:
                        self.logger.error(f"Error in failure callback for node {node_id}: {e}")
        
        # Schedule recovery if duration specified
        if duration is not None:
            # Create a recovery function for the partition
            async def recover_partition():
                for group in node_groups:
                    for node_id in group:
                        await self.recover_node_component(node_id, "network")
            
            await self.schedule_recovery(duration, recover_partition)
        
        return True
    
    async def simulate_random_failures(self, node_ids: List[str], 
                                     failure_rate: float = 0.1,
                                     min_duration: float = 5.0,
                                     max_duration: float = 30.0,
                                     run_time: float = 300.0) -> None:
        """
        Simulate random failures over a period of time
        
        Args:
            node_ids: List of node IDs to potentially fail
            failure_rate: Probability of failure per node per minute
            min_duration: Minimum failure duration in seconds
            max_duration: Maximum failure duration in seconds
            run_time: Total simulation run time in seconds
        """
        self.logger.info(f"Starting random failure simulation for {run_time} seconds")
        
        # Convert failure rate from per minute to per second
        failure_rate_per_second = failure_rate / 60.0
        
        # Run until time expires
        end_time = time.time() + run_time
        check_interval = 1.0  # Check every second
        
        while time.time() < end_time and self.running:
            # For each node, check if it should fail
            for node_id in node_ids:
                # Skip already failed nodes
                if node_id in self.failed_nodes:
                    continue
                
                # Determine if node should fail
                if random.random() < failure_rate_per_second:
                    # Determine failure type
                    failure_type = random.choice(["complete", "storage", "network", "cpu"])
                    
                    # Determine failure duration
                    duration = random.uniform(min_duration, max_duration)
                    
                    # Apply failure
                    if failure_type == "complete":
                        await self.fail_node(node_id, duration)
                    else:
                        await self.fail_node_component(node_id, failure_type, duration)
            
            # Wait before next check
            await asyncio.sleep(check_interval)
    
    async def _scheduler_loop(self):
        """Background task for executing scheduled failures and recoveries"""
        while self.running:
            try:
                current_time = time.time()
                
                # Check for scheduled failures
                pending_failures = []
                for i, (scheduled_time, failure_func, args, kwargs) in enumerate(self.scheduled_failures):
                    if current_time >= scheduled_time:
                        # Execute failure
                        try:
                            await failure_func(*args, **kwargs)
                        except Exception as e:
                            self.logger.error(f"Error executing scheduled failure: {e}")
                    else:
                        pending_failures.append((scheduled_time, failure_func, args, kwargs))
                
                # Update scheduled failures list
                self.scheduled_failures = pending_failures
                
                # Check for scheduled recoveries
                pending_recoveries = []
                for i, (scheduled_time, recovery_func, args, kwargs) in enumerate(self.scheduled_recoveries):
                    if current_time >= scheduled_time:
                        # Execute recovery
                        try:
                            await recovery_func(*args, **kwargs)
                        except Exception as e:
                            self.logger.error(f"Error executing scheduled recovery: {e}")
                    else:
                        pending_recoveries.append((scheduled_time, recovery_func, args, kwargs))
                
                # Update scheduled recoveries list
                self.scheduled_recoveries = pending_recoveries
                
                # Wait before next check
                await asyncio.sleep(0.1)
                
            except asyncio.CancelledError:
                break
            except Exception as e:
                self.logger.error(f"Error in scheduler loop: {e}")
                await asyncio.sleep(1.0)  # Longer sleep on error
    
    def is_node_failed(self, node_id: str) -> bool:
        """
        Check if a node is completely failed
        
        Args:
            node_id: ID of the node to check
            
        Returns:
            True if node is completely failed
        """
        return node_id in self.failed_nodes
    
    def is_component_failed(self, node_id: str, component: str) -> bool:
        """
        Check if a specific component of a node is failed
        
        Args:
            node_id: ID of the node to check
            component: Component to check
            
        Returns:
            True if component is failed
        """
        if node_id in self.failed_nodes:
            return True  # Complete failure includes all components
        
        if node_id not in self.partially_failed_nodes:
            return False
        
        return component in self.partially_failed_nodes[node_id]
    
    def can_communicate(self, source_node: str, target_node: str) -> bool:
        """
        Check if two nodes can communicate with each other
        
        Args:
            source_node: ID of the source node
            target_node: ID of the target node
            
        Returns:
            True if nodes can communicate
        """
        # Check for complete failures
        if source_node in self.failed_nodes or target_node in self.failed_nodes:
            return False
        
        # Check for network failures
        if source_node in self.partially_failed_nodes and "network" in self.partially_failed_nodes[source_node]:
            network_failure = self.partially_failed_nodes[source_node]["network"]
            isolated_nodes = network_failure.get("isolated_nodes", [])
            if target_node in isolated_nodes:
                return False
        
        if target_node in self.partially_failed_nodes and "network" in self.partially_failed_nodes[target_node]:
            network_failure = self.partially_failed_nodes[target_node]["network"]
            isolated_nodes = network_failure.get("isolated_nodes", [])
            if source_node in isolated_nodes:
                return False
        
        return True
    
    def get_failure_status(self) -> Dict[str, Any]:
        """
        Get current failure status
        
        Returns:
            Dict with failure status
        """
        return {
            "failed_nodes": list(self.failed_nodes),
            "partially_failed_nodes": {
                node_id: {
                    component: info.get("failed_at", 0)
                    for component, info in components.items()
                }
                for node_id, components in self.partially_failed_nodes.items()
            },
            "scheduled_failures": len(self.scheduled_failures),
            "scheduled_recoveries": len(self.scheduled_recoveries)
        }
