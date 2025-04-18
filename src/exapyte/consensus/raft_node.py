"""
src/exapyte/consensus/raft_node.py
==================================

Implementation of the Raft consensus protocol for exabyte-scale distributed systems.
This is a practical implementation focused on performance and reliability.
"""

import asyncio
import logging
import random
import time
import json
from enum import Enum
from typing import Dict, List, Optional, Any, Set

class RaftState(Enum):
    """Possible states for a Raft node"""
    FOLLOWER = "follower"
    CANDIDATE = "candidate"
    LEADER = "leader"

class LogEntry:
    """An entry in the Raft log"""
    
    def __init__(self, term: int, command: Any, index: int = None):
        self.term = term
        self.command = command
        self.index = index
    
    def to_dict(self):
        return {
            "term": self.term,
            "command": self.command,
            "index": self.index
        }
    
    @classmethod
    def from_dict(cls, data):
        return cls(
            term=data["term"],
            command=data["command"],
            index=data["index"]
        )
    
    def __repr__(self):
        return f"LogEntry(term={self.term}, index={self.index})"

class RaftNode:
    """
    Practical implementation of a Raft consensus node.
    
    This implementation focuses on real-world requirements:
    - Persistent storage of state
    - Network communication between nodes
    - Optimized leadership and replication
    - Handling of large clusters efficiently
    """
    
    def __init__(self, node_id, cluster_config, storage_engine=None, network_manager=None):
        self.node_id = node_id
        self.cluster_config = cluster_config
        self.storage_engine = storage_engine
        self.network_manager = network_manager
        
        # Set up logging
        self.logger = logging.getLogger(f"raft.{node_id}")
        
        # Cluster state
        self.peers = {id: config for id, config in cluster_config["nodes"].items() 
                      if id != self.node_id}
        
        # Raft state
        self.state = RaftState.FOLLOWER
        self.current_term = 0
        self.voted_for = None
        self.log = []
        
        # Volatile state
        self.commit_index = -1
        self.last_applied = -1
        
        # Leader state
        self.next_index = {}
        self.match_index = {}
        
        # Timeouts (in milliseconds)
        self.election_timeout_min = 150
        self.election_timeout_max = 300
        self.heartbeat_interval = 50
        
        # Runtime components
        self.election_timer = None
        self.heartbeat_timer = None
        self.running = False
        
        # Command execution tracking
        self.pending_commands = {}
        self.command_id_counter = 0
        
        # State machine
        self.state_machine = {}
    
    async def initialize(self):
        """Initialize the node, loading persistent state if available"""
        self.logger.info(f"Initializing Raft node {self.node_id}")
        
        if self.storage_engine:
            # Load persistent state
            try:
                persistent_state = await self.storage_engine.load_raft_state()
                if persistent_state:
                    self.current_term = persistent_state.get("current_term", 0)
                    self.voted_for = persistent_state.get("voted_for")
                    
                    # Load log entries
                    log_data = await self.storage_engine.load_log_entries()
                    if log_data:
                        self.log = [LogEntry.from_dict(entry) for entry in log_data]
                    
                    self.logger.info(f"Loaded persistent state: term={self.current_term}, "
                                     f"voted_for={self.voted_for}, log_entries={len(self.log)}")
            except Exception as e:
                self.logger.error(f"Failed to load persistent state: {e}")
        
        # Initialize network manager if provided
        if self.network_manager:
            self.network_manager.register_handlers({
                "request_vote": self.handle_request_vote,
                "append_entries": self.handle_append_entries
            })
    
    async def start(self):
        """Start the Raft node"""
        self.logger.info(f"Starting Raft node {self.node_id}")
        self.running = True
        
        # Start as follower
        await self.become_follower(self.current_term)
        
        # Start log application task
        asyncio.create_task(self._apply_log_entries())
    
    async def stop(self):
        """Stop the Raft node"""
        self.logger.info(f"Stopping Raft node {self.node_id}")
        self.running = False
        
        # Cancel timers
        if self.election_timer:
            self.election_timer.cancel()
        
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
    
    async def become_follower(self, term):
        """Transition to follower state"""
        if term > self.current_term:
            self.current_term = term
            self.voted_for = None
            # Persist state change
            await self._persist_state()
        
        was_leader = (self.state == RaftState.LEADER)
        self.state = RaftState.FOLLOWER
        
        # Cancel leader heartbeat if was leader
        if was_leader and self.heartbeat_timer:
            self.heartbeat_timer.cancel()
            self.heartbeat_timer = None
        
        # Reset election timeout
        self._reset_election_timer()
        
        self.logger.info(f"Became follower for term {self.current_term}")
    
    async def become_candidate(self):
        """Transition to candidate state and start election"""
        self.state = RaftState.CANDIDATE
        self.current_term += 1
        self.voted_for = self.node_id  # Vote for self
        
        # Persist state change
        await self._persist_state()
        
        # Reset election timer
        self._reset_election_timer()
        
        self.logger.info(f"Became candidate for term {self.current_term}")
        
        # Initialize vote count (vote for self)
        votes_received = 1
        votes_needed = len(self.peers) // 2 + 1 + 1  # Majority including self
        
        # Prepare vote request
        last_log_index = len(self.log) - 1
        last_log_term = self.log[last_log_index].term if last_log_index >= 0 else 0
        
        # Request votes from all peers
        vote_futures = []
        for peer_id in self.peers:
            request = {
                "term": self.current_term,
                "candidate_id": self.node_id,
                "last_log_index": last_log_index,
                "last_log_term": last_log_term
            }
            
            future = self.network_manager.send_rpc(peer_id, "request_vote", request)
            vote_futures.append((peer_id, future))
        
        # Wait for responses or until we win or lose
        for peer_id, future in vote_futures:
            try:
                # Short timeout to avoid waiting for disconnected nodes
                response = await asyncio.wait_for(future, timeout=0.5)
                
                # Check if we're still a candidate
                if self.state != RaftState.CANDIDATE:
                    return
                
                # Check if we learned of a higher term
                term = response.get("term", 0)
                if term > self.current_term:
                    self.logger.info(f"Discovered higher term {term} from {peer_id}")
                    await self.become_follower(term)
                    return
                
                # Count vote if granted
                if response.get("vote_granted", False):
                    votes_received += 1
                    self.logger.debug(f"Received vote from {peer_id}, now have {votes_received}/{votes_needed}")
                    
                    # Check if we won the election
                    if votes_received >= votes_needed:
                        await self.become_leader()
                        return
            except asyncio.TimeoutError:
                self.logger.warning(f"Request vote to {peer_id} timed out")
            except Exception as e:
                self.logger.error(f"Error requesting vote from {peer_id}: {e}")
    
    async def become_leader(self):
        """Transition to leader state"""
        if self.state != RaftState.CANDIDATE:
            return
        
        self.state = RaftState.LEADER
        
        # Initialize leader state
        self.next_index = {peer_id: len(self.log) for peer_id in self.peers}
        self.match_index = {peer_id: -1 for peer_id in self.peers}
        
        # Cancel election timer
        if self.election_timer:
            self.election_timer.cancel()
            self.election_timer = None
        
        self.logger.info(f"Became leader for term {self.current_term}")
        
        # Immediately send heartbeats
        await self._send_heartbeats()
        
        # Start heartbeat timer
        self._start_heartbeat_timer()
        
        # Append a no-op entry to establish leadership
        await self.append_entry({"type": "no-op"})
    
    def _reset_election_timer(self):
        """Reset the election timeout with randomized delay"""
        if self.election_timer:
            self.election_timer.cancel()
        
        # Randomize timeout to prevent split votes
        # Convert to integers for randint if they're floats
        min_ms = int(self.election_timeout_min) if isinstance(self.election_timeout_min, float) else self.election_timeout_min
        max_ms = int(self.election_timeout_max) if isinstance(self.election_timeout_max, float) else self.election_timeout_max
        
        timeout_ms = random.randint(min_ms, max_ms)
        timeout_sec = timeout_ms / 1000
        
        self.election_timer = asyncio.create_task(self._election_timeout(timeout_sec))
    
    async def _election_timeout(self, timeout):
        """Handle election timeout"""
        await asyncio.sleep(timeout)
        
        if not self.running:
            return
        
        if self.state == RaftState.LEADER:
            # Leader doesn't need election timer
            return
        
        self.logger.info(f"Election timeout triggered for term {self.current_term}")
        await self.become_candidate()
    
    def _start_heartbeat_timer(self):
        """Start the heartbeat timer for leaders"""
        if self.heartbeat_timer:
            self.heartbeat_timer.cancel()
        
        heartbeat_sec = self.heartbeat_interval / 1000
        self.heartbeat_timer = asyncio.create_task(self._heartbeat_timeout(heartbeat_sec))
    
    async def _heartbeat_timeout(self, timeout):
        """Handle heartbeat timeout"""
        await asyncio.sleep(timeout)
        
        if not self.running or self.state != RaftState.LEADER:
            return
        
        # Send heartbeats
        await self._send_heartbeats()
        
        # Restart timer
        self._start_heartbeat_timer()
    
    async def _send_heartbeats(self):
        """Send AppendEntries RPCs to all peers (heartbeats if no entries)"""
        if self.state != RaftState.LEADER:
            return
        
        # Create tasks for each peer
        tasks = []
        for peer_id in self.peers:
            task = asyncio.create_task(self._replicate_to_peer(peer_id))
            tasks.append(task)
        
        # Wait for all tasks to complete
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
            
        # Check if we can advance commit index
        await self._update_commit_index()
    
    async def _replicate_to_peer(self, peer_id):
        """Replicate log entries to a single peer"""
        if self.state != RaftState.LEADER:
            return
        
        next_idx = self.next_index[peer_id]
        prev_log_index = next_idx - 1
        prev_log_term = 0
        
        if prev_log_index >= 0 and prev_log_index < len(self.log):
            prev_log_term = self.log[prev_log_index].term
        
        # Get entries to send
        entries = []
        if next_idx < len(self.log):
            entries = [entry.to_dict() for entry in self.log[next_idx:]]
            
            # Limit batch size for large logs (could be tuned based on network/cluster size)
            max_batch = 100  # Configurable parameter
            if len(entries) > max_batch:
                entries = entries[:max_batch]
        
        # Create AppendEntries request
        request = {
            "term": self.current_term,
            "leader_id": self.node_id,
            "prev_log_index": prev_log_index,
            "prev_log_term": prev_log_term,
            "entries": entries,
            "leader_commit": self.commit_index
        }
        
        try:
            # Send AppendEntries RPC
            response = await self.network_manager.send_rpc(peer_id, "append_entries", request)
            
            # Process response
            if not self.running or self.state != RaftState.LEADER:
                return
            
            # Check for higher term
            term = response.get("term", 0)
            if term > self.current_term:
                self.logger.info(f"Discovered higher term {term} from {peer_id}")
                await self.become_follower(term)
                return
            
            # Update peer progress
            if response.get("success", False):
                if entries:
                    # Update match_index and next_index
                    self.match_index[peer_id] = prev_log_index + len(entries)
                    self.next_index[peer_id] = self.match_index[peer_id] + 1
                    
                    self.logger.debug(f"Successfully replicated {len(entries)} entries to {peer_id}, "
                                     f"match_index = {self.match_index[peer_id]}")
            else:
                # Log consistency check failed, decrement next_index and retry
                
                # Check if peer provided hint about conflict
                conflict_index = response.get("conflict_index")
                
                if conflict_index is not None:
                    # Use conflict index hint for faster recovery
                    self.next_index[peer_id] = conflict_index
                else:
                    # Fall back to simple decrement
                    self.next_index[peer_id] = max(0, self.next_index[peer_id] - 1)
                
                self.logger.debug(f"AppendEntries failed for {peer_id}, next_index = {self.next_index[peer_id]}")
                
        except Exception as e:
            self.logger.warning(f"Failed to replicate to {peer_id}: {e}")
    
    async def _update_commit_index(self):
        """Update commit index based on match indices from peers"""
        if self.state != RaftState.LEADER:
            return
        
        # Check each index, starting from highest and moving down
        for n in range(len(self.log) - 1, self.commit_index, -1):
            # Can only commit entries from current term (safety guarantee)
            if self.log[n].term != self.current_term:
                continue
            
            # Count replications (including self)
            replicated_count = 1  # Leader has the entry
            for peer_id in self.peers:
                if self.match_index.get(peer_id, -1) >= n:
                    replicated_count += 1
            
            # If majority, commit this entry
            if replicated_count > (len(self.peers) + 1) // 2:
                self.logger.info(f"Advancing commit index from {self.commit_index} to {n}")
                self.commit_index = n
                
                # Resolve any pending commands that are now committed
                self._resolve_pending_commands()
                
                # No need to check lower indices
                break
    
    async def _apply_log_entries(self):
        """Apply committed log entries to the state machine"""
        while self.running:
            # Check if there are new entries to apply
            if self.commit_index > self.last_applied:
                # Apply all committed but unapplied entries
                next_to_apply = self.last_applied + 1
                while next_to_apply <= self.commit_index and next_to_apply < len(self.log):
                    entry = self.log[next_to_apply]
                    
                    # Apply the command to state machine
                    self._apply_command(entry.command)
                    
                    # Update last_applied
                    self.last_applied = next_to_apply
                    self.logger.debug(f"Applied entry {next_to_apply} to state machine")
                    
                    next_to_apply += 1
            
            # Wait before checking again
            await asyncio.sleep(0.01)
    
    def _apply_command(self, command):
        """Apply a command to the state machine"""
        cmd_type = command.get("type", "")
        
        if cmd_type == "no-op":
            # No-op commands are used to commit previous entries
            pass
        elif cmd_type == "set":
            # Set command: {"type": "set", "key": "foo", "value": "bar"}
            key = command.get("key")
            value = command.get("value")
            
            if key is not None:
                self.state_machine[key] = value
        elif cmd_type == "delete":
            # Delete command: {"type": "delete", "key": "foo"}
            key = command.get("key")
            
            if key and key in self.state_machine:
                del self.state_machine[key]
        elif cmd_type == "config_change":
            # Handle configuration changes
            self._apply_config_change(command)
    
    def _apply_config_change(self, command):
        """Apply a configuration change command"""
        change_type = command.get("change_type")
        node_id = command.get("node_id")
        endpoint = command.get("endpoint")
        
        if change_type == "add_node" and node_id:
            # Add a new node to the cluster
            self.cluster_config["nodes"][node_id] = endpoint
            
            # If we're the leader, update our peer set
            if self.state == RaftState.LEADER and node_id != self.node_id:
                self.peers[node_id] = endpoint
                self.next_index[node_id] = len(self.log)
                self.match_index[node_id] = -1
                
        elif change_type == "remove_node" and node_id:
            # Remove a node from the cluster
            if node_id in self.cluster_config["nodes"]:
                del self.cluster_config["nodes"][node_id]
            
            # Update leader state if applicable
            if self.state == RaftState.LEADER and node_id in self.peers:
                del self.peers[node_id]
                
                if node_id in self.next_index:
                    del self.next_index[node_id]
                
                if node_id in self.match_index:
                    del self.match_index[node_id]
    
    def _resolve_pending_commands(self):
        """Resolve pending commands that have been committed"""
        completed_cmds = []
        
        for cmd_id, cmd_info in self.pending_commands.items():
            if cmd_info["log_index"] <= self.commit_index:
                # Command is committed
                future = cmd_info["future"]
                if not future.done():
                    future.set_result({
                        "success": True,
                        "index": cmd_info["log_index"],
                        "term": cmd_info["term"]
                    })
                completed_cmds.append(cmd_id)
        
        # Remove completed commands
        for cmd_id in completed_cmds:
            del self.pending_commands[cmd_id]
    
    async def handle_request_vote(self, request):
        """Handle a RequestVote RPC from a candidate"""
        term = request.get("term", 0)
        candidate_id = request.get("candidate_id")
        last_log_index = request.get("last_log_index", -1)
        last_log_term = request.get("last_log_term", 0)
        
        self.logger.debug(f"Received vote request from {candidate_id} for term {term}")
        
        # If term > currentTerm, update currentTerm
        if term > self.current_term:
            await self.become_follower(term)
        
        # Decide whether to grant vote
        vote_granted = False
        
        if term < self.current_term:
            # Reject if candidate's term is outdated
            vote_granted = False
        elif self.voted_for is not None and self.voted_for != candidate_id:
            # Already voted for someone else in this term
            vote_granted = False
        else:
            # Check if candidate's log is at least as up-to-date as ours
            my_last_index = len(self.log) - 1
            my_last_term = self.log[my_last_index].term if my_last_index >= 0 else 0
            
            log_ok = False
            if last_log_term > my_last_term:
                log_ok = True
            elif last_log_term == my_last_term and last_log_index >= my_last_index:
                log_ok = True
                
            if log_ok:
                # Grant vote
                vote_granted = True
                self.voted_for = candidate_id
                await self._persist_state()
                
                # Reset election timer (important for liveness)
                self._reset_election_timer()
                
                self.logger.info(f"Granted vote to {candidate_id} for term {term}")
        
        return {
            "term": self.current_term,
            "vote_granted": vote_granted
        }
    
    async def handle_append_entries(self, request):
        """Handle an AppendEntries RPC from the leader"""
        term = request.get("term", 0)
        leader_id = request.get("leader_id")
        prev_log_index = request.get("prev_log_index", -1)
        prev_log_term = request.get("prev_log_term", 0)
        entries = request.get("entries", [])
        leader_commit = request.get("leader_commit", -1)
        
        # Check if request is from legitimate leader
        if term < self.current_term:
            return {
                "term": self.current_term,
                "success": False
            }
        
        # Valid leader message, reset election timeout
        self._reset_election_timer()
        
        # If leader's term is newer, update our term
        if term > self.current_term or self.state != RaftState.FOLLOWER:
            await self.become_follower(term)
        
        # Check log consistency
        log_ok = True
        conflict_index = None
        
        if prev_log_index >= 0:
            if prev_log_index >= len(self.log):
                # Our log is too short
                log_ok = False
                conflict_index = len(self.log)
            elif self.log[prev_log_index].term != prev_log_term:
                # Term mismatch at prev_log_index
                log_ok = False
                
                # Find conflict term and earliest entry with that term
                conflict_term = self.log[prev_log_index].term
                conflict_index = prev_log_index
                
                # Scan backwards to find first entry with this term
                for i in range(prev_log_index - 1, -1, -1):
                    if self.log[i].term != conflict_term:
                        conflict_index = i + 1
                        break
        
        if not log_ok:
            return {
                "term": self.current_term,
                "success": False,
                "conflict_index": conflict_index
            }
        
        # Process entries
        if entries:
            # If existing entry conflicts with new entry, delete it and all that follow
            insert_index = prev_log_index + 1
            
            # Truncate conflicting entries
            if insert_index < len(self.log):
                for i, entry_dict in enumerate(entries):
                    idx = insert_index + i
                    if idx >= len(self.log):
                        break
                    
                    if self.log[idx].term != entry_dict["term"]:
                        # Found conflict, truncate from here
                        self.log = self.log[:idx]
                        break
            
            # Append new entries
            for i, entry_dict in enumerate(entries):
                idx = insert_index + i
                if idx < len(self.log):
                    # Skip entries already in log (at same term)
                    continue
                
                # Append new entry
                entry = LogEntry.from_dict(entry_dict)
                entry.index = idx  # Ensure index is correct
                self.log.append(entry)
            
            # Persist log changes
            await self._persist_log()
        
        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)
        
        return {
            "term": self.current_term,
            "success": True
        }
    
    async def append_entry(self, command):
        """
        Append a new entry to the log (leader only)
        
        Args:
            command: The command to append
            
        Returns:
            dict: Result with success status and info
        """
        if self.state != RaftState.LEADER:
            return {
                "success": False,
                "error": "not_leader",
                "leader_id": None  # In real implementation, would redirect
            }
        
        # Create log entry
        entry = LogEntry(
            term=self.current_term,
            command=command,
            index=len(self.log)
        )
        
        # Append to log
        self.log.append(entry)
        
        # Persist log change
        await self._persist_log()
        
        # Create future for tracking completion
        future = asyncio.Future()
        
        # Track operation
        cmd_id = self.command_id_counter
        self.command_id_counter += 1
        
        self.pending_commands[cmd_id] = {
            "log_index": entry.index,
            "term": entry.term,
            "future": future
        }
        
        # Replicate to followers
        await self._send_heartbeats()
        
        # Wait for completion or timeout
        try:
            result = await asyncio.wait_for(future, timeout=5.0)
            return result
        except asyncio.TimeoutError:
            # Operation timed out
            if cmd_id in self.pending_commands:
                del self.pending_commands[cmd_id]
            return {
                "success": False,
                "error": "timeout"
            }
    
    async def propose_command(self, command):
        """
        Propose a command to the distributed system
        
        Args:
            command: Command to propose (e.g., {"type": "set", "key": "foo", "value": "bar"})
            
        Returns:
            dict: Result with success status and info
        """
        if self.state != RaftState.LEADER:
            # Not the leader, reject command
            return {
                "success": False,
                "error": "not_leader",
                "leader_id": None  # Would include leader ID in real implementation
            }
        
        # Append to log and replicate
        return await self.append_entry(command)
    
    async def read_state_machine(self, key):
        """
        Read a value from the state machine
        
        For linearizable reads, commit a no-op first if leader
        
        Args:
            key: Key to read
            
        Returns:
            dict: Result with value if found
        """
        # For linearizable reads, leader must ensure it's still the leader
        # by committing a no-op entry first
        if self.state == RaftState.LEADER:
            await self.append_entry({"type": "no-op"})
        
        # Now read from state machine
        value = self.state_machine.get(key)
        
        return {
            "success": True,
            "key": key,
            "value": value,
            "exists": key in self.state_machine
        }
    
    async def add_server(self, node_id, endpoint):
        """
        Add a new server to the cluster
        
        Args:
            node_id: ID of the new server
            endpoint: Network endpoint for the server
            
        Returns:
            dict: Result with success status
        """
        if self.state != RaftState.LEADER:
            return {
                "success": False,
                "error": "not_leader"
            }
        
        # Create configuration change command
        command = {
            "type": "config_change",
            "change_type": "add_node",
            "node_id": node_id,
            "endpoint": endpoint
        }
        
        # Append to log and replicate
        result = await self.append_entry(command)
        return result
    
    async def remove_server(self, node_id):
        """
        Remove a server from the cluster
        
        Args:
            node_id: ID of the server to remove
            
        Returns:
            dict: Result with success status
        """
        if self.state != RaftState.LEADER:
            return {
                "success": False,
                "error": "not_leader"
            }
        
        # Create configuration change command
        command = {
            "type": "config_change",
            "change_type": "remove_node",
            "node_id": node_id
        }
        
        # Append to log and replicate
        result = await self.append_entry(command)
        return result
    
    async def _persist_state(self):
        """Persist Raft state to storage"""
        if not self.storage_engine:
            return
        
        state = {
            "current_term": self.current_term,
            "voted_for": self.voted_for
        }
        
        try:
            # Handle both async and sync storage engines
            result = self.storage_engine.save_raft_state(state)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            self.logger.error(f"Failed to persist Raft state: {e}")
    
    async def _persist_log(self):
        """Persist log entries to storage"""
        if not self.storage_engine:
            return
        
        try:
            log_data = [entry.to_dict() for entry in self.log]
            # Handle both async and sync storage engines
            result = self.storage_engine.save_log_entries(log_data)
            if asyncio.iscoroutine(result):
                await result
        except Exception as e:
            self.logger.error(f"Failed to persist log entries: {e}")
