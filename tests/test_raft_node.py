"""
tests/test_raft_node.py
======================

Unit tests for the Raft consensus protocol implementation.
"""

import pytest
import asyncio
import time
from unittest.mock import MagicMock, patch
from src.exapyte.consensus.raft_node import RaftNode, RaftState, LogEntry


@pytest.fixture
async def mock_storage():
    """Fixture for a mock storage engine"""
    storage = MagicMock()
    
    # Set up mock methods
    storage.save_raft_state = MagicMock(return_value=True)
    storage.load_raft_state = MagicMock(return_value=None)
    storage.save_log_entries = MagicMock(return_value=True)
    storage.load_log_entries = MagicMock(return_value=[])
    
    return storage


@pytest.fixture
async def mock_network():
    """Fixture for a mock network manager"""
    network = MagicMock()
    
    # Set up mock methods
    network.send_rpc = MagicMock(return_value={"success": True})
    network.register_handlers = MagicMock()
    
    return network


@pytest.fixture
async def raft_node(mock_storage, mock_network):
    """Fixture to create a RaftNode instance for testing"""
    # Create a simple cluster configuration
    cluster_config = {
        "nodes": {
            "node1": {"address": "localhost:8001"},
            "node2": {"address": "localhost:8002"},
            "node3": {"address": "localhost:8003"}
        }
    }
    
    # Create the node
    node = RaftNode("node1", cluster_config, mock_storage, mock_network)
    
    # Initialize but don't start
    await node.initialize()
    
    yield node
    
    # Clean up
    if node.running:
        await node.stop()


@pytest.mark.asyncio
async def test_raft_node_initialization(raft_node, mock_storage, mock_network):
    """Test that a RaftNode initializes correctly"""
    # Verify initial state
    assert raft_node.node_id == "node1"
    assert raft_node.state == RaftState.FOLLOWER
    assert raft_node.current_term == 0
    assert raft_node.voted_for is None
    assert len(raft_node.log) == 0
    
    # Verify storage was accessed
    mock_storage.load_raft_state.assert_called_once()
    mock_storage.load_log_entries.assert_called_once()
    
    # Verify network handlers were registered
    mock_network.register_handlers.assert_called_once()


@pytest.mark.asyncio
async def test_raft_become_follower(raft_node):
    """Test transition to follower state"""
    # Set up initial state
    raft_node.state = RaftState.CANDIDATE
    raft_node.current_term = 5
    raft_node.voted_for = "node2"
    
    # Transition to follower with higher term
    await raft_node.become_follower(10)
    
    # Verify state changes
    assert raft_node.state == RaftState.FOLLOWER
    assert raft_node.current_term == 10
    assert raft_node.voted_for is None


@pytest.mark.asyncio
async def test_raft_election_timeout(raft_node, monkeypatch):
    """Test that election timeout triggers candidacy"""
    # Mock the become_candidate method
    become_candidate_called = False
    
    async def mock_become_candidate():
        nonlocal become_candidate_called
        become_candidate_called = True
    
    monkeypatch.setattr(raft_node, "become_candidate", mock_become_candidate)
    
    # Set a short election timeout for testing
    raft_node.election_timeout_min = 0.01
    raft_node.election_timeout_max = 0.02
    
    # Start the node
    await raft_node.start()
    
    # Wait for election timeout to trigger
    await asyncio.sleep(0.05)
    
    # Stop the node
    await raft_node.stop()
    
    # Verify become_candidate was called
    assert become_candidate_called


@pytest.mark.asyncio
async def test_raft_become_candidate(raft_node, mock_network, monkeypatch):
    """Test transition to candidate state"""
    # Mock the random choice to ensure deterministic testing
    monkeypatch.setattr(asyncio, "sleep", lambda _: asyncio.Future().set_result(None))
    
    # Set up initial state
    raft_node.state = RaftState.FOLLOWER
    raft_node.current_term = 5
    raft_node.voted_for = None
    
    # Set up mock responses for RequestVote RPCs
    mock_network.send_rpc.return_value = {"term": 6, "vote_granted": True}
    
    # Transition to candidate
    await raft_node.become_candidate()
    
    # Verify state changes
    assert raft_node.state == RaftState.CANDIDATE
    assert raft_node.current_term == 6
    assert raft_node.voted_for == "node1"  # Voted for self
    
    # Verify RequestVote RPCs were sent to peers
    assert mock_network.send_rpc.call_count == 2  # Two peers


@pytest.mark.asyncio
async def test_raft_vote_counting(raft_node, mock_network, monkeypatch):
    """Test vote counting during election"""
    # Mock the become_leader method
    become_leader_called = False
    
    async def mock_become_leader():
        nonlocal become_leader_called
        become_leader_called = True
        raft_node.state = RaftState.LEADER
    
    monkeypatch.setattr(raft_node, "become_leader", mock_become_leader)
    monkeypatch.setattr(asyncio, "sleep", lambda _: asyncio.Future().set_result(None))
    
    # Set up initial state
    raft_node.state = RaftState.FOLLOWER
    raft_node.current_term = 5
    raft_node.voted_for = None
    
    # Set up mock responses for RequestVote RPCs
    # Both peers grant their votes
    mock_network.send_rpc.return_value = {"term": 6, "vote_granted": True}
    
    # Transition to candidate
    await raft_node.become_candidate()
    
    # Verify become_leader was called (received 3 votes: self + 2 peers)
    assert become_leader_called
    assert raft_node.state == RaftState.LEADER


@pytest.mark.asyncio
async def test_raft_become_leader(raft_node, mock_network):
    """Test transition to leader state"""
    # Set up initial state
    raft_node.state = RaftState.CANDIDATE
    raft_node.current_term = 6
    raft_node.voted_for = "node1"
    
    # Add some log entries
    raft_node.log = [
        LogEntry(term=1, command={"type": "set", "key": "foo", "value": "bar"}, index=0),
        LogEntry(term=2, command={"type": "set", "key": "baz", "value": "qux"}, index=1)
    ]
    
    # Transition to leader
    await raft_node.become_leader()
    
    # Verify state changes
    assert raft_node.state == RaftState.LEADER
    
    # Verify nextIndex and matchIndex initialized correctly
    for peer_id in raft_node.peers:
        assert raft_node.next_index[peer_id] == len(raft_node.log)
        assert raft_node.match_index[peer_id] == -1
    
    # Verify heartbeats were sent
    assert mock_network.send_rpc.called


@pytest.mark.asyncio
async def test_raft_step_down_on_higher_term(raft_node):
    """Test that node steps down when it sees a higher term"""
    # Set up initial state
    raft_node.state = RaftState.LEADER
    raft_node.current_term = 5
    
    # Simulate receiving a higher term
    request = {"term": 10, "candidate_id": "node2", "last_log_index": 0, "last_log_term": 0}
    response = await raft_node.handle_request_vote(request)
    
    # Verify state changes
    assert raft_node.state == RaftState.FOLLOWER
    assert raft_node.current_term == 10
    assert response["term"] == 10


@pytest.mark.asyncio
async def test_raft_append_entry(raft_node):
    """Test appending an entry to the log"""
    # Set up initial state
    raft_node.state = RaftState.LEADER
    raft_node.current_term = 5
    
    # Append an entry
    command = {"type": "set", "key": "test", "value": 42}
    result = await raft_node.append_entry(command)
    
    # Verify entry was added to log
    assert len(raft_node.log) == 1
    assert raft_node.log[0].term == 5
    assert raft_node.log[0].command == command
    
    # Verify result
    assert result["success"] is True


@pytest.mark.asyncio
async def test_raft_replicate_to_followers(raft_node, mock_network):
    """Test replication of entries to followers"""
    # Set up initial state
    raft_node.state = RaftState.LEADER
    raft_node.current_term = 5
    
    # Add an entry to replicate
    command = {"type": "set", "key": "test", "value": 42}
    await raft_node.append_entry(command)
    
    # Set up mock response for AppendEntries
    mock_network.send_rpc.return_value = {"term": 5, "success": True}
    
    # Trigger replication (normally done by heartbeat)
    await raft_node._send_heartbeats()
    
    # Verify AppendEntries RPCs were sent to peers
    assert mock_network.send_rpc.call_count >= 2  # At least one per peer
    
    # Verify matchIndex was updated for successful replication
    for peer_id in raft_node.peers:
        assert raft_node.match_index[peer_id] == 0  # First entry replicated


@pytest.mark.asyncio
async def test_raft_handle_append_entries(raft_node):
    """Test handling AppendEntries RPC"""
    # Set up initial state
    raft_node.state = RaftState.FOLLOWER
    raft_node.current_term = 5
    
    # Create an AppendEntries request
    entries = [
        {"term": 5, "command": {"type": "set", "key": "foo", "value": "bar"}, "index": 0}
    ]
    
    request = {
        "term": 5,
        "leader_id": "node2",
        "prev_log_index": -1,
        "prev_log_term": 0,
        "entries": entries,
        "leader_commit": -1
    }
    
    # Handle the request
    response = await raft_node.handle_append_entries(request)
    
    # Verify response
    assert response["success"] is True
    assert response["term"] == 5
    
    # Verify entry was added to log
    assert len(raft_node.log) == 1
    assert raft_node.log[0].term == 5
    assert raft_node.log[0].command == {"type": "set", "key": "foo", "value": "bar"}


@pytest.mark.asyncio
async def test_raft_update_commit_index(raft_node, mock_network):
    """Test updating commit index based on replication"""
    # Set up initial state
    raft_node.state = RaftState.LEADER
    raft_node.current_term = 5
    
    # Add an entry from current term
    command = {"type": "set", "key": "test", "value": 42}
    await raft_node.append_entry(command)
    
    # Set up matchIndex to simulate replication to majority
    for peer_id in raft_node.peers:
        raft_node.match_index[peer_id] = 0  # Entry replicated to all peers
    
    # Update commit index
    await raft_node._update_commit_index()
    
    # Verify commit index was updated
    assert raft_node.commit_index == 0


@pytest.mark.asyncio
async def test_raft_apply_committed_entries(raft_node, monkeypatch):
    """Test applying committed entries to state machine"""
    # Mock the _apply_command method
    applied_commands = []
    
    def mock_apply_command(command):
        applied_commands.append(command)
    
    monkeypatch.setattr(raft_node, "_apply_command", mock_apply_command)
    
    # Set up initial state
    raft_node.state = RaftState.LEADER
    raft_node.current_term = 5
    raft_node.commit_index = -1
    raft_node.last_applied = -1
    
    # Add an entry
    command = {"type": "set", "key": "test", "value": 42}
    await raft_node.append_entry(command)
    
    # Update commit index
    raft_node.commit_index = 0
    
    # Start the node to trigger apply loop
    await raft_node.start()
    
    # Wait for apply loop to process
    await asyncio.sleep(0.05)
    
    # Stop the node
    await raft_node.stop()
    
    # Verify command was applied
    assert len(applied_commands) == 1
    assert applied_commands[0] == command
    assert raft_node.last_applied == 0
