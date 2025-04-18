"""
tests/test_protocol_factory.py
============================

Unit tests for the consensus protocol factory.
"""

import pytest
from src.exapyte.consensus.protocol_factory import ProtocolFactory, ConsensusType
from src.exapyte.consensus.raft_node import RaftNode
from src.exapyte.consensus.paxos_node import PaxosNode
from src.exapyte.consensus.zab_node import ZabNode


def test_create_raft_node():
    """Test creating a Raft consensus node"""
    # Create a simple cluster configuration
    cluster_config = {
        "nodes": {
            "node1": {"address": "localhost:8001"},
            "node2": {"address": "localhost:8002"},
            "node3": {"address": "localhost:8003"}
        }
    }
    
    # Create a Raft node
    node = ProtocolFactory.create_consensus_node(
        protocol_type=ConsensusType.RAFT,
        node_id="node1",
        cluster_config=cluster_config
    )
    
    # Verify node type
    assert isinstance(node, RaftNode)
    assert node.node_id == "node1"
    assert node.cluster_config == cluster_config


def test_create_paxos_node():
    """Test creating a Paxos consensus node"""
    # Create a simple cluster configuration
    cluster_config = {
        "nodes": {
            "node1": {"address": "localhost:8001"},
            "node2": {"address": "localhost:8002"},
            "node3": {"address": "localhost:8003"}
        }
    }
    
    # Create a Paxos node
    node = ProtocolFactory.create_consensus_node(
        protocol_type=ConsensusType.PAXOS,
        node_id="node1",
        cluster_config=cluster_config
    )
    
    # Verify node type
    assert isinstance(node, PaxosNode)
    assert node.node_id == "node1"
    assert node.cluster_config == cluster_config


def test_create_zab_node():
    """Test creating a ZAB consensus node"""
    # Create a simple cluster configuration
    cluster_config = {
        "nodes": {
            "node1": {"address": "localhost:8001"},
            "node2": {"address": "localhost:8002"},
            "node3": {"address": "localhost:8003"}
        }
    }
    
    # Create a ZAB node
    node = ProtocolFactory.create_consensus_node(
        protocol_type=ConsensusType.ZAB,
        node_id="node1",
        cluster_config=cluster_config
    )
    
    # Verify node type
    assert isinstance(node, ZabNode)
    assert node.node_id == "node1"
    assert node.cluster_config == cluster_config


def test_invalid_protocol_type():
    """Test that invalid protocol type raises ValueError"""
    with pytest.raises(ValueError):
        ProtocolFactory.create_consensus_node(
            protocol_type="invalid_type",
            node_id="node1",
            cluster_config={}
        )


def test_get_protocol_characteristics():
    """Test getting protocol characteristics"""
    raft_chars = ProtocolFactory.get_protocol_characteristics(ConsensusType.RAFT)
    paxos_chars = ProtocolFactory.get_protocol_characteristics(ConsensusType.PAXOS)
    zab_chars = ProtocolFactory.get_protocol_characteristics(ConsensusType.ZAB)
    
    # Verify Raft characteristics
    assert raft_chars["name"] == "Raft"
    assert "leadership" in raft_chars
    assert "election" in raft_chars
    
    # Verify Paxos characteristics
    assert paxos_chars["name"] == "Paxos"
    assert "leadership" in paxos_chars
    assert "election" in paxos_chars
    
    # Verify ZAB characteristics
    assert zab_chars["name"] == "ZooKeeper Atomic Broadcast (ZAB)"
    assert "leadership" in zab_chars
    assert "election" in zab_chars
    
    # Test invalid type
    with pytest.raises(ValueError):
        ProtocolFactory.get_protocol_characteristics("invalid_type")
