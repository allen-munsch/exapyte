"""
tests/test_network_manager.py
===========================

Unit tests for the network manager implementation.
"""

import pytest
import asyncio
from unittest.mock import MagicMock, patch
from src.exapyte.networking.network_manager import NetworkManager


@pytest.fixture
def network_manager():
    """Fixture to create a NetworkManager instance for testing"""
    # Create a simple cluster configuration
    cluster_config = {
        "nodes": {
            "node1": {"address": "localhost:8001"},
            "node2": {"address": "localhost:8002"},
            "node3": {"address": "localhost:8003"}
        }
    }
    
    # Create the network manager in simulation mode
    manager = NetworkManager(
        node_id="node1",
        node_address="localhost:8001",
        cluster_config=cluster_config,
        simulation_mode=True
    )
    
    return manager


@pytest.mark.asyncio
async def test_network_manager_initialization(network_manager):
    """Test that a NetworkManager initializes correctly"""
    # Verify initial state
    assert network_manager.node_id == "node1"
    assert network_manager.node_address == "localhost:8001"
    assert network_manager.simulation_mode is True
    assert len(network_manager.rpc_handlers) == 0


@pytest.mark.asyncio
async def test_network_manager_register_handlers(network_manager):
    """Test registering RPC handlers"""
    # Create mock handlers
    handler1 = MagicMock()
    handler2 = MagicMock()
    
    # Register handlers
    handlers = {
        "test_rpc1": handler1,
        "test_rpc2": handler2
    }
    network_manager.register_handlers(handlers)
    
    # Verify handlers were registered
    assert "test_rpc1" in network_manager.rpc_handlers
    assert "test_rpc2" in network_manager.rpc_handlers
    assert network_manager.rpc_handlers["test_rpc1"] == handler1
    assert network_manager.rpc_handlers["test_rpc2"] == handler2


@pytest.mark.asyncio
async def test_network_manager_start_stop(network_manager):
    """Test starting and stopping the network manager"""
    # Start the network manager
    await network_manager.start()
    
    # Verify simulated network was created in simulation mode
    assert network_manager.simulated_network is not None
    
    # Stop the network manager
    await network_manager.stop()


@pytest.mark.asyncio
async def test_network_manager_send_rpc_simulation(network_manager):
    """Test sending RPC in simulation mode"""
    # Start the network manager
    await network_manager.start()
    
    # Mock the simulated network's deliver_message method
    expected_result = {"status": "ok"}
    network_manager.simulated_network.deliver_message = MagicMock(
        return_value=asyncio.Future()
    )
    network_manager.simulated_network.deliver_message.return_value.set_result(expected_result)
    
    # Send an RPC
    result = await network_manager.send_rpc(
        target_node_id="node2",
        rpc_type="test_rpc",
        payload={"test": "data"}
    )
    
    # Verify result
    assert result == expected_result
    
    # Verify deliver_message was called
    network_manager.simulated_network.deliver_message.assert_called_once()
    
    # Stop the network manager
    await network_manager.stop()


@pytest.mark.asyncio
async def test_handle_incoming_message(network_manager):
    """Test handling incoming messages in simulation mode"""
    # Create a mock handler
    handler = MagicMock(return_value={"status": "ok"})
    
    # Register the handler
    network_manager.register_handlers({"test_rpc": handler})
    
    # Start the network manager
    await network_manager.start()
    
    # Create a test message
    message = {
        "rpc_type": "test_rpc",
        "payload": {"test": "data"}
    }
    
    # Handle the message
    with patch('asyncio.run_coroutine_threadsafe') as mock_run:
        # Set up the mock to return a future with our result
        future = MagicMock()
        future.result.return_value = {"status": "ok"}
        mock_run.return_value = future
        
        # Call handle_incoming_message
        result = network_manager.handle_incoming_message("node2", message)
        
        # Verify result
        assert result == {"status": "ok"}
        
        # Verify handler was called via run_coroutine_threadsafe
        mock_run.assert_called_once()
    
    # Stop the network manager
    await network_manager.stop()


@pytest.mark.asyncio
async def test_network_manager_ping_handler(network_manager):
    """Test the built-in ping handler"""
    # Start the network manager
    await network_manager.start()
    
    # Get the ping handler
    ping_handler = network_manager.rpc_handlers.get("ping")
    assert ping_handler is not None
    
    # Call the ping handler
    result = await ping_handler({})
    
    # Verify result
    assert "status" in result
    assert result["status"] == "ok"
    assert "node_id" in result
    assert result["node_id"] == "node1"
    
    # Stop the network manager
    await network_manager.stop()


@pytest.mark.asyncio
async def test_network_manager_get_network_status(network_manager):
    """Test getting network status"""
    # Start the network manager
    await network_manager.start()
    
    # Get network status
    status = network_manager.get_network_status()
    
    # Verify status
    assert "node_id" in status
    assert status["node_id"] == "node1"
    assert "connected_nodes" in status
    assert "rpc_stats" in status
    
    # Stop the network manager
    await network_manager.stop()


@pytest.mark.asyncio
async def test_network_manager_simulate_network_partition(network_manager):
    """Test simulating a network partition"""
    # Start the network manager
    await network_manager.start()
    
    # Mock the simulated network's create_partition method
    network_manager.simulated_network.create_partition = MagicMock()
    
    # Simulate a network partition
    network_manager.simulate_network_partition(["node2"], 10.0)
    
    # Verify create_partition was called
    network_manager.simulated_network.create_partition.assert_called_once_with(
        "node1", ["node2"], 10.0
    )
    
    # Stop the network manager
    await network_manager.stop()
