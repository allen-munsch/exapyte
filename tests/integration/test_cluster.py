"""
tests/integration/test_cluster.py
================================

Integration tests for the exapyte cluster.
These tests run against a real multi-node deployment.
"""

import pytest
import asyncio
import time
import os
import random
import string
from typing import Dict, Any, List

from src.exapyte.services.client import ExabyteClient, create_client


# Skip these tests if not running in integration test mode
pytestmark = pytest.mark.skipif(
    os.environ.get("INTEGRATION_TESTS") != "1",
    reason="Integration tests only run when INTEGRATION_TESTS=1"
)


@pytest.fixture
async def client():
    """Fixture to create and connect a client to the cluster"""
    # Configure client to connect to the cluster
    config = {
        "endpoints": [
            "node1:8000",
            "node2:8000",
            "node3:8000"
        ],
        "default_keyspace": "default",
        "default_consistency": "eventual"
    }
    
    # Create and connect client
    client = await create_client(config)
    
    yield client
    
    # Disconnect client
    await client.disconnect()


@pytest.mark.asyncio
async def test_cluster_connection(client):
    """Test connecting to the cluster"""
    # Get cluster info
    cluster_info = await client.get_cluster_info()
    
    # Verify connection was successful
    assert cluster_info is not None
    assert "regions" in cluster_info
    assert "nodes" in cluster_info


@pytest.mark.asyncio
async def test_basic_operations(client):
    """Test basic operations (get, set, delete)"""
    # Generate a unique test key
    test_key = f"test_key_{int(time.time())}_{random.randint(1000, 9999)}"
    test_value = {"name": "integration_test", "value": 42}
    
    # Set value
    set_result = await client.set(test_key, test_value)
    assert set_result["success"] is True
    
    # Get value
    get_result = await client.get(test_key)
    assert get_result["success"] is True
    assert get_result["exists"] is True
    assert get_result["value"] == test_value
    
    # Delete value
    delete_result = await client.delete(test_key)
    assert delete_result["success"] is True
    
    # Verify deletion
    get_after_delete = await client.get(test_key)
    assert get_after_delete["exists"] is False


@pytest.mark.asyncio
async def test_consistency_levels(client):
    """Test different consistency levels"""
    # Generate unique test keys
    eventual_key = f"eventual_{int(time.time())}_{random.randint(1000, 9999)}"
    strong_key = f"strong_{int(time.time())}_{random.randint(1000, 9999)}"
    
    # Set with eventual consistency
    await client.set(
        eventual_key, 
        {"consistency": "eventual"},
        consistency="eventual"
    )
    
    # Set with strong consistency
    await client.set(
        strong_key, 
        {"consistency": "strong"},
        consistency="strong"
    )
    
    # Verify both values
    eventual_result = await client.get(eventual_key)
    strong_result = await client.get(strong_key)
    
    assert eventual_result["exists"] is True
    assert strong_result["exists"] is True


@pytest.mark.asyncio
async def test_batch_operations(client):
    """Test batch operations"""
    # Generate batch of test data
    batch_size = 5
    batch_keys = [f"batch_key_{i}_{int(time.time())}" for i in range(batch_size)]
    batch_data = {key: {"index": i, "value": f"batch_value_{i}"} for i, key in enumerate(batch_keys)}
    
    # Prepare batch operations
    operations = []
    for key, value in batch_data.items():
        operations.append({
            "type": "set",
            "key": key,
            "value": value
        })
    
    # Execute batch
    results = await client.batch(operations)
    
    # Verify all operations succeeded
    assert len(results) == batch_size
    for result in results:
        assert result["success"] is True
    
    # Verify values with individual gets
    for key, expected_value in batch_data.items():
        get_result = await client.get(key)
        assert get_result["exists"] is True
        assert get_result["value"] == expected_value


@pytest.mark.asyncio
async def test_concurrent_clients(client):
    """Test multiple clients operating concurrently"""
    # Number of concurrent clients
    num_clients = 3
    
    # Create additional clients
    clients = [client]  # Include the fixture client
    for i in range(1, num_clients):
        config = {
            "endpoints": [
                "node1:8000",
                "node2:8000",
                "node3:8000"
            ],
            "client_id": f"concurrent_client_{i}"
        }
        new_client = await create_client(config)
        clients.append(new_client)
    
    try:
        # Generate unique test key
        test_key = f"concurrent_key_{int(time.time())}"
        
        # Have each client perform operations
        async def client_operations(client_index, client):
            # Set value
            value = {"client": client_index, "timestamp": time.time()}
            await client.set(f"{test_key}_{client_index}", value)
            
            # Get value
            result = await client.get(f"{test_key}_{client_index}")
            return result["value"]
        
        # Run operations concurrently
        tasks = [client_operations(i, client) for i, client in enumerate(clients)]
        results = await asyncio.gather(*tasks)
        
        # Verify all operations completed
        assert len(results) == num_clients
        for i, result in enumerate(results):
            assert result["client"] == i
    
    finally:
        # Disconnect additional clients
        for i in range(1, len(clients)):
            await clients[i].disconnect()


@pytest.mark.asyncio
async def test_metrics(client):
    """Test retrieving metrics from the cluster"""
    # Get metrics
    metrics = await client.get_metrics()
    
    # Verify metrics structure
    assert "client" in metrics
    assert "cluster" in metrics
    
    # Verify client metrics
    client_metrics = metrics["client"]
    assert "requests" in client_metrics
    
    # Verify cluster metrics (if available)
    if "cluster" in metrics and metrics["cluster"]:
        cluster_metrics = metrics["cluster"]
        assert isinstance(cluster_metrics, dict)
