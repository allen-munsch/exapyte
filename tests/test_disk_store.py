"""
tests/test_disk_store.py
=======================

Unit tests for the disk-based storage implementation.
"""

import pytest
import asyncio
import os
import shutil
import tempfile
from src.exapyte.storage.disk_store import DiskStore


@pytest.fixture
async def disk_store():
    """Fixture to create and initialize a DiskStore instance for testing"""
    # Create a temporary directory for testing
    temp_dir = tempfile.mkdtemp()
    
    # Configure the store with the temp directory
    config = {
        "data_dir": temp_dir,
        "sync_writes": True,
        "compaction_interval": 0.5  # Short interval for testing
    }
    
    store = DiskStore(config)
    await store.start()
    
    yield store
    
    # Clean up
    await store.stop()
    shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_get_nonexistent_key(disk_store):
    """Test getting a non-existent key returns None"""
    result = await disk_store.get("nonexistent_key")
    assert result is None


@pytest.mark.asyncio
async def test_set_and_get(disk_store):
    """Test setting a value and then retrieving it"""
    test_key = "test_key"
    test_value = {"name": "test", "value": 42}
    
    # Set the value
    result = await disk_store.set(test_key, test_value)
    assert result is True
    
    # Get the value
    retrieved_value = await disk_store.get(test_key)
    assert retrieved_value == test_value


@pytest.mark.asyncio
async def test_delete(disk_store):
    """Test deleting a key"""
    test_key = "delete_test_key"
    test_value = "delete_test_value"
    
    # Set the value
    await disk_store.set(test_key, test_value)
    
    # Verify it exists
    assert await disk_store.get(test_key) == test_value
    
    # Delete it
    result = await disk_store.delete(test_key)
    assert result is True
    
    # Verify it's gone
    assert await disk_store.get(test_key) is None


@pytest.mark.asyncio
async def test_exists(disk_store):
    """Test checking if a key exists"""
    test_key = "exists_test_key"
    test_value = "exists_test_value"
    
    # Key should not exist initially
    assert await disk_store.exists(test_key) is False
    
    # Set the value
    await disk_store.set(test_key, test_value)
    
    # Key should exist now
    assert await disk_store.exists(test_key) is True
    
    # Delete the key
    await disk_store.delete(test_key)
    
    # Key should not exist again
    assert await disk_store.exists(test_key) is False


@pytest.mark.asyncio
async def test_get_many(disk_store):
    """Test retrieving multiple keys at once"""
    # Set multiple values
    test_data = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    }
    
    for key, value in test_data.items():
        await disk_store.set(key, value)
    
    # Get multiple values
    result = await disk_store.get_many(["key1", "key2", "nonexistent"])
    
    # Verify results
    assert len(result) == 2
    assert result["key1"] == "value1"
    assert result["key2"] == "value2"
    assert "nonexistent" not in result


@pytest.mark.asyncio
async def test_set_many(disk_store):
    """Test setting multiple keys at once"""
    test_data = {
        "batch_key1": "batch_value1",
        "batch_key2": "batch_value2",
        "batch_key3": "batch_value3"
    }
    
    # Set multiple values
    result = await disk_store.set_many(test_data)
    assert result is True
    
    # Verify all values were set
    for key, expected_value in test_data.items():
        actual_value = await disk_store.get(key)
        assert actual_value == expected_value


@pytest.mark.asyncio
async def test_delete_many(disk_store):
    """Test deleting multiple keys at once"""
    # Set multiple values
    keys = ["del_key1", "del_key2", "del_key3"]
    for key in keys:
        await disk_store.set(key, f"value_for_{key}")
    
    # Delete two of the keys
    deleted_count = await disk_store.delete_many(["del_key1", "del_key2", "nonexistent"])
    assert deleted_count == 2
    
    # Verify the keys were deleted
    assert await disk_store.get("del_key1") is None
    assert await disk_store.get("del_key2") is None
    assert await disk_store.get("del_key3") is not None


@pytest.mark.asyncio
async def test_clear(disk_store):
    """Test clearing all data"""
    # Set some values
    await disk_store.set("clear_key1", "clear_value1")
    await disk_store.set("clear_key2", "clear_value2")
    
    # Clear all data
    result = await disk_store.clear()
    assert result is True
    
    # Verify all data is gone
    assert await disk_store.get("clear_key1") is None
    assert await disk_store.get("clear_key2") is None
    assert await disk_store.count() == 0


@pytest.mark.asyncio
async def test_count(disk_store):
    """Test counting the number of items"""
    # Initially should be empty
    initial_count = await disk_store.count()
    
    # Add some items
    await disk_store.set("count_key1", "count_value1")
    await disk_store.set("count_key2", "count_value2")
    
    # Should have 2 more items now
    assert await disk_store.count() == initial_count + 2
    
    # Delete one item
    await disk_store.delete("count_key1")
    
    # Should have 1 more item than initial
    assert await disk_store.count() == initial_count + 1


@pytest.mark.asyncio
async def test_persistence(disk_store):
    """Test that data persists across store restarts"""
    # Set a value
    test_key = "persistence_key"
    test_value = {"persistent": True, "data": "test"}
    
    await disk_store.set(test_key, test_value)
    
    # Stop the store
    await disk_store.stop()
    
    # Start it again (same instance, same temp directory)
    await disk_store.start()
    
    # Verify the value is still there
    retrieved_value = await disk_store.get(test_key)
    assert retrieved_value == test_value


@pytest.mark.asyncio
async def test_transaction_log(disk_store):
    """Test that transaction log works correctly"""
    # Set some values
    await disk_store.set("txn_key1", "txn_value1")
    await disk_store.set("txn_key2", "txn_value2")
    
    # Update a value
    await disk_store.set("txn_key1", "txn_value1_updated")
    
    # Delete a value
    await disk_store.delete("txn_key2")
    
    # Verify the final state
    assert await disk_store.get("txn_key1") == "txn_value1_updated"
    assert await disk_store.get("txn_key2") is None
    
    # Force a compaction
    await disk_store._compact()
    
    # Verify state is still correct after compaction
    assert await disk_store.get("txn_key1") == "txn_value1_updated"
    assert await disk_store.get("txn_key2") is None


@pytest.mark.asyncio
async def test_metrics(disk_store):
    """Test that metrics are tracked correctly"""
    # Perform some operations
    await disk_store.get("nonexistent")
    await disk_store.set("metrics_key", "metrics_value")
    await disk_store.get("metrics_key")
    await disk_store.delete("metrics_key")
    
    # Get metrics
    metrics = await disk_store.get_metrics()
    
    # Verify metrics
    assert metrics["gets"] >= 2
    assert metrics["sets"] >= 1
    assert metrics["deletes"] >= 1
