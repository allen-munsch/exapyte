"""
tests/test_disk_store_tdd.py
===========================

TDD-focused unit tests for the disk-based storage implementation.
"""

import pytest
import asyncio
import os
import shutil
import tempfile
import json
import time
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
        "compaction_interval": 0.5,  # Short interval for testing
        "max_transaction_log": 10    # Small size for testing
    }
    
    store = DiskStore(config)
    await store.start()
    
    yield store
    
    # Clean up
    await store.stop()
    shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_disk_store_initialization():
    """Test that a DiskStore initializes correctly with different configurations"""
    # Create a temporary directory
    temp_dir = tempfile.mkdtemp()
    
    try:
        # Default configuration
        store1 = DiskStore()
        assert store1.data_dir == "data"
        assert store1.sync_writes is True
        assert store1.compaction_interval == 300
        
        # Custom configuration
        config = {
            "data_dir": temp_dir,
            "sync_writes": False,
            "compaction_interval": 60,
            "max_transaction_log": 500
        }
        store2 = DiskStore(config)
        assert store2.config == config
        assert store2.data_dir == temp_dir
        assert store2.sync_writes is False
        assert store2.compaction_interval == 60
        assert store2.max_transaction_log == 500
    finally:
        # Clean up
        shutil.rmtree(temp_dir)


@pytest.mark.asyncio
async def test_disk_store_directory_creation(disk_store):
    """Test that the store creates necessary directories"""
    # Check that directories were created
    assert os.path.exists(disk_store.data_files_dir)
    assert os.path.exists(disk_store.transaction_log_dir)
    assert os.path.exists(disk_store.temp_dir)


@pytest.mark.asyncio
async def test_disk_store_set_and_get(disk_store):
    """Test setting and getting values with different data types"""
    # String value
    await disk_store.set("string_key", "string_value")
    assert await disk_store.get("string_key") == "string_value"
    
    # Integer value
    await disk_store.set("int_key", 42)
    assert await disk_store.get("int_key") == 42
    
    # Dictionary value
    dict_value = {"name": "test", "value": 42, "nested": {"a": 1}}
    await disk_store.set("dict_key", dict_value)
    assert await disk_store.get("dict_key") == dict_value
    
    # List value
    list_value = [1, 2, 3, "test", {"a": 1}]
    await disk_store.set("list_key", list_value)
    assert await disk_store.get("list_key") == list_value
    
    # None value
    await disk_store.set("none_key", None)
    assert await disk_store.get("none_key") is None
    
    # Get non-existent key
    assert await disk_store.get("nonexistent_key") is None


@pytest.mark.asyncio
async def test_disk_store_delete(disk_store):
    """Test deleting values"""
    # Set and delete a key
    await disk_store.set("delete_key", "delete_value")
    assert await disk_store.get("delete_key") == "delete_value"
    
    result = await disk_store.delete("delete_key")
    assert result is True
    assert await disk_store.get("delete_key") is None
    
    # Delete non-existent key
    result = await disk_store.delete("nonexistent_key")
    assert result is False


@pytest.mark.asyncio
async def test_disk_store_exists(disk_store):
    """Test checking if a key exists"""
    # Key should not exist initially
    assert await disk_store.exists("test_key") is False
    
    # Set the key and check again
    await disk_store.set("test_key", "test_value")
    assert await disk_store.exists("test_key") is True
    
    # Delete the key and check again
    await disk_store.delete("test_key")
    assert await disk_store.exists("test_key") is False


@pytest.mark.asyncio
async def test_disk_store_persistence(disk_store):
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
async def test_disk_store_transaction_log(disk_store):
    """Test that transaction log works correctly"""
    # Set some values
    await disk_store.set("txn_key1", "txn_value1")
    await disk_store.set("txn_key2", "txn_value2")
    
    # Check that transaction log files were created
    assert len(os.listdir(disk_store.transaction_log_dir)) > 0
    
    # Update a value
    await disk_store.set("txn_key1", "txn_value1_updated")
    
    # Delete a value
    await disk_store.delete("txn_key2")
    
    # Verify the final state
    assert await disk_store.get("txn_key1") == "txn_value1_updated"
    assert await disk_store.get("txn_key2") is None


@pytest.mark.asyncio
async def test_disk_store_compaction(disk_store):
    """Test that compaction works correctly"""
    # Set many values to trigger compaction
    for i in range(15):  # More than max_transaction_log
        await disk_store.set(f"compaction_key_{i}", f"value_{i}")
    
    # Wait for compaction to occur
    await asyncio.sleep(1.0)
    
    # Verify that transaction log was compacted
    assert disk_store.transaction_count == 0
    
    # Verify data is still accessible
    for i in range(15):
        assert await disk_store.get(f"compaction_key_{i}") == f"value_{i}"


@pytest.mark.asyncio
async def test_disk_store_recovery(disk_store):
    """Test recovery from transaction log"""
    # Set some values
    await disk_store.set("recovery_key1", "recovery_value1")
    await disk_store.set("recovery_key2", "recovery_value2")
    
    # Stop the store
    await disk_store.stop()
    
    # Create a new store instance with the same data directory
    config = disk_store.config.copy()
    new_store = DiskStore(config)
    
    try:
        # Start the new store, which should recover from transaction log
        await new_store.start()
        
        # Verify the values were recovered
        assert await new_store.get("recovery_key1") == "recovery_value1"
        assert await new_store.get("recovery_key2") == "recovery_value2"
    finally:
        # Clean up
        await new_store.stop()


@pytest.mark.asyncio
async def test_disk_store_get_many(disk_store):
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
async def test_disk_store_set_many(disk_store):
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
async def test_disk_store_delete_many(disk_store):
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
async def test_disk_store_clear(disk_store):
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
async def test_disk_store_count(disk_store):
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
async def test_disk_store_metrics(disk_store):
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
    assert "disk_reads" in metrics
    assert "cache_hits" in metrics
    assert "size" in metrics
