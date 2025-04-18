"""
tests/test_memory_store.py
=========================

Unit tests for the in-memory storage implementation.
"""

import pytest
import asyncio
from src.exapyte.storage.memory_store import MemoryStore


@pytest.fixture
async def memory_store():
    """Fixture to create and initialize a MemoryStore instance for testing"""
    store = MemoryStore()
    await store.start()
    yield store
    await store.stop()


@pytest.mark.asyncio
async def test_get_nonexistent_key(memory_store):
    """Test getting a non-existent key returns None"""
    result = await memory_store.get("nonexistent_key")
    assert result is None


@pytest.mark.asyncio
async def test_set_and_get(memory_store):
    """Test setting a value and then retrieving it"""
    test_key = "test_key"
    test_value = {"name": "test", "value": 42}
    
    # Set the value
    result = await memory_store.set(test_key, test_value)
    assert result is True
    
    # Get the value
    retrieved_value = await memory_store.get(test_key)
    assert retrieved_value == test_value


@pytest.mark.asyncio
async def test_delete(memory_store):
    """Test deleting a key"""
    test_key = "delete_test_key"
    test_value = "delete_test_value"
    
    # Set the value
    await memory_store.set(test_key, test_value)
    
    # Verify it exists
    assert await memory_store.get(test_key) == test_value
    
    # Delete it
    result = await memory_store.delete(test_key)
    assert result is True
    
    # Verify it's gone
    assert await memory_store.get(test_key) is None


@pytest.mark.asyncio
async def test_exists(memory_store):
    """Test checking if a key exists"""
    test_key = "exists_test_key"
    test_value = "exists_test_value"
    
    # Key should not exist initially
    assert await memory_store.exists(test_key) is False
    
    # Set the value
    await memory_store.set(test_key, test_value)
    
    # Key should exist now
    assert await memory_store.exists(test_key) is True
    
    # Delete the key
    await memory_store.delete(test_key)
    
    # Key should not exist again
    assert await memory_store.exists(test_key) is False


@pytest.mark.asyncio
async def test_get_many(memory_store):
    """Test retrieving multiple keys at once"""
    # Set multiple values
    test_data = {
        "key1": "value1",
        "key2": "value2",
        "key3": "value3"
    }
    
    for key, value in test_data.items():
        await memory_store.set(key, value)
    
    # Get multiple values
    result = await memory_store.get_many(["key1", "key2", "nonexistent"])
    
    # Verify results
    assert len(result) == 2
    assert result["key1"] == "value1"
    assert result["key2"] == "value2"
    assert "nonexistent" not in result


@pytest.mark.asyncio
async def test_set_many(memory_store):
    """Test setting multiple keys at once"""
    test_data = {
        "batch_key1": "batch_value1",
        "batch_key2": "batch_value2",
        "batch_key3": "batch_value3"
    }
    
    # Set multiple values
    result = await memory_store.set_many(test_data)
    assert result is True
    
    # Verify all values were set
    for key, expected_value in test_data.items():
        actual_value = await memory_store.get(key)
        assert actual_value == expected_value


@pytest.mark.asyncio
async def test_delete_many(memory_store):
    """Test deleting multiple keys at once"""
    # Set multiple values
    keys = ["del_key1", "del_key2", "del_key3"]
    for key in keys:
        await memory_store.set(key, f"value_for_{key}")
    
    # Delete two of the keys
    deleted_count = await memory_store.delete_many(["del_key1", "del_key2", "nonexistent"])
    assert deleted_count == 2
    
    # Verify the keys were deleted
    assert await memory_store.get("del_key1") is None
    assert await memory_store.get("del_key2") is None
    assert await memory_store.get("del_key3") is not None


@pytest.mark.asyncio
async def test_clear(memory_store):
    """Test clearing all data"""
    # Set some values
    await memory_store.set("clear_key1", "clear_value1")
    await memory_store.set("clear_key2", "clear_value2")
    
    # Clear all data
    result = await memory_store.clear()
    assert result is True
    
    # Verify all data is gone
    assert await memory_store.get("clear_key1") is None
    assert await memory_store.get("clear_key2") is None
    assert await memory_store.count() == 0


@pytest.mark.asyncio
async def test_count(memory_store):
    """Test counting the number of items"""
    # Initially should be empty
    assert await memory_store.count() == 0
    
    # Add some items
    await memory_store.set("count_key1", "count_value1")
    await memory_store.set("count_key2", "count_value2")
    
    # Should have 2 items now
    assert await memory_store.count() == 2
    
    # Delete one item
    await memory_store.delete("count_key1")
    
    # Should have 1 item now
    assert await memory_store.count() == 1


@pytest.mark.asyncio
async def test_ttl_expiration(memory_store):
    """Test that keys with TTL expire"""
    # Set a key with a short TTL
    await memory_store.set("ttl_key", "ttl_value", ttl=0.1)
    
    # Verify it exists initially
    assert await memory_store.get("ttl_key") == "ttl_value"
    
    # Wait for expiration
    await asyncio.sleep(0.2)
    
    # Verify it's gone
    assert await memory_store.get("ttl_key") is None


@pytest.mark.asyncio
async def test_metrics(memory_store):
    """Test that metrics are tracked correctly"""
    # Perform some operations
    await memory_store.get("nonexistent")  # miss
    await memory_store.set("metrics_key", "metrics_value")
    await memory_store.get("metrics_key")  # hit
    await memory_store.delete("metrics_key")
    
    # Get metrics
    metrics = await memory_store.get_metrics()
    
    # Verify metrics
    assert metrics["gets"] >= 2
    assert metrics["sets"] >= 1
    assert metrics["deletes"] >= 1
    assert metrics["hits"] >= 1
    assert metrics["misses"] >= 1
