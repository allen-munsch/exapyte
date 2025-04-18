"""
tests/test_memory_store_tdd.py
=============================

TDD-focused unit tests for the in-memory storage implementation.
"""

import pytest
import asyncio
import time
from src.exapyte.storage.memory_store import MemoryStore


@pytest.fixture
async def memory_store():
    """Fixture to create and initialize a MemoryStore instance for testing"""
    store = MemoryStore()
    await store.start()
    yield store
    await store.stop()


@pytest.mark.asyncio
async def test_memory_store_initialization():
    """Test that a MemoryStore initializes correctly with different configurations"""
    # Default configuration
    store1 = MemoryStore()
    assert store1.config == {}
    assert store1.max_size is None
    assert store1.eviction_policy == "LRU"
    
    # Custom configuration
    config = {
        "max_size": 500,
        "eviction_policy": "LFU",
        "default_ttl": 60
    }
    store2 = MemoryStore(config)
    assert store2.config == config
    assert store2.max_size == 500
    assert store2.eviction_policy == "LFU"
    assert store2.default_ttl == 60


@pytest.mark.asyncio
async def test_memory_store_set_and_get(memory_store):
    """Test setting and getting values with different data types"""
    # String value
    await memory_store.set("string_key", "string_value")
    assert await memory_store.get("string_key") == "string_value"
    
    # Integer value
    await memory_store.set("int_key", 42)
    assert await memory_store.get("int_key") == 42
    
    # Dictionary value
    dict_value = {"name": "test", "value": 42, "nested": {"a": 1}}
    await memory_store.set("dict_key", dict_value)
    assert await memory_store.get("dict_key") == dict_value
    
    # List value
    list_value = [1, 2, 3, "test", {"a": 1}]
    await memory_store.set("list_key", list_value)
    assert await memory_store.get("list_key") == list_value
    
    # None value
    await memory_store.set("none_key", None)
    assert await memory_store.get("none_key") is None
    
    # Get non-existent key
    assert await memory_store.get("nonexistent_key") is None


@pytest.mark.asyncio
async def test_memory_store_delete(memory_store):
    """Test deleting values"""
    # Set and delete a key
    await memory_store.set("delete_key", "delete_value")
    assert await memory_store.get("delete_key") == "delete_value"
    
    result = await memory_store.delete("delete_key")
    assert result is True
    assert await memory_store.get("delete_key") is None
    
    # Delete non-existent key
    result = await memory_store.delete("nonexistent_key")
    assert result is False


@pytest.mark.asyncio
async def test_memory_store_exists(memory_store):
    """Test checking if a key exists"""
    # Key should not exist initially
    assert await memory_store.exists("test_key") is False
    
    # Set the key and check again
    await memory_store.set("test_key", "test_value")
    assert await memory_store.exists("test_key") is True
    
    # Delete the key and check again
    await memory_store.delete("test_key")
    assert await memory_store.exists("test_key") is False


@pytest.mark.asyncio
async def test_memory_store_ttl_expiration(memory_store):
    """Test that keys with TTL expire correctly"""
    # Set a key with a short TTL (100ms)
    await memory_store.set("ttl_key", "ttl_value", ttl=0.1)
    
    # Key should exist initially
    assert await memory_store.get("ttl_key") == "ttl_value"
    
    # Wait for expiration
    await asyncio.sleep(0.2)
    
    # Key should be gone
    assert await memory_store.get("ttl_key") is None
    assert await memory_store.exists("ttl_key") is False


@pytest.mark.asyncio
async def test_memory_store_max_size_eviction(memory_store):
    """Test that keys are evicted when max size is reached"""
    # Create a store with small max size and LRU eviction
    store = MemoryStore({"max_size": 3, "eviction_policy": "LRU"})
    await store.start()
    
    try:
        # Fill the store
        await store.set("key1", "value1")
        await store.set("key2", "value2")
        await store.set("key3", "value3")
        
        # Verify all keys exist
        assert await store.get("key1") == "value1"
        assert await store.get("key2") == "value2"
        assert await store.get("key3") == "value3"
        
        # Access key1 to make it most recently used
        await store.get("key1")
        
        # Add another key, should evict key2 (least recently used)
        await store.set("key4", "value4")
        
        # Verify key2 was evicted
        assert await store.get("key2") is None
        
        # Verify other keys still exist
        assert await store.get("key1") == "value1"
        assert await store.get("key3") == "value3"
        assert await store.get("key4") == "value4"
    finally:
        await store.stop()


@pytest.mark.asyncio
async def test_memory_store_get_many(memory_store):
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
async def test_memory_store_set_many(memory_store):
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
async def test_memory_store_delete_many(memory_store):
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
async def test_memory_store_clear(memory_store):
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
async def test_memory_store_count(memory_store):
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
async def test_memory_store_metrics(memory_store):
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
    assert "hit_ratio" in metrics
