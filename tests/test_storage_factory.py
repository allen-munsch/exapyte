"""
tests/test_storage_factory.py
============================

Unit tests for the storage factory implementation.
"""

import pytest
from src.exapyte.storage.storage_factory import StorageFactory, StorageType
from src.exapyte.storage.memory_store import MemoryStore
from src.exapyte.storage.disk_store import DiskStore


def test_create_memory_storage():
    """Test creating a memory storage instance"""
    config = {"max_size": 1000}
    storage = StorageFactory.create_storage(StorageType.MEMORY, config)
    
    assert isinstance(storage, MemoryStore)
    assert storage.config == config
    assert storage.max_size == 1000


def test_create_disk_storage():
    """Test creating a disk storage instance"""
    config = {"data_dir": "/tmp/test"}
    storage = StorageFactory.create_storage(StorageType.DISK, config)
    
    assert isinstance(storage, DiskStore)
    assert storage.config == config
    assert storage.data_dir == "/tmp/test"


def test_invalid_storage_type():
    """Test that invalid storage type raises ValueError"""
    with pytest.raises(ValueError):
        StorageFactory.create_storage("invalid_type")


def test_get_storage_characteristics():
    """Test getting storage characteristics"""
    memory_chars = StorageFactory.get_storage_characteristics(StorageType.MEMORY)
    disk_chars = StorageFactory.get_storage_characteristics(StorageType.DISK)
    tiered_chars = StorageFactory.get_storage_characteristics(StorageType.TIERED)
    
    # Verify memory characteristics
    assert memory_chars["name"] == "In-Memory Storage"
    assert "persistence" in memory_chars
    assert "performance" in memory_chars
    
    # Verify disk characteristics
    assert disk_chars["name"] == "Disk-Based Storage"
    assert "persistence" in disk_chars
    assert "performance" in disk_chars
    
    # Verify tiered characteristics
    assert tiered_chars["name"] == "Tiered Storage"
    assert "persistence" in tiered_chars
    assert "performance" in tiered_chars
    
    # Test invalid type
    with pytest.raises(ValueError):
        StorageFactory.get_storage_characteristics("invalid_type")
