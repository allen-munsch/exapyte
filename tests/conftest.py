"""
tests/conftest.py
================

Common test fixtures and configuration for the exapyte test suite.
"""

import pytest
import asyncio
import logging

# Configure logging for tests
@pytest.fixture(scope="session", autouse=True)
def configure_logging():
    """Configure logging for tests"""
    logging.basicConfig(
        level=logging.INFO,
        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    )
    # Set lower log level for tests
    logging.getLogger().setLevel(logging.WARNING)
    
    # Specific loggers can be adjusted as needed
    logging.getLogger("exapyte").setLevel(logging.ERROR)

# Ensure event loop is properly handled for asyncio tests
@pytest.fixture(scope="session")
def event_loop():
    """Create an event loop for the test session"""
    loop = asyncio.get_event_loop_policy().new_event_loop()
    yield loop
    loop.close()
