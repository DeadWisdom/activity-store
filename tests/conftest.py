import asyncio
import pytest

from activity_store import ActivityStore
from activity_store.backends.memory import InMemoryStorageBackend
from activity_store.cache.memory import InMemoryCacheBackend


@pytest.fixture
def sample_ld_object():
    """Sample Activity Streams object for testing."""
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": "https://example.com/objects/123",
        "type": "Note",
        "content": "This is a test note",
        "published": "2023-01-01T00:00:00Z"
    }


@pytest.fixture
def sample_collection_object():
    """Sample Collection object for testing."""
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": "https://example.com/collections/notes",
        "type": "Collection",
        "name": "Notes Collection",
        "totalItems": 0,
        "items": []
    }


@pytest.fixture
def mock_storage_backend():
    """Create a fresh in-memory storage backend for testing."""
    return InMemoryStorageBackend()


@pytest.fixture
def mock_cache_backend():
    """Create a fresh in-memory cache backend for testing."""
    return InMemoryCacheBackend()


@pytest.fixture
async def activity_store(mock_storage_backend, mock_cache_backend):
    """
    Create and set up an ActivityStore instance for testing.
    
    Yields an initialized ActivityStore and handles teardown after tests.
    """
    store = ActivityStore(
        backend=mock_storage_backend,
        cache=mock_cache_backend,
        namespace="test"
    )
    
    await store.setup()
    yield store
    await store.teardown()


# Use pytest-asyncio's built-in event_loop fixture instead of defining our own
# This will be automatically picked up by pytest-asyncio

# Configure pytest-asyncio to use function scope for all async fixtures by default
def pytest_configure(config):
    """Configure pytest-asyncio to use function scope for all async fixtures."""
    config.option.asyncio_default_fixture_loop_scope = "function"