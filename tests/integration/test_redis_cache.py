import asyncio
import json
import os
import pytest
import pytest_asyncio
import uuid
from typing import Dict, Any, Optional

from activity_store.interfaces import CacheBackend

from activity_store.cache.redis import RedisCacheBackend


@pytest.fixture
def redis_url() -> str:
    """Get the Redis URL from environment or use default."""
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def test_namespace() -> str:
    """Generate a unique namespace for test runs."""
    return f"persistence-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Sample LD-object data for testing."""
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": "https://example.com/objects/123",
        "type": "Note",
        "content": "This is a test note",
        "published": "2023-01-01T00:00:00Z",
    }


@pytest_asyncio.fixture
async def redis_cache(redis_url: str) -> Optional[CacheBackend]:
    """
    Create and setup a Redis cache backend for testing.

    If Redis is not available, returns None and the tests will be skipped.
    """
    # Create the cache backend with 0.5s TTL for faster testing
    cache = RedisCacheBackend(redis_url=redis_url, namespace="test")

    # Set up the cache and clear the test namespace before starting
    await cache.setup()

    # Return the initialized cache
    yield cache

    # Clean up after the tests
    await cache.teardown()


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_add_get(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test adding and retrieving data from Redis cache."""
    # Add to cache
    key = "test-key"
    await redis_cache.add(key, sample_data)

    # Get from cache
    result = await redis_cache.get(key)

    # Verify result
    assert result is not None
    assert result["id"] == sample_data["id"]
    assert result["type"] == sample_data["type"]
    assert result["content"] == sample_data["content"]


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_missing_key(redis_cache: CacheBackend):
    """Test getting a non-existent key returns None."""

    result = await redis_cache.get("non-existent-key")
    assert result is None


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_remove(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test removing a key from the Redis cache."""
    # Add to cache
    key = "test-remove"
    await redis_cache.add(key, sample_data)

    # Verify it's there
    assert await redis_cache.get(key) is not None

    # Remove it
    await redis_cache.remove(key)

    # Verify it's gone
    assert await redis_cache.get(key) is None


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_ttl(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test TTL-based expiration of Redis cache entries."""
    # Add to cache with very short TTL (1 second)
    key = "test-ttl"
    await redis_cache.add(key, sample_data, ttl=1)

    # Verify it's there initially
    assert await redis_cache.get(key) is not None

    # Wait for TTL to expire
    await asyncio.sleep(1.5)

    # Verify it's gone after TTL expires
    assert await redis_cache.get(key) is None


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_overwrite(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test that adding with the same key overwrites the previous value."""

    # Add to cache
    key = "test-overwrite"
    await redis_cache.add(key, sample_data)

    # Modify the data
    modified_data = sample_data.copy()
    modified_data["content"] = "Updated content"

    # Add again with same key
    await redis_cache.add(key, modified_data)

    # Get from cache
    result = await redis_cache.get(key)

    # Verify it's the updated value
    assert result is not None
    assert result["content"] == "Updated content"


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_complex_data(redis_cache: CacheBackend):
    """Test that complex nested data structures are preserved."""

    # A complex nested object
    complex_data = {
        "id": "test-complex",
        "type": ["Note", "Article"],
        "content": "Complex test",
        "tags": ["test", "complex", "nested"],
        "metadata": {
            "createdBy": "test-user",
            "priority": 5,
            "nested": {"deepValue": True, "deepArray": [1, 2, 3, {"key": "value"}]},
        },
        "attachments": [
            {"name": "file1.txt", "size": 1024},
            {"name": "file2.jpg", "size": 2048},
        ],
    }

    # Add to cache
    key = "test-complex"
    await redis_cache.add(key, complex_data)

    # Get from cache
    result = await redis_cache.get(key)

    # Verify result - deep comparison
    assert result is not None
    assert json.dumps(result, sort_keys=True) == json.dumps(complex_data, sort_keys=True)

    # Verify nested structures
    assert isinstance(result["type"], list)
    assert isinstance(result["metadata"]["nested"]["deepArray"], list)
    assert isinstance(result["metadata"]["nested"]["deepArray"][3], dict)


@pytest.mark.integration_test
@pytest.mark.asyncio
async def test_redis_cache_namespace_isolation(redis_url: str, sample_data: Dict[str, Any]):
    """Test that different namespaces are isolated from each other."""
    # Create two cache backends with different namespaces
    cache1 = RedisCacheBackend(redis_url=redis_url, namespace="test-ns1")
    cache2 = RedisCacheBackend(redis_url=redis_url, namespace="test-ns2")

    await cache1.setup()
    await cache2.setup()

    # Add the same key to both caches
    key = "test-key"
    await cache1.add(key, sample_data)

    # Modified data for second cache
    modified_data = sample_data.copy()
    modified_data["content"] = "Different content"
    await cache2.add(key, modified_data)

    # Get from both caches
    result1 = await cache1.get(key)
    result2 = await cache2.get(key)

    # Verify they have different values
    assert result1 is not None
    assert result2 is not None
    assert result1["content"] == sample_data["content"]
    assert result2["content"] == "Different content"

    # Clean up
    await cache1.teardown()
    await cache2.teardown()
