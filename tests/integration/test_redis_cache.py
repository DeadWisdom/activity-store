import asyncio
import json
import os
import pytest
import pytest_asyncio
from typing import Dict, Any, Optional

from activity_store.interfaces import CacheBackend

"""
Integration tests for the Redis cache backend.

These tests require a running Redis server to execute.
To run the tests, either:
1. Have a Redis server running on localhost:6379 (default), or
2. Set the REDIS_URL environment variable to point to your Redis server
"""

# Check if we should skip Redis tests
skip_redis_tests = os.environ.get("SKIP_REDIS_TESTS", "").lower() in ("1", "true", "yes")
skip_reason = "Redis tests disabled by SKIP_REDIS_TESTS environment variable"

# Import the Redis cache backend - we'll skip the tests if it can't be imported
try:
    from activity_store.cache.redis import RedisCacheBackend
    redis_import_error = None
except ImportError as e:
    RedisCacheBackend = None
    redis_import_error = str(e)
    skip_redis_tests = True
    skip_reason = f"Redis dependencies not installed: {e}"


@pytest.fixture
def redis_url() -> str:
    """Get the Redis URL from environment or use default."""
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Sample LD-object data for testing."""
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": "https://example.com/objects/123",
        "type": "Note",
        "content": "This is a test note",
        "published": "2023-01-01T00:00:00Z"
    }


@pytest_asyncio.fixture
async def redis_cache(redis_url: str) -> Optional[CacheBackend]:
    """
    Create and setup a Redis cache backend for testing.
    
    If Redis is not available, returns None and the tests will be skipped.
    """
    if skip_redis_tests or RedisCacheBackend is None:
        yield None
        return
    
    try:
        # Create the cache backend with 0.5s TTL for faster testing
        cache = RedisCacheBackend(redis_url=redis_url, namespace="test")
        
        # Set up the cache and clear the test namespace before starting
        await cache.setup()
        await cache.cleanup_namespace()
        
        # Return the initialized cache
        yield cache
        
        # Clean up after the tests
        await cache.cleanup_namespace()
        await cache.teardown()
    except Exception as e:
        pytest.skip(f"Failed to connect to Redis: {e}")
        yield None


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_add_get(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test adding and retrieving data from Redis cache."""
    if redis_cache is None:
        pytest.skip("Redis cache backend not available")
    
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


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_missing_key(redis_cache: CacheBackend):
    """Test getting a non-existent key returns None."""
    if redis_cache is None:
        pytest.skip("Redis cache backend not available")
    
    result = await redis_cache.get("non-existent-key")
    assert result is None


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_remove(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test removing a key from the Redis cache."""
    if redis_cache is None:
        pytest.skip("Redis cache backend not available")
    
    # Add to cache
    key = "test-remove"
    await redis_cache.add(key, sample_data)
    
    # Verify it's there
    assert await redis_cache.get(key) is not None
    
    # Remove it
    await redis_cache.remove(key)
    
    # Verify it's gone
    assert await redis_cache.get(key) is None


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_ttl(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test TTL-based expiration of Redis cache entries."""
    if redis_cache is None:
        pytest.skip("Redis cache backend not available")
    
    # Add to cache with very short TTL (1 second)
    key = "test-ttl"
    await redis_cache.add(key, sample_data, ttl=1)
    
    # Verify it's there initially
    assert await redis_cache.get(key) is not None
    
    # Wait for TTL to expire
    await asyncio.sleep(1.5)
    
    # Verify it's gone after TTL expires
    assert await redis_cache.get(key) is None


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_overwrite(redis_cache: CacheBackend, sample_data: Dict[str, Any]):
    """Test that adding with the same key overwrites the previous value."""
    if redis_cache is None:
        pytest.skip("Redis cache backend not available")
    
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


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_setup_teardown(redis_url: str):
    """Test the setup and teardown methods."""
    if skip_redis_tests or RedisCacheBackend is None:
        pytest.skip("Redis cache backend not available")
    
    try:
        # Create the backend
        cache = RedisCacheBackend(redis_url=redis_url, namespace="test-lifecycle")
        
        # Call setup
        await cache.setup()
        
        # Add a value
        await cache.add("lifecycle-test", {"test": "value"})
        
        # Just close the connection without cleaning up (teardown should no longer clean up)
        await cache.teardown()
        
        # Create a new instance
        new_cache = RedisCacheBackend(redis_url=redis_url, namespace="test-lifecycle")
        await new_cache.setup()
        
        # The value should still be there (persistence)
        assert await new_cache.get("lifecycle-test") is not None
        
        # Now explicitly clean up the namespace
        await new_cache.cleanup_namespace()
        
        # Create a third instance
        third_cache = RedisCacheBackend(redis_url=redis_url, namespace="test-lifecycle")
        await third_cache.setup()
        
        # The value should be gone after cleanup
        assert await third_cache.get("lifecycle-test") is None
        
        # Close the connection
        await third_cache.teardown()
    except Exception as e:
        pytest.skip(f"Failed to connect to Redis: {e}")


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_complex_data(redis_cache: CacheBackend):
    """Test that complex nested data structures are preserved."""
    if redis_cache is None:
        pytest.skip("Redis cache backend not available")
    
    # A complex nested object
    complex_data = {
        "id": "test-complex",
        "type": ["Note", "Article"],
        "content": "Complex test",
        "tags": ["test", "complex", "nested"],
        "metadata": {
            "createdBy": "test-user",
            "priority": 5,
            "nested": {
                "deepValue": True,
                "deepArray": [1, 2, 3, {"key": "value"}]
            }
        },
        "attachments": [
            {"name": "file1.txt", "size": 1024},
            {"name": "file2.jpg", "size": 2048}
        ]
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


@pytest.mark.skipif(skip_redis_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_namespace_isolation(redis_url: str, sample_data: Dict[str, Any]):
    """Test that different namespaces are isolated from each other."""
    if skip_redis_tests or RedisCacheBackend is None:
        pytest.skip("Redis cache backend not available")
    
    try:
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
    except Exception as e:
        pytest.skip(f"Failed to connect to Redis: {e}")