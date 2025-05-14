import asyncio
import os
import pytest
import pytest_asyncio
import uuid
from typing import Dict, Any, Optional

from activity_store.backends.elastic import ElasticsearchBackend
from activity_store.cache.redis import RedisCacheBackend
from activity_store.exceptions import ObjectNotFound

# Check if we should skip Redis and Elasticsearch tests
skip_redis_tests = os.environ.get("SKIP_REDIS_TESTS", "").lower() in ("1", "true", "yes")
skip_es_tests = os.environ.get("SKIP_ES_TESTS", "").lower() in ("1", "true", "yes")

# Skip reason for tests
redis_skip_reason = "Redis tests disabled by SKIP_REDIS_TESTS environment variable"
es_skip_reason = "Elasticsearch tests disabled by SKIP_ES_TESTS environment variable"


@pytest.fixture
def test_namespace() -> str:
    """Generate a unique namespace for test runs."""
    return f"persistence-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def redis_url() -> str:
    """Get the Redis URL from environment or use default."""
    return os.environ.get("REDIS_URL", "redis://localhost:6379/0")


@pytest.fixture
def es_url() -> str:
    """Get the Elasticsearch URL from environment or use default."""
    # Check for cloud ID first
    cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
    if cloud_id:
        # If using cloud ID, return None to signal the test fixture to use cloud setup
        return None
    # Otherwise use ES_URL or default
    return os.environ.get("ES_URL", "http://localhost:9200")


@pytest.fixture
def sample_data() -> Dict[str, Any]:
    """Sample LD-object data for testing."""
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"https://example.com/objects/{uuid.uuid4()}",
        "type": "Note",
        "content": "This is a test note for persistence testing",
        "published": "2023-01-01T00:00:00Z"
    }


@pytest_asyncio.fixture
async def redis_cache_first_instance(redis_url: str, test_namespace: str) -> Optional[RedisCacheBackend]:
    """Create first instance of Redis cache backend for testing."""
    if skip_redis_tests:
        yield None
        return
    
    try:
        # Create the cache backend
        cache = RedisCacheBackend(redis_url=redis_url, namespace=test_namespace)
        
        # Set up the cache
        await cache.setup()
        
        # Return the initialized cache
        yield cache
        
        # Don't tear down to test persistence
    except Exception as e:
        pytest.skip(f"Failed to connect to Redis: {e}")
        yield None


@pytest_asyncio.fixture
async def redis_cache_second_instance(redis_url: str, test_namespace: str) -> Optional[RedisCacheBackend]:
    """Create second instance of Redis cache backend for testing."""
    if skip_redis_tests:
        yield None
        return
    
    try:
        # Create the cache backend
        cache = RedisCacheBackend(redis_url=redis_url, namespace=test_namespace)
        
        # Set up the cache
        await cache.setup()
        
        # Return the initialized cache
        yield cache
        
        # Clean up after tests
        await cache.teardown()
    except Exception as e:
        pytest.skip(f"Failed to connect to Redis: {e}")
        yield None


@pytest_asyncio.fixture
async def es_backend_first_instance(es_url: str, test_namespace: str) -> Optional[ElasticsearchBackend]:
    """Create first instance of Elasticsearch backend for testing."""
    if skip_es_tests:
        yield None
        return
    
    try:
        # Check if we're using cloud configuration
        if es_url is None:
            # Get cloud credentials from environment
            cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
            password = os.environ.get("ELASTICSEARCH_PASSWORD")
            
            if not cloud_id or not password:
                pytest.skip("ELASTICSEARCH_CLOUD_ID and ELASTICSEARCH_PASSWORD are required for cloud testing")
                yield None
                return
            
            # Create the client
            from elasticsearch import AsyncElasticsearch
            client = AsyncElasticsearch(
                cloud_id=cloud_id,
                api_key=password,
            )
            
            # Create the backend with the cloud client
            backend = ElasticsearchBackend(
                client=client,
                index_prefix=test_namespace,
                refresh_on_write=True  # Ensure writes are immediately searchable for testing
            )
        else:
            # Create the backend with standard URL
            backend = ElasticsearchBackend(
                es_url=es_url, 
                index_prefix=test_namespace,
                refresh_on_write=True
            )
        
        # Set up the backend
        await backend.setup()
        
        # Return the initialized backend
        yield backend
        
        # Don't tear down to test persistence
    except Exception as e:
        pytest.skip(f"Failed to connect to Elasticsearch: {e}")
        yield None


@pytest_asyncio.fixture
async def es_backend_second_instance(es_url: str, test_namespace: str) -> Optional[ElasticsearchBackend]:
    """Create second instance of Elasticsearch backend for testing."""
    if skip_es_tests:
        yield None
        return
    
    try:
        # Check if we're using cloud configuration
        if es_url is None:
            # Get cloud credentials from environment
            cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
            password = os.environ.get("ELASTICSEARCH_PASSWORD")
            
            if not cloud_id or not password:
                pytest.skip("ELASTICSEARCH_CLOUD_ID and ELASTICSEARCH_PASSWORD are required for cloud testing")
                yield None
                return
            
            # Create the client
            from elasticsearch import AsyncElasticsearch
            client = AsyncElasticsearch(
                cloud_id=cloud_id,
                api_key=password,
            )
            
            # Create the backend with the cloud client
            backend = ElasticsearchBackend(
                client=client,
                index_prefix=test_namespace,
                refresh_on_write=True
            )
        else:
            # Create the backend with standard URL
            backend = ElasticsearchBackend(
                es_url=es_url, 
                index_prefix=test_namespace,
                refresh_on_write=True
            )
        
        # Set up the backend
        await backend.setup()
        
        # Return the initialized backend
        yield backend
        
        # Clean up after tests
        await backend.teardown()
    except Exception as e:
        pytest.skip(f"Failed to connect to Elasticsearch: {e}")
        yield None


@pytest.mark.skipif(skip_redis_tests, reason=redis_skip_reason)
@pytest.mark.asyncio
async def test_redis_cache_persistence(
    redis_cache_first_instance: RedisCacheBackend,
    redis_cache_second_instance: RedisCacheBackend,
    sample_data: Dict[str, Any]
):
    """Test Redis cache persists data across backend instances."""
    if redis_cache_first_instance is None or redis_cache_second_instance is None:
        pytest.skip("Redis cache backend not available")
    
    # Step 1: Add item to first Redis instance
    key = "persistence-test-key"
    await redis_cache_first_instance.add(key, sample_data, ttl=3600)  # 1 hour TTL
    
    # Verify it was added to first instance
    result1 = await redis_cache_first_instance.get(key)
    assert result1 is not None
    assert result1["id"] == sample_data["id"]
    
    # Step 2: Close first instance connection without teardown
    await redis_cache_first_instance._client.aclose()
    
    # Step 3: Get item from second Redis instance
    result2 = await redis_cache_second_instance.get(key)
    
    # Verify item persisted and is accessible from second instance
    assert result2 is not None
    assert result2["id"] == sample_data["id"]
    assert result2["content"] == sample_data["content"]


@pytest.mark.skipif(skip_es_tests, reason=es_skip_reason)
@pytest.mark.asyncio
async def test_elasticsearch_persistence(
    es_backend_first_instance: ElasticsearchBackend,
    es_backend_second_instance: ElasticsearchBackend,
    sample_data: Dict[str, Any]
):
    """Test Elasticsearch backend persists data across backend instances."""
    if es_backend_first_instance is None or es_backend_second_instance is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Step 1: Add object to first Elasticsearch instance
    await es_backend_first_instance.add(sample_data)
    
    # Verify it was added to first instance
    result1 = await es_backend_first_instance.get(sample_data["id"])
    assert result1 is not None
    assert result1["id"] == sample_data["id"]
    
    # Step 2: Close first instance connection without teardown
    await es_backend_first_instance._client.close()
    
    # Step 3: Get object from second Elasticsearch instance
    result2 = await es_backend_second_instance.get(sample_data["id"])
    
    # Verify object persisted and is accessible from second instance
    assert result2 is not None
    assert result2["id"] == sample_data["id"]
    assert result2["content"] == sample_data["content"]


@pytest.mark.skipif(skip_es_tests, reason=es_skip_reason)
@pytest.mark.asyncio
async def test_elasticsearch_collection_persistence(
    es_backend_first_instance: ElasticsearchBackend,
    es_backend_second_instance: ElasticsearchBackend,
    sample_data: Dict[str, Any]
):
    """Test Elasticsearch collection data persists across backend instances."""
    if es_backend_first_instance is None or es_backend_second_instance is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Step 1: Add object to collection in first Elasticsearch instance
    collection = "test-persistence-collection"
    await es_backend_first_instance.add(sample_data, collection)
    
    # Verify it was added to first instance
    result1 = await es_backend_first_instance.get(sample_data["id"], collection)
    assert result1 is not None
    assert result1["id"] == sample_data["id"]
    
    # Step 2: Close first instance connection without teardown
    await es_backend_first_instance._client.close()
    
    # Step 3: Get object from collection in second Elasticsearch instance
    result2 = await es_backend_second_instance.get(sample_data["id"], collection)
    
    # Verify object persisted in collection and is accessible from second instance
    assert result2 is not None
    assert result2["id"] == sample_data["id"]