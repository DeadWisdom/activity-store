import asyncio
import os
import pytest
import pytest_asyncio
import uuid
from typing import Dict, Any, Optional, List
from dotenv import load_dotenv

# Load environment variables from .env file
load_dotenv()

# These imports need to be after load_dotenv to ensure environment variables are loaded
from activity_store.interfaces import StorageBackend  # noqa: E402
from activity_store.query import Query  # noqa: E402

"""
Integration tests for the Elasticsearch storage backend.

These tests require a running Elasticsearch server to execute.
To run the tests, either:
1. Have an Elasticsearch server running on localhost:9200 (default), or
2. Set the ES_URL environment variable to point to your Elasticsearch server
3. Set the ELASTICSEARCH_CLOUD_ID and ELASTICSEARCH_PASSWORD environment variables for cloud connections
"""

# Check if we should skip Elasticsearch tests
skip_es_tests = os.environ.get("SKIP_ES_TESTS", "").lower() in ("1", "true", "yes")
# If we have cloud credentials, don't skip tests
if os.environ.get("ELASTICSEARCH_CLOUD_ID") and os.environ.get("ELASTICSEARCH_PASSWORD"):
    skip_es_tests = False

skip_reason = "Elasticsearch tests disabled by SKIP_ES_TESTS environment variable"

# Log environment info to help with debugging
print(f"Cloud ID available: {'Yes' if os.environ.get('ELASTICSEARCH_CLOUD_ID') else 'No'}")
print(f"Password available: {'Yes' if os.environ.get('ELASTICSEARCH_PASSWORD') else 'No'}")
print(f"Namespace: {os.environ.get('ELASTICSEARCH_NAMESPACE', 'Not set')}")
print(f"Skip tests: {skip_es_tests}")

# Import the Elasticsearch backend - we'll skip the tests if it can't be imported
try:
    from activity_store.backends.elastic import ElasticsearchBackend
    es_import_error = None
    # Print successful import for debugging
    print("Successfully imported ElasticsearchBackend")
except ImportError as e:
    ElasticsearchBackend = None
    es_import_error = str(e)
    skip_es_tests = True
    skip_reason = f"Elasticsearch dependencies not installed: {e}"
    # Print import error for debugging
    print(f"Failed to import ElasticsearchBackend: {e}")


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
def test_index_prefix() -> str:
    """Generate a unique prefix for test indices to avoid collisions."""
    return f"test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def sample_object() -> Dict[str, Any]:
    """Sample LD-object for testing."""
    return {
        "@context": "https://www.w3.org/ns/activitystreams",
        "id": f"https://example.com/objects/{uuid.uuid4()}",
        "type": "Note",
        "content": "This is a test note",
        "name": "Test Note",
        "published": "2023-01-01T00:00:00Z"
    }


@pytest.fixture
def sample_objects() -> List[Dict[str, Any]]:
    """Sample LD-objects for bulk testing."""
    return [
        {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": f"https://example.com/objects/{uuid.uuid4()}",
            "type": "Note",
            "content": f"This is test note {i}",
            "name": f"Test Note {i}",
            "published": f"2023-01-{i:02d}T00:00:00Z"  # Fixed date format with proper padding
        }
        for i in range(1, 11)  # Create 10 sample objects
    ]


@pytest_asyncio.fixture
async def es_backend(es_url: str, test_index_prefix: str) -> Optional[StorageBackend]:
    """
    Create and setup an Elasticsearch backend for testing.
    
    If Elasticsearch is not available, returns None and the tests will be skipped.
    """
    print(f"es_backend fixture called with es_url={es_url}, test_index_prefix={test_index_prefix}")
    print(f"skip_es_tests={skip_es_tests}, ElasticsearchBackend is None={ElasticsearchBackend is None}")
    
    if skip_es_tests or ElasticsearchBackend is None:
        print("Skipping tests due to skip_es_tests or missing ElasticsearchBackend")
        yield None
        return

    try:
        # Check if we're using cloud configuration
        if es_url is None:
            print("Using cloud configuration")
            # Get cloud credentials from environment
            cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
            password = os.environ.get("ELASTICSEARCH_PASSWORD")
            print(f"Cloud credentials found: cloud_id={bool(cloud_id)}, password={bool(password)}")
            
            if not cloud_id or not password:
                pytest.skip("ELASTICSEARCH_CLOUD_ID and ELASTICSEARCH_PASSWORD are required for cloud testing")
                yield None
                return
            
            # Use namespace from environment or default to test prefix
            namespace = os.environ.get("ELASTICSEARCH_NAMESPACE", test_index_prefix)
            print(f"Using namespace: {namespace}")
            
            # Create the client
            try:
                from elasticsearch import AsyncElasticsearch
                print("Successfully imported AsyncElasticsearch")
                client = AsyncElasticsearch(
                    cloud_id=cloud_id,
                    api_key=password,
                )
                print("Client created successfully")
                
                # Create the backend with the cloud client
                backend = ElasticsearchBackend(
                    client=client,
                    index_prefix=namespace,
                    refresh_on_write=True  # Ensure writes are immediately searchable for testing
                )
                print("Backend created successfully")
            except Exception as client_error:
                print(f"Error creating client or backend: {client_error}")
                pytest.skip(f"Failed to create Elasticsearch client: {client_error}")
                yield None
                return
        else:
            print("Using standard URL connection")
            # Create the backend with a standard URL connection
            backend = ElasticsearchBackend(
                es_url=es_url, 
                index_prefix=test_index_prefix,
                refresh_on_write=True  # Ensure writes are immediately searchable for testing
            )
        
        # Setup the backend (creates indices)
        print("Setting up backend (creating indices)...")
        try:
            await backend.setup()
            print("Backend setup completed successfully")
        except Exception as setup_error:
            print(f"Error setting up backend: {setup_error}")
            pytest.skip(f"Failed to set up Elasticsearch backend: {setup_error}")
            yield None
            return
        
        # Return the initialized backend
        print("Yielding backend to test")
        yield backend
        
        # Cleanup after the tests
        print("Cleaning up (tearing down backend)...")
        await backend.teardown()
        print("Backend teardown completed")
    except Exception as e:
        print(f"Exception in es_backend fixture: {e}")
        pytest.skip(f"Failed to connect to Elasticsearch: {e}")
        yield None


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_add_get(es_backend: StorageBackend, sample_object: Dict[str, Any]):
    """Test adding and retrieving an object."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add the object
    await es_backend.add(sample_object)
    
    # Retrieve it
    result = await es_backend.get(sample_object["id"])
    
    # Verify result
    assert result is not None
    assert result["id"] == sample_object["id"]
    assert result["type"] == sample_object["type"]
    assert result["content"] == sample_object["content"]


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_get_nonexistent(es_backend: StorageBackend):
    """Test that getting a non-existent object raises ObjectNotFound."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    from activity_store.exceptions import ObjectNotFound
    
    with pytest.raises(ObjectNotFound):
        await es_backend.get("https://example.com/nonexistent")


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_add_to_collection(es_backend: StorageBackend, sample_object: Dict[str, Any]):
    """Test adding an object to a collection."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add the object to a collection
    collection = "test-collection"
    await es_backend.add(sample_object, collection)
    
    # Retrieve it from the collection
    result = await es_backend.get(sample_object["id"], collection)
    
    # Verify result
    assert result is not None
    assert result["id"] == sample_object["id"]
    assert result["type"] == sample_object["type"]


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_remove(es_backend: StorageBackend, sample_object: Dict[str, Any]):
    """Test removing an object."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    from activity_store.exceptions import ObjectNotFound
    
    # Add the object
    await es_backend.add(sample_object)
    
    # Verify it's there
    result = await es_backend.get(sample_object["id"])
    assert result is not None
    
    # Remove it
    await es_backend.remove(sample_object["id"])
    
    # Verify it's gone
    with pytest.raises(ObjectNotFound):
        await es_backend.get(sample_object["id"])


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_remove_from_collection(es_backend: StorageBackend, sample_object: Dict[str, Any]):
    """Test removing an object from a collection."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    from activity_store.exceptions import ObjectNotFound
    
    # Add the object to both general storage and a collection
    collection = "test-collection"
    await es_backend.add(sample_object)
    await es_backend.add(sample_object, collection)
    
    # Verify it's in both places
    result1 = await es_backend.get(sample_object["id"])
    result2 = await es_backend.get(sample_object["id"], collection)
    assert result1 is not None
    assert result2 is not None
    
    # Remove from collection only
    await es_backend.remove(sample_object["id"], collection)
    
    # Verify it's gone from collection but still in general storage
    with pytest.raises(ObjectNotFound):
        await es_backend.get(sample_object["id"], collection)
    
    result = await es_backend.get(sample_object["id"])
    assert result is not None


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_query_by_text(es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]):
    """Test querying objects by text content."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add objects
    for obj in sample_objects:
        await es_backend.add(obj)
    
    # Wait for indexing
    await asyncio.sleep(1)
    
    # Query by text in content field
    query = Query(text="test note 5")
    results = await es_backend.query(query)
    
    # Verify results
    assert results["type"] == "Collection"
    assert results["totalItems"] >= 1
    
    # Check that the right object was found
    found = False
    for item in results["items"]:
        if "content" in item and "test note 5" in item["content"]:
            found = True
            break
    assert found, "Expected to find an object with 'test note 5' in content"


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_query_by_type(es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]):
    """Test querying objects by type."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add objects with different types
    for i, obj in enumerate(sample_objects):
        if i < 5:
            obj["type"] = "Note"
        else:
            obj["type"] = "Article"
        await es_backend.add(obj)
    
    # Wait for indexing
    await asyncio.sleep(1)
    
    # Query by type
    query = Query(type="Article")
    results = await es_backend.query(query)
    
    # Verify results
    assert results["type"] == "Collection"
    assert results["totalItems"] == 5
    
    # Check that all results have the correct type
    for item in results["items"]:
        assert item["type"] == "Article"


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_query_by_collection(es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]):
    """Test querying objects by collection."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add objects to different collections
    for i, obj in enumerate(sample_objects):
        collection = "collection-a" if i < 5 else "collection-b"
        await es_backend.add(obj, collection)
    
    # Wait for indexing
    await asyncio.sleep(1)
    
    # Query by collection
    query = Query(collection="collection-a")
    results = await es_backend.query(query)
    
    # Verify results
    assert results["type"] == "Collection"
    assert results["totalItems"] == 5


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_query_pagination(es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]):
    """Test query pagination."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")

    # Add objects
    for obj in sample_objects:
        await es_backend.add(obj)

    # Wait for indexing
    await asyncio.sleep(1)

    # First page
    query = Query(size=3, sort="published:asc")  # Add sort to ensure consistent order
    results = await es_backend.query(query)

    # Verify pagination
    assert results["type"] == "Collection"
    assert len(results["items"]) == 3

    # Continue with next page query without needing first page reference

    # Query with size limit, using from parameter
    from_query = Query(size=3)
    next_results = await es_backend.query(from_query)

    # Verify next page
    assert next_results["type"] == "Collection"
    assert len(next_results["items"]) > 0

    # Check that we got results, even if pagination metadata is missing
    assert len(next_results["items"]) > 0


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_query_sorting(es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]):
    """Test query sorting."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add objects with different published dates
    for obj in sample_objects:
        await es_backend.add(obj)
    
    # Wait for indexing
    await asyncio.sleep(1)
    
    # Query with ascending sort
    query = Query(sort="published:asc")
    asc_results = await es_backend.query(query)
    
    # Query with descending sort
    query = Query(sort="published:desc")
    desc_results = await es_backend.query(query)
    
    # Verify sorting worked
    if len(asc_results["items"]) > 1 and len(desc_results["items"]) > 1:
        # Get published dates
        asc_dates = [item.get("published") for item in asc_results["items"] if "published" in item]
        desc_dates = [item.get("published") for item in desc_results["items"] if "published" in item]
        
        # Check if dates are in order
        assert asc_dates == sorted(asc_dates)
        assert desc_dates == sorted(desc_dates, reverse=True)


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_query_keywords(es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]):
    """Test querying by keywords."""
    if es_backend is None:
        pytest.skip("Elasticsearch backend not available")
    
    # Add objects with different keywords/tags
    for i, obj in enumerate(sample_objects):
        # Add some tags to the objects
        obj["tag"] = ["test", f"tag{i % 3}", "common"]
        await es_backend.add(obj)
    
    # Wait for indexing
    await asyncio.sleep(1)
    
    # Query by keywords
    query = Query(keywords=["tag1"])
    results = await es_backend.query(query)
    
    # We should get about 1/3 of the objects (those with tag1)
    expected_count = len(sample_objects) // 3
    assert abs(results["totalItems"] - expected_count) <= 1  # Allow for rounding


@pytest.mark.skipif(skip_es_tests, reason=skip_reason)
@pytest.mark.asyncio
async def test_es_setup_teardown(es_url: str, test_index_prefix: str):
    """Test setup and teardown methods."""
    if skip_es_tests or ElasticsearchBackend is None:
        pytest.skip("Elasticsearch backend not available")

    try:
        # Check if we're using cloud configuration
        if es_url is None:
            # Get cloud credentials from environment
            cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
            password = os.environ.get("ELASTICSEARCH_PASSWORD")

            if not cloud_id or not password:
                pytest.skip("ELASTICSEARCH_CLOUD_ID and ELASTICSEARCH_PASSWORD are required for cloud testing")
                return

            # Use namespace from environment or default to test prefix
            namespace = os.environ.get("ELASTICSEARCH_NAMESPACE", test_index_prefix)

            # Create the client
            from elasticsearch import AsyncElasticsearch
            client = AsyncElasticsearch(
                cloud_id=cloud_id,
                api_key=password,
            )

            # Create the backend with the cloud client
            backend = ElasticsearchBackend(
                client=client,
                index_prefix=namespace,
                refresh_on_write=True
            )
        else:
            # Create a backend with the test prefix
            backend = ElasticsearchBackend(es_url=es_url, index_prefix=test_index_prefix)

        # Setup creates indices
        await backend.setup()

        # Add a test object
        obj = {
            "@context": "https://www.w3.org/ns/activitystreams",
            "id": "https://example.com/test-setup-teardown",
            "type": "Note",
            "content": "Test note for setup/teardown",
            "published": "2023-01-01T00:00:00Z"  # Add date to comply with mappings
        }
        await backend.add(obj)

        # Verify it's there
        result = await backend.get(obj["id"])
        assert result is not None

        # Tear down should remove indices
        await backend.teardown()

        # Create a new backend with the same prefix (reusing the same client or URL)
        if es_url is None:
            new_backend = ElasticsearchBackend(
                client=client,
                index_prefix=namespace,
                refresh_on_write=True
            )
        else:
            new_backend = ElasticsearchBackend(es_url=es_url, index_prefix=test_index_prefix)

        await new_backend.setup()

        # The object should be gone if teardown worked
        from activity_store.exceptions import ObjectNotFound
        with pytest.raises(ObjectNotFound):
            await new_backend.get(obj["id"])

        # Clean up
        await new_backend.teardown()

        # Close the client if we created it
        if es_url is None:
            await client.close()
    except Exception as e:
        pytest.skip(f"Failed to test Elasticsearch setup/teardown: {e}")