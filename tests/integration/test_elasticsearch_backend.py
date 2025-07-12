import asyncio
import os
import pytest
import pytest_asyncio
import uuid
from typing import Dict, Any, Optional, List

# These imports need to be after load_dotenv to ensure environment variables are loaded
from activity_store.interfaces import StorageBackend  # noqa: E402
from activity_store.query import Query  # noqa: E402
from activity_store.backends.elastic import ElasticsearchBackend


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
        "published": "2023-01-01T00:00:00Z",
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
            "published": f"2023-01-{i:02d}T00:00:00Z",  # Fixed date format with proper padding
        }
        for i in range(1, 11)  # Create 10 sample objects
    ]


@pytest.fixture
def test_namespace() -> str:
    """Generate a unique namespace for test runs."""
    return f"persistence-test-{uuid.uuid4().hex[:8]}"


@pytest.fixture
def es_client():
    from elasticsearch import AsyncElasticsearch

    return AsyncElasticsearch()


@pytest_asyncio.fixture
async def es_backend(test_index_prefix: str) -> Optional[StorageBackend]:
    """
    Create and setup an Elasticsearch backend for testing.
    """
    # Check if we're using cloud configuration
    namespace = os.environ.get("ELASTICSEARCH_NAMESPACE", "testing")
    backend = ElasticsearchBackend(
        index_prefix=namespace,
        refresh_on_write=True,  # Ensure writes are immediately searchable for testing
    )

    await backend.setup()

    # Return the initialized backend
    yield backend

    # Cleanup after the tests
    await backend.teardown()
    await backend.close()


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_add_get(es_backend: StorageBackend, sample_object: Dict[str, Any]):
    """Test adding and retrieving an object."""
    # Add the object
    await es_backend.add(sample_object)

    # Retrieve it
    result = await es_backend.get(sample_object["id"])

    # Verify result
    assert result is not None
    assert result["id"] == sample_object["id"]
    assert result["type"] == sample_object["type"]
    assert result["content"] == sample_object["content"]


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_get_nonexistent(es_backend: StorageBackend):
    """Test that getting a non-existent object returns None."""
    assert await es_backend.get("https://example.com/nonexistent") is None


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_add_to_collection(
    es_backend: StorageBackend, sample_object: Dict[str, Any]
):
    """Test adding an object to a collection."""
    # Add the object to a collection
    collection = "test-collection"
    await es_backend.add(sample_object, collection)

    # Retrieve it from the collection
    result = await es_backend.get(sample_object["id"], collection)

    # Verify result
    assert result is not None
    assert result["id"] == sample_object["id"]
    assert result["type"] == sample_object["type"]


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_remove(es_backend: StorageBackend, sample_object: Dict[str, Any]):
    """Test removing an object."""
    # Add the object
    await es_backend.add(sample_object)

    # Verify it's there
    result = await es_backend.get(sample_object["id"])
    assert result is not None

    # Remove it
    await es_backend.remove(sample_object["id"])

    # Verify it's gone
    assert await es_backend.get(sample_object["id"]) is None


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_remove_from_collection(
    es_backend: StorageBackend, sample_object: Dict[str, Any]
):
    """Test removing an object from a collection."""
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
    assert await es_backend.get(sample_object["id"], collection) is None

    assert await es_backend.get(sample_object["id"]) is not None


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_query_by_text(
    es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]
):
    """Test querying objects by text content."""
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


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_query_by_type(
    es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]
):
    """Test querying objects by type."""

    # Add objects with different types
    for i, obj in enumerate(sample_objects):
        if i < 5:
            obj["type"] = "Note"
        else:
            obj["type"] = "Article"
        print(obj)
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


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_query_by_collection(
    es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]
):
    """Test querying objects by collection."""
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


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_query_pagination(
    es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]
):
    """Test query pagination."""
    # Add objects
    for obj in sample_objects:
        await es_backend.add(obj)

    # Wait for indexing
    await asyncio.sleep(1)

    # First page
    query = Query(size=3, sort="published:asc")  # Add sort to ensure consistent order
    first = await es_backend.query(query)

    # Verify pagination
    assert first["type"] == "OrderdCollection"
    assert [x['published'] for x in first["items"]] == [x['published'] for x in sample_objects[:3]]

    # Second page query
    query = Query(size=3, sort="published:asc", after=first["next"])
    second = await es_backend.query(query)
    assert second["type"] == "OrderdCollection"
    assert [x['published'] for x in second["items"]] == [x['published'] for x in sample_objects[3:6]]


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_query_sorting(
    es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]
):
    """Test query sorting."""
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
        asc_dates = [
            item.get("published")
            for item in asc_results["items"]
            if "published" in item
        ]
        desc_dates = [
            item.get("published")
            for item in desc_results["items"]
            if "published" in item
        ]

        # Check if dates are in order
        assert asc_dates == sorted(asc_dates)
        assert desc_dates == sorted(desc_dates, reverse=True)


@pytest.mark.slow_integration_test
@pytest.mark.asyncio
async def test_es_query_keywords(
    es_backend: StorageBackend, sample_objects: List[Dict[str, Any]]
):
    """Test querying by keywords."""

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
