import pytest

from activity_store.backends.memory import InMemoryStorageBackend
from activity_store.cache.memory import InMemoryCacheBackend
from activity_store.query import Query
from tests.utils import create_test_ld_object


class TestInMemoryStorageBackend:
    """Test the InMemoryStorageBackend implementation."""

    @pytest.fixture
    async def backend(self):
        """Create a fresh InMemoryStorageBackend for each test."""
        backend = InMemoryStorageBackend()
        yield backend
        await backend.teardown()

    @pytest.fixture
    def sample_objects(self):
        """Create sample LD-objects for testing."""
        return [
            create_test_ld_object(
                id=f"https://example.com/objects/{i}",
                type_="Note",
                content=f"Test note {i}",
                published=f"2023-01-0{i}T00:00:00Z",
            )
            for i in range(1, 6)
        ]

    @pytest.mark.asyncio
    async def test_add_and_get(self, backend, sample_objects):
        """Test adding and retrieving objects."""
        # Add an object
        obj = sample_objects[0]
        await backend.add(obj)

        # Retrieve it
        retrieved = await backend.get(obj["id"])

        # Should be equal but not the same object (deep copied)
        assert retrieved == obj
        assert retrieved is not obj

    @pytest.mark.asyncio
    async def test_add_to_collection(self, backend, sample_objects):
        """Test adding objects to a collection."""
        # Add objects to a collection
        collection = "notes"
        for obj in sample_objects:
            await backend.add(obj, collection)

        # Retrieve from collection
        for obj in sample_objects:
            retrieved = await backend.get(obj["id"], collection)
            assert retrieved["id"] == obj["id"]
            assert retrieved["type"] == obj["type"]

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, backend):
        """Test getting a non-existent object returns None."""
        await backend.get("nonexistent") is None
        await backend.get("nonexistent", "collection") is None

    @pytest.mark.asyncio
    async def test_remove(self, backend, sample_objects):
        """Test removing objects."""
        # Add objects
        for obj in sample_objects:
            await backend.add(obj)

        # Remove one
        obj_id = sample_objects[0]["id"]
        await backend.remove(obj_id)

        # Should be gone
        assert await backend.get(obj_id) is None

        # Others should still be there
        for obj in sample_objects[1:]:
            retrieved = await backend.get(obj["id"])
            assert retrieved["id"] == obj["id"]

    @pytest.mark.asyncio
    async def test_remove_from_collection(self, backend, sample_objects):
        """Test removing objects from a collection."""
        # Add objects to a collection
        collection = "notes"
        for obj in sample_objects:
            await backend.add(obj, collection)

        # Remove one from the collection
        obj_id = sample_objects[0]["id"]
        await backend.remove(obj_id, collection)

        # Should be gone from collection
        assert await backend.get(obj_id, collection) is None

        # But should still exist in general storage
        retrieved = await backend.get(obj_id)
        assert retrieved["id"] == obj_id

    @pytest.mark.asyncio
    async def test_query_empty(self, backend):
        """Test querying with no objects returns empty collection."""
        query = Query(size=10)
        results = await backend.query(query)

        assert results["type"] == "Collection"
        assert results["totalItems"] == 0
        assert results["items"] == []

    @pytest.mark.asyncio
    async def test_query_by_type(self, backend, sample_objects):
        """Test querying objects by type."""
        # Add objects
        for obj in sample_objects:
            await backend.add(obj)

        # Add one with different type
        article = create_test_ld_object(id="https://example.com/articles/1", type_="Article", content="Test article")
        await backend.add(article)

        # Query for Notes
        query = Query(type="Note")
        results = await backend.query(query)

        assert results["type"] == "Collection"
        assert results["totalItems"] == 5
        assert len(results["items"]) == 5
        for item in results["items"]:
            assert "Note" in item["type"]

    @pytest.mark.asyncio
    async def test_query_by_collection(self, backend, sample_objects):
        """Test querying objects by collection."""
        # Add objects to different collections
        for i, obj in enumerate(sample_objects):
            collection = "notes" if i < 3 else "archive"
            await backend.add(obj, collection)

        # Query for notes collection
        query = Query(collection="notes")
        results = await backend.query(query)

        assert results["type"] == "Collection"
        assert results["totalItems"] == 3
        assert len(results["items"]) == 3

    @pytest.mark.asyncio
    async def test_query_by_text(self, backend, sample_objects):
        """Test querying objects by text content."""
        # Add objects
        for obj in sample_objects:
            await backend.add(obj)

        # Query for text in note 3
        query = Query(text="Test note 3")
        results = await backend.query(query)

        assert results["type"] == "Collection"
        assert results["totalItems"] == 1
        assert len(results["items"]) == 1
        assert results["items"][0]["content"] == "Test note 3"

    @pytest.mark.asyncio
    async def test_query_pagination(self, backend, sample_objects):
        """Test query pagination with size parameter."""
        # Add objects
        for obj in sample_objects:
            await backend.add(obj)

        # Query with size limit
        query = Query(size=2)
        results = await backend.query(query)

        assert results["type"] == "Collection"
        assert results["totalItems"] == 5  # Total count is accurate
        assert len(results["items"]) == 2  # But only 2 items returned


class TestInMemoryCacheBackend:
    """Test the InMemoryCacheBackend implementation."""

    @pytest.fixture
    def cache(self):
        """Create a fresh InMemoryCacheBackend for each test."""
        return InMemoryCacheBackend()

    @pytest.fixture
    def sample_value(self):
        """Create a sample value for caching."""
        return {"id": "test", "type": "Note", "content": "Test note"}

    @pytest.mark.asyncio
    async def test_add_and_get(self, cache, sample_value):
        """Test adding and retrieving cached values."""
        # Add to cache
        await cache.add("test_key", sample_value)

        # Retrieve from cache
        retrieved = await cache.get("test_key")

        # Should be equal but not the same object (deep copied)
        assert retrieved == sample_value
        assert retrieved is not sample_value

    @pytest.mark.asyncio
    async def test_get_nonexistent(self, cache):
        """Test getting a non-existent key returns None."""
        result = await cache.get("nonexistent")
        assert result is None

    @pytest.mark.asyncio
    async def test_remove(self, cache, sample_value):
        """Test removing cached values."""
        # Add to cache
        await cache.add("test_key", sample_value)

        # Verify it's there
        assert await cache.get("test_key") is not None

        # Remove it
        await cache.remove("test_key")

        # Should be gone
        assert await cache.get("test_key") is None

    @pytest.mark.asyncio
    async def test_ttl_expiration(self, cache, sample_value, monkeypatch):
        """Test TTL-based expiration of cached values."""
        import time

        # Mock time.time to control expiration
        current_time = 1000.0
        monkeypatch.setattr(time, "time", lambda: current_time)

        # Add to cache with short TTL
        await cache.add("test_key", sample_value, ttl=10)

        # Verify it's there
        assert await cache.get("test_key") is not None

        # Advance time past TTL
        current_time += 11

        # Should be expired now
        assert await cache.get("test_key") is None

    @pytest.mark.asyncio
    async def test_clean_expired(self, cache, sample_value, monkeypatch):
        """Test internal cleaning of expired entries."""
        import time

        # Mock time.time to control expiration
        current_time = 1000.0
        monkeypatch.setattr(time, "time", lambda: current_time)

        # Add multiple entries with different TTLs
        await cache.add("key1", sample_value, ttl=10)
        await cache.add("key2", sample_value, ttl=20)
        await cache.add("key3", sample_value, ttl=30)

        # Advance time to expire key1
        current_time += 15

        # Clean expired entries
        cache._clean_expired()

        # key1 should be gone, others should remain
        assert await cache.get("key1") is None
        assert await cache.get("key2") is not None
        assert await cache.get("key3") is not None

        # Advance time to expire key2
        current_time += 10

        # Clean expired entries
        cache._clean_expired()

        # key2 should now be gone too
        assert await cache.get("key2") is None
        assert await cache.get("key3") is not None
