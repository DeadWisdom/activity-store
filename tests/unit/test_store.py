import os
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from activity_store.store import ActivityStore, _require_id, _require_type, _to_async
from activity_store.backends.memory import InMemoryStorageBackend
from activity_store.cache.memory import InMemoryCacheBackend
from activity_store.exceptions import InvalidLDObject
from activity_store.query import Query
from tests.utils import create_test_ld_object, run_concurrently


class TestStoreHelperFunctions:
    """Test the helper functions in the store module."""

    def test_require_id(self):
        """Test the _require_id function."""
        # Valid object with ID
        obj = {"id": "test", "type": "Note"}
        assert _require_id(obj) == "test"

        # Non-dictionary
        with pytest.raises(InvalidLDObject):
            _require_id(123)

        # Missing ID
        with pytest.raises(InvalidLDObject):
            _require_id({"type": "Note"})

        # Non-string ID
        with pytest.raises(InvalidLDObject):
            _require_id({"id": 123, "type": "Note"})

        # Empty ID
        with pytest.raises(InvalidLDObject):
            _require_id({"id": "", "type": "Note"})

    def test_require_type(self):
        """Test the _require_type function."""
        # Valid object with string type
        obj = {"id": "test", "type": "Note"}
        assert _require_type(obj) == "Note"

        # Valid object with list type
        obj = {"id": "test", "type": ["Note", "Article"]}
        assert _require_type(obj) == ["Note", "Article"]

        # Non-dictionary
        with pytest.raises(InvalidLDObject):
            _require_type(123)

        # Missing type
        with pytest.raises(InvalidLDObject):
            _require_type({"id": "test"})

        # Empty type
        with pytest.raises(InvalidLDObject):
            _require_type({"id": "test", "type": ""})

    @pytest.mark.asyncio
    async def test_to_async(self):
        """Test the _to_async function that converts sync functions to async."""

        # Create a sync function
        def sync_func(a, b):
            return a + b

        # Convert to async
        async_func = _to_async(sync_func)

        # Call the async function
        result = await async_func(1, 2)
        assert result == 3


class TestActivityStore:
    """Test the ActivityStore class."""

    @pytest.fixture
    def sample_object(self):
        """Sample LD-object for testing."""
        return create_test_ld_object(
            id="https://example.com/objects/123",
            type_="Note",
            content="This is a test note",
            published="2023-01-01T00:00:00Z",
        )

    @pytest.mark.asyncio
    async def test_constructor_defaults(self):
        """Test that ActivityStore constructor uses defaults when not provided."""
        # With factory-provided backend and cache
        with patch("activity_store.store.ActivityStore.backend_factory") as mock_backend_factory:
            with patch("activity_store.store.ActivityStore.cache_factory") as mock_cache_factory:
                mock_backend = MagicMock()
                mock_cache = MagicMock()
                mock_backend_factory.return_value = mock_backend
                mock_cache_factory.return_value = mock_cache

                store = ActivityStore()

                # Check that factory methods were called
                mock_backend_factory.assert_called_once()
                mock_cache_factory.assert_called_once()

                # Check that backend and cache were set
                assert store.backend is mock_backend
                assert store.cache is mock_cache

                # Check default namespace
                assert store.namespace == "activity_store"

    @pytest.mark.asyncio
    async def test_constructor_with_args(self):
        """Test that ActivityStore constructor accepts backend, cache, and namespace."""
        backend = InMemoryStorageBackend()
        cache = InMemoryCacheBackend()
        namespace = "test"

        store = ActivityStore(backend=backend, cache=cache, namespace=namespace)

        assert store.backend is backend
        assert store.cache is cache
        assert store.namespace == namespace

    @pytest.mark.asyncio
    async def test_constructor_with_env_vars(self):
        """Test that ActivityStore respects environment variables."""
        # Set environment variable
        with patch.dict(os.environ, {"ACTIVITY_STORE_NAMESPACE": "env-test"}):
            store = ActivityStore()
            assert store.namespace == "env-test"

    @pytest.mark.asyncio
    async def test_async_context_manager(self):
        """Test using ActivityStore as an async context manager."""
        mock_backend = AsyncMock()
        mock_cache = AsyncMock()

        async with ActivityStore(backend=mock_backend, cache=mock_cache) as store:
            pass

    @pytest.mark.asyncio
    async def test_setup_teardown(self):
        """Test explicit setup and teardown."""
        mock_backend = AsyncMock()
        mock_cache = AsyncMock()

        store = ActivityStore(backend=mock_backend, cache=mock_cache)

        # Call setup
        await store.setup()
        mock_backend.setup.assert_called_once()
        mock_cache.setup.assert_called_once()

        # Call teardown
        await store.teardown(True)
        mock_backend.teardown.assert_called_once()
        mock_cache.teardown.assert_called_once()

    @pytest.mark.asyncio
    async def test_backend_factory(self):
        """Test the backend_factory method."""
        # Default (memory)
        backend = ActivityStore.backend_factory()
        assert isinstance(backend, InMemoryStorageBackend)

        # Specified via environment variable
        with patch.dict(os.environ, {"ACTIVITY_STORE_BACKEND": "memory"}):
            backend = ActivityStore.backend_factory()
            assert isinstance(backend, InMemoryStorageBackend)

        # Unknown backend type should default to in-memory
        with patch.dict(os.environ, {"ACTIVITY_STORE_BACKEND": "unknown"}):
            backend = ActivityStore.backend_factory()
            assert isinstance(backend, InMemoryStorageBackend)

    @pytest.mark.asyncio
    async def test_cache_factory(self):
        """Test the cache_factory method."""
        # Default (memory)
        cache = ActivityStore.cache_factory()
        assert isinstance(cache, InMemoryCacheBackend)

        # Specified via environment variable
        with patch.dict(os.environ, {"ACTIVITY_STORE_CACHE": "memory"}):
            cache = ActivityStore.cache_factory()
            assert isinstance(cache, InMemoryCacheBackend)

        # Unknown cache type should default to in-memory
        with patch.dict(os.environ, {"ACTIVITY_STORE_CACHE": "unknown"}):
            cache = ActivityStore.cache_factory()
            assert isinstance(cache, InMemoryCacheBackend)

    @pytest.mark.asyncio
    async def test_store_method(self, activity_store, sample_object):
        """Test the store method."""
        object_id = await activity_store.store(sample_object)

        assert object_id == sample_object["id"]

        # Verify stored in backend
        stored = await activity_store.backend.get(object_id)
        assert stored["id"] == sample_object["id"]
        assert stored["type"] == sample_object["type"]

        # Verify added to cache
        cached = await activity_store.cache.get(object_id)
        assert cached is not None
        assert cached["id"] == sample_object["id"]

    @pytest.mark.asyncio
    async def test_store_method_adds_context(self, activity_store):
        """Test that store adds a default context if missing."""
        obj = {"id": "test", "type": "Note", "content": "Test"}

        object_id = await activity_store.store(obj)

        # Retrieve and verify context was added
        stored = await activity_store.backend.get(object_id)
        assert "@context" in stored
        assert stored["@context"] == "https://www.w3.org/ns/activitystreams"

    @pytest.mark.asyncio
    async def test_store_method_validation(self, activity_store):
        """Test that store validates input objects."""
        # Missing ID
        with pytest.raises(InvalidLDObject):
            await activity_store.store({"type": "Note"})

        # Missing type
        with pytest.raises(InvalidLDObject):
            await activity_store.store({"id": "test"})

    @pytest.mark.asyncio
    async def test_dereference_method(self, activity_store, sample_object):
        """Test the dereference method."""
        # Store the object first
        await activity_store.store(sample_object)

        # Should be in cache
        cached = await activity_store.cache.get(sample_object["id"])
        assert cached is not None

        # Dereference from cache
        dereferenced = await activity_store.dereference(sample_object["id"])
        assert dereferenced["id"] == sample_object["id"]
        assert dereferenced["type"] == sample_object["type"]

        # Clear cache and verify it falls back to backend
        await activity_store.cache.remove(sample_object["id"])
        dereferenced = await activity_store.dereference(sample_object["id"])
        assert dereferenced["id"] == sample_object["id"]

        # Should be back in cache
        cached = await activity_store.cache.get(sample_object["id"])
        assert cached is not None

    @pytest.mark.asyncio
    async def test_dereference_nonexistent(self, activity_store):
        """Test dereferencing a non-existent object returns None."""
        assert await activity_store.dereference("nonexistent") is None

    @pytest.mark.asyncio
    async def test_add_to_collection(self, activity_store, sample_object):
        """Test adding an object to a collection."""
        # Store the object first
        await activity_store.store(sample_object)

        # Add to collection
        collection = "notes"
        await activity_store.add_to_collection(sample_object, collection)

        # Verify added to collection
        obj = await activity_store.backend.get(sample_object["id"], collection)
        assert obj["id"] == sample_object["id"]
        assert obj["type"] == sample_object["type"]

    @pytest.mark.asyncio
    async def test_add_to_collection_partial(self, activity_store):
        """Test that a partial representation is stored in collection."""
        # Object with lots of fields
        obj = {
            "id": "test",
            "type": "Note",
            "content": "Test note with lots of fields",
            "published": "2023-01-01T00:00:00Z",
            "updated": "2023-01-02T00:00:00Z",
            "name": "Test Note",
            "summary": "Test summary",
            "attachment": {"url": "https://example.com/attachment"},
            "attributedTo": {"id": "https://example.com/users/1", "type": "Person"},
        }

        # Add to collection
        collection = "notes"
        await activity_store.add_to_collection(obj, collection)

        # Retrieve from collection
        partial = await activity_store.backend.get(obj["id"], collection)

        # Should have core fields
        assert partial["id"] == obj["id"]
        assert partial["type"] == obj["type"]

        # Should have optional display fields
        assert partial["name"] == obj["name"]
        assert partial["summary"] == obj["summary"]
        assert partial["published"] == obj["published"]
        assert partial["updated"] == obj["updated"]

        # But should not have other fields
        assert "content" not in partial
        assert "attachment" not in partial
        assert "attributedTo" not in partial

    @pytest.mark.asyncio
    async def test_remove_from_collection(self, activity_store, sample_object):
        """Test removing an object from a collection."""
        # Store the object first
        await activity_store.store(sample_object)

        # Add to collection
        collection = "notes"
        await activity_store.add_to_collection(sample_object, collection)

        # Verify added to collection
        obj = await activity_store.backend.get(sample_object["id"], collection)
        assert obj is not None

        # Remove from collection
        await activity_store.remove_from_collection(sample_object["id"], collection)

        # Verify removed from collection
        assert await activity_store.backend.get(sample_object["id"], collection) is None

        # But still exists in general storage
        obj = await activity_store.backend.get(sample_object["id"])
        assert obj is not None

    @pytest.mark.asyncio
    async def test_query_method(self, activity_store, sample_object):
        """Test the query method."""
        # Store the object first
        await activity_store.store(sample_object)

        # Add to collection
        collection = "notes"
        await activity_store.add_to_collection(sample_object, collection)

        # Query by type with Query object
        results = await activity_store.query(Query(type="Note"))
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

        # Find our object in the results
        found = False
        for item in results["items"]:
            if item["id"] == sample_object["id"]:
                found = True
                break
        assert found is True

        # Query by collection with Query object
        results = await activity_store.query(Query(collection=collection))
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

        # Query with dict instead of Query object
        results = await activity_store.query({"type": "Note", "collection": collection})
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

    @pytest.mark.asyncio
    async def test_query_method_kwargs(self, activity_store, sample_object):
        """Test the query method using keyword arguments."""
        # Store the object first
        await activity_store.store(sample_object)

        # Add to collection
        collection = "notes"
        await activity_store.add_to_collection(sample_object, collection)

        # Query using keyword arguments
        results = await activity_store.query(type="Note")
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

        # Query with multiple keyword arguments
        results = await activity_store.query(type="Note", collection=collection)
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

        # Query with empty call (should return all objects)
        results = await activity_store.query()
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

        # Query with mixed parameters (dict + kwargs, kwargs override)
        results = await activity_store.query({"type": "Article"}, type="Note")
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

        # Find our object in the results (confirming kwargs overrode dict)
        found = False
        for item in results["items"]:
            if item["id"] == sample_object["id"]:
                found = True
                break
        assert found is True

        # Query with Query object + kwargs (kwargs override)
        results = await activity_store.query(Query(type="Article", size=5), type="Note", size=10)
        assert results["type"] == "Collection"
        assert results["totalItems"] >= 1

    @pytest.mark.asyncio
    async def test_convert_to_tombstone(self, activity_store, sample_object):
        """Test converting an object to a Tombstone."""
        # Store the object first
        await activity_store.store(sample_object)

        # Convert to tombstone
        tombstone = await activity_store.convert_to_tombstone(sample_object)

        # Check basic properties
        assert tombstone["id"] == sample_object["id"]
        assert tombstone["type"] == "Tombstone"
        assert tombstone["formerType"] == sample_object["type"]
        assert "deleted" in tombstone

        # Verify stored in backend
        stored = await activity_store.backend.get(sample_object["id"])
        assert stored["type"] == "Tombstone"

        # Verify updated in cache
        cached = await activity_store.cache.get(sample_object["id"])
        assert cached["type"] == "Tombstone"

    @pytest.mark.asyncio
    async def test_concurrent_operations(self, activity_store):
        """Test multiple concurrent operations."""
        # Create test objects
        objects = [
            create_test_ld_object(id=f"https://example.com/objects/{i}", type_="Note", content=f"Test note {i}")
            for i in range(1, 6)
        ]

        # Store objects concurrently
        store_tasks = [activity_store.store(obj) for obj in objects]
        await run_concurrently(*store_tasks)

        # Dereference objects concurrently
        dereference_tasks = [activity_store.dereference(obj["id"]) for obj in objects]
        results = await run_concurrently(*dereference_tasks)

        # Verify all objects were retrieved
        for i, result in enumerate(results):
            assert result["id"] == objects[i]["id"]
            assert result["type"] == objects[i]["type"]
