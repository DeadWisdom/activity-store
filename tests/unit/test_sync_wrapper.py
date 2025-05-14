import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from activity_store.store import ActivityStore, SyncActivityStore
from activity_store.backends.memory import InMemoryStorageBackend
from activity_store.cache.memory import InMemoryCacheBackend
from activity_store.query import Query
from tests.utils import create_test_ld_object


class TestSyncActivityStore:
    """Test the synchronous wrapper for ActivityStore."""
    
    @pytest.fixture
    def sample_object(self):
        """Sample LD-object for testing."""
        return create_test_ld_object(
            id="https://example.com/objects/sync-test",
            type_="Note",
            content="This is a sync test note",
            published="2023-01-01T00:00:00Z"
        )
    
    def test_constructor(self):
        """Test that SyncActivityStore constructor accepts the same args as ActivityStore."""
        # With no arguments
        sync_store = SyncActivityStore()
        assert isinstance(sync_store._async_store, ActivityStore)
        
        # With specific backend and cache
        backend = InMemoryStorageBackend()
        cache = InMemoryCacheBackend()
        namespace = "test"
        
        sync_store = SyncActivityStore(backend=backend, cache=cache, namespace=namespace)
        assert sync_store._async_store.backend is backend
        assert sync_store._async_store.cache is cache
        assert sync_store._async_store.namespace == namespace
    
    def test_context_manager(self):
        """Test using SyncActivityStore as a context manager."""
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.setup = AsyncMock()
        mock_async_store.teardown = AsyncMock()
        
        # Create a SyncActivityStore
        sync_store = SyncActivityStore()
        
        # Replace the async store with our mock
        sync_store._async_store = mock_async_store
        
        # Mock the event loop
        mock_loop = MagicMock()
        sync_store._loop = mock_loop
        
        # Use as context manager
        with sync_store:
            # Check that setup was called
            assert mock_loop.run_until_complete.called
            assert mock_async_store.setup.called
            
        # Check that teardown was called
        assert mock_loop.run_until_complete.called
        assert mock_async_store.teardown.called
    
    def test_run_async(self):
        """Test the _run_async method."""
        # Create a coroutine that returns a value
        async def mock_coro():
            return "mock result"
        
        # Create a SyncActivityStore with a mock loop
        store = SyncActivityStore()
        mock_loop = MagicMock()
        mock_loop.run_until_complete.return_value = "mock result"
        store._loop = mock_loop
        
        # Call _run_async
        result = store._run_async(mock_coro())
        
        # Check that run_until_complete was called with a coroutine
        mock_loop.run_until_complete.assert_called_once()
        assert result == "mock result"
    
    def test_setup_teardown(self):
        """Test explicit setup and teardown."""
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.setup = AsyncMock()
        mock_async_store.teardown = AsyncMock()
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock()
        
        # Call setup
        sync_store.setup()
        
        # Check that _run_async was called with the right coroutine
        sync_store._run_async.assert_called_once()
        
        # Reset mock
        sync_store._run_async.reset_mock()
        
        # Call teardown
        sync_store.teardown()
        
        # Check that _run_async was called with the right coroutine
        sync_store._run_async.assert_called_once()
    
    def test_store_method(self, sample_object):
        """Test the store method delegates to the async method."""
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.store = AsyncMock(return_value=sample_object["id"])
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock(return_value=sample_object["id"])
        
        # Call store
        result = sync_store.store(sample_object)
        
        # Check that _run_async was called
        sync_store._run_async.assert_called_once()
        
        # Check result
        assert result == sample_object["id"]
    
    def test_dereference_method(self, sample_object):
        """Test the dereference method delegates to the async method."""
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.dereference = AsyncMock(return_value=sample_object)
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock(return_value=sample_object)
        
        # Call dereference
        result = sync_store.dereference(sample_object["id"])
        
        # Check that _run_async was called
        sync_store._run_async.assert_called_once()
        
        # Check result
        assert result == sample_object
    
    def test_add_to_collection_method(self, sample_object):
        """Test the add_to_collection method delegates to the async method."""
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.add_to_collection = AsyncMock()
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock(return_value=None)
        
        # Call add_to_collection
        sync_store.add_to_collection(sample_object, "notes")
        
        # Check that _run_async was called
        sync_store._run_async.assert_called_once()
    
    def test_remove_from_collection_method(self):
        """Test the remove_from_collection method delegates to the async method."""
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.remove_from_collection = AsyncMock()
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock(return_value=None)
        
        # Call remove_from_collection
        sync_store.remove_from_collection("test-id", "notes")
        
        # Check that _run_async was called
        sync_store._run_async.assert_called_once()
    
    def test_convert_to_tombstone_method(self, sample_object):
        """Test the convert_to_tombstone method delegates to the async method."""
        # Create tombstone result
        tombstone = {
            "id": sample_object["id"],
            "type": "Tombstone",
            "formerType": sample_object["type"],
            "deleted": "2023-01-01T00:00:00Z"
        }
        
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.convert_to_tombstone = AsyncMock(return_value=tombstone)
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock(return_value=tombstone)
        
        # Call convert_to_tombstone
        result = sync_store.convert_to_tombstone(sample_object)
        
        # Check that _run_async was called
        sync_store._run_async.assert_called_once()
        
        # Check result
        assert result == tombstone
    
    def test_query_method(self):
        """Test the query method delegates to the async method."""
        # Create query result
        query_result = {
            "type": "Collection",
            "totalItems": 1,
            "items": [{"id": "test", "type": "Note"}]
        }
        
        # Create a proper AsyncMock for the async store
        mock_async_store = AsyncMock()
        mock_async_store.query = AsyncMock(return_value=query_result)
        
        # Create a SyncActivityStore with the mocked components
        sync_store = SyncActivityStore()
        sync_store._async_store = mock_async_store
        
        # Mock the _run_async method
        sync_store._run_async = MagicMock(return_value=query_result)
        
        # Call query with Query object
        query = Query(type="Note")
        result = sync_store.query(query)
        
        # Check that _run_async was called
        sync_store._run_async.assert_called_once()
        
        # Check result
        assert result == query_result
        
        # Reset mock
        sync_store._run_async.reset_mock()
        mock_async_store.query.reset_mock()
        
        # Call query with dict
        result = sync_store.query({"type": "Note"})
        
        # Check that _run_async was called again
        sync_store._run_async.assert_called_once()
    
    @pytest.mark.filterwarnings("ignore::pytest_asyncio.plugin.PytestDeprecationWarning")
    def test_integration(self, sample_object):
        """
        Integration test for SyncActivityStore with real backends.
        
        This test verifies that the synchronous wrapper correctly interacts with
        the async store in a real-world scenario.
        """
        backend = InMemoryStorageBackend()
        cache = InMemoryCacheBackend()
        
        # Use with context manager
        with SyncActivityStore(backend=backend, cache=cache) as store:
            # Store an object
            store.store(sample_object)
            
            # Retrieve it
            obj = store.dereference(sample_object["id"])
            assert obj["id"] == sample_object["id"]
            assert obj["type"] == sample_object["type"]
            
            # Add to collection
            store.add_to_collection(obj, "notes")
            
            # Query for objects
            results = store.query(Query(collection="notes"))
            assert results["type"] == "Collection"
            assert results["totalItems"] >= 1
            
            # Convert to tombstone
            tombstone = store.convert_to_tombstone(obj)
            assert tombstone["id"] == obj["id"]
            assert tombstone["type"] == "Tombstone"
            
            # Dereference again
            obj = store.dereference(sample_object["id"])
            assert obj["type"] == "Tombstone"