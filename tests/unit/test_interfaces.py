import asyncio
import inspect
import pytest
from abc import ABC

from activity_store.interfaces import StorageBackend, CacheBackend
from activity_store.query import Query


class TestInterfaces:
    """Test the interface definitions in activity_store.interfaces module."""
    
    def test_storage_backend_is_abstract(self):
        """Test that StorageBackend is an abstract base class."""
        assert inspect.isclass(StorageBackend)
        assert issubclass(StorageBackend, ABC)
        
        # Verify that we can't instantiate it directly
        with pytest.raises(TypeError):
            StorageBackend()
    
    def test_cache_backend_is_abstract(self):
        """Test that CacheBackend is an abstract base class."""
        assert inspect.isclass(CacheBackend)
        assert issubclass(CacheBackend, ABC)
        
        # Verify that we can't instantiate it directly
        with pytest.raises(TypeError):
            CacheBackend()
    
    def test_storage_backend_abstract_methods(self):
        """Test that StorageBackend defines the required abstract methods."""
        # Get the list of abstract methods
        abstract_methods = StorageBackend.__abstractmethods__
        
        # Check that all required methods are in the list
        assert "add" in abstract_methods
        assert "remove" in abstract_methods
        assert "get" in abstract_methods
        assert "query" in abstract_methods
        
        # Check method signatures
        methods = inspect.getmembers(StorageBackend, predicate=inspect.isfunction)
        method_dict = {name: method for name, method in methods}
        
        # Verify add method signature
        add_sig = inspect.signature(method_dict["add"])
        add_params = list(add_sig.parameters.keys())
        assert add_params[0] == "self"
        assert add_params[1] == "ld_object"
        assert add_params[2] == "collection"
        assert add_sig.parameters["collection"].default is None
        
        # Verify remove method signature
        remove_sig = inspect.signature(method_dict["remove"])
        remove_params = list(remove_sig.parameters.keys())
        assert remove_params[0] == "self"
        assert remove_params[1] == "id"
        assert remove_params[2] == "collection"
        assert remove_sig.parameters["collection"].default is None
        
        # Verify get method signature
        get_sig = inspect.signature(method_dict["get"])
        get_params = list(get_sig.parameters.keys())
        assert get_params[0] == "self"
        assert get_params[1] == "id"
        assert get_params[2] == "collection"
        assert get_sig.parameters["collection"].default is None
        
        # Verify query method signature
        query_sig = inspect.signature(method_dict["query"])
        query_params = list(query_sig.parameters.keys())
        assert query_params[0] == "self"
        assert query_params[1] == "query"
        
        # Verify setup and teardown are not abstract
        assert "setup" not in abstract_methods
        assert "teardown" not in abstract_methods
    
    def test_cache_backend_abstract_methods(self):
        """Test that CacheBackend defines the required abstract methods."""
        # Get the list of abstract methods
        abstract_methods = CacheBackend.__abstractmethods__
        
        # Check that all required methods are in the list
        assert "add" in abstract_methods
        assert "get" in abstract_methods
        assert "remove" in abstract_methods
        
        # Check method signatures
        methods = inspect.getmembers(CacheBackend, predicate=inspect.isfunction)
        method_dict = {name: method for name, method in methods}
        
        # Verify add method signature
        add_sig = inspect.signature(method_dict["add"])
        add_params = list(add_sig.parameters.keys())
        assert add_params[0] == "self"
        assert add_params[1] == "key"
        assert add_params[2] == "value"
        assert add_params[3] == "ttl"
        assert add_sig.parameters["ttl"].default == 3600  # Default TTL is 1 hour
        
        # Verify get method signature
        get_sig = inspect.signature(method_dict["get"])
        get_params = list(get_sig.parameters.keys())
        assert get_params[0] == "self"
        assert get_params[1] == "key"
        
        # Verify remove method signature
        remove_sig = inspect.signature(method_dict["remove"])
        remove_params = list(remove_sig.parameters.keys())
        assert remove_params[0] == "self"
        assert remove_params[1] == "key"
        
        # Verify setup and teardown are not abstract
        assert "setup" not in abstract_methods
        assert "teardown" not in abstract_methods
    
    def test_storage_backend_setup_teardown(self):
        """Test that StorageBackend has default setup/teardown implementations."""
        # Check that the methods exist and are not abstract
        methods = inspect.getmembers(StorageBackend, predicate=inspect.isfunction)
        method_dict = {name: method for name, method in methods}
        
        assert "setup" in method_dict
        assert "teardown" in method_dict
        
        # Verify these are coroutines
        assert asyncio.iscoroutinefunction(method_dict["setup"])
        assert asyncio.iscoroutinefunction(method_dict["teardown"])
    
    def test_cache_backend_setup_teardown(self):
        """Test that CacheBackend has default setup/teardown implementations."""
        # Check that the methods exist and are not abstract
        methods = inspect.getmembers(CacheBackend, predicate=inspect.isfunction)
        method_dict = {name: method for name, method in methods}
        
        assert "setup" in method_dict
        assert "teardown" in method_dict
        
        # Verify these are coroutines
        assert asyncio.iscoroutinefunction(method_dict["setup"])
        assert asyncio.iscoroutinefunction(method_dict["teardown"])
    
    def test_concrete_implementation_requirements(self):
        """
        Test that a concrete implementation of the interfaces must implement
        all abstract methods to be instantiable.
        """
        # Create a concrete subclass missing some methods
        class IncompleteStorageBackend(StorageBackend):
            async def add(self, ld_object, collection=None):
                pass
            
            async def remove(self, id, collection=None):
                pass
            
            # Missing get and query methods
        
        with pytest.raises(TypeError):
            IncompleteStorageBackend()
        
        # Create a concrete subclass with all methods
        class CompleteStorageBackend(StorageBackend):
            async def add(self, ld_object, collection=None):
                pass
            
            async def remove(self, id, collection=None):
                pass
            
            async def get(self, id, collection=None):
                pass
            
            async def query(self, query):
                pass
        
        # Should not raise
        CompleteStorageBackend()
        
        # Same for CacheBackend
        class IncompleteCacheBackend(CacheBackend):
            async def add(self, key, value, ttl=3600):
                pass
            
            # Missing get and remove methods
        
        with pytest.raises(TypeError):
            IncompleteCacheBackend()
        
        class CompleteCacheBackend(CacheBackend):
            async def add(self, key, value, ttl=3600):
                pass
            
            async def get(self, key):
                pass
            
            async def remove(self, key):
                pass
        
        # Should not raise
        CompleteCacheBackend()