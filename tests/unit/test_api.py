import pytest

import activity_store
from activity_store import ActivityStore, Query
from activity_store import ActivityStoreError, ObjectNotFound, InvalidLDObject


class TestPublicAPI:
    """Test the public API exports in the activity_store package."""
    
    def test_package_exports(self):
        """Test that the package exports the expected public components."""
        # These should be directly importable from the package
        assert hasattr(activity_store, "ActivityStore")
        assert hasattr(activity_store, "Query")
        assert hasattr(activity_store, "ActivityStoreError")
        assert hasattr(activity_store, "ObjectNotFound")
        assert hasattr(activity_store, "InvalidLDObject")
        
        # Check that they are the correct types
        assert activity_store.ActivityStore.__name__ == "ActivityStore"
        assert activity_store.Query.__name__ == "Query"
        assert activity_store.ActivityStoreError.__name__ == "ActivityStoreError"
        assert activity_store.ObjectNotFound.__name__ == "ObjectNotFound"
        assert activity_store.InvalidLDObject.__name__ == "InvalidLDObject"
    
    def test_package_all_variable(self):
        """Test that the package's __all__ variable contains the expected items."""
        expected_exports = [
            "ActivityStore",
            "Query",
            "ActivityStoreError",
            "ObjectNotFound",
            "InvalidLDObject"
        ]
        
        # All expected exports should be in __all__
        for export in expected_exports:
            assert export in activity_store.__all__
        
        # And __all__ should not contain any unexpected exports
        assert len(activity_store.__all__) == len(expected_exports)
        for export in activity_store.__all__:
            assert export in expected_exports
    
    def test_direct_imports(self):
        """Test that the components can be imported directly from the package."""
        # These should be directly importable from the package
        from activity_store import ActivityStore
        from activity_store import Query
        from activity_store import ActivityStoreError
        from activity_store import ObjectNotFound
        from activity_store import InvalidLDObject
        
        # Verify they are the correct types
        assert ActivityStore.__name__ == "ActivityStore"
        assert Query.__name__ == "Query"
        assert ActivityStoreError.__name__ == "ActivityStoreError"
        assert ObjectNotFound.__name__ == "ObjectNotFound"
        assert InvalidLDObject.__name__ == "InvalidLDObject"
    
    def test_implementation_details_not_exported(self):
        """Test that implementation details are not exported at the package level."""
        # These should NOT be directly importable from the package
        implementation_details = [
            "StorageBackend",
            "CacheBackend",
            "InMemoryStorageBackend",
            "InMemoryCacheBackend",
            "backends",
            "cache",
            "logging",
            "interfaces",
            "store",
            "ld",
            "utils"
        ]
        
        for item in implementation_details:
            assert not hasattr(activity_store, item)
    
    def test_version_info(self):
        """Test that the package has version information."""
        assert hasattr(activity_store, "__version__")