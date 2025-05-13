
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

    def test_direct_imports(self):
        """Test that the components can be imported directly from the package."""
        # These should be directly importable from the package

        # Verify they are the correct types
        assert ActivityStore.__name__ == "ActivityStore"
        assert Query.__name__ == "Query"
        assert ActivityStoreError.__name__ == "ActivityStoreError"
        assert ObjectNotFound.__name__ == "ObjectNotFound"
        assert InvalidLDObject.__name__ == "InvalidLDObject"

    def test_version_info(self):
        """Test that the package has version information."""
        assert hasattr(activity_store, "__version__")
