# Activity Store main package initialization
# Exports the public API components for the library

from .exceptions import ActivityStoreError, InvalidLDObject, ObjectNotFound
from .query import Query
from .store import ActivityStore

# Define package version
__version__ = "0.1.0"

# Explicitly define public API
__all__ = [
    "ActivityStore",
    "Query",
    "ActivityStoreError",
    "ObjectNotFound",
    "InvalidLDObject",
]
