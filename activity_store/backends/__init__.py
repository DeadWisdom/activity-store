# Storage backends package
# Contains implementations of the StorageBackend interface

from .memory import InMemoryStorageBackend

try:
    from .elastic import ElasticsearchBackend
    __all__ = ["InMemoryStorageBackend", "ElasticsearchBackend"]
except ImportError:
    # Elasticsearch dependencies not installed
    __all__ = ["InMemoryStorageBackend"]