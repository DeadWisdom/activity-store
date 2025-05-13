# Storage backends package
# Contains implementations of the StorageBackend interface

from .memory import InMemoryStorageBackend

__all__ = ["InMemoryStorageBackend"]