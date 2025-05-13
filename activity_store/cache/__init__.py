# Cache backends package
# Contains implementations of the CacheBackend interface

from .memory import InMemoryCacheBackend

__all__ = ["InMemoryCacheBackend"]