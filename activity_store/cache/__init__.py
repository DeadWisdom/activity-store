# Cache backends package
# Contains implementations of the CacheBackend interface

from .memory import InMemoryCacheBackend

try:
    from .redis import RedisCacheBackend
    __all__ = ["InMemoryCacheBackend", "RedisCacheBackend"]
except ImportError:
    # Redis dependencies not installed
    __all__ = ["InMemoryCacheBackend"]