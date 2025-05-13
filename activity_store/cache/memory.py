# In-memory cache backend implementation
# Provides a non-persistent cache implementation for testing and development

import copy
import time
from typing import Any, Dict, Optional, Tuple

from ..interfaces import CacheBackend


class InMemoryCacheBackend(CacheBackend):
    """
    In-memory implementation of CacheBackend for testing and development.
    
    This backend stores all cache data in memory and does not persist between
    application restarts. It implements TTL-based expiration of cache entries.
    """
    
    def __init__(self):
        # Store cache entries as (value, expiry_time) tuples
        self._cache: Dict[str, Tuple[Dict[str, Any], float]] = {}
    
    async def add(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        """
        Add an item to the cache.
        
        Args:
            key: The cache key
            value: The value to cache
            ttl: Time-to-live in seconds (default: 1 hour)
        """
        expiry_time = time.time() + ttl
        self._cache[key] = (copy.deepcopy(value), expiry_time)
    
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an item from the cache.
        
        Args:
            key: The cache key
            
        Returns:
            The cached value, or None if not found or expired
        """
        if key in self._cache:
            value, expiry_time = self._cache[key]
            
            # Check if expired
            if time.time() > expiry_time:
                # Remove expired entry
                del self._cache[key]
                return None
            
            # Return a deep copy to prevent external modification
            return copy.deepcopy(value)
        
        return None
    
    async def remove(self, key: str) -> None:
        """
        Remove an item from the cache.
        
        Args:
            key: The cache key to remove
        """
        if key in self._cache:
            del self._cache[key]
            
    def _clean_expired(self) -> None:
        """Remove all expired entries from the cache."""
        current_time = time.time()
        expired_keys = [
            key for key, (_, expiry_time) in self._cache.items()
            if current_time > expiry_time
        ]
        
        for key in expired_keys:
            del self._cache[key]