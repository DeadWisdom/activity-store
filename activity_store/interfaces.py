# Abstract base classes defining interfaces for storage and cache backends
# These interfaces define the contract that all backend implementations must follow

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from .query import Query


class StorageBackend(ABC):
    """Abstract base class for storage backends that persist LD-objects."""
    
    @abstractmethod
    async def add(self, ld_object: Dict[str, Any], collection: Optional[str] = None) -> None:
        """
        Add an LD-object to the storage.
        
        Args:
            ld_object: The LD-object to store
            collection: Optional collection to add the object to
        """
        pass
    
    @abstractmethod
    async def remove(self, id: str, collection: Optional[str] = None) -> None:
        """
        Remove an LD-object from storage.
        
        Args:
            id: The ID of the object to remove
            collection: Optional collection to remove the object from
        """
        pass
    
    @abstractmethod
    async def get(self, id: str, collection: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve an LD-object from storage.
        
        Args:
            id: The ID of the object to retrieve
            collection: Optional collection to retrieve the object from
            
        Returns:
            The retrieved LD-object
        """
        pass
    
    @abstractmethod
    async def query(self, query: Query) -> Dict[str, Any]:
        """
        Query for LD-objects matching the specified criteria.
        
        Args:
            query: The query parameters
            
        Returns:
            A collection containing the query results
        """
        pass
    
    async def setup(self) -> None:
        """Set up the backend (default no-op implementation)."""
        pass
    
    async def teardown(self) -> None:
        """Tear down the backend (default no-op implementation)."""
        pass


class CacheBackend(ABC):
    """Abstract base class for cache backends that temporarily store dereferenced LD-objects."""
    
    @abstractmethod
    async def add(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        """
        Add an item to the cache.
        
        Args:
            key: The cache key
            value: The value to cache
            ttl: Time-to-live in seconds (default: 1 hour)
        """
        pass
    
    @abstractmethod
    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an item from the cache.
        
        Args:
            key: The cache key
            
        Returns:
            The cached value, or None if not found
        """
        pass
    
    @abstractmethod
    async def remove(self, key: str) -> None:
        """
        Remove an item from the cache.
        
        Args:
            key: The cache key to remove
        """
        pass
    
    async def setup(self) -> None:
        """Set up the cache (default no-op implementation)."""
        pass
    
    async def teardown(self) -> None:
        """Tear down the cache (default no-op implementation)."""
        pass