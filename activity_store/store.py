# Activity Store core implementation
# Provides the main ActivityStore class for managing ActivityStream objects

import asyncio
import functools
import os
from datetime import datetime
import datetime as dt
from typing import Any, Callable, Dict, Optional, TypeVar, Union

from .cache import InMemoryCacheBackend
from .backends import InMemoryStorageBackend
from .exceptions import InvalidLDObject, ObjectNotFound
from .interfaces import CacheBackend, StorageBackend
from .logging import get_logger
from .query import Query

T = TypeVar("T")
R = TypeVar("R")

# Default namespace to use if none provided
DEFAULT_NAMESPACE = "activity_store"

# Logger for this module
logger = get_logger("store")


def _require_id(ld_object: Dict[str, Any]) -> str:
    """
    Extract and validate the ID from an LD-object.
    
    Args:
        ld_object: The LD-object to validate
        
    Returns:
        The object's ID
        
    Raises:
        InvalidLDObject: If the object doesn't have a valid ID
    """
    if not isinstance(ld_object, dict):
        raise InvalidLDObject("LD-object must be a dictionary")
    
    object_id = ld_object.get("id")
    if not object_id or not isinstance(object_id, str):
        raise InvalidLDObject("LD-object must have a string 'id' field")
    
    return object_id


def _require_type(ld_object: Dict[str, Any]) -> Union[str, list]:
    """
    Extract and validate the type from an LD-object.
    
    Args:
        ld_object: The LD-object to validate
        
    Returns:
        The object's type (string or list of strings)
        
    Raises:
        InvalidLDObject: If the object doesn't have a valid type
    """
    if not isinstance(ld_object, dict):
        raise InvalidLDObject("LD-object must be a dictionary")
    
    object_type = ld_object.get("type")
    if not object_type:
        raise InvalidLDObject("LD-object must have a 'type' field")
    
    if not isinstance(object_type, (str, list)):
        raise InvalidLDObject("LD-object 'type' must be a string or list of strings")
    
    return object_type


def _to_async(func: Callable[..., R]) -> Callable[..., asyncio.Future[R]]:
    """
    Convert a synchronous function to an asynchronous one.
    
    Args:
        func: The synchronous function to convert
        
    Returns:
        An asynchronous version of the function
    """
    @functools.wraps(func)
    async def wrapper(*args, **kwargs) -> R:
        loop = asyncio.get_event_loop()
        return await loop.run_in_executor(
            None, 
            lambda: func(*args, **kwargs)
        )
    
    return wrapper


class ActivityStore:
    """
    Main class for storing and retrieving ActivityStream objects.
    
    This class provides an async-first API for working with ActivityStream
    objects, with support for storage backends, caching, and collections.
    """
    
    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        cache: Optional[CacheBackend] = None,
        namespace: Optional[str] = None
    ):
        """
        Initialize an ActivityStore instance.
        
        Args:
            backend: Storage backend to use (defaults to factory-created backend)
            cache: Cache backend to use (defaults to factory-created cache)
            namespace: Namespace for this store (defaults to ACTIVITY_STORE_NAMESPACE or 'activity_store')
        """
        self.backend = backend or self.backend_factory()
        self.cache = cache or self.cache_factory()
        self.namespace = namespace or os.environ.get("ACTIVITY_STORE_NAMESPACE", DEFAULT_NAMESPACE)
        
        # Whether setup/teardown has been performed
        self._setup_done = False
        self._torn_down = False
    
    async def __aenter__(self) -> "ActivityStore":
        """
        Set up the ActivityStore for use as an async context manager.
        
        Returns:
            Self
        """
        await self.setup()
        return self
    
    async def __aexit__(self, exc_type, exc_val, exc_tb) -> None:
        """Tear down the ActivityStore when exiting the async context."""
        await self.teardown()
    
    async def setup(self) -> None:
        """Set up the backend and cache."""
        if not self._setup_done:
            await self.backend.setup()
            await self.cache.setup()
            self._setup_done = True
            self._torn_down = False
    
    async def teardown(self) -> None:
        """Tear down the backend and cache."""
        if self._setup_done and not self._torn_down:
            await self.backend.teardown()
            await self.cache.teardown()
            self._torn_down = True
    
    @staticmethod
    def backend_factory() -> StorageBackend:
        """
        Create a backend based on configuration.
        
        Respects the ACTIVITY_STORE_BACKEND environment variable.
        Supports:
        - 'memory': In-memory backend (default)
        - 'elasticsearch': Elasticsearch backend
        
        Returns:
            A StorageBackend instance
        """
        backend_type = os.environ.get("ACTIVITY_STORE_BACKEND", "memory")
        namespace = os.environ.get("ACTIVITY_STORE_NAMESPACE", DEFAULT_NAMESPACE)
        
        if backend_type == "memory":
            return InMemoryStorageBackend()
        elif backend_type == "elasticsearch":
            try:
                from .backends.elastic import ElasticsearchBackend
                
                # Check for cloud configuration
                cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
                password = os.environ.get("ELASTICSEARCH_PASSWORD")
                
                if cloud_id and password:
                    from elasticsearch import AsyncElasticsearch
                    client = AsyncElasticsearch(
                        cloud_id=cloud_id,
                        api_key=password,
                    )
                    return ElasticsearchBackend(
                        client=client,
                        index_prefix=namespace
                    )
                else:
                    # Use standard URL connection
                    es_url = os.environ.get("ES_URL", "http://localhost:9200")
                    return ElasticsearchBackend(
                        es_url=es_url,
                        index_prefix=namespace
                    )
            except ImportError:
                logger.error(
                    "Failed to create Elasticsearch backend, missing dependencies. Install with `pip install activity-store[es]`",
                    metadata={"requested_backend": backend_type}
                )
                return InMemoryStorageBackend()
        else:
            # Default to in-memory if backend type not recognized
            logger.warning(
                f"Unknown backend type: {backend_type}, using InMemoryStorageBackend",
                metadata={"requested_backend": backend_type}
            )
            return InMemoryStorageBackend()
    
    @staticmethod
    def cache_factory() -> CacheBackend:
        """
        Create a cache based on configuration.
        
        Respects the ACTIVITY_STORE_CACHE environment variable.
        Supports:
        - 'memory': In-memory cache (default)
        - 'redis': Redis cache
        
        Returns:
            A CacheBackend instance
        """
        cache_type = os.environ.get("ACTIVITY_STORE_CACHE", "memory")
        namespace = os.environ.get("ACTIVITY_STORE_NAMESPACE", DEFAULT_NAMESPACE)
        
        if cache_type == "memory":
            return InMemoryCacheBackend()
        elif cache_type == "redis":
            try:
                from .cache.redis import RedisCacheBackend
                redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
                return RedisCacheBackend(
                    redis_url=redis_url,
                    namespace=namespace
                )
            except ImportError:
                logger.error(
                    "Failed to create Redis cache, missing dependencies. Install with `pip install activity-store[redis]`",
                    metadata={"requested_cache": cache_type}
                )
                return InMemoryCacheBackend()
        else:
            # Default to in-memory if cache type not recognized
            logger.warning(
                f"Unknown cache type: {cache_type}, using InMemoryCacheBackend",
                metadata={"requested_cache": cache_type}
            )
            return InMemoryCacheBackend()
    
    async def store(self, ld_object: Dict[str, Any]) -> str:
        """
        Store an LD-object.
        
        Args:
            ld_object: The LD-object to store
            
        Returns:
            The object's ID
            
        Raises:
            InvalidLDObject: If the object doesn't have a valid ID or type
        """
        object_id = _require_id(ld_object)
        _require_type(ld_object)
        
        # Ensure the object has a context
        if "@context" not in ld_object:
            ld_object["@context"] = "https://www.w3.org/ns/activitystreams"
        
        # Store in backend
        await self.backend.add(ld_object)
        
        # Update cache
        await self.cache.add(object_id, ld_object)
        
        logger.info(
            f"Stored object {object_id}",
            metadata={"object_id": object_id, "object_type": ld_object.get("type")}
        )
        
        return object_id
    
    async def dereference(self, id: str) -> Dict[str, Any]:
        """
        Dereference an object by its ID.
        
        First checks the cache, then falls back to the backend if not found.
        Updates the cache if found in the backend.
        
        Args:
            id: The ID of the object to dereference
            
        Returns:
            The dereferenced LD-object
            
        Raises:
            ObjectNotFound: If the object doesn't exist
        """
        # Try cache first
        cached = await self.cache.get(id)
        if cached:
            logger.debug(f"Cache hit for {id}", metadata={"object_id": id})
            return cached
        
        # Fall back to backend
        try:
            logger.debug(f"Cache miss for {id}, fetching from backend", metadata={"object_id": id})
            obj = await self.backend.get(id)
            
            # Update cache
            await self.cache.add(id, obj)
            
            return obj
        except ObjectNotFound:
            logger.error(f"Object not found: {id}", metadata={"object_id": id})
            raise
    
    async def add_to_collection(self, ld_object: Dict[str, Any], collection: str) -> None:
        """
        Add an LD-object to a collection.
        
        Args:
            ld_object: The LD-object to add
            collection: The collection to add to
            
        Raises:
            InvalidLDObject: If the object doesn't have a valid ID or type
        """
        object_id = _require_id(ld_object)
        _require_type(ld_object)
        
        # Create a partial representation for the collection
        partial = {
            "id": object_id,
            "type": ld_object.get("type")
        }
        
        # Add optional fields if they exist
        for field in ["name", "summary", "published", "updated"]:
            if field in ld_object:
                partial[field] = ld_object[field]
        
        # Store in backend with collection
        await self.backend.add(partial, collection)
        
        logger.info(
            f"Added object {object_id} to collection {collection}",
            metadata={
                "object_id": object_id,
                "collection": collection,
                "object_type": ld_object.get("type")
            }
        )
    
    async def remove_from_collection(self, id: str, collection: str) -> None:
        """
        Remove an object from a collection.
        
        Args:
            id: The ID of the object to remove
            collection: The collection to remove from
            
        Raises:
            ObjectNotFound: If the object doesn't exist in the collection
        """
        await self.backend.remove(id, collection)
        
        logger.info(
            f"Removed object {id} from collection {collection}",
            metadata={"object_id": id, "collection": collection}
        )
    
    async def convert_to_tombstone(self, ld_object: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert an LD-object to a Tombstone.
        
        Args:
            ld_object: The LD-object to convert
            
        Returns:
            The Tombstone object
            
        Raises:
            InvalidLDObject: If the object doesn't have a valid ID or type
        """
        object_id = _require_id(ld_object)
        object_type = _require_type(ld_object)
        
        # Create tombstone object
        tombstone = {
            "id": object_id,
            "type": "Tombstone",
            "formerType": object_type,
            "deleted": datetime.now(dt.UTC).isoformat() + "Z"
        }
        
        # Add context if it exists in the original
        if "@context" in ld_object:
            tombstone["@context"] = ld_object["@context"]
        else:
            tombstone["@context"] = "https://www.w3.org/ns/activitystreams"
        
        # Store the tombstone
        await self.store(tombstone)
        
        # Update cache
        await self.cache.add(object_id, tombstone)
        
        logger.info(
            f"Converted object {object_id} to Tombstone",
            metadata={"object_id": object_id, "former_type": object_type}
        )
        
        return tombstone
    
    async def query(self, query: Union[Query, Dict[str, Any]]) -> Dict[str, Any]:
        """
        Query for objects matching the specified criteria.
        
        Args:
            query: Query parameters (either a Query object or a dict)
            
        Returns:
            A collection containing the query results
        """
        # Convert dict to Query if needed
        if isinstance(query, dict):
            query = Query(**query)
        
        # Execute query on backend
        results = await self.backend.query(query)
        
        logger.info(
            "Executed query",
            metadata={"query": query.model_dump(), "result_count": results.get("totalItems", 0)}
        )
        
        return results


class SyncActivityStore:
    """
    Synchronous wrapper for ActivityStore.
    
    Provides a synchronous API that wraps the async ActivityStore methods
    for use in synchronous contexts.
    """
    
    def __init__(
        self,
        backend: Optional[StorageBackend] = None,
        cache: Optional[CacheBackend] = None,
        namespace: Optional[str] = None
    ):
        """
        Initialize a synchronous ActivityStore wrapper.
        
        Args:
            backend: Storage backend to use
            cache: Cache backend to use
            namespace: Namespace for this store
        """
        self._async_store = ActivityStore(backend, cache, namespace)
        self._loop = asyncio.new_event_loop()
    
    def __enter__(self) -> "SyncActivityStore":
        """Set up the store for use as a context manager."""
        self._loop.run_until_complete(self._async_store.setup())
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb) -> None:
        """Tear down the store when exiting the context."""
        self._loop.run_until_complete(self._async_store.teardown())
        self._loop.close()
    
    def _run_async(self, coro):
        """Run an async coroutine synchronously."""
        return self._loop.run_until_complete(coro)
    
    def setup(self) -> None:
        """Set up the backend and cache synchronously."""
        self._run_async(self._async_store.setup())
    
    def teardown(self) -> None:
        """Tear down the backend and cache synchronously."""
        self._run_async(self._async_store.teardown())
    
    def store(self, ld_object: Dict[str, Any]) -> str:
        """Store an LD-object synchronously."""
        return self._run_async(self._async_store.store(ld_object))
    
    def dereference(self, id: str) -> Dict[str, Any]:
        """Dereference an object by its ID synchronously."""
        return self._run_async(self._async_store.dereference(id))
    
    def add_to_collection(self, ld_object: Dict[str, Any], collection: str) -> None:
        """Add an LD-object to a collection synchronously."""
        self._run_async(self._async_store.add_to_collection(ld_object, collection))
    
    def remove_from_collection(self, id: str, collection: str) -> None:
        """Remove an object from a collection synchronously."""
        self._run_async(self._async_store.remove_from_collection(id, collection))
    
    def convert_to_tombstone(self, ld_object: Dict[str, Any]) -> Dict[str, Any]:
        """Convert an LD-object to a Tombstone synchronously."""
        return self._run_async(self._async_store.convert_to_tombstone(ld_object))
    
    def query(self, query: Union[Query, Dict[str, Any]]) -> Dict[str, Any]:
        """Query for objects matching criteria synchronously."""
        return self._run_async(self._async_store.query(query))