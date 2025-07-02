import json
import os
from typing import Any, Dict, Optional

import redis.asyncio as redis

from ..exceptions import ActivityStoreError
from ..interfaces import CacheBackend
from ..logging import get_logger

# Logger for this module
logger = get_logger("cache.redis")


class RedisCacheBackend(CacheBackend):
    """
    Redis implementation of the CacheBackend interface.

    This backend uses Redis for caching LD-objects, with support for TTL-based expiration.
    """

    def __init__(
        self,
        redis_url: str = "redis://localhost:6379/0",
        namespace: str = "activity_store",
        client: Optional[redis.Redis] = None,
    ):
        """
        Initialize the Redis cache backend.

        Args:
            redis_url: Redis connection URL (default: redis://localhost:6379/0)
            namespace: Namespace for keys to avoid collisions (default: activity_store)
            client: Optional pre-configured Redis client
        """
        self.redis_url = redis_url or os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        self.namespace = namespace
        self._client = redis.from_url(self.redis_url)

    async def close(self):
        if self._client is not None:
            await self._client.aclose()
        self._client = None

    async def teardown(self) -> None:
        """
        Clean up all keys in the current namespace.

        This is a separate method from teardown to allow explicit cleanup
        when needed, without affecting persistence by default.
        """
        # Clean up namespace keys
        pattern = f"{self.namespace}:*"
        cursor = 0
        deleted_keys = 0

        while True:
            cursor, keys = await self._client.scan(cursor, pattern, 100)
            if keys:
                await self._client.delete(*keys)
                deleted_keys += len(keys)
            if cursor == 0:
                break

        logger.info(
            f"Cleaned up namespace {self.namespace}",
            metadata={"namespace": self.namespace, "deleted_keys": deleted_keys},
        )

    def _get_key(self, key: str) -> str:
        """
        Generate a namespaced Redis key.

        Args:
            key: The original key

        Returns:
            The namespaced Redis key
        """
        return f"{self.namespace}:{key}"

    async def add(self, key: str, value: Dict[str, Any], ttl: int = 3600) -> None:
        """
        Add an item to the Redis cache.

        Args:
            key: The cache key
            value: The value to cache
            ttl: Time-to-live in seconds (default: 1 hour)
        """
        redis_key = self._get_key(key)

        try:
            # Serialize the value to JSON
            serialized = json.dumps(value)

            # Add to Redis with TTL
            await self._client.setex(redis_key, ttl, serialized)

            logger.debug(
                f"Added key {key} to Redis cache",
                metadata={"key": key, "ttl": ttl, "namespace": self.namespace},
            )
        except Exception as e:
            logger.error(
                f"Failed to add key {key} to Redis cache",
                metadata={"key": key, "error": str(e), "namespace": self.namespace},
            )
            raise ActivityStoreError(f"Redis add operation failed: {e}") from e

    async def get(self, key: str) -> Optional[Dict[str, Any]]:
        """
        Retrieve an item from the Redis cache.

        Args:
            key: The cache key

        Returns:
            The cached value, or None if not found or expired
        """
        redis_key = self._get_key(key)

        try:
            # Get from Redis
            serialized = await self._client.get(redis_key)

            if serialized is None:
                logger.debug(
                    f"Cache miss for key {key}",
                    metadata={"key": key, "namespace": self.namespace},
                )
                return None

            # Deserialize the value from JSON
            value = json.loads(serialized)

            logger.debug(
                f"Cache hit for key {key}",
                metadata={"key": key, "namespace": self.namespace},
            )
            return value
        except json.JSONDecodeError as e:
            logger.error(
                f"Failed to decode JSON for key {key}",
                metadata={"key": key, "error": str(e), "namespace": self.namespace},
            )
            # Remove corrupted data
            await self.remove(key)
            return None
        except Exception as e:
            logger.error(
                f"Failed to get key {key} from Redis cache",
                metadata={"key": key, "error": str(e), "namespace": self.namespace},
            )
            return None

    async def remove(self, key: str) -> None:
        """
        Remove an item from the Redis cache.

        Args:
            key: The cache key to remove
        """
        redis_key = self._get_key(key)

        try:
            # Remove from Redis
            await self._client.delete(redis_key)

            logger.debug(
                f"Removed key {key} from Redis cache",
                metadata={"key": key, "namespace": self.namespace},
            )
        except Exception as e:
            logger.error(
                f"Failed to remove key {key} from Redis cache",
                metadata={"key": key, "error": str(e), "namespace": self.namespace},
            )
            # Swallow the exception - removal is a best-effort operation
