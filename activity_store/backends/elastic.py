# Elasticsearch backend for storing and querying Activity Streams objects.
# Implements the StorageBackend interface with Elasticsearch as the storage engine.

import weakref
import os
import copy
import hashlib
from typing import Any, Dict, Optional

from elasticsearch import AsyncElasticsearch, NotFoundError

from ..exceptions import ActivityStoreError
from ..interfaces import StorageBackend
from ..logging import get_logger
from ..query import Query

# Logger for this module
logger = get_logger("backends.elastic")

# Default index settings and mappings
DEFAULT_INDEX_SETTINGS = {
    "number_of_shards": 1,
    "number_of_replicas": 0,
    "refresh_interval": "1s",
}

DEFAULT_MAPPINGS = {
    "properties": {
        "id": {"type": "keyword"},
        "type": {"type": "keyword"},
        "content": {"type": "text"},
        "name": {"type": "text"},
        "summary": {"type": "text"},
        "tag": {"type": "keyword"},
        "published": {
            "type": "date",
            "format": "date_optional_time||strict_date_optional_time",
        },
        "updated": {
            "type": "date",
            "format": "date_optional_time||strict_date_optional_time",
        },
        "_collection": {"type": "keyword"},
        # Special field for text search across multiple fields
        "_all_text": {"type": "text"},
    },
    "dynamic": "true",  # Allow new fields to be indexed
}


class ElasticsearchBackend(StorageBackend):
    """
    Elasticsearch implementation of the StorageBackend interface.

    This backend uses Elasticsearch for storing and querying LD-objects,
    with support for full-text search and complex queries.
    """

    def __init__(
        self,
        url: str | None = None,  # "http://localhost:9200",
        password: str | None = None,
        cloud_id: str | None = None,
        api_key: str | None = None,
        index_prefix: str = "activity_store",
        client: Optional[AsyncElasticsearch] = None,
        refresh_on_write: bool = False,
    ):
        """
        Initialize the Elasticsearch backend.

        Args:
            url: Elasticsearch URL (default: http://localhost:9200)
            index_prefix: Prefix for Elasticsearch indices (default: activity_store)
            client: Optional pre-configured Elasticsearch client
            refresh_on_write: Whether to refresh indices immediately after writes
                              (useful for testing, but can impact performance)
        """
        self._client = self._create_client(cloud_id=cloud_id, api_key=api_key, url=url, password=password)

        self.index_prefix = index_prefix
        self.refresh_on_write = refresh_on_write

        # Index names
        self.main_index = f"{self.index_prefix}-objects"
        self.collection_index = f"{self.index_prefix}-collections"

    def _create_client(self, cloud_id=None, api_key=None, url=None, password=None):
        password = password or os.environ.get("ELASTICSEARCH_PASSWORD")
        api_key = api_key or os.environ.get("ELASTICSEARCH_API_KEY")

        if cloud_id:
            return AsyncElasticsearch(cloud_id=cloud_id, api_key=api_key)
        if url:
            return AsyncElasticsearch(url=url, password=password)

        cloud_id = os.environ.get("ELASTICSEARCH_CLOUD_ID")
        url = os.environ.get("ELASTICSEARCH_URL")

        if cloud_id:
            return AsyncElasticsearch(cloud_id=cloud_id, api_key=api_key)
        if url:
            return AsyncElasticsearch(url=url, password=password)

        raise RuntimeError(
            "Need environment variables ELASTICSEARCH_URL or ELASTICSEARCH_CLOUD_ID + ELASTICSEARCH_API_KEY"
        )

    async def close(self):
        """Close the Elasticsearch client connection."""
        if self._client is not None:
            try:
                await self._client.close()
            finally:
                self._client = None

    async def setup(self) -> None:
        """Create the required Elasticsearch indices if they don't exist."""
        # Create main objects index
        if not await self._client.indices.exists(index=self.main_index):
            await self._client.indices.create(
                index=self.main_index,
                body={"settings": DEFAULT_INDEX_SETTINGS, "mappings": DEFAULT_MAPPINGS},
            )
            logger.info(f"Created index {self.main_index}")

        # Create collection objects index
        if not await self._client.indices.exists(index=self.collection_index):
            await self._client.indices.create(
                index=self.collection_index,
                body={"settings": DEFAULT_INDEX_SETTINGS, "mappings": DEFAULT_MAPPINGS},
            )
            logger.info(f"Created index {self.collection_index}")

    async def teardown(self) -> None:
        """
        Delete the indices created by this backend.

        This is a separate method from teardown to allow explicit cleanup
        when needed, without affecting persistence by default.
        """
        # Delete indices
        result = await self._client.options(ignore_status=404).indices.delete(
            index=[self.main_index, self.collection_index]
        )

        logger.info(
            "Deleted Elasticsearch indices",
            metadata={
                "indices": [self.main_index, self.collection_index],
                "result": result,
            },
        )

    def _get_collection_id(self, object_id: str, collection: str) -> str:
        """
        Generate a unique ID for an object in a collection.

        Args:
            object_id: The object ID
            collection: The collection name

        Returns:
            A unique ID combining the object ID and collection
        """
        return hashlib.sha256(f"{collection}-{object_id}".encode()).hexdigest()

    def _prepare_object_for_indexing(
        self, ld_object: Dict[str, Any], collection: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Prepare an LD-object for indexing in Elasticsearch.

        Adds metadata fields and generates text for full-text search.

        Args:
            ld_object: The LD-object to prepare
            collection: Optional collection name

        Returns:
            The prepared object
        """
        # Deep copy to avoid modifying the original
        prepared = copy.deepcopy(ld_object)

        # Add collection metadata if provided
        if collection:
            prepared["_collection"] = collection

        # Generate full-text search field from text fields
        text_fields = []
        for field in ["name", "content", "summary"]:
            if field in prepared and prepared[field]:
                text_fields.append(str(prepared[field]))

        # Add tags/keywords if they exist
        if "tag" in prepared:
            tags = prepared["tag"]
            if isinstance(tags, list):
                text_fields.extend(str(tag) for tag in tags)
            else:
                text_fields.append(str(tags))

        # Add combined text
        if text_fields:
            prepared["_all_text"] = " ".join(text_fields)

        return prepared

    def _strip_metadata_fields(self, ld_object: Dict[str, Any]) -> Dict[str, Any]:
        """
        Remove internal metadata fields from an LD-object.

        Args:
            ld_object: The LD-object to clean

        Returns:
            The cleaned object
        """
        # Deep copy to avoid modifying the original
        result = copy.deepcopy(ld_object)

        # Remove internal fields
        for field in ["_collection", "_all_text", "_id", "_index", "_score"]:
            if field in result:
                del result[field]

        return result

    async def add(self, ld_object: Dict[str, Any], collection: Optional[str] = None) -> None:
        """
        Add an LD-object to Elasticsearch.

        Args:
            ld_object: The LD-object to store
            collection: Optional collection to add the object to
        """
        # Ensure the object has an ID
        if "id" not in ld_object:
            raise ValueError("LD-object must have an id field")

        object_id = ld_object["id"]

        try:
            # Prepare the object
            prepared = self._prepare_object_for_indexing(ld_object, collection)

            if collection:
                # If adding to a collection, use the collection index with a derived ID
                collection_id = self._get_collection_id(object_id, collection)
                await self._client.index(
                    index=self.collection_index,
                    id=collection_id,
                    document=prepared,
                    refresh="wait_for" if self.refresh_on_write else False,
                )
                logger.info(
                    f"Added object {object_id} to collection {collection}",
                    metadata={"object_id": object_id, "collection": collection},
                )
            else:
                # If adding to main storage, use the main index with the object ID
                await self._client.index(
                    index=self.main_index,
                    id=object_id,
                    document=prepared,
                    refresh="wait_for" if self.refresh_on_write else False,
                )
                logger.info(f"Added object {object_id}", metadata={"object_id": object_id})
        except Exception as e:
            logger.error(
                f"Failed to add object {object_id}",
                metadata={
                    "object_id": object_id,
                    "collection": collection,
                    "error": str(e),
                },
            )
            raise ActivityStoreError(f"Elasticsearch add operation failed: {e}") from e

    async def remove(self, id: str, collection: Optional[str] = None) -> None:
        """
        Remove an LD-object from Elasticsearch.

        Args:
            id: The ID of the object to remove
            collection: Optional collection to remove the object from
        """
        if collection:
            # If removing from a collection, use the collection index with the derived ID
            collection_id = self._get_collection_id(id, collection)
            await self._client.options(ignore_status=404).delete(
                index=self.collection_index,
                id=collection_id,
                refresh="wait_for" if self.refresh_on_write else False,
            )
            logger.info(
                f"Removed object {id} from collection {collection}",
                metadata={"object_id": id, "collection": collection},
            )
        else:
            # If removing from main storage, use the main index with the object ID
            await self._client.options(ignore_status=404).delete(
                index=self.main_index,
                id=id,
                refresh="wait_for" if self.refresh_on_write else False,
            )
            logger.info(f"Removed object {id}", metadata={"object_id": id})

    async def get(self, id: str, collection: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve an LD-object from Elasticsearch.

        Args:
            id: The ID of the object to retrieve
            collection: Optional collection to retrieve the object from

        Returns:
            The retrieved LD-object
        """
        try:
            if collection:
                # If getting from a collection, use the collection index with the derived ID
                collection_id = self._get_collection_id(id, collection)
                response = await self._client.get(index=self.collection_index, id=collection_id)
            else:
                # If getting from main storage, use the main index with the object ID
                response = await self._client.get(index=self.main_index, id=id)
        except NotFoundError:
            return None

        # Get the source document and strip metadata
        result = self._strip_metadata_fields(response["_source"])

        logger.info(
            f"Retrieved object {id}",
            metadata={"object_id": id, "collection": collection},
        )
        return result

    async def query(self, query: Query) -> Dict[str, Any]:
        """
        Query for LD-objects matching specified criteria.

        Args:
            query: The query parameters

        Returns:
            A collection containing the query results
        """
        # Convert Query to dict if needed
        query_dict = query.to_dict() if hasattr(query, "to_dict") else query

        # Determine which index to search
        index = self.collection_index if query_dict.get("collection") else self.main_index

        # Build Elasticsearch query
        es_query: Dict[str, Any] = {"query": {"bool": {"must": []}}}

        # Add text search if specified
        if "text" in query_dict and query_dict["text"]:
            es_query["query"]["bool"]["must"].append(
                {
                    "multi_match": {
                        "query": query_dict["text"],
                        "fields": ["_all_text^3", "name^2", "content", "summary"],
                        "type": "best_fields",
                    }
                }
            )

        # Add type filter if specified
        if "type" in query_dict and query_dict["type"]:
            type_value = query_dict["type"]
            if isinstance(type_value, list):
                es_query["query"]["bool"]["must"].append({"terms": {"type": type_value}})
            else:
                es_query["query"]["bool"]["must"].append({"term": {"type": type_value}})

        # Add collection filter if specified
        if "collection" in query_dict and query_dict["collection"]:
            es_query["query"]["bool"]["must"].append({"term": {"_collection": query_dict["collection"]}})

        # Add keywords filter if specified
        if "keywords" in query_dict and query_dict["keywords"]:
            keywords = query_dict["keywords"]
            if isinstance(keywords, list):
                es_query["query"]["bool"]["must"].append({"terms": {"tag": keywords}})
            else:
                es_query["query"]["bool"]["must"].append({"term": {"tag": keywords}})

        # Add sorting if specified
        if "sort" in query_dict and query_dict["sort"]:
            sort_parts = query_dict["sort"].split(":")
            field = sort_parts[0]
            direction = "desc" if len(sort_parts) > 1 and sort_parts[1] == "desc" else "asc"
            es_query["sort"] = [{field: {"order": direction}}]

        # Add pagination
        size = query_dict.get("size", 10)
        es_query["size"] = size

        # Add search after if specified
        if "after" in query_dict and query_dict["after"]:
            # Handle cursor-based pagination
            es_query["search_after"] = [query_dict["after"]]

        print(es_query)

        # Execute the search
        response = await self._client.search(index=index, body=es_query)

        # Build result collection
        items = []
        for hit in response["hits"]["hits"]:
            # Strip internal fields and add to results
            item = self._strip_metadata_fields(hit["_source"])
            items.append(item)

        # Create collection with pagination
        result = {
            "type": "Collection",
            "totalItems": response["hits"]["total"]["value"],
            "items": items,
        }

        # Add pagination cursor if there are more results
        if len(items) == size and len(response["hits"]["hits"]) > 0:
            # Get the sort values from the last hit for search_after pagination
            last_hit = response["hits"]["hits"][-1]
            if "sort" in last_hit:
                result["after"] = last_hit["sort"][0]

                # Add next/prev links for link-based pagination
                after_param = f"after={last_hit['sort'][0]}"
                result["next"] = f"?{after_param}&size={size}"

        logger.info(
            "Executed query",
            metadata={
                "total_hits": response["hits"]["total"]["value"],
                "returned_hits": len(items),
                "collection": query_dict.get("collection"),
            },
        )
        return result
