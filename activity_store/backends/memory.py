# In-memory storage backend implementation
# Provides a non-persistent storage backend for testing and development

import copy
from typing import Any, Dict, List, Optional, Set

from ..exceptions import ObjectNotFound
from ..interfaces import StorageBackend
from ..query import Query


class InMemoryStorageBackend(StorageBackend):
    """
    In-memory implementation of StorageBackend for testing and development.
    
    This backend stores all data in memory and does not persist between application restarts.
    It implements a simplified version of querying and collection management.
    """
    
    def __init__(self):
        # Store objects by their ID
        self._objects: Dict[str, Dict[str, Any]] = {}
        
        # Store collection membership: collection_name -> set of object IDs
        self._collections: Dict[str, Set[str]] = {}
    
    async def add(self, ld_object: Dict[str, Any], collection: Optional[str] = None) -> None:
        """
        Add an LD-object to the storage.
        
        Args:
            ld_object: The LD-object to store
            collection: Optional collection to add the object to
        """
        if "id" not in ld_object:
            raise ValueError("LD-object must have an id field")
        
        obj_id = ld_object["id"]
        
        # Store a deep copy to prevent external modification
        self._objects[obj_id] = copy.deepcopy(ld_object)
        
        # If a collection is specified, add the object to it
        if collection:
            if collection not in self._collections:
                self._collections[collection] = set()
            self._collections[collection].add(obj_id)
    
    async def remove(self, id: str, collection: Optional[str] = None) -> None:
        """
        Remove an LD-object from storage.
        
        Args:
            id: The ID of the object to remove
            collection: Optional collection to remove the object from
        """
        if collection:
            # If collection specified, only remove from that collection
            if collection in self._collections and id in self._collections[collection]:
                self._collections[collection].remove(id)
        else:
            # Remove from all collections first
            for coll_ids in self._collections.values():
                if id in coll_ids:
                    coll_ids.remove(id)
            
            # Then remove the object itself
            if id in self._objects:
                del self._objects[id]
    
    async def get(self, id: str, collection: Optional[str] = None) -> Dict[str, Any]:
        """
        Retrieve an LD-object from storage.
        
        Args:
            id: The ID of the object to retrieve
            collection: Optional collection to retrieve the object from
            
        Returns:
            The retrieved LD-object
            
        Raises:
            ObjectNotFound: If the object doesn't exist or isn't in the specified collection
        """
        if collection and (
            collection not in self._collections or id not in self._collections[collection]
        ):
            raise ObjectNotFound(f"Object {id} not found in collection {collection}")
        
        if id not in self._objects:
            raise ObjectNotFound(f"Object {id} not found")
        
        # Return a deep copy to prevent external modification
        return copy.deepcopy(self._objects[id])
    
    async def query(self, query: Query) -> Dict[str, Any]:
        """
        Query for LD-objects matching the specified criteria.
        
        This is a simplified implementation that supports basic filtering by
        collection, type, and text search.
        
        Args:
            query: The query parameters
            
        Returns:
            A collection containing the query results
        """
        results: List[Dict[str, Any]] = []
        collection = query.collection
        
        # Determine which objects to search through
        if collection and collection in self._collections:
            # If collection specified, only search within that collection
            object_ids = self._collections[collection]
            objects_to_search = [self._objects[oid] for oid in object_ids if oid in self._objects]
        else:
            # Otherwise search all objects
            objects_to_search = list(self._objects.values())
        
        # Apply type filter if specified
        if query.type:
            type_list = [query.type] if isinstance(query.type, str) else query.type
            objects_to_search = [
                obj for obj in objects_to_search
                if "type" in obj and any(t in obj["type"] for t in type_list)
            ]
        
        # Apply text search if specified
        if query.text:
            text = query.text.lower()
            filtered_objects = []
            
            for obj in objects_to_search:
                # Very simple text search implementation
                obj_str = str(obj).lower()
                if text in obj_str:
                    filtered_objects.append(obj)
            
            objects_to_search = filtered_objects
        
        # Apply keywords filter if specified
        if query.keywords:
            filtered_objects = []
            for obj in objects_to_search:
                # Check if any keyword is in the object's string representation
                obj_str = str(obj).lower()
                if any(keyword.lower() in obj_str for keyword in query.keywords):
                    filtered_objects.append(obj)
            
            objects_to_search = filtered_objects
        
        # Handle pagination
        # Simple implementation that doesn't use 'after' token
        results = objects_to_search[:query.size]
        
        # Format the results as a collection
        return {
            "type": "Collection",
            "totalItems": len(objects_to_search),
            "items": copy.deepcopy(results)
        }