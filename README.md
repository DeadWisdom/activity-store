# Activity Store

Activity Store is an async-first Python library for securely storing and retrieving [Activity Streams](https://www.w3.org/ns/activitystreams) JSON-LD objects and collections. It provides a flexible backend and caching abstraction layer with support for dereferencing, normalization, and querying, while adhering to JSON-LD and Activity Streams conventions.

## Features

- Async-first API with synchronous wrapper
- Pluggable storage backends
- Built-in in-memory implementations for testing
- Dereferencing with TTL-based caching
- Collection management
- Structured querying
- Tombstone support for deleted objects
- Context-managed usage pattern

## Installation

```bash
# Basic installation
pip install activity-store

# With Elasticsearch backend
pip install activity-store[es]

# With Redis cache
pip install activity-store[redis]
```

## Basic Usage

```python
import asyncio
from activity_store import ActivityStore, Query

async def main():
    # Context-managed usage
    async with ActivityStore() as store:
        # Store an object
        object_id = await store.store({
            "id": "https://example.com/objects/123",
            "type": "Note",
            "content": "Hello, world!",
            "published": "2023-01-01T00:00:00Z"
        })
        
        # Retrieve the object
        note = await store.dereference(object_id)
        
        # Add to a collection
        await store.add_to_collection(note, "notes")
        
        # Query for objects
        results = await store.query(Query(
            collection="notes",
            type="Note",
            size=10
        ))
        
        # Convert to tombstone when deleting
        tombstone = await store.convert_to_tombstone(note)

# Run the example
asyncio.run(main())
```

## Synchronous Usage

```python
from activity_store import SyncActivityStore, Query

# Context-managed usage
with SyncActivityStore() as store:
    # Same methods as async API but synchronous
    object_id = store.store({
        "id": "https://example.com/objects/123",
        "type": "Note",
        "content": "Hello, world!"
    })
    
    note = store.dereference(object_id)
    store.add_to_collection(note, "notes")
```

## Configuration

The library respects the following environment variables:

- `ACTIVITY_STORE_BACKEND`: Backend type to use (default: "memory")
- `ACTIVITY_STORE_CACHE`: Cache type to use (default: "memory")
- `ACTIVITY_STORE_NAMESPACE`: Namespace for this store (default: "activity_store")

## Documentation

For more detailed documentation, see the comments in the source code and the `SPEC.md` file.

## Testing

```bash
# Run tests
pytest

# With coverage
pytest --cov=activity_store
```