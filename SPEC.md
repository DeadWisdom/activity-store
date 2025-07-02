# Activity Store Specification

## Overview

Activity Store is an async-first Python library for securely storing and retrieving [Activity Streams](https://www.w3.org/ns/activitystreams) JSON-LD objects and collections. It provides a flexible backend and caching abstraction layer with support for dereferencing, normalization, and querying, while adhering to JSON-LD and Activity Streams conventions.

---

## Core Concepts

- **LD-object**: A JSON-LD dictionary with a `@context` and `type`. Represented as a raw `dict`.
- **Canonical Representation**: Stored using `id` as key; only one per `id`. Overwrites are allowed, old versions discarded.
- **Collections**: Groupings of LD-objects. Membership is stored as partial objects.
- **Tombstone**: Deleted object marked with `type: Tombstone`, `formerType`, and `deleted` timestamp.

---

## Design Principles

- Store all LD-objects as **compacted JSON-LD**
- **Dereferencing** uses cache first, then backend, with TTL of 1 hour
- **Collections** contain partials; clients are responsible for representation
- **Querying** uses a structured `Query` object
- Access control is external to the library

---

## Interfaces

### StorageBackend (ABC)

- `add(ld_object: dict, collection: str | None)`
- `remove(id: str, collection: str | None)`
- `get(id: str, collection: str | None)`
- `query(query: Query)`
- `setup()` (optional)
- `teardown()` (optional)

### CacheBackend (ABC)

- `add(key: str, value: dict, ttl: int = 3600)`
- `get(key: str) -> dict | None`
- `remove(key: str)`
- `setup()` (optional)
- `teardown()` (optional)

---

## `ActivityStore`

- Async-first, context-managed (`async with`)
- Accepts optional `backend`, `cache`, and `namespace`
- Uses `cache_factory()` and `backend_factory()` if none provided
- Uses `ACTIVITY_STORE_BACKEND`, `ACTIVITY_STORE_CACHE`, `ACTIVITY_STORE_NAMESPACE` for config
- Has synchronous wrapper

### Key Methods

- `store(ld_object)`
- `dereference(id)`
- `add_to_collection(ld_object, collection)`
- `remove_from_collection(id, collection)`
- `convert_to_tombstone(ld_object)`
- `query(query: Query)`

---

## Collection Details

- Collections may have canonical representations, but it's optional
- Membership entries use partial LD-objects
- Duplicate `id`s are overwritten
- Elasticsearch keys = hash(collection-id + object-id)
- Metadata like `_collection` used in backend but stripped on read
- Pagination via `next`, `prev`, `items`, `totalItems`; not stored

---

## Caching

- Only used for canonical dereferencing
- Keys = raw object `id`
- TTL = 1 hour
- No preloading

---

## Utilities

- Normalize to list-based format
- Convert to Tombstone
- Structured logger with overridable adapter

---

## Exceptions

- `ActivityStoreError`
- `InvalidLDObject`

All subclass standard Python exceptions

---

## Testing

- `pytest`
- Full TDD coverage
- In-memory implementations for `StorageBackend` and `CacheBackend`
- Must conform to interface contract, not production backend behavior

---

## Packaging

- Python 3.10+
- `pyproject.toml` with extras: `[es]`, `[redis]`
- Minimal public API via `__init__.py`

---

## Directory Layout

```plaintext
activity_store/
├── __init__.py
├── store.py
├── interfaces.py
├── backends/
│   ├── __init__.py
│   ├── memory.py
│   └── elastic.py
├── cache/
│   ├── __init__.py
│   ├── memory.py
│   └── redis.py
├── utils.py
├── ld.py
├── exceptions.py
├── query.py
├── logging.py
```
