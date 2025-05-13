# Development Plan: Step-by-Step Prompts for LLM Code Generation

This document outlines a step-by-step set of developer prompts to be given to a code generation LLM in order to build the `activity_store` library according to the specification.

---

## Step-by-Step Prompt Plan

### 1. Set Up Project Scaffold

> Create a Python package called `activity_store`. Include a `pyproject.toml` configured for Python 3.10+, and basic directory structure with modules: `store.py`, `interfaces.py`, `exceptions.py`, `query.py`, `logging.py`, `utils.py`, `ld.py`, and subpackages `backends/`, `cache/`. Include `__init__.py` files.

### 2. Define Base Exceptions

> Define custom exceptions in `exceptions.py`: `ActivityStoreError`, `ObjectNotFound`, `InvalidLDObject`. These should inherit from appropriate Python exceptions.

### 3. Define Interfaces

> Create abstract base classes for `StorageBackend` and `CacheBackend` in `interfaces.py`. Each should have `add`, `remove`, `get`, `query` methods, plus `setup` and `teardown` with default no-op implementations.

### 4. Define Query Object

> Define a Pydantic model `Query` in `query.py` with fields: `text`, `keywords`, `sort`, `size`, `after`, `collection`, `type`. Include sensible defaults and type annotations.

### 5. Implement In-Memory Backends

> Implement `InMemoryStorageBackend` and `InMemoryCacheBackend` in `backends/memory.py`. These must conform to the interface, not production backend behavior.

### 6. Implement LD-Handling functions

> In `ld.py`, create functions to:
>
> - Normalize an LD-object by expanding all multi-valued fields to lists
> - Convert an LD-object to a Tombstone object (with formerType and deleted fields)

### 7. Implement Logging Wrapper

> In `logging.py`, create a structured logger that can take and pass through key-value metadata. Use Python's built-in logging by default but make it pluggable.

### 8. Implement ActivityStore (Core)

> In `store.py`, create the async `ActivityStore` class:
>
> - Accepts optional backend, cache, namespace
> - Uses factories if not provided, based on env vars
> - Handles context manager setup/teardown
> - Implements: `store`, `dereference`, `add_to_collection`, `remove_from_collection`, `convert_to_tombstone`, `query`

### 9. Implement Default Factories

> Implement `cache_factory()` and `backend_factory()` static methods on `ActivityStore`. These should respect `ACTIVITY_STORE_BACKEND`, `ACTIVITY_STORE_CACHE`, and `ACTIVITY_STORE_NAMESPACE` env vars.

### 10. Add Synchronous Wrapper

> Add a sync wrapper class for `ActivityStore` that mirrors the async API where appropriate.

### 11. Document Public API

> Limit `__init__.py` exports to: `ActivityStore`, `Query`, and the public exceptions.

### 12. Polish `pyproject.toml`

> Finalize dependencies. Add extras for `[es]` and `[redis]` backends. Mark test dependencies.
