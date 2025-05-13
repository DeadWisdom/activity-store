from datetime import datetime
from functools import lru_cache
from typing import Any, Callable

import hishel
import orjson
from pyld import jsonld

from .utils import chain, first, gather

ALLOWED_URLS = [
    "https://www.w3.org/",
]

OptionalPrefixDict = dict[str, str] | None
OptionalKeyList = list[str] | None


### Patch is Numeric
def _is_numeric_or_datetime(v):
    if isinstance(v, datetime):
        return True
    try:
        float(v)
        return True
    except Exception:
        return False


jsonld._is_numeric = _is_numeric_or_datetime


### Override JSONLD Loader
storage = hishel.FileStorage()


@lru_cache(maxsize=512)
def load_url(url):
    headers = {
        "Accept": "application/ld+json;profile=http://www.w3.org/ns/json-ld#context, application/ld+json, application/json"
    }
    if not any(url.startswith(x) for x in ALLOWED_URLS):
        raise ValueError(f"Remote document not in allowed domain: {url}")

    with hishel.CacheClient(storage=storage) as client:
        response = client.get(url, headers=headers)

    return {
        "contentType": response.headers.get("content-type", "application/ld+json"),
        "contextUrl": None,
        "documentUrl": response.url,
        "document": orjson.loads(response.content),
    }


def load_document(url, options={}):
    return load_url(str(url))


jsonld.set_document_loader(load_document)


### Prefixes
def with_prefixes(context: Any, prefixes: OptionalPrefixDict):
    if not prefixes:
        return context
    if isinstance(context, list):
        return context + [prefixes]
    else:
        return [context, prefixes]


### Matching
def any_none(doc: dict):
    """Returns True if any value in the document is None"""
    if not isinstance(doc, dict):
        return doc is None
    for k, v in doc.items():
        if isinstance(v, list):
            for item in v:
                if any_none(item):
                    return True
        else:
            if any_none(v):
                return True
    return False


### Interface
def expand(
    doc: dict,
    context=None,
    base_url=None,
    options=None,
    extra_options=None,
    prefixes: OptionalPrefixDict = None,
):
    context = context or doc.get("@context", ["https://www.w3.org/ns/activitystreams"])
    context = with_prefixes(context, prefixes)

    options = options or {"base": base_url}
    return jsonld.expand(doc, options)[0]


def map_property(doc: dict, names: set[str], callback: Callable):
    """Runs a callable on every property with the given name in the doc and sub-docs"""
    if not isinstance(doc, dict):
        return

    for k, v in doc.items():
        if k in names:
            v = doc[k] = callback(v)
        match v:
            case dict():
                map_property(v, names, callback)
            case list():
                for item in v:
                    map_property(item, names, callback)


def compact_property(doc: dict, name: str | list[str]):
    """
    Compact a single property of a document and sub-documents in place.
    """
    return map_property(doc, set(chain(name)), first)


def expand_property(doc: dict, name: str | list[str]):
    """
    Expands a single property of a document and sub-documents in place.
    """
    return map_property(doc, set(chain(name)), gather)


def normalize(
    doc: dict,
    context=None,
    base_url=None,
    options=None,
    extra_options=None,
    compact_keys: OptionalKeyList = None,
    prefixes: OptionalPrefixDict = None,
):
    context = context or doc.get("@context", "https://www.w3.org/ns/activitystreams")
    context = with_prefixes(context, prefixes)

    options = options or {
        "compactArrays": False,
        "expandContext": context,
        "base": base_url,
    }
    if extra_options:
        options.update(extra_options)

    normal = jsonld.compact(doc, context, options)

    ### Fix the fact that when you don't compactArrays then it is necessarily a graph
    graph = normal.pop("@graph", None)
    if graph:
        normal.update(graph[0])

    ### Fix weird bug that type gets compacted
    expand_property(normal, "type")

    ## Compact some of the fields
    if compact_keys:
        compact_property(normal, compact_keys)

    return normal


def compact(
    doc: dict,
    context=None,
    base_url=None,
    options=None,
    extra_options=None,
    prefixes: OptionalPrefixDict = None,
):
    context = context or doc.get("@context", ["https://www.w3.org/ns/activitystreams"])
    context = with_prefixes(context, prefixes)

    options = options or {
        "compactArrays": True,
        "omitGraph": True,
        "expandContext": context,
        "base": base_url,
    }
    if extra_options:
        options.update(extra_options)

    return jsonld.compact(doc, context, options)


def frame(
    doc: dict,
    input: dict,
    context=None,
    base_url=None,
    options=None,
    extra_options=None,
    prefixes: OptionalPrefixDict = None,
    compact=False,
    normalize=False,
    require_match=False,
) -> dict | None:
    context = context or doc.get("@context", ["https://www.w3.org/ns/activitystreams"])
    context = with_prefixes(context, prefixes)

    options = options or {
        "expandContext": context,
        "embed": "@once",
        "requireAll": True,
        "omitGraph": True,
        "base": base_url,
    }
    if extra_options:
        options.update(extra_options)

    result = jsonld.frame(doc, input, options)  # type: ignore
    if normalize:
        result = normalize(result, context, options, extra_options, prefixes)
    if compact:
        result = compact(result, context, options, extra_options, prefixes)
    if require_match and any_none(result):
        return None
    return result
