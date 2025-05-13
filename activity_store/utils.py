from collections.abc import Iterable, Iterator, Mapping
from typing import Any, TypeVar, cast

T = TypeVar("T")


### Chains ###
def chain(*values, type: type[T] = Any) -> Iterator[T]:
    for v in values:
        match v:
            case None:
                continue
            case str() | bytes() | Mapping():
                yield cast(type, v)
            case dict():
                yield cast(type, v)
            case Iterable():
                yield from chain(*v, type=type)
            case _:
                yield v


def chain_ids(*values) -> Iterator[str]:
    for val in chain(*values):
        match val:
            case {"id": id} if id:
                yield id
            case str():
                yield val


def chain_urls(*values) -> Iterator[str]:
    for val in chain(*values):
        match val:
            case str():
                yield val
            case {"href": url} if url:
                yield url
            case dict():
                yield from chain_urls(val)


### Firsts ###
def first(*values):
    return next(chain(*values), None)


def first_id(*values) -> str | None:
    return next(chain_ids(*values), None)


### Gather ###
def gather(*values) -> list:
    """
    Gather the given values into a list
    """
    return list(chain(*values))


def gather_urls(*values) -> list[str]:
    return list(chain_urls(*values))
