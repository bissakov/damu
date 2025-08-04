from __future__ import annotations

from typing import TYPE_CHECKING
from itertools import islice

if TYPE_CHECKING:
    from collections.abc import Callable, Generator, Iterable
    from typing import Protocol, TypeVar

    class SupportsLT(Protocol):
        def __lt__(self: LT, other: LT) -> bool: ...

    T = TypeVar("T")
    LT = TypeVar("LT", bound=SupportsLT)


class Unset: ...


UNSET = Unset()


def index(
    items: list[T],
    item: T | None = None,
    condition: Callable[[T], bool] | None = None,
    default: int | None | Unset = UNSET,
) -> int | None:
    if item is not None and condition is not None:
        raise ValueError("Provide only one of `item` or `condition`, not both.")

    if condition is not None:
        for idx, element in enumerate(items):
            if condition(element):
                return idx
        if not isinstance(default, Unset):
            return default
        raise IndexError("No item matching the condition was found.")

    if item is not None:
        try:
            return items.index(item)
        except ValueError:
            if not isinstance(default, Unset):
                return default
            raise

    if not isinstance(default, Unset):
        return default

    raise ValueError("Either `item` or `condition` must be provided.")


def rindex(
    items: list[T],
    item: T | None = None,
    condition: Callable[[T], bool] | None = None,
    default: int | None = None,
) -> int:
    if item is not None and condition is not None:
        raise ValueError("Provide only one of `item` or `condition`, not both.")

    if condition is not None:
        for idx in range(len(items) - 1, -1, -1):
            if condition(items[idx]):
                return idx
        if default is not None:
            return default
        raise IndexError("No item matching the condition was found.")

    if item is not None:
        for idx in range(len(items) - 1, -1, -1):
            if items[idx] == item:
                return idx
        if default is not None:
            return default
        raise ValueError(f"{item} is not in list.")

    if default is not None:
        return default

    raise ValueError("Either `item` or `condition` must be provided.")


def find(items: Iterable[T], condition: Callable[[T], bool]) -> T | None:
    return next((item for item in items if condition(item)), None)


def find_all(items: Iterable[T], condition: Callable[[T], bool]) -> list[T]:
    return [item for item in items if condition(item)]


def filter_by(items: Iterable[T], condition: Callable[[T], bool]) -> list[T]:
    return [item for item in items if condition(item)]


def is_progressive(items: Iterable[LT]) -> bool:
    items = iter(items)

    try:
        prev = next(items)
    except StopIteration:
        return True

    for current in items:
        if not prev < current:
            return False
        prev = current

    return True


def extend_set(set1: set[T], set2: set[T]) -> None:
    for item in set2:
        set1.add(item)


def batched(
    iterable: Iterable[T], n: int, *, strict: bool = False
) -> Generator[tuple[T, ...]]:
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError("batched(): incomplete batch")
        yield batch
