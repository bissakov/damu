from itertools import islice
from typing import (
    Callable,
    Generator,
    Iterable,
    List,
    Optional,
    Protocol,
    Set,
    Tuple,
    TypeVar,
)


class Hashable(Protocol):
    def __hash__(self) -> int: ...

    def __eq__(self, other: object) -> bool: ...


T = TypeVar("T")
K = TypeVar("K")
H = TypeVar("H", bound=Hashable)


def index(
    items: List[T],
    item: Optional[T] = None,
    condition: Optional[Callable[[T], bool]] = None,
    default: Optional[int] = None,
) -> int:
    if item is not None and condition is not None:
        raise ValueError("Provide only one of `item` or `condition`, not both.")

    if condition is not None:
        for idx, element in enumerate(items):
            if condition(element):
                return idx
        if default is not None:
            return default
        raise IndexError("No item matching the condition was found.")

    if item is not None:
        return items.index(item)

    if default:
        return default

    raise ValueError("Either `item` or `condition` must be provided.")


def rindex(
    items: List[T],
    item: Optional[T] = None,
    condition: Optional[Callable[[T], bool]] = None,
    default: Optional[int] = None,
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


def find(items: Iterable[T], condition: Callable[[T], bool]) -> Optional[T]:
    return next((item for item in items if condition(item)), None)


def find_all(items: Iterable[T], condition: Callable[[T], bool]) -> List[T]:
    return [item for item in items if condition(item)]


def filter_by(items: Iterable[T], condition: Callable[[T], bool]) -> List[T]:
    return [item for item in items if condition(item)]


def is_progressive(items: Iterable[T]) -> bool:
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


def extend_set(set1: Set[T], set2: Set[T]) -> None:
    for item in set2:
        set1.add(item)


def batched(
    iterable: Iterable[T], n: int, *, strict: bool = False
) -> Generator[Tuple[T, ...], None, None]:
    if n < 1:
        raise ValueError("n must be at least one")
    iterator = iter(iterable)
    while batch := tuple(islice(iterator, n)):
        if strict and len(batch) != n:
            raise ValueError("batched(): incomplete batch")
        yield batch
