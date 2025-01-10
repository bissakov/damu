from typing import Callable, Generic, Iterator, List, Optional, TypeVar

T = TypeVar("T")
K = TypeVar("K")


class CustomList(Generic[T]):
    def __init__(self, items: Optional[List[T]] = None) -> None:
        self.items: List[T] = items if items else []

    def append(self, item: T) -> None:
        self.items.append(item)

    def extend(self, other: "CustomList[T]") -> None:
        self.items.extend(other.items)

    def remove(self, item: T) -> None:
        self.items.remove(item)

    def clear(self) -> None:
        self.items.clear()

    def filter_by(self, condition: Callable[[T], bool]) -> "CustomList[T]":
        return CustomList([item for item in self.items if condition(item)])

    def find(self, condition) -> Optional[T]:
        return next((item for item in self.items if condition(item)), None)

    def sort(
        self,
        key: Optional[Callable[[T], K]] = None,
        reverse: bool = False,
    ) -> None:
        self.items.sort(key=key, reverse=reverse)

    def index(
        self,
        item: Optional[T] = None,
        condition: Optional[Callable[[T], bool]] = None,
    ) -> int:
        if item is not None and condition is not None:
            raise ValueError("Provide only one of `item` or `condition`, not both.")

        if condition is not None:
            for idx, element in enumerate(self.items):
                if condition(element):
                    return idx
            raise ValueError("No item matching the condition was found.")

        if item is not None:
            return self.items.index(item)

        raise ValueError("Either `item` or `condition` must be provided.")

    def __len__(self) -> int:
        return len(self.items)

    def __getitem__(self, idx: int) -> T:
        return self.items[idx]

    def __setitem__(self, idx: int, item: T) -> None:
        self.items[idx] = item

    def __delitem__(self, idx: int) -> None:
        del self.items[idx]

    def __iter__(self) -> Iterator[T]:
        return iter(self.items)

    def __contains__(self, item: T) -> bool:
        return item in self.items

    def __bool__(self) -> bool:
        return bool(self.items)

    def __eq__(self, other: "CustomList") -> bool:
        return self.items == other.items if isinstance(other, self.__class__) else False

    def __repr__(self):
        return f"{self.__class__.__name__}({self.items!r})"

    def __str__(self) -> str:
        return "\n".join(self.items)
