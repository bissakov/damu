from typing import Iterable, Optional, Tuple, TypeVar

T = TypeVar("T")
I = TypeVar("I", bound=Iterable)
Result = Tuple[Optional[T], Optional[str]]
IterableResult = Tuple[I, Optional[str]]
