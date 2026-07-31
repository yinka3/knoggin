"""Small immutable collection primitives for frozen boundary models."""

from __future__ import annotations

import copy
from collections.abc import Mapping
from typing import Any


class FrozenDict(dict):
    """A recursively immutable dictionary that remains JSON serializable."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        values = dict(*args, **kwargs)
        for key, value in values.items():
            dict.__setitem__(self, key, freeze_value(value))

    @staticmethod
    def _immutable(*_args: object, **_kwargs: object) -> None:
        raise TypeError("FrozenDict is immutable")

    __setitem__ = _immutable
    __delitem__ = _immutable
    __ior__ = _immutable
    clear = _immutable
    pop = _immutable
    popitem = _immutable
    setdefault = _immutable
    update = _immutable

    def copy(self) -> "FrozenDict":
        return self

    def __deepcopy__(self, memo: dict[int, object]) -> "FrozenDict":
        return self


def freeze_value(value: Any) -> Any:
    """Recursively copy mutable JSON-like values into immutable equivalents."""

    if isinstance(value, Mapping):
        return FrozenDict(value)
    if isinstance(value, (list, tuple)):
        return tuple(freeze_value(item) for item in value)
    if isinstance(value, (set, frozenset)):
        return frozenset(freeze_value(item) for item in value)
    return copy.deepcopy(value)
