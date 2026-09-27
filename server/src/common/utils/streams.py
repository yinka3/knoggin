"""Explicit close propagation for adapters around an owned async stream."""

from collections.abc import AsyncIterator
from typing import Generic, TypeVar

T = TypeVar("T")


class ClosingAsyncIterator(Generic[T]):
    """Close the owner even when the adapter has never been iterated."""

    def __init__(self, stream: AsyncIterator[T], owner: AsyncIterator) -> None:
        self.stream = stream
        self.owner = owner

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        try:
            return await anext(self.stream)
        except BaseException:
            await self.aclose()
            raise

    async def aclose(self) -> None:
        try:
            await self.stream.aclose()
        finally:
            close = getattr(self.owner, "aclose", None)
            if close is not None:
                await close()
