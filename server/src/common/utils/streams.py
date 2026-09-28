"""Explicit close propagation for adapters around an owned async stream."""

import asyncio
from collections.abc import AsyncIterator
from typing import Generic, TypeVar

from loguru import logger

from common.utils.lifecycle import settle_owned_task

T = TypeVar("T")


class ClosingAsyncIterator(Generic[T]):
    """Close the owner even when the adapter has never been iterated."""

    def __init__(self, stream: AsyncIterator[T], owner: AsyncIterator) -> None:
        self.stream = stream
        self.owner = owner
        self._close_lock = asyncio.Lock()
        self._stream_closed = False
        self._owner_closed = False

    def __aiter__(self):
        return self

    async def __anext__(self) -> T:
        try:
            return await anext(self.stream)
        except BaseException:
            try:
                await self.aclose()
            except Exception:
                logger.error("Owned stream cleanup failed; preserving iteration outcome")
            raise

    async def aclose(self) -> None:
        async with self._close_lock:
            owned = asyncio.create_task(self._close())
            try:
                await asyncio.shield(owned)
            except asyncio.CancelledError:
                await settle_owned_task(owned)
                raise

    async def _close(self) -> None:
        try:
            if not self._stream_closed:
                await self.stream.aclose()
                self._stream_closed = True
        finally:
            if not self._owner_closed:
                close = getattr(self.owner, "aclose", None)
                if close is not None:
                    await close()
                self._owner_closed = True
