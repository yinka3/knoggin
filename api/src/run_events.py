"""Process-owned agent runs and bounded event replay for reconnecting clients."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field
from itertools import islice


@dataclass
class _Run:
    session_id: str
    run_id: str
    max_events: int
    max_bytes: int
    frames: deque[tuple[int, bytes]] = field(default_factory=deque)
    buffered_bytes: int = 0
    done: bool = False
    condition: asyncio.Condition = field(default_factory=asyncio.Condition)
    task: asyncio.Task | None = None
    watchers: int = 0
    unwatched_stop: asyncio.Task | None = None
    cancel_reason: str = "shutdown"

    @property
    def first_sequence(self) -> int:
        return self.frames[0][0] if self.frames else 0


class RunEvents:
    """Run tasks outlive a dropped connection long enough to reconnect, stop
    once nobody has watched them for the grace period, and end with the API
    process."""

    def __init__(self, settings):
        self._settings = settings
        self._runs: dict[tuple[str, str], _Run] = {}

    def start(
        self, session_id: str, run_id: str, frames: AsyncIterator[bytes]
    ) -> None:
        settings = self._settings()
        for key, run in tuple(self._runs.items()):
            if len(self._runs) < settings.max_retained_runs:
                break
            if run.done:
                del self._runs[key]
        run = _Run(
            session_id=session_id,
            run_id=run_id,
            max_events=settings.max_buffered_events,
            max_bytes=settings.max_buffered_bytes,
        )
        self._runs[(session_id, run_id)] = run
        run.task = asyncio.create_task(self._collect(run, frames))

    async def _collect(self, run: _Run, frames: AsyncIterator[bytes]) -> None:
        sequence = 0
        try:
            async for frame in frames:
                await self._append(run, sequence, frame)
                sequence += 1
        except asyncio.CancelledError:
            payload = json.dumps({
                "run_id": run.run_id, "sequence": sequence, "reason": run.cancel_reason,
            })
            await self._append(
                run, sequence, f"event: run_cancelled\ndata: {payload}\n\n".encode()
            )
            raise
        finally:
            try:
                await frames.aclose()
            finally:
                async with run.condition:
                    run.done = True
                    run.condition.notify_all()
                if run.unwatched_stop is not None:
                    run.unwatched_stop.cancel()

    @staticmethod
    async def _append(run: _Run, sequence: int, frame: bytes) -> None:
        async with run.condition:
            run.frames.append((sequence, frame))
            run.buffered_bytes += len(frame)
            while len(run.frames) > 1 and (
                len(run.frames) > run.max_events or run.buffered_bytes > run.max_bytes
            ):
                _, old = run.frames.popleft()
                run.buffered_bytes -= len(old)
            run.condition.notify_all()

    def get(self, session_id: str, run_id: str) -> _Run | None:
        return self._runs.get((session_id, run_id))

    async def stream(
        self, run: _Run, *, after: int | None = None
    ) -> AsyncIterator[bytes]:
        cursor = run.first_sequence - 1 if after is None else after
        run.watchers += 1
        if run.unwatched_stop is not None:
            run.unwatched_stop.cancel()
            run.unwatched_stop = None
        try:
            async for frame in self._frames_after(run, cursor):
                yield frame
        finally:
            run.watchers -= 1
            if run.watchers == 0 and not run.done:
                run.unwatched_stop = asyncio.create_task(self._stop_when_unwatched(run))

    async def _stop_when_unwatched(self, run: _Run) -> None:
        """Stop a run nobody reconnected to, so it does not keep spending."""

        await asyncio.sleep(self._settings().unwatched_run_grace_seconds)
        if run.watchers == 0 and run.task is not None and not run.task.done():
            run.cancel_reason = "unwatched"
            run.task.cancel()

    @staticmethod
    async def _frames_after(run: _Run, cursor: int) -> AsyncIterator[bytes]:
        while True:
            async with run.condition:
                gap = None
                if cursor < run.first_sequence - 1:
                    gap = {
                        "run_id": run.run_id,
                        "after_sequence": cursor,
                        "first_available_sequence": run.first_sequence,
                    }
                    cursor = run.first_sequence - 1
                # Sequences are consecutive, so the new frames are the last few.
                newest = run.frames[-1][0] if run.frames else cursor
                available = [
                    frame for _, frame in islice(reversed(run.frames), max(0, newest - cursor))
                ][::-1]
                if available:
                    cursor = newest
                elif run.done:
                    return
                else:
                    await run.condition.wait()
                    continue
            if gap is not None:
                yield f"event: run_events_gap\ndata: {json.dumps(gap)}\n\n".encode()
            for frame in available:
                yield frame

    async def close(self) -> None:
        for run in self._runs.values():
            if run.unwatched_stop is not None:
                run.unwatched_stop.cancel()
        tasks = [run.task for run in self._runs.values() if run.task and not run.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._runs.clear()
