"""Process-owned agent runs and bounded event replay for reconnecting clients."""

from __future__ import annotations

import asyncio
import json
from collections import deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field


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

    @property
    def first_sequence(self) -> int:
        return self.frames[0][0] if self.frames else 0


class RunEvents:
    """Run tasks outlive HTTP subscribers but end with the API process."""

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
            payload = json.dumps({"run_id": run.run_id, "sequence": sequence})
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
                available = [frame for sequence, frame in run.frames if sequence > cursor]
                if available:
                    # Event numbers are monotonic even when old frames expire.
                    cursor = run.frames[-1][0]
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
        tasks = [run.task for run in self._runs.values() if run.task and not run.task.done()]
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._runs.clear()
