import asyncio
from typing import Any, Coroutine, Optional, Set

from loguru import logger


class BackgroundTaskGroup:
    """Manages background asyncio tasks, ensuring they are tracked, logged, and gracefully shut down."""

    def __init__(self, name: str = "BackgroundTaskGroup"):
        self.name = name
        self._tasks: Set[asyncio.Task] = set()

    def create_task(self, coro: Coroutine[Any, Any, Any], name: Optional[str] = None) -> asyncio.Task:
        task = asyncio.create_task(coro, name=name)
        self._tasks.add(task)
        task.add_done_callback(self._tasks.discard)
        task.add_done_callback(self._handle_result)
        return task

    def _handle_result(self, task: asyncio.Task):
        if task.cancelled():
            return
        if exc := task.exception():
            task_name = task.get_name() or "Unnamed"
            logger.error(f"[{self.name}] Background task '{task_name}' failed: {exc}")

    async def shutdown(self, timeout: float = 5.0):
        if not self._tasks:
            return

        logger.info(f"[{self.name}] Shutting down {len(self._tasks)} background tasks...")
        for task in self._tasks:
            task.cancel()

        try:
            await asyncio.wait_for(
                asyncio.gather(*self._tasks, return_exceptions=True),
                timeout=timeout
            )
        except asyncio.TimeoutError:
            logger.warning(f"[{self.name}] Timeout waiting for background tasks to shut down")
        except Exception as e:
            logger.error(f"[{self.name}] Error during task shutdown: {e}")
        finally:
            self._tasks.clear()
