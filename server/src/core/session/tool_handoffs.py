"""Tool work that keeps running after its run ends, and reports when done.

A tool hands off work that is still going, either because the user chose to
let it continue in the background or because its run stopped (nobody was
watching). The work keeps running under this registry, is recorded at each
change, and adds a report turn to its chat when it finishes. It never
resumes after a restart: a running item becomes `interrupted`.
"""

from __future__ import annotations

import asyncio
from collections import OrderedDict
from typing import TYPE_CHECKING, Any, Callable
from uuid import uuid4

from loguru import logger

from common.conf.manager import ConfigManager
from common.utils.time_utils import get_now_iso
from core.session.background_work_store import BackgroundWorkStore

if TYPE_CHECKING:
    from core.session.session_manager import SessionManager

_FINISHED = frozenset({"completed", "failed", "stopped", "interrupted"})
_KEPT_FINISHED = 100
_MAX_RESULT_CHARS = 4_000


class HandoffLimitReached(Exception):
    """The chat already has its most background handoffs running."""


class ToolHandoffs:
    def __init__(self, sessions: SessionManager, config_manager=None) -> None:
        self._sessions = sessions
        postgres = getattr(sessions, "pg", None)
        self._store = (
            BackgroundWorkStore(postgres, sessions.user_name) if postgres is not None else None
        )
        self._config_manager = config_manager
        self._items: OrderedDict[str, dict[str, Any]] = OrderedDict()
        self._tasks: dict[str, asyncio.Task] = {}
        self._closed = False

    def _limit(self) -> int:
        config = (self._config_manager or ConfigManager.get()).config
        return config.developer_settings.background_tasks.max_handoffs_per_chat

    def adopt(
        self, work: asyncio.Task, *, project_id: str, session_id: str, tool: str,
        label: str, describe: Callable[[Any], str] = str,
    ) -> dict[str, Any]:
        """Take over a running task. Raises HandoffLimitReached when the chat
        is at its limit, or RuntimeError while shutting down; the caller then
        stops the work itself."""

        if self._closed:
            raise RuntimeError("Background handoffs are shutting down")
        running = sum(
            item["session_id"] == session_id and item["state"] == "running"
            for item in self._items.values()
        )
        if running >= self._limit():
            raise HandoffLimitReached(
                f"This chat already has {running} item(s) running in the background "
                "(developer_settings.background_tasks.max_handoffs_per_chat)"
            )
        item = {
            "item_id": str(uuid4()), "project_id": project_id, "session_id": session_id,
            "tool": tool, "label": label[:200], "state": "running", "result": None,
            "error": None, "created_at": get_now_iso(), "finished_at": None,
        }
        self._remember(item)
        self._tasks[item["item_id"]] = asyncio.create_task(
            self._follow(item, work, describe), name=f"tool-handoff:{item['item_id']}"
        )
        return dict(item)

    async def _follow(self, item: dict[str, Any], work: asyncio.Task, describe) -> None:
        await self._persist(item)
        try:
            result = await work
            item.update(state="completed", result=describe(result)[:_MAX_RESULT_CHARS])
        except asyncio.CancelledError:
            if not work.done():
                work.cancel()
                await asyncio.gather(work, return_exceptions=True)
            item["state"] = "interrupted" if self._closed else "stopped"
        except Exception as exc:
            logger.exception("Background {} failed", item["label"])
            item.update(state="failed", error=(str(exc) or type(exc).__name__)[:500])
        item["finished_at"] = get_now_iso()
        self._tasks.pop(item["item_id"], None)
        await asyncio.shield(self._persist(item))
        if item["state"] != "interrupted":
            await self._report(item)

    async def _report(self, item: dict[str, Any]) -> None:
        try:
            chat = await self._sessions.get_or_resume_session(item["session_id"])
        except Exception:
            logger.exception("Background {} could not reopen its chat", item["label"])
            return
        if chat is None:
            return
        header = f"Background {item['tool']} ({item['label']}) {item['state']}."
        if item["state"] == "completed":
            text = f"{header}\n\nResult:\n{item['result']}\n\nTell the user what happened, briefly."
        elif item["state"] == "failed":
            text = f"{header}\n\nError: {item['error']}\n\nTell the user, and offer to try again."
        else:
            text = f"{header}\n\nThe user stopped it. Acknowledge briefly."
        try:
            await chat.queue_report(
                text, origin="tool_handoff_report", metadata={"item_id": item["item_id"]}
            )
        except Exception:
            logger.exception("Background {} report was not queued", item["label"])

    def list(self, *, session_id: str) -> list[dict[str, Any]]:
        return [dict(item) for item in self._items.values() if item["session_id"] == session_id]

    async def stop(self, item_id: str, *, session_id: str) -> dict[str, Any] | None:
        item = self._items.get(item_id)
        if item is None or item["session_id"] != session_id:
            return None
        task = self._tasks.get(item_id)
        if task is not None and not task.done():
            task.cancel()
            await asyncio.gather(task, return_exceptions=True)
        return dict(item)

    async def resume(self) -> None:
        """After a restart: work that was running is gone, so mark it."""

        if self._store is None:
            return
        for row in await self._store.load("tool_handoff", limit=_KEPT_FINISHED):
            item = dict(row["payload"])
            if item["state"] == "running":
                item.update(state="interrupted", finished_at=get_now_iso())
                await self._persist(item)
            self._remember(item)

    async def shutdown(self) -> None:
        self._closed = True
        tasks = [task for task in self._tasks.values() if not task.done()]
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _persist(self, item: dict[str, Any]) -> None:
        if self._store is None:
            return
        try:
            await self._store.save(
                "tool_handoff", item["item_id"], project_id=item["project_id"],
                session_id=item["session_id"], state=item["state"], payload=item,
            )
        except Exception:
            logger.exception("Background {} was not recorded", item["label"])

    def _remember(self, item: dict[str, Any]) -> None:
        self._items[item["item_id"]] = item
        finished = [key for key, old in self._items.items() if old["state"] in _FINISHED]
        for key in finished[: max(0, len(self._items) - _KEPT_FINISHED)]:
            del self._items[key]
