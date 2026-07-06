import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional

from psycopg import sql

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.document import DocumentFocus
from common.utils.events import EventEmitter
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_iso
from infrastructure.redis_client import RedisKeys
from core.project.project_manager import ProjectManager
from core.session.context import Session


class SessionManager:
    def __init__(
        self,
        resources: Any,
        user_name: str,
        active_sessions: Dict[str, Session],
        project_manager: ProjectManager,
    ):
        self.resources = resources
        self.user_name = user_name
        self.active_sessions = active_sessions
        self.project_manager = project_manager
        self.pg = resources.postgres
        self._session_locks: Dict[str, asyncio.Lock] = {}
        self._lock = asyncio.Lock()

    async def _read_redis_session_metadata(self, session_id: str) -> Optional[dict]:
        raw = await self.resources.redis.hget(
            RedisKeys.sessions(self.user_name),
            session_id,
        )
        if not raw:
            return None
        metadata = safe_json_loads(raw)
        return metadata if isinstance(metadata, dict) else None

    async def _write_redis_session_metadata(
        self,
        session_id: str,
        metadata: dict,
    ) -> None:
        await self.resources.redis.hset(
            RedisKeys.sessions(self.user_name),
            session_id,
            json.dumps(metadata, default=str),
        )
        project_id = metadata.get("project_id")
        if project_id:
            await self.resources.redis.sadd(
                RedisKeys.project_sessions(self.user_name, project_id),
                session_id,
            )

    async def _delete_redis_session_metadata(
        self,
        session_id: str,
        project_id: Optional[str] = None,
    ) -> None:
        await self.resources.redis.hdel(RedisKeys.sessions(self.user_name), session_id)
        if project_id:
            await self.resources.redis.srem(
                RedisKeys.project_sessions(self.user_name, project_id),
                session_id,
            )

    async def list_sessions(self) -> Dict[str, dict]:
        try:
            query = """
                SELECT session_id, project_id, model, agent_id, enabled_tools,
                       document_focus, status, created_at, last_active_at
                FROM public.sessions
                WHERE user_name = %(user_name)s
            """
            rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

            result = {}
            for row in rows:
                meta = dict(row)
                sid = meta["session_id"]
                # Convert timestamps to ISO string
                if meta.get("created_at"): meta["created_at"] = meta["created_at"].isoformat()
                if meta.get("last_active_at"): meta["last_active_at"] = meta["last_active_at"].isoformat()

                # Parse JSONB fields back to dict/lists if needed
                if isinstance(meta.get("enabled_tools"), str):
                    meta["enabled_tools"] = safe_json_loads(meta["enabled_tools"])
                if isinstance(meta.get("document_focus"), str):
                    meta["document_focus"] = safe_json_loads(meta["document_focus"])

                result[sid] = meta
            return result
        except Exception as e:
            logger.error(f"Failed to list sessions (check Postgres connection): {e}")
            raise

    async def create_session(
        self,
        project_id: str,
        topics_config: Optional[dict] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
    ) -> Session:
        if not project_id or not project_id.strip():
            raise ValueError("create_session requires a project_id from an existing project")

        session_id = str(uuid.uuid4())

        async with self._lock:
            project_state = await self.project_manager.acquire_project_for_session(
                project_id, session_id, topics_config=topics_config
            )

            try:
                context = await Session.create(
                    user_name=self.user_name,
                    resources=self.resources,
                    session_id=session_id,
                    model=model,
                    project_state=project_state,
                )
            except Exception:
                await self.project_manager.remove_session(project_id, session_id)
                await self.project_manager.release_project(project_id)
                raise

            tools_json = json.dumps(enabled_tools) if enabled_tools else None

            query = """
                INSERT INTO public.sessions (
                    session_id, user_name, project_id, model, agent_id, enabled_tools, status
                ) VALUES (
                    %(session_id)s, %(user_name)s, %(project_id)s, %(model)s, %(agent_id)s, %(enabled_tools)s, 'open'
                )
            """
            await self.pg.execute(query, {
                "session_id": session_id,
                "user_name": self.user_name,
                "project_id": project_id,
                "model": model,
                "agent_id": agent_id,
                "enabled_tools": tools_json
            })
            await self._write_redis_session_metadata(
                session_id,
                {
                    "project_id": project_id,
                    "model": model,
                    "agent_id": agent_id,
                    "enabled_tools": enabled_tools,
                    "created_at": get_now_iso(),
                    "last_active": get_now_iso(),
                },
            )

            self.active_sessions[session_id] = context
            logger.info(f"Created session: {session_id}")
            return context

    async def get_or_resume_session(self, session_id: str) -> Optional[Session]:
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]

        async with self._lock:
            if session_id not in self._session_locks:
                self._session_locks[session_id] = asyncio.Lock()
            lock = self._session_locks[session_id]

        async with lock:
            if session_id in self.active_sessions:
                return self.active_sessions[session_id]

            query = "SELECT project_id, model FROM public.sessions WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
            rows = await self.pg.fetch_all(query, {"user_name": self.user_name, "session_id": session_id})

            redis_metadata = None
            if not rows:
                redis_metadata = await self._read_redis_session_metadata(session_id)
                if not redis_metadata:
                    return None
                metadata = redis_metadata
            else:
                metadata = rows[0]

            project_id = metadata.get("project_id")
            if not project_id:
                raise ValueError(f"Session {session_id} has no valid project_id")

            model = metadata.get("model")

            if not rows and redis_metadata is None:
                return None

            project_state = await self.project_manager.acquire_project_for_session(
                project_id, session_id
            )

            try:
                context = await Session.create(
                    user_name=self.user_name,
                    resources=self.resources,
                    session_id=session_id,
                    model=model,
                    project_state=project_state,
                )
            except Exception:
                await self.project_manager.release_project(project_id)
                raise

            self.active_sessions[session_id] = context

            update_query = "UPDATE public.sessions SET last_active_at = now() WHERE session_id = %(session_id)s"
            await self.pg.execute(update_query, {"session_id": session_id})
            redis_metadata = (
                redis_metadata
                or await self._read_redis_session_metadata(session_id)
                or {"project_id": project_id, "model": model}
            )
            redis_metadata["last_active"] = get_now_iso()
            await self._write_redis_session_metadata(session_id, redis_metadata)

            logger.info(f"Resumed session: {session_id}")
            return context

    async def close_session(self, session_id: str) -> bool:
        async with self._lock:
            if session_id not in self.active_sessions:
                return False

            context = self.active_sessions.pop(session_id)
            self._session_locks.pop(session_id, None)

        if context.project_id:
            try:
                if hasattr(context, "shutdown"):
                    await context.shutdown()
            finally:
                EventEmitter.get().unregister_session(
                    context.project_id, session_id
                )
                await self.project_manager.release_project(context.project_id)
        elif hasattr(context, "shutdown"):
            await context.shutdown()

        update_query = "UPDATE public.sessions SET last_active_at = now() WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
        await self.pg.execute(update_query, {"user_name": self.user_name, "session_id": session_id})
        metadata = await self._read_redis_session_metadata(session_id)
        if metadata is not None:
            metadata["last_active"] = get_now_iso()
            await self._write_redis_session_metadata(session_id, metadata)

        logger.info(f"Closed session: {session_id}")
        return True

    async def get_session_history_readonly(
        self, session_id: str, limit: int = 1000
    ) -> List[Dict]:
        """Read conversation history natively from Postgres."""
        query = """
            SELECT message_id, role, content, timestamp_ms as timestamp
            FROM public.messages
            WHERE user_name = %(user_name)s AND session_id = %(session_id)s
            ORDER BY timestamp_ms ASC
            LIMIT %(limit)s
        """
        rows = await self.pg.fetch_all(query, {
            "user_name": self.user_name,
            "session_id": session_id,
            "limit": limit
        })
        if not rows:
            recent_key = RedisKeys.recent_conversation(self.user_name, session_id)
            conv_key = RedisKeys.conversation(self.user_name, session_id)
            message_ids = await self.resources.redis.zrange(recent_key, 0, -1)
            if limit:
                message_ids = message_ids[-limit:]
            rows = []
            for message_id in message_ids:
                raw = await self.resources.redis.hget(conv_key, message_id)
                data = safe_json_loads(raw) if raw else None
                if not isinstance(data, dict):
                    continue
                rows.append(
                    {
                        "message_id": data.get("message_id", int(message_id)),
                        "role": data.get("role", "user"),
                        "content": data.get(
                            "content",
                            data.get("message", ""),
                        ),
                        "timestamp": data.get("timestamp", ""),
                    }
                )

        turns = []
        for row in rows:
            turns.append({
                "message_id": row["message_id"],
                "role": row["role"],
                "content": row["content"],
                "timestamp": row["timestamp"],
            })

        return turns

    async def delete_session_data(self, session_id: str) -> int:
        """
        Delete Postgres session and all ephemeral Redis keys associated with a session.
        Returns count of Redis keys deleted.
        """
        user = self.user_name
        redis = self.resources.redis
        project_id = None

        query = "SELECT project_id FROM public.sessions WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
        rows = await self.pg.fetch_all(query, {"user_name": user, "session_id": session_id})
        if rows:
            project_id = rows[0]["project_id"]

        if not project_id and session_id in self.active_sessions:
            project_id = self.active_sessions[session_id].project_id
        if not project_id:
            metadata = await self._read_redis_session_metadata(session_id)
            if metadata:
                project_id = metadata.get("project_id")

        # Ephemeral Redis Cleanup
        direct_keys = RedisKeys.session_keys(user, session_id)
        deleted = 0
        for pattern in (
            RedisKeys.session_memory_pattern(user, session_id),
            RedisKeys.message_dedup_pattern(user, session_id),
        ):
            cursor = 0
            while True:
                cursor, keys = await redis.scan(cursor, match=pattern, count=100)
                if keys:
                    deleted += int(await redis.delete(*keys))
                if cursor == 0:
                    break

        if direct_keys:
            deleted += await redis.delete(*direct_keys)

        # Postgres Durable Deletion
        del_msgs_query = "DELETE FROM public.messages WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
        await self.pg.execute(del_msgs_query, {"user_name": user, "session_id": session_id})

        del_query = "DELETE FROM public.sessions WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
        await self.pg.execute(del_query, {"user_name": user, "session_id": session_id})

        if project_id:
            await self.project_manager.remove_session(project_id, session_id)
        await self._delete_redis_session_metadata(session_id, project_id)

        logger.info(f"Cleaned up {deleted} Redis keys and deleted Postgres session {session_id}")
        return deleted

    async def update_session_metadata(self, session_id: str, new_data: dict) -> dict:
        """Update session metadata directly."""
        cols = {k: json.dumps(v) if isinstance(v, (dict, list)) else v for k, v in new_data.items()}

        if not cols:
            return {}

        stmt = sql.SQL(
            "UPDATE public.sessions SET {fields} WHERE user_name = %s AND session_id = %s"
        ).format(
            fields=sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(k)) for k in cols
            )
        )
        await self.pg.execute(stmt, [*cols.values(), self.user_name, session_id])
        return new_data

    async def get_document_focus(
        self,
        session_id: str,
    ) -> Optional[dict]:
        """Return validated persisted document focus without resuming a session."""
        query = "SELECT document_focus FROM public.sessions WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name, "session_id": session_id})

        if not rows or not rows[0].get("document_focus"):
            metadata = await self._read_redis_session_metadata(session_id)
            if not metadata or not metadata.get("document_focus"):
                return None
            focus = metadata["document_focus"]
        else:
            focus = rows[0]["document_focus"]

        if isinstance(focus, str):
            focus = safe_json_loads(focus)

        if not isinstance(focus, dict):
            return None

        return DocumentFocus.model_validate(focus).model_dump(mode="json")

    async def set_document_focus(
        self,
        session_id: str,
        *,
        document_id: Optional[str] = None,
        folder_root_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> dict:
        """Validate and persist one pinned document focus for a session."""
        context = await self.get_or_resume_session(session_id)
        if context is None:
            raise FileNotFoundError("Session not found")
        if context.document_service is None:
            raise RuntimeError("Session document service is unavailable")

        target = await context.document_service.resolve_focus_target(
            session_id=session_id,
            document_id=document_id,
            folder_root_id=folder_root_id,
            path_prefix=path_prefix,
        )
        focus = DocumentFocus(
            mode="pinned",
            created_at=get_now_iso(),
            **target,
        ).model_dump(mode="json")

        query = "UPDATE public.sessions SET document_focus = %(focus)s WHERE session_id = %(session_id)s"
        await self.pg.execute(query, {"focus": json.dumps(focus), "session_id": session_id})
        metadata = await self._read_redis_session_metadata(session_id) or {
            "project_id": getattr(context, "project_id", None),
        }
        metadata["document_focus"] = focus
        await self._write_redis_session_metadata(session_id, metadata)

        return focus

    async def clear_document_focus(self, session_id: str) -> bool:
        """Remove persisted focus while preserving all other session metadata."""
        query = """
            UPDATE public.sessions
            SET document_focus = NULL
            WHERE user_name = %(user_name)s
              AND session_id = %(session_id)s
            RETURNING session_id
        """
        # We simulate returning by checking row count, but psycopg returns rowcount
        rowcount = await self.pg.execute(
            query,
            {"user_name": self.user_name, "session_id": session_id},
        )
        metadata = await self._read_redis_session_metadata(session_id)
        if rowcount == 0 and metadata is None:
            raise FileNotFoundError("Session not found")
        if metadata is not None:
            had_focus = (
                "document_focus" in metadata
                and metadata["document_focus"] is not None
            )
            metadata["document_focus"] = None
            await self._write_redis_session_metadata(session_id, metadata)
            return had_focus
        return True
