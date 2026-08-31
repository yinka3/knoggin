import asyncio
import json
import uuid
from typing import Any, Dict, List, Optional

from loguru import logger
from psycopg import sql

from common.schema.document import (
    create_document_focus,
    dump_document_focus,
    parse_document_focus,
)
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_iso
from core.knowledge.db.writers.session_deletion_writer import SessionDeletionWriter
from core.project.project_manager import ProjectManager
from runtime.session_runtime import SessionRuntime
from runtime.session_runtime_factory import SessionRuntimeFactory


class SessionManager:
    _METADATA_UPDATE_COLUMNS = frozenset({"model", "agent_id", "enabled_tools"})

    def __init__(
        self,
        resources: Any,
        user_name: str,
        project_manager: ProjectManager,
        agent_orchestrator: Any | None = None,
    ):
        self.resources = resources
        self.user_name = user_name
        self._active_sessions: Dict[str, SessionRuntime] = {}
        self._health_service: Any | None = None
        self._agent_orchestrator = agent_orchestrator
        self.project_manager = project_manager
        self.pg = resources.postgres
        self._session_deletion_writer = SessionDeletionWriter(self.pg)
        # Lifecycle work is deliberately rare for the local single-engine
        # runtime. Keeping it in one critical section makes publication,
        # shutdown, and deletion atomic with respect to each other.
        self._lifecycle_lock = asyncio.Lock()
        self._closed = False

    def attach_health_service(self, health_service: Any) -> None:
        """Attach the application-owned health service before sessions are exposed."""

        if self._health_service is not None:
            raise RuntimeError("SessionManager health service is already attached")
        self._health_service = health_service

    def attach_agent_orchestrator(self, agent_orchestrator: Any) -> None:
        """Attach the application-owned agent service before sessions are exposed."""
        if self._agent_orchestrator is not None:
            raise RuntimeError("SessionManager agent orchestrator is already attached")
        self._agent_orchestrator = agent_orchestrator

    def get_runtime_session(self, session_id: str) -> SessionRuntime | None:
        """Return one active session for read-only runtime inspection."""

        return self._active_sessions.get(session_id)

    def active_runtime_count(self) -> int:
        """Return the number of loaded session runtimes."""

        return len(self._active_sessions)

    async def list_sessions(self) -> Dict[str, dict]:
        try:
            query = """
                SELECT session_id, project_id, model, agent_id, enabled_tools,
                       document_focus, status, created_at, last_active_at
                FROM public.sessions
                WHERE user_name = %(user_name)s
                  AND status = 'open'
            """
            rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

            result = {}
            for row in rows:
                meta = dict(row)
                sid = meta["session_id"]
                # Normalize database JSON fields while preserving native values.
                if isinstance(meta.get("enabled_tools"), str):
                    meta["enabled_tools"] = safe_json_loads(meta["enabled_tools"])
                if isinstance(meta.get("document_focus"), str):
                    meta["document_focus"] = safe_json_loads(meta["document_focus"])

                result[sid] = meta
            return result
        except Exception as e:
            logger.error(f"Failed to list sessions (check Postgres connection): {e}")
            raise

    def _session_runtime_factory(self) -> SessionRuntimeFactory:
        return SessionRuntimeFactory(
            self.user_name,
            self.resources,
            health_service=self._health_service,
            agent_orchestrator=self._agent_orchestrator,
        )

    async def _hard_delete_failed_session(self, session_id: str) -> None:
        """Remove a partially created session instead of preserving a tombstone."""

        try:
            await self.pg.execute(
                "DELETE FROM public.sessions "
                "WHERE user_name = %(user_name)s AND session_id = %(session_id)s",
                {"user_name": self.user_name, "session_id": session_id},
            )
        except Exception:
            logger.exception(
                "Failed to hard-delete session {} after creation failed", session_id
            )

    async def create_session(
        self,
        project_id: str,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
    ) -> SessionRuntime:
        if not project_id or not project_id.strip():
            raise ValueError("create_session requires a project_id from an existing project")

        session_id = str(uuid.uuid4())

        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("SessionManager is shutting down")
            context = None
            project_leased = False
            persisted = False

            try:
                tools_json = (
                    json.dumps(enabled_tools)
                    if enabled_tools is not None
                    else None
                )

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
                persisted = True

                project_state = await self.project_manager.acquire_project_for_session(
                    project_id,
                    session_id,
                )
                project_leased = True
                context = await self._session_runtime_factory().create(
                    project_state,
                    session_id=session_id,
                    model=model,
                    agent_id=agent_id,
                    enabled_tools=enabled_tools,
                )

                self._active_sessions[session_id] = context
                logger.info(f"Created session: {session_id}")
                return context
            except Exception:
                if context is not None:
                    try:
                        await context.shutdown()
                    except Exception:
                        logger.exception(
                            "Failed to unload runtime for failed session {}", session_id
                        )
                if project_leased:
                    try:
                        await self.project_manager.release_project_for_session(
                            project_id,
                            session_id,
                        )
                    except Exception:
                        logger.exception(
                            "Failed to release project lease for failed session {}",
                            session_id,
                        )
                if persisted:
                    await self._hard_delete_failed_session(session_id)
                raise

    async def get_or_resume_session(self, session_id: str) -> Optional[SessionRuntime]:
        async with self._lifecycle_lock:
            return await self._get_or_resume_session_locked(session_id)

    async def _get_or_resume_session_locked(
        self, session_id: str
    ) -> Optional[SessionRuntime]:
        """Return a live runtime while the manager lifecycle is serialized."""

        if self._closed:
            raise RuntimeError("SessionManager is shutting down")
        active_session = self._active_sessions.get(session_id)
        if active_session is not None:
            return active_session

        query = """
            SELECT project_id, model, agent_id, enabled_tools, document_focus
            FROM public.sessions
            WHERE user_name = %(user_name)s
              AND session_id = %(session_id)s
              AND status = 'open'
        """
        rows = await self.pg.fetch_all(
            query,
            {"user_name": self.user_name, "session_id": session_id},
        )
        if not rows:
            return None
        metadata = rows[0]

        project_id = metadata.get("project_id")
        if not project_id:
            raise ValueError(f"Session {session_id} has no valid project_id")

        model = metadata.get("model")
        agent_id = metadata.get("agent_id")
        enabled_tools = metadata.get("enabled_tools")
        if isinstance(enabled_tools, str):
            enabled_tools = safe_json_loads(enabled_tools)
        if enabled_tools is not None and not isinstance(enabled_tools, list):
            raise ValueError(f"Session {session_id} has invalid enabled_tools")

        document_focus = metadata.get("document_focus")
        if isinstance(document_focus, str):
            document_focus = safe_json_loads(document_focus)
        if document_focus is not None:
            document_focus = parse_document_focus(document_focus)

        context = None
        project_leased = False
        try:
            project_state = await self.project_manager.acquire_project_for_session(
                project_id,
                session_id,
            )
            project_leased = True
            context = await self._session_runtime_factory().create(
                project_state,
                session_id=session_id,
                model=model,
                agent_id=agent_id,
                enabled_tools=enabled_tools,
                document_focus=document_focus,
            )
            update_query = """
                UPDATE public.sessions
                SET last_active_at = now()
                WHERE user_name = %(user_name)s
                  AND session_id = %(session_id)s
                  AND status = 'open'
            """
            updated = await self.pg.execute(
                update_query,
                {"user_name": self.user_name, "session_id": session_id},
            )
            if updated != 1:
                raise RuntimeError("Session disappeared while resuming")
            self._active_sessions[session_id] = context
            logger.info(f"Resumed session: {session_id}")
            return context
        except Exception:
            if context is not None:
                try:
                    await context.shutdown()
                except Exception:
                    logger.exception(
                        "Failed to unload runtime for session {}", session_id
                    )
            if project_leased:
                await self.project_manager.release_project_for_session(
                    project_id,
                    session_id,
                )
            raise

    async def deactivate_runtime_session(self, session_id: str) -> bool:
        """Unload a live session while retaining its durable session record."""
        async with self._lifecycle_lock:
            return await self._deactivate_runtime_session_locked(session_id)

    async def _deactivate_runtime_session_locked(self, session_id: str) -> bool:
        """Unload one runtime while the manager lifecycle is serialized."""

        context = self._active_sessions.pop(session_id, None)
        if context is None:
            return False

        project_id = context.project_id
        if not project_id:
            raise RuntimeError(f"Session {session_id} is missing its project id")
        try:
            await context.shutdown()
        finally:
            await self.project_manager.release_project_for_session(
                project_id,
                session_id,
            )
        logger.info(f"Deactivated runtime session: {session_id}")
        return True

    async def shutdown(self) -> None:
        """Unload every live session before global infrastructure shuts down."""
        async with self._lifecycle_lock:
            if self._closed:
                return
            self._closed = True
            session_ids = list(self._active_sessions)
            failures: list[Exception] = []
            for session_id in session_ids:
                try:
                    await self._deactivate_runtime_session_locked(session_id)
                except Exception as exc:
                    failures.append(exc)
            if failures:
                raise RuntimeError(
                    f"Failed to deactivate {len(failures)} session runtime(s)"
                ) from failures[0]

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
        turns = []
        for row in rows:
            turns.append({
                "message_id": row["message_id"],
                "role": row["role"],
                "content": row["content"],
                "timestamp": row["timestamp"],
            })

        return turns

    async def delete_session(self, session_id: str) -> None:
        """
        Tombstone a session and purge its session-owned documents.

        Canonical messages and their graph projections remain as read-only
        evidence for existing project memories. The durable tombstone excludes
        them from future runtime, ingestion, and episode work.

        """
        user = self.user_name
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("SessionManager is shutting down")
            await self._deactivate_runtime_session_locked(session_id)
            await self._session_deletion_writer.delete_session(
                user_name=user,
                session_id=session_id,
            )
            logger.info("Deleted durable session state for {}", session_id)

    async def update_session(self, session_id: str, new_data: dict) -> dict:
        """Update explicitly allowed session configuration fields."""
        if not isinstance(session_id, str) or not session_id.strip():
            raise ValueError("session_id must not be empty")
        if not isinstance(new_data, dict):
            raise ValueError("new_data must be a dictionary")

        unknown_columns = set(new_data) - self._METADATA_UPDATE_COLUMNS
        if unknown_columns:
            raise ValueError(
                "update_session does not allow: "
                + ", ".join(sorted(unknown_columns))
            )

        cols = {
            key: json.dumps(value) if isinstance(value, (dict, list)) else value
            for key, value in new_data.items()
        }

        if not cols:
            return {}

        stmt = sql.SQL(
            "UPDATE public.sessions SET {fields} WHERE user_name = %s AND session_id = %s AND status = 'open'"
        ).format(
            fields=sql.SQL(", ").join(
                sql.SQL("{} = %s").format(sql.Identifier(k)) for k in cols
            )
        )
        async with self._lifecycle_lock:
            if self._closed:
                raise RuntimeError("SessionManager is shutting down")
            updated = await self.pg.execute(
                stmt,
                [*cols.values(), self.user_name, session_id],
            )
            if updated != 1:
                raise FileNotFoundError("Session not found")

            context = self.get_runtime_session(session_id)
            if context is not None:
                for key, value in new_data.items():
                    setattr(
                        context,
                        key,
                        list(value)
                        if key == "enabled_tools" and value is not None
                        else value,
                    )
        return new_data

    async def get_document_focus(
        self,
        session_id: str,
    ) -> Optional[dict]:
        """Return validated persisted document focus without resuming a session."""
        query = """
            SELECT document_focus
            FROM public.sessions
            WHERE user_name = %(user_name)s
              AND session_id = %(session_id)s
              AND status = 'open'
        """
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name, "session_id": session_id})

        if not rows:
            raise FileNotFoundError("Session not found")
        if not rows[0].get("document_focus"):
            return None
        focus = rows[0]["document_focus"]

        if isinstance(focus, str):
            focus = safe_json_loads(focus)

        if not isinstance(focus, dict):
            return None

        return dump_document_focus(parse_document_focus(focus))

    async def set_document_focus(
        self,
        session_id: str,
        *,
        document_id: Optional[str] = None,
        path_prefix: Optional[str] = None,
    ) -> dict:
        """Validate and persist one pinned document focus for a session."""
        async with self._lifecycle_lock:
            context = await self._get_or_resume_session_locked(session_id)
            if context is None:
                raise FileNotFoundError("Session not found")
            if context.document_service is None:
                raise RuntimeError("Session document service is unavailable")

            target = await context.document_service.resolve_focus_target(
                session_id=session_id,
                document_id=document_id,
                path_prefix=path_prefix,
            )
            focus = dump_document_focus(
                create_document_focus(
                    mode="pinned",
                    created_at=get_now_iso(),
                    **target,
                )
            )

            query = "UPDATE public.sessions SET document_focus = %(focus)s WHERE user_name = %(user_name)s AND session_id = %(session_id)s AND status = 'open'"
            updated = await self.pg.execute(
                query,
                {
                    "focus": json.dumps(focus),
                    "user_name": self.user_name,
                    "session_id": session_id,
                },
            )
            if updated != 1:
                raise FileNotFoundError("Session not found")
            context.document_focus = parse_document_focus(focus)
            return focus

    async def clear_document_focus(self, session_id: str) -> bool:
        """Remove persisted focus while preserving all other session metadata."""
        query = """
            UPDATE public.sessions
            SET document_focus = NULL
            WHERE user_name = %(user_name)s
              AND session_id = %(session_id)s
              AND status = 'open'
            RETURNING session_id
        """
        # We simulate returning by checking row count, but psycopg returns rowcount
        async with self._lifecycle_lock:
            rowcount = await self.pg.execute(
                query,
                {"user_name": self.user_name, "session_id": session_id},
            )
            if rowcount == 0:
                raise FileNotFoundError("Session not found")
            context = self.get_runtime_session(session_id)
            if context is not None:
                context.document_focus = None
        return True
