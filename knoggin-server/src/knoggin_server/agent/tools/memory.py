from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from common.utils.agent_identity import (
    BRAIN_SNAPSHOT_INTERVAL,
    BRAIN_SNAPSHOT_POLICY,
    EDITABLE_BRAIN_SECTIONS,
    build_brain_snapshot_summary,
    extract_brain_section,
    normalize_agent_brain,
    replace_brain_section,
    should_snapshot_brain_revision,
)

if TYPE_CHECKING:
    import redis.asyncio as aioredis

    from infrastructure.knowledge_store import KnowledgeStore
    from infrastructure.postgres_client import PostgresClient
    from knoggin_server.knowledge.services.entity_service import EntityManager


class MemoryTools:
    redis: aioredis.Redis
    knowledge_store: KnowledgeStore
    postgres: PostgresClient
    entities: EntityManager

    async def read_brain(self) -> Dict:
        """Read the current durable Markdown brain and its revision."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            rows = await self.postgres.fetch_all(
                """
                SELECT brain, persona, brain_revision
                FROM public.agents
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
                """,
                {"user_name": self.user_name, "agent_id": target_agent},
            )
            if not rows:
                return {"error": "Active agent identity was not found"}
            row = rows[0]
            return {
                "content": normalize_agent_brain(
                    row.get("brain") or "",
                    row.get("persona") or "",
                ),
                "revision": row.get("brain_revision", 1),
                "editable_sections": list(EDITABLE_BRAIN_SECTIONS),
                "snapshot_policy": BRAIN_SNAPSHOT_POLICY,
            }
        except Exception as exc:
            return {"error": f"Failed to read brain: {exc}"}

    async def list_brain_snapshots(self) -> Dict:
        """List available restore points for the active durable Brain."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            current_rows = await self.postgres.fetch_all(
                """
                SELECT brain_revision
                FROM public.agents
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
                """,
                {"user_name": self.user_name, "agent_id": target_agent},
            )
            if not current_rows:
                return {"error": "Active agent identity was not found"}
            snapshot_rows = await self.postgres.fetch_all(
                """
                SELECT
                    revision,
                    edited_by,
                    change_type,
                    changed_section,
                    change_summary,
                    restored_from_revision,
                    created_at
                FROM public.agent_brain_snapshots
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
                ORDER BY revision DESC
                """,
                {"user_name": self.user_name, "agent_id": target_agent},
            )
            return {
                "current_revision": int(current_rows[0].get("brain_revision", 1)),
                "snapshot_interval": BRAIN_SNAPSHOT_INTERVAL,
                "snapshot_policy": BRAIN_SNAPSHOT_POLICY,
                "snapshots": snapshot_rows,
            }
        except Exception as exc:
            return {"error": f"Failed to list brain snapshots: {exc}"}

    async def read_brain_snapshot(self, revision: int) -> Dict:
        """Read one stored Brain restore point."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            rows = await self.postgres.fetch_all(
                """
                SELECT
                    revision,
                    content,
                    edited_by,
                    change_type,
                    changed_section,
                    change_summary,
                    restored_from_revision,
                    created_at
                FROM public.agent_brain_snapshots
                WHERE user_name = %(user_name)s
                  AND agent_id = %(agent_id)s
                  AND revision = %(revision)s
                """,
                {
                    "user_name": self.user_name,
                    "agent_id": target_agent,
                    "revision": revision,
                },
            )
            if not rows:
                return {"error": "Brain snapshot was not found"}
            row = rows[0]
            row["content"] = normalize_agent_brain(row.get("content") or "")
            return row
        except Exception as exc:
            return {"error": f"Failed to read brain snapshot: {exc}"}

    async def edit_brain(
        self,
        section: str,
        content: str,
        expected_revision: int,
        change_note: str = None,
    ) -> Dict:
        """Update one editable Brain section using optimistic concurrency."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            rows = await self.postgres.fetch_all(
                """
                SELECT brain, persona, brain_revision
                FROM public.agents
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
                """,
                {"user_name": self.user_name, "agent_id": target_agent},
            )
            if not rows:
                return {"error": "Active agent identity was not found"}

            current = rows[0]
            current_revision = int(current.get("brain_revision", 1))
            if expected_revision != current_revision:
                return {
                    "error": "Brain changed since it was read",
                    "current_revision": current_revision,
                }

            current_content = normalize_agent_brain(
                current.get("brain") or "",
                current.get("persona") or "",
            )
            updated_content = replace_brain_section(
                current_content,
                section,
                content,
            )
            new_revision = current_revision + 1
            params = {
                "user_name": self.user_name,
                "agent_id": target_agent,
                "expected_revision": expected_revision,
                "content": updated_content,
            }
            if should_snapshot_brain_revision(new_revision):
                params.update(
                    {
                        "changed_section": section,
                        "change_summary": build_brain_snapshot_summary(
                            "section_edit",
                            section=section,
                            change_note=change_note,
                        ),
                    }
                )
                query = """
                    WITH changed AS (
                        UPDATE public.agents
                        SET brain = %(content)s,
                            brain_revision = brain_revision + 1,
                            updated_at = now()
                        WHERE user_name = %(user_name)s
                          AND agent_id = %(agent_id)s
                          AND brain_revision = %(expected_revision)s
                        RETURNING agent_id, user_name, brain_revision, brain
                    )
                    INSERT INTO public.agent_brain_snapshots (
                        agent_id,
                        revision,
                        user_name,
                        content,
                        edited_by,
                        change_type,
                        changed_section,
                        change_summary
                    )
                    SELECT
                        agent_id,
                        brain_revision,
                        user_name,
                        brain,
                        'agent',
                        'section_edit',
                        %(changed_section)s,
                        %(change_summary)s
                    FROM changed
                """
            else:
                query = """
                    UPDATE public.agents
                    SET brain = %(content)s,
                        brain_revision = brain_revision + 1,
                        updated_at = now()
                    WHERE user_name = %(user_name)s
                      AND agent_id = %(agent_id)s
                      AND brain_revision = %(expected_revision)s
                """
            updated = await self.postgres.execute(query, params)
            if updated != 1:
                latest = await self.read_brain()
                return {
                    "error": "Brain changed before the edit could be committed",
                    "current_revision": latest.get("revision"),
                }

            return {
                "success": True,
                "section": section,
                "revision": new_revision,
                "message": "Brain section updated.",
                "snapshot_created": should_snapshot_brain_revision(new_revision),
            }
        except ValueError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": f"Failed to edit brain: {exc}"}

    async def restore_brain_section(
        self,
        section: str,
        from_snapshot_revision: int,
        expected_current_revision: int,
        change_note: str = None,
    ) -> Dict:
        """Restore one editable section from a stored Brain snapshot."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            current_rows = await self.postgres.fetch_all(
                """
                SELECT brain, persona, brain_revision
                FROM public.agents
                WHERE user_name = %(user_name)s AND agent_id = %(agent_id)s
                """,
                {"user_name": self.user_name, "agent_id": target_agent},
            )
            if not current_rows:
                return {"error": "Active agent identity was not found"}

            current = current_rows[0]
            current_revision = int(current.get("brain_revision", 1))
            if expected_current_revision != current_revision:
                return {
                    "error": "Brain changed since it was read",
                    "current_revision": current_revision,
                }

            snapshot_rows = await self.postgres.fetch_all(
                """
                SELECT content
                FROM public.agent_brain_snapshots
                WHERE user_name = %(user_name)s
                  AND agent_id = %(agent_id)s
                  AND revision = %(revision)s
                """,
                {
                    "user_name": self.user_name,
                    "agent_id": target_agent,
                    "revision": from_snapshot_revision,
                },
            )
            if not snapshot_rows:
                return {"error": "Brain snapshot was not found"}

            current_content = normalize_agent_brain(
                current.get("brain") or "",
                current.get("persona") or "",
            )
            snapshot_content = normalize_agent_brain(
                snapshot_rows[0].get("content") or "",
                current.get("persona") or "",
            )
            restored_section = extract_brain_section(snapshot_content, section)
            updated_content = replace_brain_section(
                current_content,
                section,
                restored_section,
            )
            new_revision = current_revision + 1
            updated = await self.postgres.execute(
                """
                WITH changed AS (
                    UPDATE public.agents
                    SET brain = %(content)s,
                        brain_revision = brain_revision + 1,
                        updated_at = now()
                    WHERE user_name = %(user_name)s
                      AND agent_id = %(agent_id)s
                      AND brain_revision = %(expected_revision)s
                    RETURNING agent_id, user_name, brain_revision, brain
                )
                INSERT INTO public.agent_brain_snapshots (
                    agent_id,
                    revision,
                    user_name,
                    content,
                    edited_by,
                    change_type,
                    changed_section,
                    change_summary,
                    restored_from_revision
                )
                SELECT
                    agent_id,
                    brain_revision,
                    user_name,
                    brain,
                    'agent',
                    'section_restore',
                    %(changed_section)s,
                    %(change_summary)s,
                    %(restored_from_revision)s
                FROM changed
                """,
                {
                    "user_name": self.user_name,
                    "agent_id": target_agent,
                    "expected_revision": expected_current_revision,
                    "content": updated_content,
                    "changed_section": section,
                    "change_summary": build_brain_snapshot_summary(
                        "section_restore",
                        section=section,
                        restored_from_revision=from_snapshot_revision,
                        change_note=change_note,
                    ),
                    "restored_from_revision": from_snapshot_revision,
                },
            )
            if updated != 1:
                latest = await self.read_brain()
                return {
                    "error": "Brain changed before the restore could be committed",
                    "current_revision": latest.get("revision"),
                }

            return {
                "success": True,
                "section": section,
                "revision": new_revision,
                "restored_from_revision": from_snapshot_revision,
                "snapshot_created": True,
                "message": "Brain section restored from snapshot.",
            }
        except ValueError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": f"Failed to restore brain section: {exc}"}

    async def save_insight(self, content: str) -> Dict:
        return {"error": "save_insight is only available in community discussions."}

    async def spawn_specialist(
        self,
        name: str,
        persona: str,
        initial_directives: List[Dict] = None,
    ) -> Dict:
        return {"error": "spawn_specialist is only available in community discussions."}
