from __future__ import annotations

from typing import TYPE_CHECKING, Dict, List

from common.utils.agent_identity import (
    EDITABLE_BRAIN_SECTIONS,
    normalize_agent_brain,
    replace_brain_section,
)

if TYPE_CHECKING:
    import redis.asyncio as aioredis

    from infrastructure.graph_interface import GraphInterface
    from infrastructure.postgres_client import PostgresClient
    from knoggin_server.knowledge.services.entity_service import EntityManager


class MemoryTools:
    redis: aioredis.Redis
    postgres: PostgresClient
    graph_client: GraphInterface
    entities: EntityManager

    async def read_brain(self) -> Dict:
        """Read the current durable Markdown brain and its revision."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            rows = await self.postgres.execute_read(
                """
                SELECT instructions, persona, brain_revision
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
                    row.get("instructions") or "",
                    row.get("persona") or "",
                ),
                "revision": row.get("brain_revision", 1),
                "editable_sections": list(EDITABLE_BRAIN_SECTIONS),
            }
        except Exception as exc:
            return {"error": f"Failed to read brain: {exc}"}

    async def edit_brain(
        self,
        section: str,
        content: str,
        expected_revision: int,
    ) -> Dict:
        """Update one editable Brain section using optimistic concurrency."""
        target_agent = getattr(self, "agent_id", None)
        if not target_agent:
            return {"error": "No durable agent identity is active"}

        try:
            rows = await self.postgres.execute_read(
                """
                SELECT instructions, persona, brain_revision
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
                current.get("instructions") or "",
                current.get("persona") or "",
            )
            updated_content = replace_brain_section(
                current_content,
                section,
                content,
            )
            new_revision = current_revision + 1
            updated = await self.postgres.execute_write(
                """
                WITH changed AS (
                    UPDATE public.agents
                    SET instructions = %(content)s,
                        brain_revision = brain_revision + 1,
                        updated_at = now()
                    WHERE user_name = %(user_name)s
                      AND agent_id = %(agent_id)s
                      AND brain_revision = %(expected_revision)s
                    RETURNING agent_id, user_name, brain_revision, instructions
                )
                INSERT INTO public.agent_brain_revisions (
                    agent_id, revision, user_name, content, edited_by
                )
                SELECT agent_id, brain_revision, user_name, instructions, 'agent'
                FROM changed
                """,
                {
                    "user_name": self.user_name,
                    "agent_id": target_agent,
                    "expected_revision": expected_revision,
                    "content": updated_content,
                },
            )
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
            }
        except ValueError as exc:
            return {"error": str(exc)}
        except Exception as exc:
            return {"error": f"Failed to edit brain: {exc}"}

    async def save_insight(self, content: str) -> Dict:
        return {"error": "save_insight is only available in community discussions."}

    async def spawn_specialist(
        self,
        name: str,
        persona: str,
        initial_directives: List[Dict] = None,
    ) -> Dict:
        return {"error": "spawn_specialist is only available in community discussions."}
