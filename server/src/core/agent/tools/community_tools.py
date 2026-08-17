import json
import uuid
from typing import Dict, List, Mapping

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.community_tools import AAC_DEFAULT_ENABLED_TOOLS
from common.schema.agent.identity import PersonaProfile
from common.utils.agent_identity import (
    build_brain_snapshot_summary,
    normalize_agent_brain,
)
from common.utils.events import emit_community
from core.agent.tools.registry import Tools
from core.community.community_store import CommunityStore

MAX_SPAWNED_SPECIALISTS = 10


class CommunityTools(Tools):
    """
    Restricted suite of tools for Autonomous Agent Community (AAC) agents.
    Inherits from core Tools for read access, but restricts write operations
    strictly to the community's isolated discussion space.
    """

    def __init__(
        self,
        user_name: str,
        base_tools: Tools,
        community_store: CommunityStore,
        discussion_id: str,
        agent_id: str,
        participants: List[str] = None,
    ):
        super().__init__(
            user_name=user_name,
            entities=base_tools.entities,
            session_id=base_tools.session_id,
            compiled_domain=getattr(base_tools, "compiled_domain", None),
            search_config=base_tools.search_cfg,
            document_service=getattr(base_tools, "document_service", None),
            workspace_service=getattr(base_tools, "workspace_service", None),
            document_focus=getattr(base_tools, "document_focus", None),
            knowledge_store=base_tools.knowledge_store,
            postgres=base_tools.postgres,
            redis=base_tools.redis,
            agent_id=agent_id,
        )
        self.readable_project_ids = list(base_tools.readable_project_ids)
        self.community_store = community_store
        self.discussion_id = discussion_id
        self.agent_id = agent_id
        self.current_participants = participants or []

    async def save_insight(self, content: str) -> Dict:
        """Saves a synthesized insight back to the community discussion stream."""
        await self.community_store.add_message(
            discussion_id=self.discussion_id,
            agent_id="system",
            content=f"INSIGHT: {content}",
            role="insight",
            user_name=self.user_name,
            project_id=self.project_id,
        )
        return {"saved": True, "type": "insight"}



    async def spawn_specialist(
        self,
        name: str,
        persona: Mapping[str, str],
        initial_directives: List[Dict] = None,
    ) -> Dict:
        """Create a Postgres-backed specialist with an immutable birth persona."""
        clean_name = (name or "").strip()
        if not clean_name:
            return {"error": "Specialist name is required."}
        if len(clean_name) > 100:
            return {"error": "Specialist name must be 100 characters or fewer."}

        try:
            persona_profile = PersonaProfile.from_value(persona)
            persona_markdown = persona_profile.to_markdown()
            directives = self._format_initial_directives(initial_directives or [])
        except ValueError as exc:
            return {"error": str(exc)}

        if await self._count_spawned_participants() >= MAX_SPAWNED_SPECIALISTS:
            return {
                "error": (
                    "Spawn limit reached. "
                    f"Max {MAX_SPAWNED_SPECIALISTS} sub-agents per discussion."
                )
            }

        agent_id = f"spawned_{uuid.uuid4().hex}"
        brain = normalize_agent_brain(directives, persona_markdown)
        model = ConfigManager.get().config.llm.agent_model

        try:
            async with self.postgres.transaction() as cursor:
                await cursor.execute(
                    """
            WITH new_agent AS (
                INSERT INTO public.agents (
                    agent_id, user_name, project_id, name, persona, brain,
                    model, temperature, enabled_tools, is_default, is_spawned,
                    spawned_by
                ) VALUES (
                    %(agent_id)s, %(user_name)s, %(project_id)s, %(name)s,
                    %(persona)s, %(brain)s, %(model)s, 0.7,
                    %(enabled_tools)s, false, true, %(spawned_by)s
                )
                RETURNING agent_id, user_name, brain_revision, brain
            )
            INSERT INTO public.agent_brain_snapshots (
                agent_id, revision, user_name, content, edited_by,
                change_type, change_summary
            )
            SELECT
                agent_id,
                brain_revision,
                user_name,
                brain,
                'aac_spawn',
                'specialist_spawn',
                %(change_summary)s
            FROM new_agent
            """,
                    {
                        "agent_id": agent_id,
                        "user_name": self.user_name,
                        "project_id": self.project_id,
                        "name": clean_name,
                        "persona": persona_markdown,
                        "brain": brain,
                        "model": model,
                        "enabled_tools": json.dumps(AAC_DEFAULT_ENABLED_TOOLS),
                        "spawned_by": self.agent_id,
                        "change_summary": build_brain_snapshot_summary(
                            "specialist_spawn"
                        ),
                    },
                )
                await self.community_store.register_agent_spawn(
                    parent_id=self.agent_id,
                    child_id=agent_id,
                    detail=persona_markdown,
                    user_name=self.user_name,
                    project_id=self.project_id,
                    cursor=cursor,
                )
        except Exception:
            logger.exception("AAC: Failed to create specialist and lineage")
            return {"error": "Specialist could not be created."}
        if agent_id not in self.current_participants:
            self.current_participants.append(agent_id)

        seeded_directives = len(initial_directives or [])
        await emit_community(
            self.user_name,
            "community",
            "agent_spawned",
            {
                "discussion_id": self.discussion_id,
                "agent_id": agent_id,
                "parent_id": self.agent_id,
                "seeded_directives": seeded_directives,
            },
        )
        return {
            "id": agent_id,
            "name": clean_name,
            "persona": persona_profile.to_dict(),
            "persona_markdown": persona_markdown,
            "seeded_directives": seeded_directives,
        }

    async def _count_spawned_participants(self) -> int:
        if not self.current_participants:
            return 0
        rows = await self.postgres.fetch_all(
            """
            SELECT count(*) AS count
            FROM public.agents
            WHERE user_name = %(user_name)s
              AND agent_id = ANY(%(agent_ids)s)
              AND is_spawned = true
            """,
            {
                "user_name": self.user_name,
                "agent_ids": self.current_participants,
            },
        )
        return int(rows[0]["count"]) if rows else 0

    @staticmethod
    def _format_initial_directives(directives: List[Dict]) -> str:
        if not directives:
            return ""

        labels = {
            "require": "Required",
            "prefer": "Preferred",
            "avoid": "Avoid",
        }
        grouped = {mode: [] for mode in labels}
        for directive in directives:
            mode = str(directive.get("mode", "")).strip().lower()
            content = str(directive.get("content", "")).strip()
            if mode not in labels:
                raise ValueError(
                    "Directive mode must be require, prefer, or avoid."
                )
            if not content:
                raise ValueError("Directive content cannot be empty.")
            grouped[mode].append(content)

        sections = []
        for mode, title in labels.items():
            if grouped[mode]:
                sections.append(
                    f"### {title}\n"
                    + "\n".join(f"- {item}" for item in grouped[mode])
                )
        return "\n\n".join(sections)
