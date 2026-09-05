"""AAC-local tools layered over the normal read-only tool composition."""

from __future__ import annotations

from typing import Awaitable, Callable, Dict, List, Mapping, Optional

from common.schema.agent.identity import AgentConfig, PersonaProfile
from common.utils.agent_identity import normalize_agent_brain
from core.agent.services.agent_manager import AgentManager
from core.agent.tools.registry import Tools
from core.community.aac_store import AACStore

SpecialistRunner = Callable[[AgentConfig, str], Awaitable[object]]


class AACTools(Tools):
    """Tool boundary for AAC-local writes and private specialist work.

    Canonical entity, relationship, document, and workspace mutation is not
    part of this boundary.  The normal read methods are inherited so retrieval
    policy remains shared with the rest of Knoggin.
    """

    def __init__(
        self,
        *,
        user_name: str,
        base_tools: Tools,
        store: AACStore,
        agent_manager: AgentManager,
        discussion_id: str,
        agent_id: str,
        specialist_runner: Optional[SpecialistRunner] = None,
    ) -> None:
        super().__init__(
            user_name=user_name,
            entities=base_tools.entities,
            session_id=base_tools.session_id,
            compiled_domain=None,
            search_config=base_tools.search_cfg,
            document_service=base_tools.document_service,
            document_focus=None,
            knowledge_retrieval=base_tools.knowledge_retrieval,
            knowledge_store=base_tools.knowledge_store,
            postgres=base_tools.postgres,
            agent_id=agent_id,
            health_service=None,
            entity_maintenance_service=base_tools.entity_maintenance_service,
        )
        self.aac_store = store
        self.agent_manager = agent_manager
        self.discussion_id = discussion_id
        self.agent_id = agent_id
        self._specialist_runner = specialist_runner

    async def save_insight(
        self,
        content: str,
        visibility: str = "shared",
    ) -> Dict[str, object]:
        insight_id = await self.aac_store.create_insight(
            user_name=self.user_name,
            discussion_id=self.discussion_id,
            author_agent_id=self.agent_id,
            content=content,
            visibility=visibility,
        )
        return {"saved": True, "insight_id": insight_id, "visibility": visibility}

    async def search_insights(
        self,
        query: str = "",
        limit: int = 20,
    ) -> List[Dict]:
        return await self.aac_store.search_insights(
            user_name=self.user_name,
            viewer_agent_id=self.agent_id,
            query=query,
            limit=limit,
        )

    async def vote_insight(
        self,
        insight_id: str,
        vote: str,
        reason: str,
    ) -> Dict[str, object]:
        await self.aac_store.cast_insight_vote(
            insight_id=insight_id,
            user_name=self.user_name,
            voter_agent_id=self.agent_id,
            vote=vote,
            reason=reason,
        )
        return {"voted": True, "insight_id": insight_id, "vote": vote}

    async def remove_insight_vote(self, insight_id: str) -> Dict[str, object]:
        removed = await self.aac_store.remove_insight_vote(
            insight_id=insight_id,
            user_name=self.user_name,
            voter_agent_id=self.agent_id,
        )
        return {"removed": removed, "insight_id": insight_id}

    async def spawn_specialist(
        self,
        name: str,
        persona: Mapping[str, str],
        initial_directives: Optional[List[Dict]] = None,
    ) -> Dict[str, object]:
        specialist = await self.agent_manager.create_specialist(
            parent_id=self.agent_id,
            name=name,
            persona=PersonaProfile.from_value(persona),
            brain=normalize_agent_brain(
                self._format_initial_directives(initial_directives or []),
                PersonaProfile.from_value(persona).to_markdown(),
            ),
        )
        return {
            "id": specialist.id,
            "name": specialist.name,
            "persona": specialist.persona.to_dict(),
            "spawned_by": specialist.spawned_by,
            "seeded_directives": len(initial_directives or []),
        }

    async def consult_specialist(
        self,
        specialist_id: str,
        question: str,
    ) -> Dict[str, object]:
        specialist = await self.agent_manager.get_agent(specialist_id)
        if specialist is None or specialist.spawned_by != self.agent_id:
            raise ValueError("Agent may only consult its own specialists")
        if self._specialist_runner is None:
            raise RuntimeError("Specialist consultation is not available")
        result = await self._specialist_runner(specialist, question)
        return {"specialist_id": specialist.id, "result": result}

    @staticmethod
    def _format_initial_directives(directives: List[Dict]) -> str:
        labels = {"require": "Required", "prefer": "Preferred", "avoid": "Avoid"}
        grouped = {mode: [] for mode in labels}
        for directive in directives:
            mode = str(directive.get("mode", "")).strip().lower()
            content = str(directive.get("content", "")).strip()
            if mode not in labels:
                raise ValueError("Directive mode must be require, prefer, or avoid.")
            if not content:
                raise ValueError("Directive content cannot be empty.")
            grouped[mode].append(content)
        return "\n\n".join(
            f"### {labels[mode]}\n" + "\n".join(f"- {item}" for item in grouped[mode])
            for mode in labels
            if grouped[mode]
        )
