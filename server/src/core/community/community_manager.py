import asyncio
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.aac_schema import AAC_READ_TOOL_NAMES, AAC_SPECIFIC_SCHEMAS
from common.schema.agent_contracts import AgentConfig
from common.scoping import IDENTITY_SCOPE
from common.utils.events import emit_community
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now
from core.agent.executor import AgentExecutor
from core.agent.services.agent_manager import AgentManager
from core.agent.system_prompt import get_agent_prompt
from core.agent.tools.community_tools import CommunityTools
from core.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)
from core.project.state import ProjectState
from infrastructure.redis_client import RedisKeys

COMMUNITY_ENABLED_TOOLS = AAC_READ_TOOL_NAMES
ACTIVE_DISCUSSION_TTL_SECONDS = 2 * 60 * 60

COMMUNITY_RUN_CONFIG = AgentRunConfig(
    max_calls=5,
    max_attempts=6,
    max_history_turns=4,
    max_accumulated_messages=10,
    max_consecutive_errors=2,
    tool_limits=(
        ("search_entity", 4),
        ("fact_check", 6),
        ("get_connections", 3),
        ("search_messages", 3),
        ("search_documents", 4),
        ("read_document", 4),
        ("list_documents", 2),
        ("read_brain", 4),
        ("edit_brain", 2),
        ("save_insight", 4),
        ("spawn_specialist", 2),
    ),
)


@dataclass(frozen=True)
class CommunityExecutionContext:
    """Minimal project context for AAC agent turns without a session runtime."""

    session_id: str
    project: ProjectState
    document_service: Optional[object]


class CommunityManager:
    """Orchestrates autonomous agent discussions."""

    _RELEASE_ACTIVE_DISCUSSION_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """

    def __init__(self, project_state: ProjectState, user_name: str, resources):
        self.project_state = project_state
        self.user_name = user_name
        self.resources = resources
        self._active_discussion_id: Optional[str] = None
        self._discussion_task: Optional[asyncio.Task] = None

    async def _get_agent_directives(self, agent_id: str) -> str:
        """Legacy directives are now part of the durable Markdown brain."""
        return ""

    async def _is_discussion_active(self) -> bool:
        return bool(await self.resources.redis.get(self._active_discussion_key()))

    def _active_discussion_key(self) -> str:
        return RedisKeys.community_discussion_active(
            self.user_name,
            self.project_state.project_id,
        )

    async def _resolve_project_scope(self) -> List[str]:
        rows = await self.resources.postgres.fetch_all(
            """
            SELECT project_id
            FROM public.projects
            WHERE user_name = %(user_name)s
              AND status IN ('active', 'archived')
            ORDER BY project_id
            """,
            {"user_name": self.user_name},
        )
        readable = {str(row["project_id"]) for row in rows}

        configured = (
            ConfigManager.get().config.developer_settings.community.project_ids
        )
        if configured:
            projects = [
                project_id
                for project_id in dict.fromkeys(configured)
                if project_id in readable
            ]
        else:
            projects = sorted(readable)
        return [IDENTITY_SCOPE, *projects] if projects else []

    async def _agent_exists(self, agent_id: str) -> bool:
        manager = AgentManager(self.resources, self.user_name, {})
        return await manager.get_agent(agent_id) is not None

    async def _get_default_agent_id(self) -> str:
        """Get the default agent ID for fallback."""
        manager = AgentManager(self.resources, self.user_name, {})
        return await manager.get_default_agent_id()

    async def _get_agent_config(self, agent_id: str) -> Optional[AgentConfig]:
        manager = AgentManager(self.resources, self.user_name, {})
        return await manager.get_agent(agent_id)

    def _track_discussion_task(self, task: asyncio.Task) -> None:
        self._discussion_task = task
        if hasattr(self.project_state, "track_community_task"):
            self.project_state.track_community_task(task)

    async def _release_active_discussion(self, discussion_id: str) -> None:
        await self.resources.redis.eval(
            self._RELEASE_ACTIVE_DISCUSSION_SCRIPT,
            1,
            self._active_discussion_key(),
            discussion_id,
        )

    async def trigger_discussion(self) -> None:
        """Main entry point called by scheduler."""
        if await self._is_discussion_active():
            logger.info("AAC: Discussion already in progress, skipping.")
            return

        discussion_id = str(uuid.uuid4())
        claimed = await self.resources.redis.set(
            self._active_discussion_key(),
            discussion_id,
            ex=ACTIVE_DISCUSSION_TTL_SECONDS,
            nx=True,
        )
        if not claimed:
            logger.info("AAC: Discussion claimed by another runtime, skipping.")
            return
        self._active_discussion_id = discussion_id

        try:
            seed_data = await self._seed_discussion()
            if not seed_data:
                await self._release_active_discussion(discussion_id)
                self._active_discussion_id = None
                return

            raw_agent_ids = seed_data.get("agent_ids", [])
            valid_agent_ids = []
            for aid in raw_agent_ids:
                if aid == "default_stella" or await self._agent_exists(aid):
                    valid_agent_ids.append(aid)
                else:
                    logger.warning(
                        f"AAC: Seeded agent_id '{aid}' not found, skipping"
                    )

            if not valid_agent_ids:
                logger.warning("AAC: No valid agents after validation, using default")
                valid_agent_ids = [await self._get_default_agent_id()]

            topic = seed_data["topic"]
            await self.resources.knowledge_store.community.create_discussion(
                discussion_id, topic, valid_agent_ids
            )
        except BaseException:
            await self._release_active_discussion(discussion_id)
            self._active_discussion_id = None
            raise

        await emit_community(
            self.user_name,
            "community",
            "discussion_started",
            {"id": discussion_id, "topic": topic, "agents": valid_agent_ids},
        )

        async def _run_and_cleanup():
            try:
                await self._run_loop(discussion_id, topic, valid_agent_ids)
            except asyncio.CancelledError:
                logger.info(f"AAC discussion {discussion_id} cancelled")
                raise
            except Exception as e:
                logger.error(f"AAC discussion {discussion_id} error: {e}")
            finally:
                try:
                    await self._release_active_discussion(discussion_id)
                except Exception as e:
                    logger.warning(
                        "AAC: Failed to release active discussion marker "
                        f"for {discussion_id}: {e}"
                    )
                self._active_discussion_id = None
                try:
                    await self.resources.knowledge_store.community.close_discussion(
                        discussion_id
                    )
                except Exception as e:
                    logger.error(
                        f"AAC: Failed to close discussion {discussion_id} in DB: {e}"
                    )

                await emit_community(
                    self.user_name,
                    "community",
                    "discussion_ended",
                    {"id": discussion_id},
                )

        task = asyncio.create_task(
            _run_and_cleanup(),
            name=f"aac:{self.user_name}:{self.project_state.project_id}",
        )
        self._track_discussion_task(task)

    async def _run_loop(
        self, discussion_id: str, topic: str, initial_agent_ids: List[str]
    ) -> None:
        ctx = CommunityExecutionContext(
            session_id=f"aac_{discussion_id}",
            project=self.project_state,
            document_service=getattr(self.project_state, "document_service", None),
        )

        config = ConfigManager.get().config
        comm_cfg = config.developer_settings.community
        max_turns = comm_cfg.max_turns

        participants = list(initial_agent_ids)
        history = []

        for turn in range(max_turns):
            active_id = await self.resources.redis.get(
                self._active_discussion_key()
            )
            if not active_id or active_id != discussion_id:
                logger.info(
                    f"AAC [{discussion_id}]: Discussion manually closed or "
                    "superseded. Aborting loop."
                )
                break

            agent_id = participants[turn % len(participants)]
            agent_config = await self._get_agent_config(agent_id)
            if not agent_config:
                continue

            logger.info(
                f"AAC [{discussion_id}]: Turn {turn}, Agent {agent_config.name}"
            )

            try:
                message = await asyncio.wait_for(
                    self._agent_turn(
                        discussion_id, agent_config, topic, history, participants, ctx
                    ),
                    timeout=1200.0,
                )
            except asyncio.TimeoutError:
                logger.warning(
                    f"AAC [{discussion_id}]: Turn {turn} timed out after "
                    "20 minutes. Closing discussion."
                )
                break

            if not message:
                break

            history.append(
                {
                    "role": "assistant",
                    "agent_id": agent_id,
                    "content": message,
                    "name": agent_config.name,
                }
            )

            await self.resources.knowledge_store.community.add_message(
                discussion_id, agent_id, message, "assistant"
            )

            await emit_community(
                self.user_name,
                "community",
                "message_added",
                {
                    "discussion_id": discussion_id,
                    "agent_id": agent_id,
                    "agent_name": agent_config.name,
                    "content": message,
                },
            )

            if "[[END_DISCUSSION]]" in message:
                break

    async def _agent_turn(
        self,
        discussion_id: str,
        agent: AgentConfig,
        topic: str,
        history: List[Dict],
        participants: List[str],
        ctx: CommunityExecutionContext,
    ) -> Optional[str]:
        """Runs a single agent turn using the core AgentExecutor."""

        agent_state = AgentState()
        evidence = RetrievedEvidence()

        agent_directives = await self._get_agent_directives(agent.id)

        readable_project_ids = await self._resolve_project_scope()
        base_tools = SimpleNamespace(
            entities=ctx.project.entities,
            session_id=ctx.session_id,
            topic_config=ctx.project.topic_config,
            search_cfg={},
            knowledge_store=self.resources.knowledge_store,
            document_service=ctx.document_service,
            document_focus=None,
            postgres=self.resources.postgres,
            redis=self.resources.redis,
            readable_project_ids=readable_project_ids,
        )

        comm_tools = CommunityTools(
            self.user_name,
            base_tools,
            self.resources.knowledge_store.community,
            discussion_id,
            agent.id,
            participants,
        )

        agent_ctx = AgentContext(
            config=COMMUNITY_RUN_CONFIG,
            state=agent_state,
            evidence=evidence,
            user_name=self.user_name,
            user_query=f"Community Discussion Topic: {topic}",
            session_id=ctx.session_id,
            run_id=f"run_{uuid.uuid4().hex[:8]}",
            agent_id=agent.id,
            agent_name=agent.name,
            agent_persona=agent.persona_markdown,
            history=history,
            is_community=True,
            current_participants=participants,
        )

        executor = AgentExecutor(
            ctx=agent_ctx,
            llm=self.resources.llm_service,
            tools=comm_tools,
        )

        full_response: str = ""
        enabled_tools, client_tools = self._resolve_agent_tools(agent)

        try:
            async for event in executor.execute(
                model=agent.model,
                agent_temperature=agent.temperature,
                agent_brain=agent.brain,
                agent_directives=agent_directives,
                enabled_tools=enabled_tools,
                client_tools=client_tools,
            ):
                e_type = event.get("event")
                data = event.get("data", {})

                if e_type == "token":
                    full_response += data.get("content", "")
                elif e_type == "response":
                    full_response = data.get("content", "").strip()
                    break
                elif e_type == "clarification":
                    full_response = data.get("question", "").strip()
                    break
                elif e_type == "thinking":
                    reasoning = data.get("content", "")
                    if reasoning:
                        await emit_community(
                            self.user_name,
                            "community",
                            "agent_reasoning",
                            {
                                "discussion_id": discussion_id,
                                "agent_id": agent.id,
                                "reasoning": reasoning,
                            },
                        )
        finally:
            await comm_tools.close()

        return full_response.strip() if full_response else None

    def _resolve_agent_tools(self, agent: AgentConfig) -> tuple[List[str], List[Dict]]:
        if not agent.enabled_tools:
            return COMMUNITY_ENABLED_TOOLS, AAC_SPECIFIC_SCHEMAS

        allowed = set(agent.enabled_tools)
        enabled_tools = [name for name in COMMUNITY_ENABLED_TOOLS if name in allowed]
        client_tools = [
            schema
            for schema in AAC_SPECIFIC_SCHEMAS
            if schema["function"]["name"] in allowed
        ]
        return enabled_tools, client_tools

    async def _seed_discussion(self) -> Optional[Dict]:
        """Use seeding agent to analyze graph and initiate a discussion."""
        config = ConfigManager.get().config
        comm_cfg = config.developer_settings.community
        seeding_agent_id = comm_cfg.seeding_agent_id

        seeding_agent = None
        if seeding_agent_id:
            seeding_agent = await self._get_agent_config(seeding_agent_id)

        if not seeding_agent:
            default_id = await self._get_default_agent_id()
            seeding_agent = await self._get_agent_config(default_id)

        if not seeding_agent:
            logger.error("AAC: No seeding agent available")
            return None

        directives_str = await self._get_agent_directives(seeding_agent.id)

        base_prompt = get_agent_prompt(
            user_name=self.user_name,
            current_time=get_now().strftime("%Y-%m-%d %H:%M UTC"),
            persona=seeding_agent.persona_markdown,
            agent_name=seeding_agent.name,
            agent_directives=directives_str,
            agent_brain=seeding_agent.brain,
            documents_context="",
            is_community=False,
            current_mode="Architect",
        )

        seeding_instructions = """
    <seeding_role>
    You are the SEEDING AGENT for an autonomous community discussion.

    Your job is to analyze the knowledge graph context below and initiate a
    meaningful discussion.

    You must decide:
    1. TOPIC: What specific subject should agents discuss? Be concrete, not vague.
    2. OBJECTIVE: What should they achieve? (e.g., resolve a contradiction,
       explore a connection, brainstorm applications, debate a decision)
    3. DISCUSSION_TYPE: "brainstorm" | "debate" | "investigation" | "synthesis"
    4. REASONING: Why this topic now? What makes it valuable?
    5. AGENT_IDS: Which agents should participate? Pick 2-4 agents whose
       personas are relevant. You may include yourself.

    Guidelines:
    - Prioritize topics with recent activity or unresolved questions
    - Avoid repeating recent discussion topics
    - Match agents to topics based on their personas
    - Prefer depth over breadth — focused discussions are better
    - You now have access to project documents via search_documents. Consider initiating discussions around analyzing uploaded files!
    </seeding_role>
    """

        system_prompt = base_prompt + seeding_instructions

        graph_context = await self._build_seeding_context()
        agent_ids, agent_descriptions = await self._build_agent_pool_context()

        user_prompt = f"""
    {graph_context}

    === AVAILABLE AGENTS ===
    {agent_descriptions}

    === YOUR TASK ===
    Based on the above context, decide what discussion to initiate.

    Respond with ONLY valid JSON (double quotes, no trailing commas):
    {{
        "topic": "specific discussion topic",
        "objective": "what the discussion should achieve",
        "discussion_type": "brainstorm|debate|investigation|synthesis",
        "reasoning": "why this topic is valuable right now",
        "agent_ids": ["id1", "id2"]
    }}
    """

        await emit_community(
            self.user_name,
            "community",
            "seeding_started",
            {
                "seeding_agent_id": seeding_agent.id,
                "seeding_agent_name": seeding_agent.name,
            },
        )

        response = await self.resources.llm_service.generate_text(
            system_prompt,
            user_prompt,
            model=seeding_agent.model,
            temperature=seeding_agent.temperature,
        )

        try:
            clean = response.strip() if response else ""
            if clean.startswith("```"):
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
            clean = clean.strip()

            data = safe_json_loads(clean)
            if not data or not isinstance(data, dict):
                logger.warning(
                    "AAC: Failed to parse seeding response as valid JSON dict"
                )
                logger.debug(f"Raw response: {response}")
                raise ValueError("Seeding response not valid JSON dict")

            required_keys = ["topic", "agent_ids"]
            if not all(k in data for k in required_keys):
                raise ValueError(f"Missing required keys. Got: {list(data.keys())}")

            valid_agent_ids = []
            for aid in data["agent_ids"]:
                if aid in agent_ids or aid == seeding_agent.id:
                    valid_agent_ids.append(aid)
                else:
                    logger.warning(f"AAC: Seeded agent_id '{aid}' not found, skipping")

            if not valid_agent_ids:
                logger.warning("AAC: No valid agents selected, using seeding agent")
                valid_agent_ids = [seeding_agent.id]

            data["agent_ids"] = valid_agent_ids

            await emit_community(
                self.user_name,
                "community",
                "discussion_seeded",
                {
                    "seeding_agent": seeding_agent.name,
                    "topic": data.get("topic"),
                    "objective": data.get("objective"),
                    "discussion_type": data.get("discussion_type"),
                    "reasoning": data.get("reasoning"),
                    "agent_ids": data["agent_ids"],
                },
            )

            return data

        except Exception as e:
            logger.warning(f"AAC: Seeding failed: {e}")

        return {
            "topic": "Knowledge graph exploration and insight discovery",
            "objective": (
                "Find interesting patterns or connections in the user's knowledge"
            ),
            "discussion_type": "brainstorm",
            "reasoning": "Fallback due to seeding failure",
            "agent_ids": [seeding_agent.id],
        }

    async def _build_seeding_context(self) -> str:
        """Gather rich context for seeding agent decision-making."""
        lines = []

        try:
            visible_project_ids = await self._resolve_project_scope()
            if not visible_project_ids:
                return "No community project scope is configured."
            stats = await self.resources.knowledge_store.get_graph_stats(
                visible_project_ids=visible_project_ids
            )
            notable = await self.resources.knowledge_store.get_notable_entities(
                visible_project_ids=visible_project_ids,
                limit=8,
            )
            recent_entities = (
                await self.resources.knowledge_store.get_recently_active_entities(
                    visible_project_ids=visible_project_ids,
                    days=7,
                    limit=5,
                )
            )
            recent_facts = await self.resources.knowledge_store.get_recent_facts(
                visible_project_ids=visible_project_ids,
                days=7,
                limit=10,
            )
            past_discussions = (
                await self.resources.knowledge_store.community.get_recent_discussions(5)
            )
            insights = (
                await self.resources.knowledge_store.community.get_discussion_insights(5)
            )
        except Exception as e:
            logger.warning(f"Failed to gather seeding context: {e}")
            return "Knowledge graph is available for exploration."

        if isinstance(stats, dict):
            lines.append("=== GRAPH OVERVIEW ===")
            lines.append(f"Entities: {stats.get('entities', 0)}")
            lines.append(f"Facts: {stats.get('facts', 0)}")
            lines.append(f"Relationships: {stats.get('relationships', 0)}")
            lines.append("")
        elif isinstance(stats, Exception):
            logger.warning(f"Failed to get graph stats: {stats}")

        if not isinstance(notable, Exception) and notable:
            lines.append("=== NOTABLE ENTITIES ===")
            for ent in notable:
                lines.append(
                    f"- {ent['name']} ({ent['type']}, {ent['topic']}): "
                    f"{ent['connection_count']} connections, {ent['fact_count']} facts"
                )
            lines.append("")
        elif isinstance(notable, Exception):
            logger.warning(f"Failed to get notable entities: {notable}")

        if not isinstance(recent_entities, Exception) and recent_entities:
            lines.append("=== RECENTLY ACTIVE (last 7 days) ===")
            for ent in recent_entities:
                lines.append(
                    f"- {ent['name']} ({ent['type']}): {ent['recent_facts']} new facts"
                )
            lines.append("")
        elif isinstance(recent_entities, Exception):
            logger.warning(f"Failed to get recent entities: {recent_entities}")

        if not isinstance(recent_facts, Exception) and recent_facts:
            lines.append("=== RECENT FACTS ===")
            for fact in recent_facts:
                content = (
                    fact["content"][:100] + "..."
                    if len(fact["content"]) > 100
                    else fact["content"]
                )
                lines.append(f"- [{fact['entity_name']}] {content}")
            lines.append("")
        elif isinstance(recent_facts, Exception):
            logger.warning(f"Failed to get recent facts: {recent_facts}")

        if not isinstance(past_discussions, Exception) and past_discussions:
            lines.append("=== PREVIOUS DISCUSSIONS ===")
            for disc in past_discussions:
                status = disc.get("status", "unknown")
                topic = disc.get("topic", "Unknown topic")[:80]
                msg_count = disc.get("message_count", 0)
                lines.append(f'- "{topic}" ({status}, {msg_count} messages)')
            lines.append("")
        elif isinstance(past_discussions, Exception):
            logger.warning(f"Failed to get past discussions: {past_discussions}")

        if not isinstance(insights, Exception) and insights:
            lines.append("=== INSIGHTS FROM PAST DISCUSSIONS ===")
            for ins in insights:
                content = ins["content"].replace("INSIGHT: ", "")[:100]
                lines.append(f"- {content}")
            lines.append("")
        elif isinstance(insights, Exception):
            logger.warning(f"Failed to get insights: {insights}")

        return (
            "\n".join(lines)
            if lines
            else "Knowledge graph is available for exploration."
        )

    async def _build_agent_pool_context(self) -> tuple[List[str], str]:
        """Build descriptive agent pool. Returns (agent_ids, formatted_description)."""
        manager = AgentManager(self.resources, self.user_name, {})
        agents = await manager.list_agents()
        pool_ids = set(
            ConfigManager.get().config.developer_settings.community.agent_pool_ids
            or []
        )
        if pool_ids:
            agents = [agent for agent in agents if agent.id in pool_ids]
        if not agents:
            default_id = await manager.get_default_agent_id()
            default_agent = await manager.get_agent(default_id)
            agents = [default_agent] if default_agent else []

        agent_ids = [agent.id for agent in agents]
        descriptions = []
        for agent in agents:
            spawned_tag = " [spawned]" if agent.is_spawned else ""
            persona_preview = " ".join(agent.persona_markdown.split())[:160]
            descriptions.append(
                f"- {agent.name}{spawned_tag} (id: {agent.id}): {persona_preview}"
            )
        return agent_ids, "\n".join(descriptions)
