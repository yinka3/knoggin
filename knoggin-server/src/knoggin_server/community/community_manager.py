import asyncio
import uuid
from types import SimpleNamespace
from typing import Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.aac_schema import AAC_READ_TOOL_NAMES, AAC_SPECIFIC_SCHEMAS
from common.schema.agent_contracts import AgentConfig
from common.utils.events import emit_community
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now
from infrastructure.redis_client import RedisKeys
from knoggin_server.agent.executor import AgentExecutor
from knoggin_server.agent.system_prompt import get_agent_prompt
from knoggin_server.agent.tools.community_tools import CommunityTools
from knoggin_server.agent.types import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)
from knoggin_server.knowledge.services.memory_service import MemoryManager
from knoggin_server.project.state import ProjectState
from knoggin_server.session.boot import SessionAssembler
from knoggin_server.session.context import Context

COMMUNITY_ENABLED_TOOLS = AAC_READ_TOOL_NAMES

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
        ("save_memory", 4),
        ("save_insight", 4),
        ("spawn_specialist", 2),
    ),
)


class CommunityManager:
    """Orchestrates autonomous agent discussions."""

    def __init__(self, project_state: ProjectState, user_name: str, resources):
        self.project_state = project_state
        self.user_name = user_name
        self.resources = resources
        self._active_discussion_id: Optional[str] = None
        self._discussion_task: Optional[asyncio.Task] = None

    async def _get_agent_directives(self, agent_id: str) -> str:
        """Fetch and format an agent's directives."""
        memory_mgr = MemoryManager(
            redis=self.resources.redis,
            user_name=self.user_name,
            session_id="community_system",
            agent_id=agent_id,
            topic_config=self.project_state.topic_config,
        )

        return await memory_mgr.load_directive_string()

    async def _is_discussion_active(self) -> bool:
        return bool(await self.resources.redis.get(self._active_discussion_key()))

    def _active_discussion_key(self) -> str:
        return (
            f"{RedisKeys.community_discussion_active()}:"
            f"{self.user_name}:{self.project_state.project_id}"
        )

    async def _agent_exists(self, agent_id: str) -> bool:
        return bool(
            await self.resources.redis.hget(RedisKeys.agents(self.user_name), agent_id)
        )

    async def _get_default_agent_id(self) -> str:
        """Get the default agent ID for fallback."""
        default_id = await self.resources.redis.get(
            RedisKeys.agents_default(self.user_name)
        )
        return default_id or "default_stella"

    async def _get_agent_config(self, agent_id: str) -> Optional[AgentConfig]:
        raw = await self.resources.redis.hget(
            RedisKeys.agents(self.user_name), agent_id
        )
        if raw:
            data = safe_json_loads(raw)
            if data and isinstance(data, dict):
                return AgentConfig.from_dict(data)

        default_id = await self.resources.redis.get(
            RedisKeys.agents_default(self.user_name)
        )
        if default_id and default_id != agent_id:
            return await self._get_agent_config(default_id)

        logger.warning(f"AAC: Agent '{agent_id}' not found, using ephemeral default")
        llm_config = ConfigManager.get().config.llm
        return AgentConfig(
            id=agent_id,
            name="STELLA",
            persona="Default AAC Facilitator. Warm and observant.",
            model=llm_config.agent_model,
        )

    async def trigger_discussion(self) -> None:
        """Main entry point called by scheduler."""
        if await self._is_discussion_active():
            logger.info("AAC: Discussion already in progress, skipping.")
            return

        seed_data = await self._seed_discussion()
        if not seed_data:
            return

        raw_agent_ids = seed_data.get("agent_ids", [])
        valid_agent_ids = []
        for aid in raw_agent_ids:
            if aid == "default_stella" or await self._agent_exists(aid):
                valid_agent_ids.append(aid)
            else:
                logger.warning(f"AAC: Seeded agent_id '{aid}' not found, skipping")

        if not valid_agent_ids:
            logger.warning("AAC: No valid agents after validation, using default")
            valid_agent_ids = [await self._get_default_agent_id()]

        discussion_id = str(uuid.uuid4())
        self._active_discussion_id = discussion_id

        topic = seed_data["topic"]
        await self.resources.redis.set(
            self._active_discussion_key(), discussion_id
        )
        await self.resources.graph_client.community.create_discussion(
            discussion_id, topic, valid_agent_ids
        )

        await emit_community(
            self.user_name,
            "community",
            "discussion_started",
            {"id": discussion_id, "topic": topic, "agents": valid_agent_ids},
        )

        async def _run_and_cleanup():
            try:
                await self._run_loop(discussion_id, topic, valid_agent_ids)
            except Exception as e:
                logger.error(f"AAC discussion {discussion_id} error: {e}")
            finally:
                await self.resources.redis.delete(
                    self._active_discussion_key()
                )
                self._active_discussion_id = None
                try:
                    await self.resources.graph_client.community.close_discussion(
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

        self._discussion_task = asyncio.create_task(_run_and_cleanup())

    async def _run_loop(
        self, discussion_id: str, topic: str, initial_agent_ids: List[str]
    ) -> None:
        assembler = SessionAssembler(self.user_name, self.resources)
        ctx = await assembler.assemble(
            project_state=self.project_state,
            session_id=f"aac_{discussion_id}",
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

            await self.resources.graph_client.community.add_message(
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
        ctx: Context,
    ) -> Optional[str]:
        """Runs a single agent turn using the core AgentExecutor."""

        agent_state = AgentState()
        evidence = RetrievedEvidence()

        agent_directives = await self._get_agent_directives(agent.id)

        comm_memory = MemoryManager(
            redis=self.resources.redis,
            user_name=self.user_name,
            session_id=ctx.session_id,
            agent_id=agent.id,
            topic_config=ctx.project.topic_config,
        )

        base_tools = SimpleNamespace(
            entities=ctx.project.entities,
            session_id=ctx.session_id,
            topic_config=ctx.project.topic_config,
            search_cfg={},
            file_rag=ctx.file_rag,
            graph_client=self.resources.graph_client,
            redis=self.resources.redis,
        )

        comm_tools = CommunityTools(
            self.user_name,
            base_tools,
            self.resources.graph_client.community,
            discussion_id,
            agent.id,
            comm_memory,
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
            agent_persona=agent.persona,
            history=history,
            is_community=True,
            current_participants=participants,
        )

        executor = AgentExecutor(
            ctx=agent_ctx,
            llm=self.resources.llm_service,
            tools=comm_tools,
            memory_mgr=comm_memory,
        )

        full_response: str = ""
        enabled_tools, client_tools = self._resolve_agent_tools(agent)

        try:
            async for event in executor.execute(
                model=agent.model,
                agent_temperature=agent.temperature,
                agent_instructions=agent.instructions,
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

        comm_mem_key = RedisKeys.community_agent_memory(
            self.user_name, seeding_agent.id
        )
        raw_mem = await self.resources.redis.hgetall(comm_mem_key)
        mem_entries = []
        if raw_mem:
            for v in raw_mem.values():
                parsed = safe_json_loads(v)
                if parsed and isinstance(parsed, dict):
                    content = parsed.get("content", "")
                    if content:
                        mem_entries.append(content)
        agent_memory_context = "\n".join(mem_entries)

        base_prompt = get_agent_prompt(
            user_name=self.user_name,
            current_time=get_now().strftime("%Y-%m-%d %H:%M UTC"),
            persona=seeding_agent.persona,
            agent_name=seeding_agent.name,
            memory_context=agent_memory_context,
            agent_directives=directives_str,
            instructions=seeding_agent.instructions,
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

        response = await self.resources.llm_service.call_llm(
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
            stats = await self.resources.graph_client.get_graph_stats()
            notable = await self.resources.graph_client.get_notable_entities(8)
            recent_entities = (
                await self.resources.graph_client.get_recently_active_entities(7, 5)
            )
            recent_facts = await self.resources.graph_client.get_recent_facts(7, 10)
            past_discussions = (
                await self.resources.graph_client.community.get_recent_discussions(5)
            )
            insights = (
                await self.resources.graph_client.community.get_discussion_insights(5)
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
        raw_agents = await self.resources.redis.hgetall(
            RedisKeys.agents(self.user_name)
        )

        if not raw_agents:
            return ["default_stella"], "- STELLA (default): General purpose assistant."

        pool_ids = (
            ConfigManager.get().config.developer_settings.community.agent_pool_ids
        )
        if pool_ids:
            raw_agents = {
                aid: raw
                for aid, raw in raw_agents.items()
                if aid in pool_ids
            }

        if not raw_agents:
            return ["default_stella"], "- STELLA (default): General purpose assistant."

        agent_ids = list(raw_agents.keys())
        descriptions = []

        for aid, raw in raw_agents.items():
            data = safe_json_loads(raw)
            if data and isinstance(data, dict):
                try:
                    name = data.get("name", "Unknown")
                    persona = data.get("persona", "")[:120]
                    is_spawned = data.get("is_spawned", False)

                    spawned_tag = " [spawned]" if is_spawned else ""
                    descriptions.append(f"- {name}{spawned_tag} (id: {aid}): {persona}")
                except Exception:
                    descriptions.append(f"- Unknown (id: {aid})")
            else:
                descriptions.append(f"- Unknown (id: {aid})")

        return agent_ids, "\n".join(descriptions)
