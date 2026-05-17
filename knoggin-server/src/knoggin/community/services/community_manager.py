import asyncio
import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

from common.conf.base import get_config
from common.schema.dtypes import AgentConfig
from common.utils.events import emit_community
from infrastructure.redis_client import RedisKeys
from infrastructure.redis.resources import ResourceManager
from knoggin.agent.executor import AgentExecutor
from knoggin.agent.internals import (
    AgentContext,
    AgentRunConfig,
    AgentState,
    RetrievedEvidence,
)
from knoggin.agent.tools.community_tools import CommunityTools
from knoggin.agent.tools.registry import Tools
from knoggin.session.boot import SessionAssembler
from knoggin.session.context import Context
from common.conf.topics_config import TopicConfig
from knoggin.knowledge.services.memory_service import MemoryManager


class CommunityManager:
    """Orchestrates autonomous agent discussions."""

    def __init__(self, resources: ResourceManager, user_name: str):
        self.resources = resources
        self.user_name = user_name
        self._active_discussion_id: Optional[str] = None
        self._discussion_task: Optional[asyncio.Task] = None

    async def _get_agent_working_memory(self, agent_id: str) -> Dict[str, List[str]]:
        """Fetch and safely parse an agent's working memory (rules, preferences, icks)."""
        memory_mgr = MemoryManager(
            redis=self.resources.redis,
            user_name=self.user_name,
            session_id="community_system",
            agent_id=agent_id,
            topic_config=TopicConfig(TopicConfig.DEFAULT_CONFIG),
        )

        result = await memory_mgr.list_working_memory()
        
        # Format for community loop (List[str] of content)
        return {
            cat: [e.content for e in entries]
            for cat, entries in result.blocks.items()
        }

    async def _is_discussion_active(self) -> bool:
        return await self.resources.redis.exists(
            RedisKeys.community_discussion_active()
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
            data = json.loads(raw)
            return AgentConfig.from_dict(data)

        default_id = await self.resources.redis.get(
            RedisKeys.agents_default(self.user_name)
        )
        if default_id and default_id != agent_id:
            return await self._get_agent_config(default_id)

        logger.warning(f"AAC: Agent '{agent_id}' not found, using ephemeral default")
        llm_config = get_config().llm
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
            if aid == "default_stella" or await self.resources.redis.hexists(
                RedisKeys.agents(self.user_name), aid
            ):
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
            RedisKeys.community_discussion_active(), discussion_id
        )
        await self.resources.memgraph.community.create_discussion(
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
                    RedisKeys.community_discussion_active()
                )
                self._active_discussion_id = None
                try:
                    await self.resources.memgraph.community.close_discussion(
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
        # We use a system-level topic config for community discussions
        ctx = await assembler.assemble(session_id=f"aac_{discussion_id}")

        config = get_config()
        comm_cfg = config.developer_settings.community
        max_turns = comm_cfg.max_turns

        participants = list(initial_agent_ids)
        history = []

        for turn in range(max_turns):
            active_id = await self.resources.redis.get(
                RedisKeys.community_discussion_active()
            )
            if not active_id or active_id != discussion_id:
                logger.info(
                    f"AAC [{discussion_id}]: Discussion manually closed or superseded. Aborting loop."
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
                    f"AAC [{discussion_id}]: Turn {turn} timed out after 20 minutes. Closing discussion."
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

            await self.resources.memgraph.community.add_message(
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

        working_memory = await self._get_agent_working_memory(agent.id)
        agent_rules = working_memory["rules"] or None
        agent_preferences = working_memory["preferences"] or None
        agent_icks = working_memory["icks"] or None

        # Build restricted community tools
        base_tools = Tools(
            user_name=self.user_name,
            memgraph=self.resources.memgraph,
            entities=ctx.entities,
            redis_client=self.resources.redis,
            session_id=ctx.session_id,
            topic_config=ctx.topic_config,
            search_config={},
            file_rag=ctx.file_rag,
            mcp_manager=self.resources.mcp_manager,
            memory=None,
        )

        comm_tools = CommunityTools(
            self.user_name,
            base_tools,
            self.resources.memgraph.community,
            discussion_id,
            agent.id,
            None,
            participants,
        )

        agent_ctx = AgentContext(
            config=AgentRunConfig(),
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
            memory_mgr=None,
        )

        from common.schema.aac_schema import AAC_SPECIFIC_SCHEMAS

        community_enabled_tools = [
            "search_entity",
            "get_connections",
            "search_messages",
            "get_recent_activity",
            "find_path",
            "get_hierarchy",
            "fact_check",
            "web_search",
            "news_search",
        ]

        full_response: str = ""

        async for event in executor.execute(
            agent_rules=agent_rules,
            agent_preferences=agent_preferences,
            agent_icks=agent_icks,
            enabled_tools=community_enabled_tools,
            client_tools=AAC_SPECIFIC_SCHEMAS,
        ):
            e_type = event.get("event")
            data = event.get("data", {})

            if e_type == "token":
                full_response += data.get("content", "")
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
            elif e_type == "tool_end":
                if data.get("tool") == "spawn_specialist":
                    res = data.get("result", "")
                    if "ID:" in res:
                        new_id = res.split("ID:")[1].split()[0]
                        if new_id not in participants:
                            participants.append(new_id)

        return full_response.strip() if full_response else None

    async def _seed_discussion(self) -> Optional[Dict]:
        """Use seeding agent to analyze graph and initiate a discussion."""
        from agent.system_prompt import get_agent_prompt

        config = get_config()
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

        working_memory = await self._get_agent_working_memory(seeding_agent.id)
        rules_str = "\n".join(working_memory["rules"])
        prefs_str = "\n".join(working_memory["preferences"])
        icks_str = "\n".join(working_memory["icks"])

        comm_mem_key = RedisKeys.community_agent_memory(
            self.user_name, seeding_agent.id
        )
        raw_mem = await self.resources.redis.hgetall(comm_mem_key)
        mem_entries = []
        if raw_mem:
            for v in raw_mem.values():
                try:
                    parsed = json.loads(v)
                    content = parsed.get("content", "")
                    if content:
                        mem_entries.append(content)
                except json.JSONDecodeError:
                    continue
        agent_memory_context = "\n".join(mem_entries)

        base_prompt = get_agent_prompt(
            user_name=self.user_name,
            current_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            persona=seeding_agent.persona,
            agent_name=seeding_agent.name,
            memory_context=agent_memory_context,
            agent_rules=rules_str,
            agent_preferences=prefs_str,
            agent_icks=icks_str,
            instructions=seeding_agent.instructions,
        )

        seeding_instructions = """
    <seeding_role>
    You are the SEEDING AGENT for an autonomous community discussion.

    Your job is to analyze the knowledge graph context below and initiate a meaningful discussion.

    You must decide:
    1. TOPIC: What specific subject should agents discuss? Be concrete, not vague.
    2. OBJECTIVE: What should they achieve? (e.g., resolve a contradiction, explore a connection, brainstorm applications, debate a decision)
    3. DISCUSSION_TYPE: "brainstorm" | "debate" | "investigation" | "synthesis"
    4. REASONING: Why this topic now? What makes it valuable?
    5. AGENT_IDS: Which agents should participate? Pick 2-4 agents whose personas are relevant. You may include yourself.

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

        response = await self.resources.llm_service.call_llm(system_prompt, user_prompt)

        try:
            clean = response.strip() if response else ""
            if clean.startswith("```"):
                clean = clean.split("```")[1]
                if clean.startswith("json"):
                    clean = clean[4:]
            clean = clean.strip()

            data = json.loads(clean)

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

        except json.JSONDecodeError as e:
            logger.warning(f"AAC: Failed to parse seeding response as JSON: {e}")
            logger.debug(f"Raw response: {response}")
        except Exception as e:
            logger.warning(f"AAC: Seeding failed: {e}")

        return {
            "topic": "Knowledge graph exploration and insight discovery",
            "objective": "Find interesting patterns or connections in the user's knowledge",
            "discussion_type": "brainstorm",
            "reasoning": "Fallback due to seeding failure",
            "agent_ids": [seeding_agent.id],
        }

    async def _build_seeding_context(self) -> str:
        """Gather rich context for seeding agent decision-making."""
        lines = []

        try:
            stats = await self.resources.memgraph.get_graph_stats()
            notable = await self.resources.memgraph.get_notable_entities(8)
            recent_entities = (
                await self.resources.memgraph.get_recently_active_entities(7, 5)
            )
            recent_facts = await self.resources.memgraph.get_recent_facts(7, 10)
            past_discussions = (
                await self.resources.memgraph.community.get_recent_discussions(5)
            )
            insights = await self.resources.memgraph.community.get_discussion_insights(
                5
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

        agent_ids = list(raw_agents.keys())
        descriptions = []

        for aid, raw in raw_agents.items():
            try:
                data = json.loads(raw)
                name = data.get("name", "Unknown")
                persona = data.get("persona", "")[:120]
                is_spawned = data.get("is_spawned", False)

                spawned_tag = " [spawned]" if is_spawned else ""
                descriptions.append(f"- {name}{spawned_tag} (id: {aid}): {persona}")
            except Exception:
                descriptions.append(f"- Unknown (id: {aid})")

        return agent_ids, "\n".join(descriptions)
