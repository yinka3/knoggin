import asyncio
from datetime import datetime, timezone
from functools import partial
import json
import uuid
from typing import List, Dict, Optional
from loguru import logger

from main.entity_resolve import EntityResolver
from shared.services.memory import MemoryManager
from shared.infra.redis import RedisKeys
from shared.utils.events import emit, emit_community
from shared.config.base import get_config_value
from shared.infra.resources import ResourceManager
from shared.models.schema.dtypes import AgentConfig
from shared.models.schema.community_tool_schema import COMMUNITY_TOOL_SCHEMAS

from db.community_store import CommunityStore
from agent.community_tools import CommunityTools
from agent.internals import (
    AgentRunConfig,
    AgentState,
    AgentContext,
    RetrievedEvidence,
    execute_tool,
    build_user_message,
    update_accumulators
)
from shared.config.topics import TopicConfig

class CommunityManager:
    """Orchestrates autonomous agent discussions."""
    
    def __init__(self, resources: ResourceManager, user_name: str):
        self.resources = resources
        self.user_name = user_name
        self.store = CommunityStore(resources.store.driver)
        self._active_discussion_id = None
    
    async def _is_discussion_active(self) -> bool:
        return await self.resources.redis.exists(RedisKeys.community_discussion_active())
    
    async def _get_default_agent_id(self) -> str:
        """Get the default agent ID for fallback."""
        default_id = await self.resources.redis.get(RedisKeys.agents_default(self.user_name))
        return default_id or "default_stella"
    
    async def _get_agent_config(self, agent_id: str) -> Optional[AgentConfig]:
        raw = await self.resources.redis.hget(RedisKeys.agents(self.user_name), agent_id)
        if raw:
            data = json.loads(raw)
            return AgentConfig.from_dict(data)
        
        default_id = await self.resources.redis.get(RedisKeys.agents_default(self.user_name))
        if default_id and default_id != agent_id:
            return await self._get_agent_config(default_id)
        
        logger.warning(f"AAC: Agent '{agent_id}' not found, using ephemeral default")
        llm_config = get_config_value("llm", {})
        return AgentConfig(
            id=agent_id,
            name="STELLA",
            persona="Default AAC Facilitator. Warm and observant.",
            model=llm_config.get("agent_model")
        )

    async def trigger_discussion(self):
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
        await self.resources.redis.set(RedisKeys.community_discussion_active(), discussion_id)
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(self.resources.executor, self.store.create_discussion, discussion_id, topic, valid_agent_ids)
        
        await emit_community(self.user_name, "community", "discussion_started", {
            "id": discussion_id,
            "topic": topic,
            "agents": valid_agent_ids
        })

        async def _run_and_cleanup():
            try:
                await self._run_loop(discussion_id, topic, valid_agent_ids)
            except Exception as e:
                logger.error(f"AAC discussion {discussion_id} error: {e}")
            finally:
                await self.resources.redis.delete(RedisKeys.community_discussion_active())
                self._active_discussion_id = None
                await loop.run_in_executor(self.resources.executor, self.store.close_discussion, discussion_id)
                await emit_community(self.user_name, "community", "discussion_ended", {"id": discussion_id})

        asyncio.create_task(_run_and_cleanup())


    async def _run_loop(self, discussion_id: str, topic: str, initial_agent_ids: List[str]):
        from main.entity_resolve import EntityResolver
        
        dev_settings = get_config_value("developer_settings") or {}
        config = dev_settings.get("community", {})
        max_turns = config.get("max_turns", 10)
        
        resolver = EntityResolver(
            store=self.resources.store,
            embedding_service=self.resources.embedding,
            session_id=f"aac_{discussion_id}"
        )
        
        participants = list(initial_agent_ids)
        history = []
        loop = asyncio.get_running_loop()
        
        for turn in range(max_turns):
            active_id = await self.resources.redis.get(RedisKeys.community_discussion_active())
            if not active_id or active_id.decode("utf-8") != discussion_id:
                logger.info(f"AAC [{discussion_id}]: Discussion manually closed or superseded. Aborting loop.")
                break

            agent_id = participants[turn % len(participants)]
            agent_config = await self._get_agent_config(agent_id)
            if not agent_config:
                continue

            logger.info(f"AAC [{discussion_id}]: Turn {turn}, Agent {agent_config.name}")
            
            try:
                message = await asyncio.wait_for(
                    self._agent_turn(
                        discussion_id, agent_config, topic, history, participants, resolver
                    ),
                    timeout=1200.0
                )
            except asyncio.TimeoutError:
                logger.warning(f"AAC [{discussion_id}]: Turn {turn} timed out after 20 minutes. Closing discussion.")
                break
                
            if not message:
                break
                
            history.append({
                "role": "assistant", 
                "agent_id": agent_id, 
                "content": message, 
                "name": agent_config.name
            })
            
            await loop.run_in_executor(
                self.resources.executor, 
                self.store.add_message, 
                discussion_id, agent_id, message, "assistant"
            )
            
            await emit_community(self.user_name, "community", "message_added", {
                "discussion_id": discussion_id,
                "agent_id": agent_id,
                "agent_name": agent_config.name,
                "content": message
            })

            if "[[END_DISCUSSION]]" in message:
                break

    async def _agent_turn(
        self,
        discussion_id: str,
        agent: AgentConfig,
        topic: str,
        history: List[Dict],
        participants: List[str],
        resolver: 'EntityResolver'
    ) -> Optional[str]:
        from agent.system_prompt import get_agent_prompt
        from agent.tools import Tools as BaseTools

        categories = ["rules", "preferences", "icks"]
        memory_blocks = {}
        for category in categories:
            key = RedisKeys.agent_working_memory(agent.id, category)
            raw = await self.resources.redis.hgetall(key)
            entries = [json.loads(v).get("content", "") for v in raw.values()] if raw else []
            memory_blocks[category] = "\n".join(entries)

        comm_mem_key = RedisKeys.community_agent_memory(self.user_name, agent.id)
        raw_mem = await self.resources.redis.hgetall(comm_mem_key)
        agent_memory_context = "\n".join(
            json.loads(v).get("content", "") for v in raw_mem.values()
        ) if raw_mem else ""

        participant_names = []
        for pid in participants:
            if pid == agent.id:
                continue
            pcfg = await self._get_agent_config(pid)
            if pcfg:
                participant_names.append(pcfg.name)

        base_prompt = get_agent_prompt(
            user_name=self.user_name,
            current_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            persona=agent.persona,
            agent_name=agent.name,
            memory_context=agent_memory_context,
            agent_rules=memory_blocks.get("rules", ""),
            agent_preferences=memory_blocks.get("preferences", ""),
            agent_icks=memory_blocks.get("icks", ""),
            instructions=agent.instructions
        )

        aac_context = (
            f"\n<community_context>\n"
            f"You are participating in an autonomous community discussion.\n"
            f"Topic: {topic}\n"
            f"Other participants: {', '.join(participant_names) if participant_names else 'None yet'}\n"
            f"Rules:\n"
            f"1. Start your response with <REASONING>[Your internal plan]</REASONING>.\n"
            f"2. Use tools to search both the internal knowledge graph AND the external web (web_search, news_search) to ground your insights. Be equally curious about finding external information to bring new perspectives.\n"
            f"3. Build on what others have said — don't repeat points already made.\n"
            f"4. Use [[END_DISCUSSION]] only when the topic is genuinely exhausted or resolved.\n"
            f"5. Use save_insight to record any valuable findings worth persisting.\n"
            f"6. Use save_memory to record anything useful for future discussions.\n"
            f"7. Use spawn_specialist only if the topic requires expertise clearly outside your scope.\n"
            f"</community_context>"
        )

        system_prompt = base_prompt + aac_context

        memory_mgr = MemoryManager(
            redis=self.resources.redis,
            user_name=self.user_name,
            session_id=f"aac_{discussion_id}",
            agent_id=agent.id,
            topic_config=TopicConfig(TopicConfig.DEFAULT_CONFIG),
            on_event=lambda src, evt, data: asyncio.create_task(
                emit(f"aac_{discussion_id}", src, evt, data)
            ),
        )

        base_tools = BaseTools(
            user_name=self.user_name,
            store=self.resources.store,
            ent_resolver=resolver,
            redis_client=self.resources.redis,
            session_id=f"aac_{discussion_id}",
            topic_config=None,
            memory=memory_mgr
        )
        comm_tools = CommunityTools(self.user_name, base_tools, self.store, discussion_id, agent.id)

        state = AgentState()
        ctx = AgentContext(
            config=AgentRunConfig(),
            state=state,
            evidence=RetrievedEvidence(),
            user_query=topic,
            history=history,
            agent_id=agent.id,
            agent_name=agent.name,
            agent_persona=agent.persona
        )

        last_results = None

        for _ in range(ctx.config.max_attempts):
            prompt = build_user_message(ctx, last_result=last_results)
            
            content = ""
            tool_calls = []
            
            async for chunk in self.resources.llm_service.call_llm_with_tools_streaming(
                system=system_prompt,
                user=prompt,
                tools=COMMUNITY_TOOL_SCHEMAS,
                model=agent.model,
                temperature=agent.temperature
            ):
                if chunk.get("type") == "tool_calls":
                    content = chunk.get("content", "")
                    tool_calls = chunk.get("calls", [])
                elif chunk.get("type") == "done" and not tool_calls:
                    content = chunk.get("content", "")

            if "<REASONING>" in content:
                parts = content.split("</REASONING>", 1)
                reasoning = parts[0].replace("<REASONING>", "").strip()
                content = parts[1].strip() if len(parts) > 1 else content
                await emit_community(self.user_name, "community", "agent_reasoning", {
                    "discussion_id": discussion_id,
                    "agent_id": agent.id,
                    "reasoning": reasoning
                })

            if tool_calls:
                last_results = []
                for call in tool_calls:
                    try:
                        args = json.loads(call["arguments"]) if isinstance(call["arguments"], str) else call["arguments"]
                    except json.JSONDecodeError:
                        continue
                    res = await self._dispatch_comm_tool(comm_tools, call["name"], args, participants)
                    update_accumulators(ctx, call["name"], res)
                    state.record_call(call["name"], args)
                    last_results.append({
                        "tool": call["name"],
                        "result": res
                    })
            else:
                return content

        return "Reached maximum attempts."

    async def _seed_discussion(self) -> Optional[Dict]:
        """Use seeding agent to analyze graph and initiate a discussion."""
        from agent.system_prompt import get_agent_prompt
        
        dev_settings = get_config_value("developer_settings") or {}
        config = dev_settings.get("community", {})
        seeding_agent_id = config.get("seeding_agent_id")
        
        seeding_agent = None
        if seeding_agent_id:
            seeding_agent = await self._get_agent_config(seeding_agent_id)
        
        if not seeding_agent:
            default_id = await self._get_default_agent_id()
            seeding_agent = await self._get_agent_config(default_id)
        
        if not seeding_agent:
            logger.error("AAC: No seeding agent available")
            return None

        memory_blocks = {}
        for category in ["rules", "preferences", "icks"]:
            key = RedisKeys.agent_working_memory(seeding_agent.id, category)
            raw = await self.resources.redis.hgetall(key)
            entries = [json.loads(v).get("content", "") for v in raw.values()] if raw else []
            memory_blocks[category] = "\n".join(entries)

        comm_mem_key = RedisKeys.community_agent_memory(self.user_name, seeding_agent.id)
        raw_mem = await self.resources.redis.hgetall(comm_mem_key)
        agent_memory_context = "\n".join(
            json.loads(v).get("content", "") for v in raw_mem.values()
        ) if raw_mem else ""

        base_prompt = get_agent_prompt(
            user_name=self.user_name,
            current_time=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
            persona=seeding_agent.persona,
            agent_name=seeding_agent.name,
            memory_context=agent_memory_context,
            agent_rules=memory_blocks.get("rules", ""),
            agent_preferences=memory_blocks.get("preferences", ""),
            agent_icks=memory_blocks.get("icks", ""),
            instructions=seeding_agent.instructions
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

        await emit_community(self.user_name, "community", "seeding_started", {
            "seeding_agent_id": seeding_agent.id,
            "seeding_agent_name": seeding_agent.name
        })

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
            
            await emit_community(self.user_name, "community", "discussion_seeded", {
                "seeding_agent": seeding_agent.name,
                "topic": data.get("topic"),
                "objective": data.get("objective"),
                "discussion_type": data.get("discussion_type"),
                "reasoning": data.get("reasoning"),
                "agent_ids": data["agent_ids"]
            })
            
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
            "agent_ids": [seeding_agent.id]
        }

    async def _dispatch_comm_tool(
        self, 
        comm_tools: CommunityTools, 
        name: str, 
        args: Dict, 
        discussion_participants: List[str]
    ) -> Dict:
        if name == "save_insight": 
            return {"data": await comm_tools.save_insight(args.get("content", ""))}
        if name == "save_memory": 
            return {"data": await comm_tools.save_memory(args.get("content", ""))}
        if name == "spawn_specialist":
            result = await comm_tools.spawn_specialist(
                name=args.get("name", "Specialist"),
                persona=args.get("persona", "A specialist sub-agent."),
                discussion_participants=discussion_participants,
                initial_rules=args.get("initial_rules"),
                initial_preferences=args.get("initial_preferences"),
                initial_icks=args.get("initial_icks")
            )
            # Assuming 'result' contains the new agent's ID if successful
            new_agent_id = result.get("id") if isinstance(result, dict) else None
            if new_agent_id and new_agent_id not in discussion_participants:
                discussion_participants.append(new_agent_id)
            return {"data": result}
            
        return await execute_tool(comm_tools.base, name, args)
    
    async def _build_seeding_context(self) -> str:
        """Gather rich context for seeding agent decision-making."""
        loop = asyncio.get_running_loop()
        lines = []

        try:
            stats = await loop.run_in_executor(self.resources.executor, self.resources.store.get_graph_stats)
            notable = await loop.run_in_executor(self.resources.executor, partial(self.resources.store.get_notable_entities, 8))
            recent_entities = await loop.run_in_executor(self.resources.executor, partial(self.resources.store.get_recently_active_entities, 7, 5))
            recent_facts = await loop.run_in_executor(self.resources.executor, partial(self.resources.store.get_recent_facts, 7, 10))
            past_discussions = await loop.run_in_executor(self.resources.executor, partial(self.store.get_recent_discussions, 5))
            insights = await loop.run_in_executor(self.resources.executor, partial(self.store.get_discussion_insights, 5))
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
                content = fact['content'][:100] + "..." if len(fact['content']) > 100 else fact['content']
                lines.append(f"- [{fact['entity_name']}] {content}")
            lines.append("")
        elif isinstance(recent_facts, Exception):
            logger.warning(f"Failed to get recent facts: {recent_facts}")

        if not isinstance(past_discussions, Exception) and past_discussions:
            lines.append("=== PREVIOUS DISCUSSIONS ===")
            for disc in past_discussions:
                status = disc.get('status', 'unknown')
                topic = disc.get('topic', 'Unknown topic')[:80]
                msg_count = disc.get('message_count', 0)
                lines.append(f"- \"{topic}\" ({status}, {msg_count} messages)")
            lines.append("")
        elif isinstance(past_discussions, Exception):
            logger.warning(f"Failed to get past discussions: {past_discussions}")

        if not isinstance(insights, Exception) and insights:
            lines.append("=== INSIGHTS FROM PAST DISCUSSIONS ===")
            for ins in insights:
                content = ins['content'].replace("INSIGHT: ", "")[:100]
                lines.append(f"- {content}")
            lines.append("")
        elif isinstance(insights, Exception):
            logger.warning(f"Failed to get insights: {insights}")

        return "\n".join(lines) if lines else "Knowledge graph is available for exploration."


    async def _build_agent_pool_context(self) -> tuple[List[str], str]:
        """Build descriptive agent pool. Returns (agent_ids, formatted_description)."""
        raw_agents = await self.resources.redis.hgetall(RedisKeys.agents(self.user_name))
        
        if not raw_agents:
            return ["default_stella"], "- STELLA (default): General purpose assistant."
        
        agent_ids = list(raw_agents.keys())
        descriptions = []
        
        for aid, raw in raw_agents.items():
            try:
                data = json.loads(raw)
                name = data.get('name', 'Unknown')
                persona = data.get('persona', '')[:120]
                is_spawned = data.get('is_spawned', False)
                
                spawned_tag = " [spawned]" if is_spawned else ""
                descriptions.append(f"- {name}{spawned_tag} (id: {aid}): {persona}")
            except Exception:
                descriptions.append(f"- Unknown (id: {aid})")
        
        return agent_ids, "\n".join(descriptions)
