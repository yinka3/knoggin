import asyncio
import json
import uuid
from dataclasses import dataclass
from types import SimpleNamespace
from typing import Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.schema.agent.community_tools import (
    AAC_READ_TOOL_NAMES,
    AAC_SPECIFIC_SCHEMAS,
)
from common.schema.agent.identity import AgentConfig
from common.scoping import IDENTITY_SCOPE
from common.utils.events import emit_community
from common.utils.json_utils import safe_json_loads
from common.utils.local_references import build_local_id_maps, resolve_local_id
from common.utils.time_utils import get_now
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.services.agent_manager import AgentManager
from core.agent.system_prompt import get_agent_prompt
from core.agent.tools.community_tools import CommunityTools
from core.community.policy import (
    CommunityDiscussionAdmission,
    CommunityDiscussionAdmissionOutcome,
    CommunityDiscussionPolicy,
)
from infrastructure.redis_client import RedisKeys
from runtime.project_runtime import ProjectRuntime

COMMUNITY_ENABLED_TOOLS = [*AAC_READ_TOOL_NAMES, "restore_brain_section"]
ACTIVE_DISCUSSION_TTL_SECONDS = 2 * 60 * 60

COMMUNITY_RUN_LIMITS = AgentRunLimits(
    max_calls=5,
    max_attempts=6,
    max_history_turns=4,
    max_accumulated_messages=10,
    max_consecutive_errors=2,
    tool_limits=(
        ("search_entity", 4),
        ("episode_check", 6),
        ("read_episode", 4),
        ("get_connections", 3),
        ("search_messages", 3),
        ("search_documents", 4),
        ("read_document", 4),
        ("list_documents", 2),
        ("read_brain", 4),
        ("edit_brain", 2),
        ("restore_brain_section", 2),
        ("save_insight", 4),
        ("spawn_specialist", 2),
    ),
)


@dataclass(frozen=True)
class CommunityExecutionContext:
    """Minimal project context for AAC agent turns without a session runtime."""

    session_id: str
    project: ProjectRuntime
    document_service: Optional[object]


class CommunityManager:
    """Orchestrates autonomous agent discussions."""

    _RELEASE_ACTIVE_DISCUSSION_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('del', KEYS[1])
    end
    return 0
    """

    _RENEW_ACTIVE_DISCUSSION_SCRIPT = """
    if redis.call('get', KEYS[1]) == ARGV[1] then
        return redis.call('expire', KEYS[1], ARGV[2])
    end
    return 0
    """

    def __init__(self, project_state: ProjectRuntime, user_name: str, resources):
        self.project_state = project_state
        self.user_name = user_name
        self.resources = resources
        self._active_discussion_id: Optional[str] = None
        self._discussion_task: Optional[asyncio.Task] = None

    async def _get_agent_directives(self, agent_id: str) -> str:
        """Legacy directives are now part of the durable Markdown brain."""
        return ""

    async def _load_project_context(self) -> str:
        """Load canonical project context without blocking community startup."""
        workspace_service = getattr(self.project_state, "workspace_service", None)
        reader = getattr(workspace_service, "read_project_context", None)
        if reader is None:
            return ""
        try:
            content = await reader()
        except Exception as exc:
            logger.warning(
                "CommunityManager: project context unavailable ({})",
                type(exc).__name__,
            )
            return ""
        return content if isinstance(content, str) else ""

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
        manager = AgentManager(self.resources, self.user_name)
        return await manager.get_agent(agent_id) is not None

    async def _get_default_agent_id(self) -> str:
        """Get the default agent ID for fallback."""
        manager = AgentManager(self.resources, self.user_name)
        return await manager.get_default_agent_id()

    async def _get_agent_config(self, agent_id: str) -> Optional[AgentConfig]:
        manager = AgentManager(self.resources, self.user_name)
        return await manager.get_agent(agent_id)

    def _track_discussion_task(self, task: asyncio.Task) -> None:
        self._discussion_task = task
        task.add_done_callback(self._clear_discussion_task)
        if hasattr(self.project_state, "track_community_task"):
            self.project_state.track_community_task(task)

    def _clear_discussion_task(self, task: asyncio.Task) -> None:
        if self._discussion_task is task:
            self._discussion_task = None

    async def _release_active_discussion(self, discussion_id: str) -> None:
        await self.resources.redis.eval(
            self._RELEASE_ACTIVE_DISCUSSION_SCRIPT,
            1,
            self._active_discussion_key(),
            discussion_id,
        )

    async def _renew_active_discussion(self, discussion_id: str) -> bool:
        renewed = await self.resources.redis.eval(
            self._RENEW_ACTIVE_DISCUSSION_SCRIPT,
            1,
            self._active_discussion_key(),
            discussion_id,
            ACTIVE_DISCUSSION_TTL_SECONDS,
        )
        return bool(renewed)

    async def trigger_discussion(self) -> CommunityDiscussionAdmission:
        """Claim and start one project-owned community discussion."""
        if await self._is_discussion_active():
            logger.info("AAC: Discussion already in progress, skipping.")
            return CommunityDiscussionAdmission(
                outcome=CommunityDiscussionAdmissionOutcome.SKIPPED,
                reason="already_active",
            )

        discussion_id = str(uuid.uuid4())
        claimed = await self.resources.redis.set(
            self._active_discussion_key(),
            discussion_id,
            ex=ACTIVE_DISCUSSION_TTL_SECONDS,
            nx=True,
        )
        if not claimed:
            logger.info("AAC: Discussion claimed by another runtime, skipping.")
            return CommunityDiscussionAdmission(
                outcome=CommunityDiscussionAdmissionOutcome.SKIPPED,
                reason="claimed_elsewhere",
            )
        self._active_discussion_id = discussion_id

        try:
            community_settings = (
                ConfigManager.get().config.developer_settings.community
            )
            policy = CommunityDiscussionPolicy.capture(community_settings)
            seed_data = await asyncio.wait_for(
                self._seed_discussion(),
                timeout=policy.seeding_timeout_seconds,
            )
            if not seed_data:
                await self._release_active_discussion(discussion_id)
                self._active_discussion_id = None
                return CommunityDiscussionAdmission(
                    outcome=CommunityDiscussionAdmissionOutcome.SKIPPED,
                    reason="no_seed",
                    policy_version=policy.version,
                )

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
                discussion_id,
                topic,
                valid_agent_ids,
                user_name=self.user_name,
                project_id=self.project_state.project_id,
            )
        except asyncio.TimeoutError:
            logger.warning(
                "AAC: Seeding exceeded the configured timeout; skipping discussion."
            )
            await self._release_active_discussion(discussion_id)
            self._active_discussion_id = None
            return CommunityDiscussionAdmission(
                outcome=CommunityDiscussionAdmissionOutcome.SKIPPED,
                reason="seeding_timeout",
                policy_version=policy.version,
            )
        except BaseException:
            await self._release_active_discussion(discussion_id)
            self._active_discussion_id = None
            raise

        try:
            task = asyncio.create_task(
                self._run_discussion(
                    discussion_id,
                    topic,
                    valid_agent_ids,
                    policy,
                ),
                name=f"aac:{self.user_name}:{self.project_state.project_id}",
            )
            self._track_discussion_task(task)
        except BaseException:
            if "task" in locals():
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
            await self._finalize_discussion(
                discussion_id,
                outcome="admission_failed",
                policy=policy,
            )
            raise
        return CommunityDiscussionAdmission(
            outcome=CommunityDiscussionAdmissionOutcome.STARTED,
            reason="admitted",
            discussion_id=discussion_id,
            policy_version=policy.version,
        )

    async def _run_discussion(
        self,
        discussion_id: str,
        topic: str,
        agent_ids: List[str],
        policy: CommunityDiscussionPolicy,
    ) -> None:
        """Run and finalize one task owned by this project's runtime."""
        outcome = "completed"
        try:
            await self._safe_emit_community(
                "discussion_started",
                {
                    "id": discussion_id,
                    "topic": topic,
                    "agents": agent_ids,
                    "policy_version": policy.version,
                },
            )
            await self._run_loop(
                discussion_id,
                topic,
                agent_ids,
                policy=policy,
            )
        except asyncio.CancelledError:
            outcome = "cancelled"
            logger.info(f"AAC discussion {discussion_id} cancelled")
            raise
        except Exception as exc:
            outcome = "failed"
            logger.error(f"AAC discussion {discussion_id} error: {exc}")
        finally:
            await self._finalize_discussion(
                discussion_id,
                outcome=outcome,
                policy=policy,
            )

    async def _finalize_discussion(
        self,
        discussion_id: str,
        *,
        outcome: str,
        policy: CommunityDiscussionPolicy,
    ) -> None:
        """Release the distributed lease and close durable discussion state."""
        try:
            await self._release_active_discussion(discussion_id)
        except Exception as exc:
            logger.warning(
                "AAC: Failed to release active discussion marker for {}: {}",
                discussion_id,
                exc,
            )
        if self._active_discussion_id == discussion_id:
            self._active_discussion_id = None
        try:
            await self.resources.knowledge_store.community.close_discussion(
                discussion_id,
                user_name=self.user_name,
                project_id=self.project_state.project_id,
            )
        except Exception as exc:
            logger.error("AAC: Failed to close discussion {} in DB: {}", discussion_id, exc)
        await self._safe_emit_community(
            "discussion_ended",
            {
                "id": discussion_id,
                "outcome": outcome,
                "policy_version": policy.version,
            },
        )

    async def _safe_emit_community(self, event: str, data: Dict) -> None:
        try:
            await emit_community(self.user_name, "community", event, data)
        except Exception as exc:
            logger.warning("AAC: Failed to emit {}: {}", event, exc)

    async def _run_loop(
        self,
        discussion_id: str,
        topic: str,
        initial_agent_ids: List[str],
        *,
        policy: Optional[CommunityDiscussionPolicy] = None,
    ) -> None:
        ctx = CommunityExecutionContext(
            session_id=f"aac_{discussion_id}",
            project=self.project_state,
            document_service=getattr(self.project_state, "document_service", None),
        )

        if policy is None:
            policy = CommunityDiscussionPolicy.capture(
                ConfigManager.get().config.developer_settings.community
            )

        participants = list(initial_agent_ids)
        history = []

        for turn in range(policy.max_turns):
            if not await self._renew_active_discussion(discussion_id):
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
                    timeout=policy.turn_timeout_seconds,
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
                discussion_id,
                agent_id,
                message,
                "assistant",
                user_name=self.user_name,
                project_id=self.project_state.project_id,
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

        agent_directives = await self._get_agent_directives(agent.id)
        enabled_tools, additional_tool_schemas = self._resolve_agent_tools(agent)

        readable_project_ids = await self._resolve_project_scope()
        base_tools = SimpleNamespace(
            entities=ctx.project.entities,
            session_id=ctx.session_id,
            compiled_domain=getattr(ctx.project, "compiled_domain", None),
            search_cfg={},
            knowledge_store=self.resources.knowledge_store,
            document_service=ctx.document_service,
            workspace_service=getattr(ctx.project, "workspace_service", None),
            document_focus=None,
            postgres=self.resources.postgres,
            redis=self.resources.redis,
            readable_project_ids=readable_project_ids,
            knowledge_retrieval=ctx.project.knowledge_retrieval,
        )

        comm_tools = CommunityTools(
            self.user_name,
            base_tools,
            self.resources.knowledge_store.community,
            discussion_id,
            agent.id,
            participants,
        )

        run = AgentRun.open(
            user_name=self.user_name,
            session_id=ctx.session_id,
            project_id=ctx.project.project_id,
            user_query=f"Community Discussion Topic: {topic}",
            run_id=f"run_{uuid.uuid4().hex[:8]}",
            agent=AgentIdentity(
                config=agent,
                name=agent.name,
                persona=agent.persona_markdown,
            ),
            limits=COMMUNITY_RUN_LIMITS,
            model=agent.model,
            temperature=agent.temperature,
            brain=agent.brain,
            directives=agent_directives,
            enabled_tools=enabled_tools,
            additional_tool_schemas=additional_tool_schemas,
            history=history,
            is_community=True,
            current_participants=participants,
        )

        executor = AgentExecutor(
            ctx=run,
            llm=self.resources.llm_service,
            tools=comm_tools,
        )

        full_response: str = ""

        try:
            async for event in executor.execute():
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
        if agent.enabled_tools is None:
            return COMMUNITY_ENABLED_TOOLS, AAC_SPECIFIC_SCHEMAS

        allowed = set(agent.enabled_tools)
        enabled_tools = [name for name in COMMUNITY_ENABLED_TOOLS if name in allowed]
        additional_tool_schemas = [
            schema
            for schema in AAC_SPECIFIC_SCHEMAS
            if schema["function"]["name"] in allowed
        ]
        return enabled_tools, additional_tool_schemas

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
        project_context = await self._load_project_context()

        base_prompt = get_agent_prompt(
            user_name=self.user_name,
            current_time=get_now().strftime("%Y-%m-%d %H:%M UTC"),
            persona=seeding_agent.persona_markdown,
            agent_name=seeding_agent.name,
            agent_directives=directives_str,
            agent_brain=seeding_agent.brain,
            project_context=project_context,
            documents_context="",
            is_community=False,
            phase="PLAN",
        )

        seeding_instructions = """
    <seeding_role>
    You are the SEEDING AGENT for an autonomous community discussion.

    Your job is to analyze the knowledge graph context below and initiate a
    meaningful discussion.

    You must decide:
    1. TOPIC: What specific subject should agents discuss? Be concrete, not \
       vague.
    2. OBJECTIVE: What should they achieve? (e.g., resolve a contradiction,
       explore a connection, brainstorm applications, debate a decision)
    3. DISCUSSION_TYPE: "brainstorm" | "debate" | "investigation" | "synthesis"
    4. REASONING: Why this topic now? What makes it valuable?
    5. AGENT_IDS: Which listed agent IDs should participate? Pick
       2-4 agents whose personas are relevant. You may include yourself.

    Guidelines:
    - Prioritize topics with recent activity or unresolved questions
    - Avoid repeating recent discussion topics
    - Match agents to topics based on their personas
    - Prefer depth over breadth — focused discussions are better
    - You now have access to project documents via search_documents. Consider \
      initiating discussions around analyzing uploaded files!
    </seeding_role>
    """

        graph_context = await self._build_seeding_context()
        agent_ids_by_local, agent_descriptions = await self._build_agent_pool_context(
            required_agent=seeding_agent,
        )
        system_prompt = base_prompt + seeding_instructions

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
        "agent_ids": {json.dumps(list(agent_ids_by_local))}
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
            for local_agent_id in data["agent_ids"]:
                try:
                    agent_id = str(
                        resolve_local_id(local_agent_id, agent_ids_by_local)
                    )
                except ValueError:
                    logger.warning(
                        "AAC: Seeded an unknown local agent reference "
                        f"'{local_agent_id}', skipping"
                    )
                    await emit_community(
                        self.user_name,
                        "community",
                        "local_reference_resolution_failed",
                        {
                            "pipeline": "community_seeding",
                            "reference_type": "agent",
                            "reason": "unknown_id",
                        },
                    )
                    continue
                valid_agent_ids.append(agent_id)

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
        """Gather graph and discussion context for seeding decisions."""
        lines = []

        try:
            visible_project_ids = await self._resolve_project_scope()
            if not visible_project_ids:
                return "No community project scope is configured."
            stats = await self.resources.knowledge_store.get_graph_stats(
                visible_project_ids=visible_project_ids
            )
            community_store = self.resources.knowledge_store.community
            past_discussions = await community_store.get_recent_discussions(
                5,
                user_name=self.user_name,
                project_id=self.project_state.project_id,
            )
            insights = await community_store.get_discussion_insights(
                5,
                user_name=self.user_name,
                project_id=self.project_state.project_id,
            )
        except Exception as e:
            logger.warning(f"Failed to gather seeding context: {e}")
            return "Knowledge graph is available for exploration."

        if isinstance(stats, dict):
            lines.append("=== GRAPH OVERVIEW ===")
            lines.append(f"Entities: {stats.get('entities', 0)}")
            lines.append(f"Episodes: {stats.get('episodes', 0)}")
            lines.append(f"Relationships: {stats.get('relationships', 0)}")
            lines.append("")
        elif isinstance(stats, Exception):
            logger.warning(f"Failed to get graph stats: {stats}")

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

    async def _build_agent_pool_context(
        self,
        *,
        required_agent: AgentConfig | None = None,
    ) -> tuple[dict[str, str], str]:
        """Build the model-facing agent pool with one local-reference map."""

        manager = AgentManager(self.resources, self.user_name)
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

        if required_agent and all(agent.id != required_agent.id for agent in agents):
            agents.append(required_agent)
        agents = sorted(agents, key=lambda agent: agent.id)
        agent_local_ids, agent_ids_by_local = build_local_id_maps(
            (agent.id for agent in agents),
            "a",
        )
        descriptions = []
        for agent in agents:
            spawned_tag = " [spawned]" if agent.is_spawned else ""
            persona_preview = " ".join(agent.persona_markdown.split())[:160]
            descriptions.append(
                f"- {agent_local_ids[agent.id]}: "
                f"{agent.name}{spawned_tag}: {persona_preview}"
            )
        return agent_ids_by_local, "\n".join(descriptions)
