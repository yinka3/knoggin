from __future__ import annotations

import asyncio
import re
import uuid
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from enum import StrEnum
from typing import AsyncGenerator, Dict, List, Optional
from zoneinfo import ZoneInfo

from loguru import logger

from common.exceptions import ConfigurationError, LLMError, ToolExecutionError
from common.schema.agent.stream import (
    AgentExecutionEvent,
    ErrorEvent,
    InternalAgentStreamEvent,
    ResponseEvent,
    StreamToolCall,
    StreamUsage,
)
from common.schema.artifacts import (
    ArtifactDraft,
    default_artifact_from_answer,
)
from common.schema.source.references import SourceReferenceCandidate
from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now
from core.agent.formatters import (
    format_document_focus_context,
    format_documents_context,
)
from core.agent.prompt_context import (
    build_evidence_context,
    build_user_message,
)
from core.agent.run import AgentRun
from core.agent.sources.tool_results import capture_tool_source_candidates
from core.agent.system_prompt import (
    get_agent_prompt,
    get_fallback_summary_prompt,
)
from core.agent.tool_references import localize_agent_tool_result
from core.agent.tool_runtime import execute_tool, summarize_result
from core.agent.tools.registry import (
    Tools,
    get_runtime_instructions,
    get_tool_definition,
    install_tool_runtime,
)
from infrastructure.llm_client import LLMService

MAX_TOKEN_CHUNK_SIZE = 10000
PUBLIC_AGENT_FAILURE_MESSAGE = "The agent couldn't complete this request. Please try again."


class _AgentPhase(StrEnum):
    PLAN = "PLAN"
    EXECUTE = "EXECUTE"
    SYNTHESIZE = "SYNTHESIZE"


@dataclass
class _ToolCall:
    """Temporary parsed call state owned by the executor loop."""

    name: str
    args: Dict = field(default_factory=dict)
    thinking: Optional[str] = None
    call_id: str = field(default_factory=lambda: str(uuid.uuid4()))


def _is_local_reference_resolution_error(message: str) -> bool:
    """Recognize resolver failures without exposing the supplied ID in metrics."""

    return "Unknown local ID" in message or "UUID handle" in message


def _local_reference_type(tool_name: str) -> str:
    if tool_name in {"read_episode", "propose_entity_merge"}:
        return "episode"
    if tool_name in {"get_document_info", "read_document"}:
        return "document"
    if tool_name in {
        "list_documents",
        "get_folder_upload_summary",
        "list_folder_tree",
        "search_documents",
    }:
        return "folder"
    return "tool_argument"


class AgentExecutor:
    """
    Handles the reasoning loop, tool execution, and evidence gathering for an agent run.
    """

    def __init__(
        self,
        ctx: AgentRun,
        llm: LLMService,
        tools: Tools,
        *,
        on_successful_completion: Callable[[str], Awaitable[object]] | None = None,
        aac_budget: object | None = None,
    ):
        self.ctx = ctx
        self.llm = llm
        self.tools = tools
        self._on_successful_completion = on_successful_completion
        self._aac_budget = aac_budget
        install_tool_runtime(
            tools,
            ctx.tool_runtime,
            ctx.short_uuid_references,
        )
        for candidate in ctx.initial_source_candidates:
            ctx.record_source(candidate)

    async def execute(
        self,
        user_timezone: Optional[str] = None,
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Run one execution from the policy captured on ``AgentRun``."""

        with diagnostic_scope(
            user_name=self.ctx.user_name,
            project_id=self.ctx.project_id,
            session_id=self.ctx.session_id,
            agent_run_id=self.ctx.run_id,
        ):
            try:
                async for event in self._execute_run(
                    user_timezone=user_timezone,
                ):
                    yield event
            finally:
                self.ctx.release()

    async def _execute_run(
        self,
        user_timezone: Optional[str] = None,
    ) -> AsyncGenerator[AgentExecutionEvent, None]:
        """Runs the reasoning loop and yields events."""

        # Prepare environment
        tz = ZoneInfo(user_timezone) if user_timezone else ZoneInfo("UTC")
        current_time = get_now().astimezone(tz).strftime("%Y-%m-%d %H:%M %Z")

        documents_context = ""
        if self.tools.document_service:
            manifest = await self.tools.get_document_manifest()
            if manifest:
                documents_context = format_documents_context(manifest)
        document_focus_context = format_document_focus_context(
            getattr(self.tools, "document_focus", None),
            getattr(self.ctx, "document_selection_context", None),
        )
        project_context = await self._load_project_context()

        last_result = None

        # The executor, not the model, owns phase transitions.
        needs_replan = False
        needs_final_synthesis = False

        while (
            self.ctx.attempt_count < self.ctx.limits.max_attempts
            or needs_final_synthesis
        ):
            if self.ctx.consecutive_errors >= self.ctx.limits.max_consecutive_errors:
                yield self._terminal_error()
                return

            is_final_synthesis = needs_final_synthesis
            if is_final_synthesis:
                if not self.ctx.begin_final_synthesis_attempt():
                    break
                phase = _AgentPhase.SYNTHESIZE
                current_model = self.ctx.model or self.llm.agent_model
                current_reasoning = "high"
                logger.info("AgentExecutor: synthesizing the final response.")
            elif not self.ctx.begin_attempt():
                break
            elif self.ctx.attempt_count == 1 or needs_replan:
                phase = _AgentPhase.PLAN
                current_model = self.ctx.model or self.llm.agent_model
                current_reasoning = "high"
                if needs_replan:
                    logger.info("AgentExecutor: replanning after an insufficient step.")
            else:
                phase = _AgentPhase.EXECUTE
                current_model = self.ctx.model or self.llm.extraction_model
                current_reasoning = "medium"

            # Reset flags so a successful retrieval defaults back to execution.
            needs_replan = False
            needs_final_synthesis = False

            # Monitoring/Emits
            await self._emit_llm_call(current_model, current_reasoning)

            # Call LLM for this step
            pending_tool_calls: List[_ToolCall] = []
            step_failed = False
            step_completed = False

            async for event in self._step(
                current_time,
                current_model,
                current_reasoning,
                phase,
                documents_context,
                document_focus_context,
                last_result,
                project_context=project_context,
            ):
                event_type = event["event"]
                data = event["data"]

                if event_type in ("token", "thinking"):
                    yield event
                    continue

                if event_type == "tool_calls":
                    pending_tool_calls.extend(
                        self._parse_tool_calls(
                            data["calls"],
                            data["content"],
                        )
                    )
                    continue

                if event_type == "step_error":
                    self._accumulate_usage(data.get("usage"))
                    self._record_step_error(data["message"], data["kind"])
                    step_failed = True
                    break

                if event_type == "step_completed":
                    step_completed = True
                    self._accumulate_usage(data["usage"])
                    current_results = []

                    if not pending_tool_calls:
                        self._record_step_error(
                            "LLM step completed without tool calls",
                            "formatting",
                        )
                        step_failed = True
                        break

                    submit = next(
                        (
                            call
                            for call in pending_tool_calls
                            if call.name == "submit_answer"
                        ),
                        None,
                    )
                    if submit:
                        content = submit.args.get("content", "")
                        if not isinstance(content, str) or not content.strip():
                            self._record_step_error(
                                "submit_answer requires non-empty content",
                                "formatting",
                            )
                            step_failed = True
                            break
                        artifact = None
                        raw_artifact = submit.args.get("artifact")
                        if raw_artifact is not None:
                            try:
                                artifact = ArtifactDraft.model_validate(
                                    raw_artifact
                                )
                            except Exception as exc:
                                self._record_step_error(
                                    f"Invalid submit_answer artifact: {exc}",
                                    "formatting",
                                )
                                step_failed = True
                                break

                        if (
                            phase is not _AgentPhase.SYNTHESIZE
                            and self.ctx.new_evidence_gathered
                        ):
                            logger.info(
                                "AgentExecutor: evidence is ready; scheduling synthesis."
                            )
                            needs_final_synthesis = True
                            break

                        artifact = self._complete_artifact(content, artifact)
                        response = self._wrap_final_response(
                            content=content,
                            usage=dict(self.ctx.usage),
                            sources_consulted=list(self.ctx.source_candidates),
                            artifact=(
                                artifact.model_dump(mode="json")
                                if artifact is not None
                                else None
                            ),
                        )
                        await self._finalize_successfully(content)
                        yield response
                        return

                    clarification = next(
                        (
                            call
                            for call in pending_tool_calls
                            if call.name == "request_clarification"
                        ),
                        None,
                    )
                    if clarification:
                        question = clarification.args.get(
                            "question", "Could you clarify?"
                        )
                        self.ctx.finish_without_response()
                        yield {
                            "event": "clarification",
                            "data": {
                                "question": question,
                                "usage": self.ctx.usage,
                            },
                        }
                        return

                    async for tool_event in self._execute_tools(
                        pending_tool_calls,
                        current_results,
                    ):
                        yield tool_event

                    last_result = current_results
                    await self._manage_context_size()

                    all_empty = (
                        all(
                            "error" in result
                            or not result.get("result", {}).get("data")
                            for result in current_results
                        )
                        if current_results
                        else True
                    )

                    if all_empty:
                        if self.ctx.record_empty_result():
                            logger.info(
                                f"AgentExecutor: "
                                f"{self.ctx.consecutive_empty_results} "
                                "consecutive empty results. Forcing replan."
                            )
                            needs_replan = True
                            self.ctx.clear_empty_results()
                    else:
                        self.ctx.clear_empty_results()

                    break

            if not step_failed and not step_completed:
                self._record_step_error(
                    "LLM stream ended without a terminal step event",
                    "provider",
                )
                step_failed = True

            if step_failed:
                if (
                    self.ctx.consecutive_errors
                    >= self.ctx.limits.max_consecutive_errors
                ):
                    yield self._terminal_error()
                    return
                needs_replan = True
                continue

        # Fallback if max attempts reached
        if self.ctx.consecutive_errors:
            yield self._terminal_error()
            return
        yield await self._fallback()

    def _tool_schemas_for_phase(self, phase: _AgentPhase) -> list[dict]:
        """Return the model-visible tools allowed for one executor phase."""

        if phase is not _AgentPhase.SYNTHESIZE:
            return list(self.ctx.tool_runtime.schemas)
        return [
            schema
            for schema in self.ctx.tool_runtime.schemas
            if (
                definition := get_tool_definition(schema["function"]["name"])
            ) is not None
            and definition.executor_protocol
        ]

    async def _step(
        self,
        date: str,
        model: Optional[str],
        reasoning: str,
        phase: _AgentPhase,
        documents_context: str,
        document_focus_context: str,
        last_result: Optional[List[Dict]],
        project_context: str = "",
    ) -> AsyncGenerator[InternalAgentStreamEvent, None]:
        """A single LLM reasoning step."""
        tool_schemas = self._tool_schemas_for_phase(phase)
        runtime_instructions = (
            get_runtime_instructions(tool_schemas)
            if phase is _AgentPhase.SYNTHESIZE
            else self.ctx.tool_runtime.runtime_instructions
        )
        system_prompt = get_agent_prompt(
            user_name=self.ctx.user_name,
            current_time=date,
            persona=self.ctx.agent.persona,
            agent_name=self.ctx.agent.name,
            documents_context=documents_context,
            document_focus_context=document_focus_context,
            agent_brain=self.ctx.brain,
            project_context=project_context,
            runtime_instructions=runtime_instructions,
            active_topics=self.ctx.active_topics,
            is_community=self.ctx.is_community,
            participants=self.ctx.current_participants,
            phase=phase,
            research_profile=self.ctx.research_profile,
        )

        user_message = build_user_message(self.ctx, last_result)
        self.ctx.clear_last_error()
        saw_tool_calls = False

        try:
            async for event in self.llm.stream_with_tools(
                system=system_prompt,
                user=user_message,
                tools=tool_schemas,
                model=model or self.llm.agent_model,
                temperature=self.ctx.temperature,
                reasoning=reasoning,
                aac_budget=self._aac_budget,
            ):
                if event["event"] == "tool_calls":
                    saw_tool_calls = True
                    yield event
                elif event["event"] == "step_completed" and not saw_tool_calls:
                    yield {
                        "event": "step_error",
                        "data": {
                            "kind": "formatting",
                            "message": (
                                "You must either call an investigative tool or call "
                                "submit_answer. Do not output raw text."
                            ),
                            "usage": event["data"]["usage"],
                        },
                    }
                else:
                    yield event
        except (ConfigurationError, LLMError) as e:
            logger.error(f"LLM API Stream failed: {e}")
            yield {
                "event": "step_error",
                "data": {
                    "kind": "provider",
                    "message": "LLM provider unavailable",
                },
            }

    async def _load_project_context(self) -> str:
        """Load canonical project context without making it a run blocker."""
        document_service = getattr(self.tools, "document_service", None)
        reader = getattr(document_service, "read_project_context", None)
        if reader is None:
            return ""
        try:
            content = await reader()
        except FileNotFoundError:
            return ""
        except Exception as exc:
            logger.warning(
                "AgentExecutor: project context unavailable ({})",
                type(exc).__name__,
            )
            return ""
        if not isinstance(content, str):
            return ""
        return content

    def _parse_tool_calls(
        self,
        calls: List[StreamToolCall],
        content: str,
    ) -> List[_ToolCall]:
        thinking = content.strip() or None
        return [
            _ToolCall(
                name=call["name"],
                args=self._safe_parse_args(call.get("arguments", "{}")),
                thinking=thinking,
                call_id=str(call.get("id") or uuid.uuid4()),
            )
            for call in calls
        ]

    def _record_step_error(self, message: str, kind: str) -> None:
        self.ctx.record_error(message)
        logger.warning(
            f"AgentExecutor: {kind} step failure "
            f"({self.ctx.consecutive_errors}/"
            f"{self.ctx.limits.max_consecutive_errors}): {message}"
        )

    def _terminal_error(self) -> ErrorEvent:
        self.ctx.finish_without_response()
        return {
            "event": "error",
            "data": {"message": PUBLIC_AGENT_FAILURE_MESSAGE},
        }

    @staticmethod
    def _safe_parse_args(json_str: str) -> Dict:
        """Parse tool arguments from JSON string with LLM formatting fixups."""
        # Try standard JSON
        parsed = safe_json_loads(json_str)
        if isinstance(parsed, dict):
            return parsed

        # Try to fix common LLM formatting issues (trailing commas, missing quotes)
        cleaned = json_str.strip()
        cleaned = re.sub(r",\s*([\]}])", r"\1", cleaned)

        parsed_clean = safe_json_loads(cleaned)
        if isinstance(parsed_clean, dict):
            return parsed_clean

        logger.warning(f"Failed to parse tool arguments: {json_str[:200]}")
        return {"_parse_error": True, "_raw": json_str[:500]}

    async def _execute_tools(
        self, tool_calls: List[_ToolCall], results_out: List[Dict]
    ) -> AsyncGenerator[Dict, None]:
        """Executes a batch of tool calls sequentially to avoid shared state races."""

        if self.ctx.call_count >= self.ctx.limits.max_calls:
            err_msg = f"Global call limit reached ({self.ctx.limits.max_calls})"
            results_out.append({"tool": "all", "error": err_msg})
            yield {
                "event": "tool_error",
                "data": {
                    "tool": "all",
                    "error": err_msg,
                    "call_id": self._global_limit_call_id(),
                },
            }
            return

        for call in tool_calls:
            if self.ctx.call_count >= self.ctx.limits.max_calls:
                err_msg = f"Global call limit reached ({self.ctx.limits.max_calls})"
                results_out.append({"tool": "all", "error": err_msg})
                yield {
                    "event": "tool_error",
                    "data": {
                        "tool": "all",
                        "error": err_msg,
                        "call_id": self._global_limit_call_id(),
                    },
                }
                return

            yield {
                "event": "tool_start",
                "data": {
                    "tool": call.name,
                    "args": call.args,
                    "thinking": call.thinking,
                    "call_id": call.call_id,
                },
            }

            try:
                if self.ctx.tool_limit_reached(call.name, self.ctx.limits):
                    error_message = f"Tool '{call.name}' has reached its call limit"
                    self.ctx.note_nonfatal_error(error_message)
                    results_out.append({"tool": call.name, "error": error_message})
                    yield {
                        "event": "tool_error",
                        "data": {
                            "tool": call.name,
                            "error": f"Call limit reached for {call.name}",
                            "call_id": call.call_id,
                        },
                    }
                    continue

                if self.ctx.is_duplicate(call.name, call.args):
                    error_message = (
                        f"Duplicate call to '{call.name}' with same arguments"
                    )
                    self.ctx.note_nonfatal_error(error_message)
                    results_out.append({"tool": call.name, "error": error_message})
                    yield {
                        "event": "tool_error",
                        "data": {
                            "tool": call.name,
                            "error": "Duplicate call skipped",
                            "call_id": call.call_id,
                        },
                    }
                    continue

                self.ctx.record_tool_call(call.name, call.args)

                if call.args.get("_parse_error"):
                    error_message = f"Failed to parse arguments for '{call.name}'"
                    self.ctx.note_nonfatal_error(error_message)
                    results_out.append({"tool": call.name, "error": error_message})
                    yield {
                        "event": "tool_error",
                        "data": {
                            "tool": call.name,
                            "error": "Argument parse failure",
                            "call_id": call.call_id,
                        },
                    }
                    continue

                async with asyncio.timeout(self.ctx.limits.tool_timeout):
                    result = await execute_tool(
                        self.tools,
                        call.name,
                        call.args,
                    )

                self.ctx.record_sources(
                    capture_tool_source_candidates(self.ctx, call, result)
                )

                summary, _ = summarize_result(call.name, result)
                model_result = localize_agent_tool_result(self.ctx, call.name, result)
                self.ctx.accumulate_tool_result(call.name, model_result)
                self.ctx.record_tool_success()
                results_out.append({"tool": call.name, "result": model_result})

                yield {
                    "event": "tool_end",
                    "data": {
                        "tool": call.name,
                        "result": summary,
                        "call_id": call.call_id,
                    },
                }

            except TimeoutError:
                message = (
                    "Tool execution timed out after "
                    f"{self.ctx.limits.tool_timeout:g} seconds"
                )
                logger.warning(f"Tool {call.name}: {message}")
                self.ctx.record_error(message)
                results_out.append({"tool": call.name, "error": message})
                yield {
                    "event": "tool_error",
                    "data": {
                        "tool": call.name,
                        "error": message,
                        "call_id": call.call_id,
                    },
                }
            except ToolExecutionError as e:
                if _is_local_reference_resolution_error(e.message):
                    await emit(
                        self.ctx.session_id,
                        "agent",
                        "local_reference_resolution_failed",
                        {
                            "pipeline": "agent_tool_loop",
                            "reference_type": _local_reference_type(call.name),
                            "reason": "unknown_or_wrong_type",
                        },
                    )
                self.ctx.record_error(e.message)
                results_out.append({"tool": call.name, "error": e.message})
                yield {
                    "event": "tool_error",
                    "data": {
                        "tool": call.name,
                        "error": e.message,
                        "call_id": call.call_id,
                    },
                }
            except Exception as e:
                logger.exception(f"Tool {call.name} unexpected failure: {e}")
                self.ctx.record_error("Internal tool failure")
                results_out.append(
                    {"tool": call.name, "error": "Internal tool failure"}
                )
                yield {
                    "event": "tool_error",
                    "data": {
                        "tool": call.name,
                        "error": "Internal tool failure",
                        "call_id": call.call_id,
                    },
                }

    def _global_limit_call_id(self) -> str:
        """Return a stable synthetic ID for a run-level, non-tool-specific limit."""

        return f"{self.ctx.run_id or 'agent'}:global-call-limit"

    def _accumulate_usage(self, usage: Optional[StreamUsage]):
        self.ctx.record_usage(usage)

    def _wrap_final_response(
        self,
        *,
        content: str,
        usage: StreamUsage,
        sources_consulted: List[SourceReferenceCandidate],
        artifact: Optional[Dict] = None,
    ) -> ResponseEvent:
        event: ResponseEvent = {
            "event": "response",
            "data": {
                "content": content,
                "usage": usage,
                "research_mode": self.ctx.research_profile.mode,
            },
        }
        if sources_consulted:
            event["data"]["sources_consulted"] = [
                candidate.model_dump(mode="json") for candidate in sources_consulted
            ]
        if artifact is not None:
            event["data"]["artifact"] = artifact
        return event

    def _complete_artifact(
        self,
        content: str,
        artifact: ArtifactDraft | None,
    ) -> ArtifactDraft | None:
        """Apply the selected mode's final artifact policy."""

        profile = self.ctx.research_profile
        expected_kind = profile.default_artifact_kind
        if artifact is None and expected_kind is not None:
            artifact = default_artifact_from_answer(content, kind=expected_kind)
        elif artifact is not None and expected_kind is not None:
            if artifact.kind != expected_kind:
                artifact = artifact.model_copy(update={"kind": expected_kind})
        return artifact

    async def _finalize_successfully(self, content: str) -> None:
        """Seal a successful run, then persist its agent's completion clock."""

        self.ctx.finalize(content)
        if self._on_successful_completion is None:
            return
        try:
            await self._on_successful_completion(self.ctx.agent.config.id)
        except Exception:
            # The completed response remains valid even if a secondary
            # last-turn statistic cannot be persisted right now.
            logger.exception(
                "AgentExecutor: failed to record successful turn for {}",
                self.ctx.agent.config.id,
            )

    async def _fallback(self) -> Dict:
        """Unified fallback when agent exhausts attempts."""
        logger.warning(
            "AgentExecutor: Entering fallback after "
            f"{self.ctx.attempt_count} attempts, "
            f"{self.ctx.call_count} tool calls. "
            f"Evidence: {self.ctx.has_any()}"
        )
        if self.ctx.has_any():
            summary = await self._generate_fallback_summary()
            content = summary or "I found information but couldn't summarize it."
            artifact = self._complete_artifact(content, None)
            await self._finalize_successfully(content)
            event = {
                "event": "response",
                "data": {
                    "content": content,
                    "usage": dict(self.ctx.usage),
                    "fallback": True,
                    "research_mode": self.ctx.research_profile.mode,
                },
            }
            if artifact is not None:
                event["data"]["artifact"] = artifact.model_dump(mode="json")
            if self.ctx.source_candidates:
                event["data"]["sources_consulted"] = [
                    candidate.model_dump(mode="json")
                    for candidate in self.ctx.source_candidates
                ]
            return event
        else:
            self.ctx.finish_without_response()
            return {
                "event": "clarification",
                "data": {
                    "question": "I'm having trouble with that. Could you rephrase?",
                    "usage": self.ctx.usage,
                    "fallback": True,
                },
            }

    async def _generate_fallback_summary(self) -> Optional[str]:
        """Generate a final response summary from accumulated evidence."""
        evidence_ctx = build_evidence_context(self.ctx)

        prompt = get_fallback_summary_prompt(
            self.ctx.user_name, self.ctx.user_query, evidence_ctx
        )

        return await self.llm.generate_text(
            system=(
                "You are a helpful assistant providing a summary of found information."
            ),
            user=prompt,
            temperature=0.3,
            aac_budget=self._aac_budget,
        )

    async def _manage_context_size(self):
        """Monitor accumulated evidence and summarize if it approaches token limits."""
        evidence_str = build_evidence_context(self.ctx)
        self.ctx.set_evidence_token_count(self.llm.count_tokens(evidence_str))

        if self.ctx.evidence_token_count > MAX_TOKEN_CHUNK_SIZE:
            logger.info(
                f"Evidence size ({self.ctx.evidence_token_count} tokens) "
                "exceeds limit. Summarizing..."
            )

            summary = await self._generate_evidence_summary(evidence_str)

            if summary:
                self.ctx.compact_evidence(summary)
            else:
                logger.warning(
                    "Evidence summarization failed. Truncating raw evidence as "
                    "fallback."
                )

            if not summary:
                self.ctx.compact_evidence(None)

            # Recalculate against the actual bounded state retained by the run.
            post_compaction = build_evidence_context(self.ctx)
            self.ctx.set_evidence_token_count(
                self.llm.count_tokens(post_compaction)
            )

    async def _generate_evidence_summary(self, evidence_text: str) -> Optional[str]:
        """Call LLM to condense existing evidence into a core summary."""
        prompt = (
            "I have gathered the following evidence regarding: "
            f"'{self.ctx.user_query}'\n\n"
            f"{evidence_text}\n\n"
            "Summarize the key details, connections, and relevant information into "
            "a concise summary. Preserve the relevant evidence and any local "
            "references already present; never invent or request system IDs."
        )

        try:
            return await self.llm.generate_text(
                system=(
                    "You are a data librarian. Condense retrieved evidence into "
                    "a factual summary without losing key details."
                ),
                user=prompt,
                temperature=0.0,  # Strict factual summary
                aac_budget=self._aac_budget,
            )
        except (ConfigurationError, LLMError) as e:
            logger.error(f"Failed to summarize evidence: {e}")
            return None

    async def _emit_llm_call(self, model: Optional[str], reasoning: str):
        await emit(
            self.ctx.session_id,
            "agent",
            "llm_call",
            {
                "run_id": self.ctx.run_id,
                "model": model,
                "reasoning": reasoning,
                "turn": self.ctx.attempt_count,
                "evidence_state": {
                    "profiles": len(self.ctx.profiles),
                    "messages": len(self.ctx.messages),
                    "graph": len(self.ctx.graph),
                },
            },
            verbose_only=True,
        )
