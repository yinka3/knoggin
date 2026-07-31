from __future__ import annotations

import asyncio
import re
import uuid
from typing import AsyncGenerator, Dict, List, Optional
from zoneinfo import ZoneInfo

from loguru import logger

from common.exceptions import ConfigurationError, LLMError, ToolExecutionError
from common.schema.agent.stream import (
    ErrorEvent,
    InternalAgentStreamEvent,
    PublicAgentStreamEvent,
    ResponseEvent,
    StreamToolCall,
    StreamUsage,
)
from common.schema.source.references import SourceReferenceCandidate
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now
from core.agent.formatters import (
    format_document_focus_context,
    format_documents_context,
    format_entity_results,
    format_graph_results,
    format_retrieved_messages,
)
from core.agent.internals import (
    build_evidence_context,
    build_user_message,
    execute_tool,
    localize_agent_tool_result,
    summarize_result,
)
from core.agent.run import AgentRun
from core.agent.source_adapters import capture_tool_source_candidates
from core.agent.system_prompt import (
    get_agent_prompt,
    get_fallback_summary_prompt,
)
from core.agent.tools.registry import (
    Tools,
    apply_tool_error_hooks,
    apply_tool_result_hooks,
    configure_tool_authorization,
    get_active_tool_names,
    get_runtime_instructions,
    get_tool_schemas,
)
from core.agent.types import ToolCall
from infrastructure.llm_client import LLMService

MAX_TOKEN_CHUNK_SIZE = 10000


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
    ):
        self.ctx = ctx
        self.llm = llm
        self.tools = tools
        for candidate in ctx.initial_source_candidates:
            ctx.record_source(candidate)

    async def execute(
        self,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        simulated_date: Optional[str] = None,
        agent_temperature: float = 0.7,
        agent_brain: Optional[str] = None,
        agent_directives: Optional[str] = None,
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[PublicAgentStreamEvent, None]:
        """Run one agent execution and discard its model-only UUID handles."""

        try:
            async for event in self._execute_run(
                user_timezone=user_timezone,
                model=model,
                enabled_tools=enabled_tools,
                simulated_date=simulated_date,
                agent_temperature=agent_temperature,
                agent_brain=agent_brain,
                agent_directives=agent_directives,
                client_tools=client_tools,
            ):
                yield event
        finally:
            self.ctx.release()

    async def _execute_run(
        self,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        simulated_date: Optional[str] = None,
        agent_temperature: float = 0.7,
        agent_brain: Optional[str] = None,
        agent_directives: Optional[str] = None,
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[PublicAgentStreamEvent, None]:
        """Runs the reasoning loop and yields events."""

        # Prepare environment
        tz = ZoneInfo(user_timezone) if user_timezone else ZoneInfo("UTC")
        current_time = simulated_date or get_now().astimezone(tz).strftime(
            "%Y-%m-%d %H:%M %Z"
        )

        documents_context = ""
        if self.tools.document_service:
            manifest = await self.tools.get_document_manifest()
            if manifest:
                documents_context = format_documents_context(manifest)
        document_focus_context = format_document_focus_context(
            getattr(self.tools, "document_focus", None)
        )

        a_directives = agent_directives or ""

        last_result = None

        # Reasoning Loop
        needs_replanning = False
        needs_final_synthesis = False

        while self.ctx.attempt_count < self.ctx.limits.max_attempts:
            if (
                self.ctx.consecutive_errors
                >= self.ctx.limits.max_consecutive_errors
            ):
                yield self._terminal_error()
                return

            if not self.ctx.begin_attempt():
                break

            current_model = None
            current_reasoning = None

            if (
                self.ctx.attempt_count == 1
                or needs_replanning
                or needs_final_synthesis
            ):
                # Architect Mode: Strategic planning or final synthesis
                current_mode_name = "Architect"
                current_model = model or self.llm.agent_model
                current_reasoning = "high"
                if needs_replanning:
                    logger.info(
                        "AgentExecutor: Escalating back to Architect for re-planning."
                    )
                elif needs_final_synthesis:
                    logger.info(
                        "AgentExecutor: Architect performing final synthesis/review."
                    )
            else:
                # Librarian Mode: Execution, use the lighter extraction model
                current_mode_name = "Librarian"
                current_model = model or self.llm.extraction_model
                current_reasoning = "medium"

            # Reset flags so the next turn defaults to Librarian.
            needs_replanning = False
            needs_final_synthesis = False

            # Monitoring/Emits
            await self._emit_llm_call(current_model, current_reasoning)

            # Call LLM for this step
            pending_tool_calls: List[ToolCall] = []
            step_failed = False
            step_completed = False

            async for event in self._step(
                current_time,
                current_model,
                current_reasoning,
                current_mode_name,
                enabled_tools,
                documents_context,
                document_focus_context,
                a_directives,
                agent_temperature,
                agent_brain or "",
                last_result,
                client_tools,
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
                        response = self._wrap_final_response(
                            content=content,
                            usage=dict(self.ctx.usage),
                            sources=list(self.ctx.sources),
                            sources_consulted=list(self.ctx.source_candidates),
                        )

                        if current_mode_name == "Librarian":
                            logger.info(
                                "AgentExecutor: Librarian believes we have the answer. "
                                "Promoting to Architect for final synthesis."
                            )
                            needs_final_synthesis = True
                            break

                        self.ctx.finalize(content)
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

                    replanning = next(
                        (
                            call
                            for call in pending_tool_calls
                            if call.name == "request_replanning"
                        ),
                        None,
                    )
                    if replanning:
                        reason = replanning.args.get("reason", "No reason provided")
                        logger.info(
                            "AgentExecutor: Librarian requested re-planning. "
                            f"Reason: {reason}"
                        )
                        needs_replanning = True
                        break

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
                            needs_replanning = True
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
                continue

        # Fallback if max attempts reached
        if self.ctx.consecutive_errors:
            yield self._terminal_error()
            return
        yield await self._fallback()

    async def _step(
        self,
        date: str,
        model: Optional[str],
        reasoning: str,
        current_mode: str,
        enabled_tools: Optional[List[str]],
        documents_context: str,
        document_focus_context: str,
        directives: str,
        temp: float,
        agent_brain: str,
        last_result: Optional[List[Dict]],
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[InternalAgentStreamEvent, None]:
        """A single LLM reasoning step."""
        active_schemas = get_tool_schemas(enabled_tools)
        if client_tools:
            active_schemas = active_schemas + client_tools

        active_tool_names = get_active_tool_names(active_schemas)
        runtime_instructions = get_runtime_instructions(self.ctx, active_tool_names)

        system_prompt = get_agent_prompt(
            user_name=self.ctx.scope.user_name,
            current_time=date,
            persona=self.ctx.agent.persona,
            agent_name=self.ctx.agent.name,
            documents_context=documents_context,
            document_focus_context=document_focus_context,
            agent_directives=directives,
            agent_brain=agent_brain,
            runtime_instructions=runtime_instructions,
            active_topics=self.ctx.active_topics,
            is_community=self.ctx.is_community,
            participants=self.ctx.current_participants,
            current_mode=current_mode,
        )

        user_message = build_user_message(self.ctx, last_result)
        self.ctx.clear_last_error()
        configure_tool_authorization(
            self.tools,
            active_schemas,
            user_name=self.ctx.scope.user_name,
            agent_id=self.ctx.agent.config.id or getattr(self.tools, "agent_id", ""),
            project_id=(
                self.ctx.scope.project_id or str(getattr(self.tools, "project_id", ""))
            ),
            session_id=self.ctx.scope.session_id,
            run_id=self.ctx.run_id,
        )

        saw_tool_calls = False

        try:
            async for event in self.llm.stream_with_tools(
                system=system_prompt,
                user=user_message,
                tools=active_schemas,
                model=model or self.llm.agent_model,
                temperature=temp,
                reasoning=reasoning,
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
                    "message": f"LLM API failure: {str(e)}",
                },
            }

    def _parse_tool_calls(
        self,
        calls: List[StreamToolCall],
        content: str,
    ) -> List[ToolCall]:
        thinking = content.strip() or None
        return [
            ToolCall(
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
        message = self.ctx.last_error or "Agent execution failed"
        self.ctx.finish_without_response()
        return {
            "event": "error",
            "data": {
                "message": (
                    "Agent stopped after "
                    f"{self.ctx.consecutive_errors} consecutive errors: "
                    f"{message}"
                )
            },
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
        self, tool_calls: List[ToolCall], results_out: List[Dict]
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
                    results_out.append(
                        {"tool": call.name, "error": error_message}
                    )
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
                    results_out.append(
                        {"tool": call.name, "error": error_message}
                    )
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

                if call.name == "request_clarification":
                    question = call.args.get("question", "Could you clarify?")
                    self.ctx.finish_without_response()
                    yield {"event": "clarification", "data": {"question": question}}
                    return

                if call.args.get("_parse_error"):
                    error_message = f"Failed to parse arguments for '{call.name}'"
                    self.ctx.note_nonfatal_error(error_message)
                    results_out.append(
                        {"tool": call.name, "error": error_message}
                    )
                    yield {
                        "event": "tool_error",
                        "data": {
                            "tool": call.name,
                            "error": "Argument parse failure",
                            "call_id": call.call_id,
                        },
                    }
                    continue

                missing = object()
                previous_short_uuid_references = getattr(
                    self.tools, "short_uuid_references", missing
                )
                self.tools.short_uuid_references = self.ctx.short_uuid_references
                try:
                    async with asyncio.timeout(self.ctx.limits.tool_timeout):
                        result = await execute_tool(
                            self.tools,
                            call.name,
                            call.args,
                        )
                finally:
                    if previous_short_uuid_references is missing:
                        delattr(self.tools, "short_uuid_references")
                    else:
                        self.tools.short_uuid_references = (
                            previous_short_uuid_references
                        )

                await apply_tool_result_hooks(
                    self.ctx,
                    self.tools,
                    call.name,
                    result,
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
                handled_as_maintenance = await apply_tool_error_hooks(
                    self.ctx,
                    self.tools,
                    call.name,
                )
                if not handled_as_maintenance:
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
                        self.ctx.scope.session_id,
                        "agent",
                        "local_reference_resolution_failed",
                        {
                            "pipeline": "agent_tool_loop",
                            "reference_type": _local_reference_type(call.name),
                            "reason": "unknown_or_wrong_type",
                        },
                    )
                handled_as_maintenance = await apply_tool_error_hooks(
                    self.ctx, self.tools, call.name
                )
                if not handled_as_maintenance:
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
                handled_as_maintenance = await apply_tool_error_hooks(
                    self.ctx, self.tools, call.name
                )
                if not handled_as_maintenance:
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
        sources: List[Dict],
        sources_consulted: List[SourceReferenceCandidate],
    ) -> ResponseEvent:
        event: ResponseEvent = {
            "event": "response",
            "data": {
                "content": content,
                "usage": usage,
                "sources": sources or None,
            },
        }
        if sources_consulted:
            event["data"]["sources_consulted"] = [
                candidate.model_dump(mode="json") for candidate in sources_consulted
            ]
        return event

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
            self.ctx.finalize(content)
            event = {
                "event": "response",
                "data": {
                    "content": content,
                    "usage": dict(self.ctx.usage),
                    "sources": list(self.ctx.sources) or None,
                    "fallback": True,
                },
            }
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
        evidence_ctx = ""
        if self.ctx.profiles:
            evidence_ctx += (
                "\nProfiles FOUND:\n"
                f"{format_entity_results(self.ctx.profiles)}\n"
            )
        if self.ctx.messages:
            evidence_ctx += (
                "\nRelevant Messages:\n"
                f"{format_retrieved_messages(self.ctx.messages)}\n"
            )
        if self.ctx.graph:
            evidence_ctx += (
                f"\nGraph Context:\n{format_graph_results(self.ctx.graph)}\n"
            )

        prompt = get_fallback_summary_prompt(
            self.ctx.scope.user_name, self.ctx.user_query, evidence_ctx
        )

        return await self.llm.generate_text(
            system=(
                "You are a helpful assistant providing a summary of found information."
            ),
            user=prompt,
            temperature=0.3,
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

            # Re-calculate token count
            if summary:
                self.ctx.set_evidence_token_count(self.llm.count_tokens(summary))
            else:
                new_evidence_str = build_evidence_context(self.ctx)
                self.ctx.set_evidence_token_count(
                    self.llm.count_tokens(new_evidence_str)
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
            )
        except (ConfigurationError, LLMError) as e:
            logger.error(f"Failed to summarize evidence: {e}")
            return None

    async def _emit_llm_call(self, model: Optional[str], reasoning: str):
        await emit(
            self.ctx.scope.session_id,
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
