from __future__ import annotations

import re
import uuid
from typing import AsyncGenerator, Dict, List, Optional, Union
from zoneinfo import ZoneInfo

from loguru import logger

from common.exceptions import ToolExecutionError
from common.schema.tool_schema import get_filtered_schemas
from common.utils.events import emit
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now
from infrastructure.llm_client import LLMService
from knoggin_server.agent.formatters import (
    format_entity_results,
    format_files_context,
    format_graph_results,
    format_retrieved_messages,
)
from knoggin_server.agent.internals import (
    AgentContext,
    build_evidence_context,
    build_user_message,
    execute_tool,
    summarize_result,
    update_accumulators,
)
from knoggin_server.agent.system_prompt import (
    get_agent_prompt,
    get_fallback_summary_prompt,
)
from knoggin_server.agent.tools.registry import Tools
from knoggin_server.agent.types import ClarificationRequest, FinalResponse, ToolCall
from knoggin_server.knowledge.services.memory_service import MemoryManager


class AgentExecutor:
    """
    Handles the reasoning loop, tool execution, and evidence gathering for an agent run.
    """

    def __init__(
        self,
        ctx: AgentContext,
        llm: LLMService,
        tools: Tools,
        memory_mgr: MemoryManager,
    ):
        self.ctx = ctx
        self.llm = llm
        self.tools = tools
        self.memory_mgr = memory_mgr

    async def execute(
        self,
        user_timezone: Optional[str] = None,
        model: Optional[str] = None,
        enabled_tools: Optional[List[str]] = None,
        simulated_date: Optional[str] = None,
        agent_temperature: float = 0.7,
        agent_instructions: Optional[str] = None,
        agent_directives: Optional[str] = None,
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[Dict, None]:
        """Runs the reasoning loop and yields events."""

        # Prepare environment
        tz = ZoneInfo(user_timezone) if user_timezone else ZoneInfo("UTC")
        current_time = simulated_date or get_now().astimezone(tz).strftime(
            "%Y-%m-%d %H:%M %Z"
        )

        if self.memory_mgr:
            (
                memory_context,
                directives_str,
            ) = await self.memory_mgr.load_prompt_strings(self.ctx.hot_topics)
        else:
            memory_context, directives_str = "", ""

        files_context = ""
        if self.tools.file_rag:
            manifest = self.tools.get_file_manifest()
            if manifest:
                files_context = format_files_context(manifest)

        a_directives = (
            agent_directives if agent_directives is not None else directives_str
        )

        last_result = None

        # Reasoning Loop
        needs_replanning = False
        needs_final_synthesis = False

        while self.ctx.state.attempt_count < self.ctx.config.max_attempts:
            if (
                self.ctx.state.consecutive_errors
                >= self.ctx.config.max_consecutive_errors
            ):
                logger.warning(
                    f"AgentExecutor: Global consecutive error limit reached ({self.ctx.state.consecutive_errors}). Aborting."
                )
                break

            self.ctx.state.attempt_count += 1

            current_model = None
            current_reasoning = None

            if self.ctx.state.attempt_count == 1 or needs_replanning or needs_final_synthesis:
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

            # Reset flags so the next turn defaults back to Librarian unless triggered again
            needs_replanning = False
            needs_final_synthesis = False

            # Monitoring/Emits
            await self._emit_llm_call(current_model, current_reasoning)

            # Call LLM for this step
            async for event in self._step(
                current_time,
                current_model,
                current_reasoning,
                current_mode_name,
                enabled_tools,
                memory_context,
                files_context,
                a_directives,
                agent_temperature,
                agent_instructions or "",
                last_result,
                client_tools,
            ):
                event_type = event.get("event")
                data = event.get("data")

                if event_type == "formatting_error":
                    logger.warning(
                        "AgentExecutor: Model emitted text without tool calls. "
                        "Returning error to loop."
                    )
                    current_results = [{"error": data}]
                    last_result = current_results
                    break

                if event_type == "done":
                    # If step returned ToolCalls (List[ToolCall]), execute them
                    if isinstance(data, list):
                        current_results = []

                        if not data:
                            logger.warning(
                                "AgentExecutor: Librarian stalled with empty tool calls."
                            )
                            needs_replanning = True
                            break

                        # Intercept submit_answer before tool execution
                        submit = next(
                            (tc for tc in data if tc.name == "submit_answer"),
                            None,
                        )
                        if submit:
                            content = submit.args.get("content", "")
                            response = FinalResponse(content=content)

                            if current_mode_name == "Librarian":
                                logger.info(
                                    "AgentExecutor: Librarian believes we have the answer. Promoting to Architect for final synthesis."
                                )
                                needs_final_synthesis = True
                                break

                            yield self._wrap_final_response(response)
                            return

                        # Intercept clarification before tool execution
                        clarification = next(
                            (tc for tc in data if tc.name == "request_clarification"),
                            None,
                        )
                        if clarification:
                            question = clarification.args.get(
                                "question", "Could you clarify?"
                            )
                            yield {
                                "event": "clarification",
                                "data": {
                                    "question": question,
                                    "usage": self.ctx.state.usage,
                                },
                            }
                            return

                        # Intercept replanning request before tool execution
                        replanning = next(
                            (tc for tc in data if tc.name == "request_replanning"),
                            None,
                        )
                        if replanning:
                            reason = replanning.args.get("reason", "No reason provided")
                            logger.info(
                                f"AgentExecutor: Librarian requested re-planning. Reason: {reason}"
                            )
                            needs_replanning = True
                            break

                        async for tool_event in self._execute_tools(
                            data, current_results
                        ):
                            yield tool_event

                        last_result = current_results
                        await self._manage_context_size()

                        # Track consecutive empty results for deterministic replanning
                        all_empty = all(
                            "error" in r or not r.get("result", {}).get("data")
                            for r in current_results
                        ) if current_results else True

                        if all_empty:
                            self.ctx.state.consecutive_empty_results += 1
                            if (
                                self.ctx.state.consecutive_empty_results
                                >= self.ctx.config.empty_result_replan_threshold
                            ):
                                logger.info(
                                    f"AgentExecutor: {self.ctx.state.consecutive_empty_results} "
                                    "consecutive empty results. Forcing replan."
                                )
                                needs_replanning = True
                                self.ctx.state.consecutive_empty_results = 0
                        else:
                            self.ctx.state.consecutive_empty_results = 0

                        break
                else:
                    yield event

        # Fallback if max attempts reached
        yield await self._fallback()

    async def _step(
        self,
        date: str,
        model: Optional[str],
        reasoning: str,
        current_mode: str,
        enabled_tools: Optional[List[str]],
        memory_context: str,
        files_context: str,
        directives: str,
        temp: float,
        agent_instructions: str,
        last_result: Optional[List[Dict]],
        client_tools: Optional[List[Dict]] = None,
    ) -> AsyncGenerator[Dict, None]:
        """A single LLM reasoning step."""

        system_prompt = get_agent_prompt(
            self.ctx.user_name,
            date,
            self.ctx.agent_persona,
            self.ctx.agent_name,
            memory_context=memory_context,
            files_context=files_context,
            agent_directives=directives,
            instructions=agent_instructions,
            is_community=self.ctx.is_community,
            participants=self.ctx.current_participants,
            current_mode=current_mode,
            active_topics=self.ctx.active_topics,
        )

        user_message = build_user_message(self.ctx, last_result)
        self.ctx.state.last_error = None
        active_schemas = get_filtered_schemas(enabled_tools)
        if client_tools:
            active_schemas = active_schemas + client_tools

        pending_tool_calls = []
        content_accumulator = ""

        try:
            async for chunk in self.llm.call_llm_with_tools_streaming(
                system=system_prompt,
                user=user_message,
                tools=active_schemas,
                model=model or self.llm.agent_model,
                temperature=temp,
                reasoning=reasoning,
            ):
                chunk_type = chunk.get("type")

                if chunk_type == "token":
                    content_accumulator += chunk["content"]
                    yield {"event": "token", "data": {"content": chunk["content"]}}
                elif chunk_type == "thinking":
                    yield {"event": "thinking", "data": {"content": chunk["content"]}}
                elif chunk_type == "tool_calls":
                    for call in chunk.get("calls", []):
                        args = self._safe_parse_args(call.get("arguments", "{}"))
                        pending_tool_calls.append(
                            ToolCall(
                                name=call["name"],
                                args=args,
                                thinking=content_accumulator.strip() or None,
                                call_id=call.get("id", str(uuid.uuid4())),
                            )
                        )
                elif chunk_type == "done":
                    u = chunk.get("usage")
                    if u:
                        u["approximate"] = True
                    self._accumulate_usage(u)
                    if not pending_tool_calls:
                        # Model chatted without calling tools. This is a formatting error.
                        yield {
                            "event": "formatting_error",
                            "data": "Error: You must either call an investigative tool or call submit_answer. Do not output raw text.",
                        }
                    else:
                        yield {"event": "done", "data": pending_tool_calls}
                elif chunk_type == "error":
                    yield {"event": "error", "data": {"message": chunk["message"]}}
        except Exception as e:
            logger.error(f"LLM API Stream failed: {e}")
            yield {"event": "error", "data": {"message": f"LLM API failure: {str(e)}"}}

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

        if self.ctx.state.call_count >= self.ctx.config.max_calls:
            err_msg = f"Global call limit reached ({self.ctx.config.max_calls})"
            results_out.append({"tool": "all", "error": err_msg})
            yield {"event": "tool_error", "data": {"tool": "all", "error": err_msg}}
            return

        for call in tool_calls:
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
                if self.ctx.state.tool_limit_reached(call.name, self.ctx.config):
                    self.ctx.state.last_error = (
                        f"Tool '{call.name}' has reached its call limit"
                    )
                    results_out.append(
                        {"tool": call.name, "error": self.ctx.state.last_error}
                    )
                    yield {
                        "event": "tool_error",
                        "data": {
                            "tool": call.name,
                            "error": f"Call limit reached for {call.name}",
                        },
                    }
                    continue

                if self.ctx.state.is_duplicate(call.name, call.args):
                    self.ctx.state.last_error = (
                        f"Duplicate call to '{call.name}' with same arguments"
                    )
                    results_out.append(
                        {"tool": call.name, "error": self.ctx.state.last_error}
                    )
                    yield {
                        "event": "tool_error",
                        "data": {"tool": call.name, "error": "Duplicate call skipped"},
                    }
                    continue

                self.ctx.state.record_call(call.name, call.args)

                if call.name == "request_clarification":
                    question = call.args.get("question", "Could you clarify?")
                    yield {"event": "clarification", "data": {"question": question}}
                    return

                if call.args.get("_parse_error"):
                    self.ctx.state.last_error = (
                        f"Failed to parse arguments for '{call.name}'"
                    )
                    results_out.append(
                        {"tool": call.name, "error": self.ctx.state.last_error}
                    )
                    yield {
                        "event": "tool_error",
                        "data": {"tool": call.name, "error": "Argument parse failure"},
                    }
                    continue

                result = await execute_tool(self.tools, call.name, call.args)

                summary, _ = summarize_result(call.name, result)
                update_accumulators(self.ctx, call.name, result)

                self.ctx.state.consecutive_errors = 0
                results_out.append({"tool": call.name, "result": result})

                yield {
                    "event": "tool_end",
                    "data": {
                        "tool": call.name,
                        "result": summary,
                        "call_id": call.call_id,
                    },
                }

            except ToolExecutionError as e:
                self.ctx.state.last_error = e.message
                self.ctx.state.consecutive_errors += 1
                yield {
                    "event": "tool_error",
                    "data": {"tool": call.name, "error": e.message},
                }
            except Exception as e:
                logger.exception(f"Tool {call.name} unexpected failure: {e}")
                self.ctx.state.consecutive_errors += 1
                yield {
                    "event": "tool_error",
                    "data": {"tool": call.name, "error": "Internal tool failure"},
                }

    def _accumulate_usage(self, usage: Optional[Dict]):
        if usage:
            self.ctx.state.usage["prompt_tokens"] += usage.get("prompt_tokens", 0)
            self.ctx.state.usage["completion_tokens"] += usage.get(
                "completion_tokens", 0
            )
            self.ctx.state.usage["total_tokens"] += usage.get("total_tokens", 0)

    def _wrap_final_response(
        self, response: Union[FinalResponse, ClarificationRequest]
    ) -> Dict:
        if isinstance(response, FinalResponse):
            return {
                "event": "response",
                "data": {
                    "content": response.content,
                    "usage": self.ctx.state.usage,
                    "sources": self.ctx.evidence.sources
                    if self.ctx.evidence.sources
                    else None,
                },
            }
        else:
            return {
                "event": "clarification",
                "data": {"question": response.question, "usage": self.ctx.state.usage},
            }

    async def _fallback(self) -> Dict:
        """Unified fallback when agent exhausts attempts."""
        logger.warning(
            f"AgentExecutor: Entering fallback after {self.ctx.state.attempt_count} attempts, "
            f"{self.ctx.state.call_count} tool calls. "
            f"Evidence: {self.ctx.evidence.has_any()}"
        )
        if self.ctx.evidence.has_any():
            summary = await self._generate_fallback_summary()
            return {
                "event": "response",
                "data": {
                    "content": summary
                    or "I found information but couldn't summarize it.",
                    "usage": self.ctx.state.usage,
                    "sources": self.ctx.evidence.sources
                    if self.ctx.evidence.sources
                    else None,
                    "fallback": True,
                },
            }
        else:
            return {
                "event": "clarification",
                "data": {
                    "question": "I'm having trouble with that. Could you rephrase?",
                    "usage": self.ctx.state.usage,
                    "fallback": True,
                },
            }

    async def _generate_fallback_summary(self) -> Optional[str]:
        """Generate a final response summary from accumulated evidence."""
        evidence_ctx = ""
        if self.ctx.evidence.profiles:
            evidence_ctx += f"\nProfiles FOUND:\n{format_entity_results(self.ctx.evidence.profiles)}\n"
        if self.ctx.evidence.messages:
            evidence_ctx += f"\nRelevant Messages:\n{format_retrieved_messages(self.ctx.evidence.messages)}\n"
        if self.ctx.evidence.graph:
            evidence_ctx += (
                f"\nGraph Context:\n{format_graph_results(self.ctx.evidence.graph)}\n"
            )

        prompt = get_fallback_summary_prompt(
            self.ctx.user_name, self.ctx.user_query, evidence_ctx
        )

        return await self.llm.call_llm(
            system="You are a helpful assistant providing a summary of found information.",
            user=prompt,
            temperature=0.3,
        )

    async def _manage_context_size(self):
        """Monitor accumulated evidence and summarize if it approaches token limits."""
        evidence_str = build_evidence_context(self.ctx.evidence)
        self.ctx.evidence.token_count = self.llm.count_tokens(evidence_str)

        if self.ctx.evidence.token_count > 10000:
            logger.info(
                f"Evidence size ({self.ctx.evidence.token_count} tokens) exceeds limit. Summarizing..."
            )

            summary = await self._generate_evidence_summary(evidence_str)

            if summary:
                self.ctx.evidence.summary = summary
            else:
                logger.warning(
                    "Evidence summarization failed. Truncating raw evidence as fallback."
                )

            self.ctx.evidence.messages = self.ctx.evidence.messages[-5:]
            self.ctx.evidence.profiles = self.ctx.evidence.profiles[-5:]
            self.ctx.evidence.graph = self.ctx.evidence.graph[-15:]
            self.ctx.evidence.facts = []
            self.ctx.evidence.paths = []
            self.ctx.evidence.hierarchy = []

            # Re-calculate token count
            if summary:
                self.ctx.evidence.token_count = self.llm.count_tokens(summary)
            else:
                new_evidence_str = build_evidence_context(self.ctx.evidence)
                self.ctx.evidence.token_count = self.llm.count_tokens(new_evidence_str)

    async def _generate_evidence_summary(self, evidence_text: str) -> Optional[str]:
        """Call LLM to condense existing evidence into a core summary."""
        prompt = (
            f"I have gathered the following evidence regarding: '{self.ctx.user_query}'\n\n"
            f"{evidence_text}\n\n"
            "Summarize the key facts, connections, and relevant information into a concise summary. "
            "Keep important IDs (message IDs, entity IDs) if they are critical for further operations."
        )

        try:
            return await self.llm.call_llm(
                system="You are a data librarian. Condense retrieved evidence into a factual summary without losing key details.",
                user=prompt,
                temperature=0.0,  # Strict factual summary
            )
        except Exception as e:
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
                "turn": self.ctx.state.attempt_count,
                "evidence_state": {
                    "profiles": len(self.ctx.evidence.profiles),
                    "messages": len(self.ctx.evidence.messages),
                    "graph": len(self.ctx.evidence.graph),
                },
            },
            verbose_only=True,
        )
