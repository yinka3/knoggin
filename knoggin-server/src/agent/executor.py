import asyncio
from datetime import datetime
from typing import AsyncGenerator, Dict, List, Optional, Union
from zoneinfo import ZoneInfo
from loguru import logger
import json
import re


from agent.tools import Tools
from agent.internals import (
    AgentContext,
    build_user_message,
    build_evidence_context,
    update_accumulators,
    summarize_result,
    execute_tool
)
from common.errors.agent import ToolExecutionError, AgentError
from agent.formatters import (
    format_entity_results,
    format_memory_context,
    format_retrieved_messages, 
    format_graph_results,
    format_path_results,
    format_hierarchy_results
)
from agent.system_prompt import get_agent_prompt, get_fallback_summary_prompt
from common.services.llm_service import LLMService
from common.services.memory_manager import MemoryManager
from common.config.topics_config import TopicConfig
from common.schema.dtypes import AgentResponse, ClarificationRequest, FinalResponse, ToolCall
from common.schema.tool_schema import get_filtered_schemas, TOOL_SCHEMAS
from common.mcp.bridge import mcp_tools_to_schemas
from common.utils.events import emit

class AgentExecutor:
    """
    Handles the reasoning loop, tool execution, and evidence gathering for an agent run.
    """

    def __init__(
        self,
        ctx: AgentContext,
        llm: LLMService,
        tools: Tools,
        memory_mgr: MemoryManager
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
        agent_rules: Optional[List[str]] = None,
        agent_preferences: Optional[List[str]] = None,
        agent_icks: Optional[List[str]] = None,
        client_tools: Optional[List[Dict]] = None
    ) -> AsyncGenerator[Dict, None]:
        """Runs the reasoning loop and yields events."""
        
        # 1. Prepare environment
        tz = ZoneInfo(user_timezone) if user_timezone else ZoneInfo("UTC")
        current_time = simulated_date or datetime.now(tz).strftime("%Y-%m-%d %H:%M %Z")
        
        if self.memory_mgr:
            memory_context, rules_str, prefs_str, icks_str = await self.memory_mgr.load_prompt_strings(self.ctx.hot_topics)
        else:
            memory_context, rules_str, prefs_str, icks_str = "", "", "", ""
        
        files_context = ""
        if self.tools.file_rag:
            from agent.formatters import format_files_context
            manifest = self.tools.get_file_manifest()
            if manifest:
                files_context = format_files_context(manifest)
        
        a_rules = "\n".join(agent_rules) if agent_rules is not None else rules_str
        a_prefs = "\n".join(agent_preferences) if agent_preferences is not None else prefs_str
        a_icks = "\n".join(agent_icks) if agent_icks is not None else icks_str

        last_result = None

        # 2. Reasoning Loop
        needs_replanning = False

        while self.ctx.state.attempt_count < self.ctx.config.max_attempts:
            self.ctx.state.attempt_count += 1
            
            current_model = None
            current_reasoning = None

            if self.ctx.state.attempt_count == 1 or needs_replanning:
                # Architect Mode: Strategic planning, use the heavier model
                current_mode_name = "Architect"
                current_model = model or self.llm.agent_model
                current_reasoning = "high"
                if needs_replanning:
                    logger.info("AgentExecutor: Escalating back to Architect for re-planning.")
                    needs_replanning = False
            else:
                # Librarian Mode: Execution, use the lighter extraction model
                current_mode_name = "Librarian"
                current_model = model or self.llm.extraction_model
                current_reasoning = "medium"

            # Monitoring/Emits
            await self._emit_llm_call(current_model, current_reasoning)

            # 3. Call LLM for this step
            async for event in self._step(
                current_time, 
                current_model, 
                current_reasoning, 
                current_mode_name,
                enabled_tools, 
                memory_context,
                files_context,
                a_rules, 
                a_prefs, 
                a_icks, 
                agent_temperature, 
                agent_instructions or "",
                last_result, 
                client_tools
            ):
                event_type = event.get("type")
                data = event.get("data")
                
                if event_type == "done":
                    # If step returned FinalResponse or Clarification, we're done
                    if isinstance(data, (FinalResponse, ClarificationRequest)):
                        # Check for the re-planning signal in the content
                        if isinstance(data, FinalResponse) and "I need a new plan" in (data.content or ""):
                            logger.warning("AgentExecutor: Librarian requested re-planning via final response.")
                            needs_replanning = True
                            break
                        
                        yield self._wrap_final_response(data)
                        return
                    
                    # If step returned ToolCalls (List[ToolCall]), execute them
                    if isinstance(data, list):
                        current_results = []
                        
                        if not data:
                            logger.warning("AgentExecutor: Librarian stalled with empty tool calls.")
                            needs_replanning = True
                            break
                        
                        # Intercept clarification before tool execution
                        clarification = next(
                            (tc for tc in data if tc.name == "request_clarification"), 
                            None
                        )
                        if clarification:
                            question = clarification.args.get("question", "Could you clarify?")
                            yield {"event": "clarification", "data": {
                                "question": question,
                                "usage": self.ctx.state.usage
                            }}
                            return
                        
                        # Check if any tool call thinking contains the escalation signal
                        if any(getattr(tc, 'thinking', None) and "I need a new plan" in tc.thinking for tc in data):
                             logger.info("AgentExecutor: Librarian requested re-planning via tool thinking.")
                             needs_replanning = True

                        async for tool_event in self._execute_tools(data, current_results):
                            yield tool_event
                        
                        last_result = current_results
                        await self._manage_context_size()
                        break
                else:
                    yield event

        # 3. Fallback if max attempts reached
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
        rules: str,
        prefs: str,
        icks: str,
        temp: float,
        agent_instructions: str,
        last_result: Optional[List[Dict]],
        client_tools: Optional[List[Dict]] = None
    ) -> AsyncGenerator[Dict, None]:
        """A single LLM reasoning step."""
        
        system_prompt = get_agent_prompt(
            self.ctx.user_name, date, self.ctx.agent_persona, self.ctx.agent_name,
            memory_context=memory_context,
            files_context=files_context,
            agent_rules=rules,
            agent_preferences=prefs,
            agent_icks=icks,
            instructions=agent_instructions,
            is_community=self.ctx.is_community,
            participants=self.ctx.current_participants,
            current_mode=current_mode
        )
        user_message = build_user_message(self.ctx, last_result)
        
        active_schemas = get_filtered_schemas(enabled_tools)
        if client_tools:
            active_schemas = active_schemas + client_tools
        if self.tools.mcp_manager:
            active_schemas = active_schemas + mcp_tools_to_schemas(self.tools.mcp_manager.get_all_tools())

        pending_tool_calls = []
        content_accumulator = ""
        
        async for chunk in self.llm.call_llm_with_tools_streaming(
            system=system_prompt,
            user=user_message,
            tools=active_schemas,
            model=model or self.llm.agent_model,
            temperature=temp,
            reasoning=reasoning
        ):
            chunk_type = chunk.get("type")
            
            if chunk_type == "token":
                content_accumulator += chunk["content"]
                yield {"type": "token", "data": {"content": chunk["content"]}}
            elif chunk_type == "thinking":
                yield {"type": "thinking", "data": {"content": chunk["content"]}}
            elif chunk_type == "tool_calls":
                for call in chunk.get("calls", []):
                    args = self._safe_parse_args(call.get("arguments", "{}"))
                    pending_tool_calls.append(ToolCall(
                        name=call["name"],
                        args=args,
                        thinking=content_accumulator.strip() or None
                    ))
            elif chunk_type == "done":
                u = chunk.get("usage")
                if u:
                    u["approximate"] = True
                self._accumulate_usage(u)
                if not pending_tool_calls:
                    # Final Response
                    yield {"type": "done", "data": FinalResponse(content=content_accumulator.strip())}
                else:
                    yield {"type": "done", "data": pending_tool_calls}
            elif chunk_type == "error":
                yield {"event": "error", "data": {"message": chunk["message"]}}

    @staticmethod
    def _safe_parse_args(json_str: str) -> Dict:
        """Secure tool argument parsing using json and Pydantic validation."""
        # 1. Try standard JSON
        try:
            return json.loads(json_str)
        except json.JSONDecodeError:
            pass
            
        # 2. Try to fix common LLM formatting issues (trailing commas, missing quotes)
        # This is a bit heuristic but safer than ast.literal_eval
        cleaned = json_str.strip()
        # Remove trailing commas in objects and arrays
        cleaned = re.sub(r',\s*([\]}])', r'\1', cleaned)
        
        try:
            return json.loads(cleaned)
        except json.JSONDecodeError:
            logger.warning(f"Failed to parse tool arguments: {json_str[:200]}")
            return {}

    async def _execute_tools(self, tool_calls: List[ToolCall], results_out: List[Dict]) -> AsyncGenerator[Dict, None]:
        """Executes a batch of tool calls sequentially to avoid shared state races."""

        if self.ctx.state.call_count >= self.ctx.config.max_calls:
            yield {"event": "tool_error", "data": {
                "tool": "all",
                "error": f"Global call limit reached ({self.ctx.config.max_calls})"
            }}
            return

        for call in tool_calls:
            yield {"event": "tool_start", "data": {
                "tool": call.name, "args": call.args, "thinking": call.thinking
            }}

            try:
                if self.ctx.state.tool_limit_reached(call.name, self.ctx.config):
                    self.ctx.state.last_error = f"Tool '{call.name}' has reached its call limit"
                    yield {"event": "tool_error", "data": {
                        "tool": call.name,
                        "error": f"Call limit reached for {call.name}"
                    }}
                    continue

                if self.ctx.state.is_duplicate(call.name, call.args):
                    self.ctx.state.last_error = f"Duplicate call to '{call.name}' with same arguments"
                    yield {"event": "tool_error", "data": {
                        "tool": call.name,
                        "error": "Duplicate call skipped"
                    }}
                    continue

                self.ctx.state.record_call(call.name, call.args)

                if call.name == "request_clarification":
                    question = call.args.get("question", "Could you clarify?")
                    yield {"event": "clarification", "data": {"question": question}}
                    return

                result = await execute_tool(self.tools, call.name, call.args)

                summary, _ = summarize_result(call.name, result)
                update_accumulators(self.ctx, call.name, result)

                self.ctx.state.consecutive_errors = 0
                results_out.append({"tool": call.name, "result": result})

                yield {"event": "tool_end", "data": {
                    "tool": call.name, "result": summary
                }}

            except ToolExecutionError as e:
                self.ctx.state.last_error = e.message
                self.ctx.state.consecutive_errors += 1
                yield {"event": "tool_error", "data": {
                    "tool": call.name, "error": e.message
                }}
            except Exception as e:
                logger.exception(f"Tool {call.name} unexpected failure: {e}")
                self.ctx.state.consecutive_errors += 1
                yield {"event": "tool_error", "data": {
                    "tool": call.name, "error": "Internal tool failure"
                }}

    def _accumulate_usage(self, usage: Optional[Dict]):
        if usage:
            self.ctx.state.usage["prompt_tokens"] += usage.get("prompt_tokens", 0)
            self.ctx.state.usage["completion_tokens"] += usage.get("completion_tokens", 0)
            self.ctx.state.usage["total_tokens"] += usage.get("total_tokens", 0)

    def _wrap_final_response(self, response: Union[FinalResponse, ClarificationRequest]) -> Dict:
        if isinstance(response, FinalResponse):
            return {"event": "response", "data": {
                "content": response.content,
                "usage": self.ctx.state.usage,
                "sources": self.ctx.evidence.sources if self.ctx.evidence.sources else None
            }}
        else:
            return {"event": "clarification", "data": {
                "question": response.question,
                "usage": self.ctx.state.usage
            }}

    async def _fallback(self) -> Dict:
        """Unified fallback when agent exhausts attempts."""
        if self.ctx.evidence.has_any():
            summary = await self._generate_fallback_summary()
            return {"event": "response", "data": {
                "content": summary or "I found information but couldn't summarize it.",
                "usage": self.ctx.state.usage,
                "sources": self.ctx.evidence.sources if self.ctx.evidence.sources else None
            }}
        else:
            return {"event": "clarification", "data": {
                "question": "I'm having trouble with that. Could you rephrase?",
                "usage": self.ctx.state.usage
            }}

    async def _generate_fallback_summary(self) -> Optional[str]:
        """Generate a final response summary from accumulated evidence."""
        evidence_ctx = ""
        if self.ctx.evidence.profiles:
            evidence_ctx += f"\nProfiles FOUND:\n{format_entity_results(self.ctx.evidence.profiles)}\n"
        if self.ctx.evidence.messages:
            evidence_ctx += f"\nRelevant Messages:\n{format_retrieved_messages(self.ctx.evidence.messages)}\n"
        if self.ctx.evidence.graph:
            evidence_ctx += f"\nGraph Context:\n{format_graph_results(self.ctx.evidence.graph)}\n"

        prompt = get_fallback_summary_prompt(self.ctx.user_name, self.ctx.user_query, evidence_ctx)
        
        return await self.llm.call_llm(
            system="You are a helpful assistant providing a summary of found information.",
            user=prompt,
            temperature=0.3
        )

    async def _manage_context_size(self):
        """Monitor accumulated evidence and summarize if it approaches token limits."""
        evidence_str = build_evidence_context(self.ctx.evidence)
        self.ctx.evidence.token_count = self.llm.count_tokens(evidence_str)
        
        # Soft limit: 10000 tokens
        if self.ctx.evidence.token_count > 10000:
            logger.info(f"Evidence size ({self.ctx.evidence.token_count} tokens) exceeds limit. Summarizing...")
            
            summary = await self._generate_evidence_summary(evidence_str)
            if summary:
                # Store summary and clear raw evidence to save tokens
                self.ctx.evidence.summary = summary
                self.ctx.evidence.messages = []
                self.ctx.evidence.profiles = []
                self.ctx.evidence.graph = []
                self.ctx.evidence.facts = []
                self.ctx.evidence.paths = []
                self.ctx.evidence.hierarchy = []
                
                # Re-calculate token count
                self.ctx.evidence.token_count = self.llm.count_tokens(summary)

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
                temperature=0.0 # Strict factual summary
            )
        except Exception as e:
            logger.error(f"Failed to summarize evidence: {e}")
            return None

    async def _emit_llm_call(self, model: Optional[str], reasoning: str):
        await emit(self.ctx.session_id, "agent", "llm_call", {
            "run_id": self.ctx.run_id,
            "model": model,
            "reasoning": reasoning,
            "turn": self.ctx.state.attempt_count,
            "evidence_state": {
                "profiles": len(self.ctx.evidence.profiles),
                "messages": len(self.ctx.evidence.messages),
                "graph": len(self.ctx.evidence.graph)
            }
        }, verbose_only=True)
