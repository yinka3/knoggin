# ========================
# This is meant to be used for benchmarking testing if you choose to
# test the systems capabilities
# When I get a job, I will do a proper benchmark testing
# ========================



# from datetime import datetime, timezone
# import json
# from typing import Dict, List, Optional, TYPE_CHECKING
# import uuid

# from loguru import logger
# import redis.asyncio as aioredis

# from agent.formatters import format_entity_results, format_graph_results, format_retrieved_messages
# from agent.tools import Tools
# from main.service import LLMService
# from agent.system_prompt import get_agent_prompt, get_fallback_summary_prompt
# from main.topics_config import TopicConfig
# from schema.dtypes import (
#     ClarificationRequest,
#     ClarificationResult,
#     CompleteResult,
#     FinalResponse,
#     QueryTrace,
#     RunResult,
#     AgentResponse,
#     ToolCall,
#     TraceEntry
# )
# from agent.internals import (
#     AgentConfig,
#     AgentState,
#     RetrievedEvidence,
#     AgentContext,
#     build_user_message,
#     update_accumulators,
#     summarize_result,
#     execute_tool,
#     _log_trace_summary
# )

# from schema.tool_schema import TOOL_SCHEMAS
# import time

# if TYPE_CHECKING:
#     from db.store import MemGraphStore
#     from main.entity_resolve import EntityResolver



# async def call_agent(
#     llm: LLMService,
#     ctx: AgentContext,
#     user_name: str,
#     last_result: Optional[Dict] = None,
#     persona: str = "",
#     date: str = ""
# ) -> AgentResponse:
    
#     system_prompt = get_agent_prompt(user_name, date, persona)
#     user_message = build_user_message(ctx, last_result)

#     response = await llm.call_llm_with_tools(
#         system=system_prompt,
#         user=user_message,
#         tools=TOOL_SCHEMAS
#     )

#     if not response:
#         return FinalResponse(content="System Error: LLM failed to respond.")
    
#     content = response.get("content", "") or ""
    
#     if not response.get("tool_calls"):
#         return FinalResponse(content=content)
    
#     if content:
#         logger.info(f"[AGENT THOUGHT]: {content}")

#     tool_calls = response["tool_calls"]
#     if len(tool_calls) == 1:
#         tc = tool_calls[0]
#         name = tc["name"]
#         args = json.loads(tc["arguments"])

#         if name == "request_clarification":
#             return ClarificationRequest(question=args.get("question", ""))
#         return ToolCall(name=name, args=args, thinking=content if content else None)

#     return [ToolCall(name=tc["name"], args=json.loads(tc["arguments"]), thinking=content if content else None) for tc in tool_calls]







# async def _process_tool_calls(
#     ctx: AgentContext,
#     trace: QueryTrace,
#     tools: Tools,
#     response: ToolCall | List[ToolCall],
#     step: int
# ) -> List[Dict]:
#     """Process tool calls, returns results for next iteration."""
#     tool_calls = [response] if isinstance(response, ToolCall) else response
#     all_results = []
#     step_start = time.perf_counter()

#     for tc in tool_calls:
#         tool_name = tc.name
#         args = tc.args

#         # Check duplicate
#         if ctx.state.is_duplicate(tool_name, args):
#             ctx.state.consecutive_errors += 1
#             if ctx.state.consecutive_errors >= 3:
#                 logger.warning(f"Breaking loop: {ctx.state.consecutive_errors} consecutive errors.")
#                 ctx.state.last_error = "Too many repeated errors. Stopping to save cost."
#                 break

#             ctx.state.last_error = f"Already called {tool_name} with these args. Use accumulated context or try different parameters."
#             trace.entries.append(TraceEntry(
#                 step=step,
#                 state="active",
#                 tool=tool_name,
#                 args=args,
#                 resolved_args={},
#                 result_summary="Rejected: duplicate call",
#                 result_count=0,
#                 duration_ms=(time.perf_counter() - step_start) * 1000,
#                 error=ctx.state.last_error
#             ))
#             all_results.append({"tool": tool_name, "error": ctx.state.last_error})
#             continue

#         # Check per-tool limit
#         if ctx.state.tool_limit_reached(tool_name, ctx.config):
#             ctx.state.last_error = f"{tool_name} limit reached ({ctx.config.get_tool_limit(tool_name)}). Use a different tool or conclude."
#             all_results.append({"tool": tool_name, "error": ctx.state.last_error})
#             continue

#         # Check call limit
#         if ctx.state.call_count >= ctx.config.max_calls:
#             ctx.state.last_error = "Call limit reached. You must finish with accumulated evidence or request clarification."
#             trace.entries.append(TraceEntry(
#                 step=step,
#                 state="active",
#                 tool=tool_name,
#                 args=args,
#                 resolved_args={},
#                 result_summary="Rejected: call limit",
#                 result_count=0,
#                 duration_ms=(time.perf_counter() - step_start) * 1000,
#                 error=ctx.state.last_error
#             ))
#             all_results.append({"tool": tool_name, "error": ctx.state.last_error})
#             continue
        
#         ctx.state.consecutive_errors = 0
        
#         # Execute
#         result = await execute_tool(tools, tool_name, args)
#         logger.info(f"[TOOL RESULT] {tool_name}: {json.dumps(result, default=str)[:1000]}")
#         result_summary, result_count = summarize_result(tool_name, result)
        
#         trace.entries.append(TraceEntry(
#             step=step,
#             state="active",
#             tool=tool_name,
#             args=args,
#             resolved_args=args,
#             result_summary=result_summary,
#             result_count=result_count,
#             duration_ms=(time.perf_counter() - step_start) * 1000,
#             error=result.get("error") if isinstance(result, dict) else None
#         ))
        
#         ctx.state.record_call(tool_name, args)
#         update_accumulators(ctx, tool_name, result)
#         all_results.append({"tool": tool_name, "result": result})

#     return all_results  

# def _log_trace_summary(trace: QueryTrace, state: str, query: str = ""):
#     """Log trace summary at exit points."""
#     if state == "complete":
#         logger.info(f"[AGENT] Trace {trace.trace_id} completed: {len(trace.entries)} tool calls")
#     elif state == "clarify":
#         logger.info(f"[AGENT] Trace {trace.trace_id} ended with clarification: {len(trace.entries)} tool calls")
#     elif state == "fallback":
#         logger.warning(f"[AGENT] Max attempts reached for query: {query[:50]}...")
#         logger.info(f"[AGENT] Trace {trace.trace_id} fallback: {len(trace.entries)} tool calls")
#         for entry in trace.entries:
#             logger.debug(f"  Step {entry.step}: {entry.tool} -> {entry.result_summary} ({entry.duration_ms:.0f}ms)")


# async def run(
#     user_query: str,
#     user_name: str,
#     session_id: str,
#     conversation_history: List[Dict],
#     hot_topics: List[str],
#     topic_config: TopicConfig,
#     llm: LLMService,
#     store: 'MemGraphStore',
#     ent_resolver: 'EntityResolver',
#     redis_client: aioredis.Redis,
#     persona: str = "",
#     slim_hot_context: bool = False,
#     date: str = ""
# ) -> RunResult:
    
#     # Check for system warnings
#     system_warning = ""
#     try:
#         raw_warning = await redis_client.get("system:active_job_warning")
#         if raw_warning:
#             system_warning = f"{raw_warning}\n\n---\n\n"
#     except Exception as e:
#         logger.error(f"Failed to check system warning: {e}")

#     trace = QueryTrace(
#         trace_id=str(uuid.uuid4()),
#         user_query=user_query,
#         started_at=datetime.now(timezone.utc)
#     )

#     config = AgentConfig()
#     state = AgentState()
#     evidence = RetrievedEvidence()
#     valid_hot_topics = topic_config.validate_hot_topics(hot_topics)

#     ctx = AgentContext(
#         config=config,
#         state=state,
#         evidence=evidence,
#         user_query=user_query,
#         hot_topics=valid_hot_topics,
#         active_topics=topic_config.active_topics,
#         trace_id=trace.trace_id,
#         history=conversation_history
#     )

#     tools = Tools(user_name, store, ent_resolver, redis_client, session_id, topic_config)

#     if hot_topics:
#         ctx.hot_topic_context = await tools.get_hot_topic_context(hot_topics, slim=slim_hot_context)
#         logger.info(f"[HOT CONTEXT] {json.dumps(ctx.hot_topic_context, indent=2, default=str)[:1000]}")

#     last_result = None
#     step = 0

#     while ctx.state.attempt_count < ctx.config.max_attempts:
#         ctx.state.attempt_count += 1
#         should_force_conclusion = (
#             ctx.state.attempt_count >= ctx.config.max_attempts - 1
#             and ctx.evidence.has_any()
#         )
#         step += 1

#         if should_force_conclusion:
#             ctx.state.last_error = "Final attempt. You MUST respond now using accumulated evidence. Do not call any tools."

#         response = await call_agent(llm, ctx, user_name, last_result, persona, date)

#         if isinstance(response, FinalResponse):
#             _log_trace_summary(trace, "complete")
#             final_text = system_warning + response.content if system_warning else response.content
#             return CompleteResult(
#                 status="complete",
#                 response=final_text,
#                 tools_used=ctx.state.tools_used,
#                 state="complete",
#                 messages=ctx.evidence.messages,
#                 profiles=ctx.evidence.profiles,
#                 graph=ctx.evidence.graph
#             )

#         if isinstance(response, ClarificationRequest):
#             _log_trace_summary(trace, "clarify")
#             final_q = system_warning + response.question if system_warning else response.question
#             return ClarificationResult(
#                 status="clarification_needed",
#                 question=final_q,
#                 tools_used=ctx.state.tools_used,
#                 state="clarify"
#             )

#         last_result = await _process_tool_calls(ctx, trace, tools, response, step)
    
#     _log_trace_summary(trace, "fallback", user_query)
    
#     if ctx.evidence.has_any():
#         evidence_ctx = ""
#         if ctx.evidence.profiles:
#             evidence_ctx += f"Profiles:\n{format_entity_results(ctx.evidence.profiles)}\n\n"
#         if ctx.evidence.messages:
#             evidence_ctx += f"Messages:\n{format_retrieved_messages(ctx.evidence.messages)}\n\n"
#         if ctx.evidence.graph:
#             evidence_ctx += f"Connections:\n{format_graph_results(ctx.evidence.graph)}\n\n"
        
#         summary = await llm.call_llm(
#             system=get_fallback_summary_prompt(user_name),
#             user=f"Query: {user_query}\n\nEvidence:\n{evidence_ctx}"
#         )
        
#         return CompleteResult(
#             status="complete",
#             response=system_warning + (summary or "I found some information but couldn't summarize it."),
#             tools_used=ctx.state.tools_used,
#             state="fallback",
#             messages=ctx.evidence.messages,
#             profiles=ctx.evidence.profiles,
#             graph=ctx.evidence.graph
#         )

#     return ClarificationResult(
#         status="clarification_needed",
#         question=system_warning + "I'm having trouble with that. Could you rephrase or be more specific?",
#         tools_used=ctx.state.tools_used,
#         state="fallback"
#     )
