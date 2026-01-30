import asyncio
import json
import os
import re
from typing import AsyncGenerator, Dict, List, Optional, TypeVar
from openai import AsyncOpenAI
from loguru import logger
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

REASONING_MODEL = os.environ.get("REASONING_MODEL", "google/gemini-3-flash-preview")
AGENT_MODEL = os.environ.get("AGENT_MODEL", "google/gemini-3-flash-preview")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

T = TypeVar('T', bound=BaseModel)

class LLMService:

    def __init__(
        self,
        api_key: Optional[str] = None,
        trace_logger=None,
        reasoning_model: Optional[str] = None,
        agent_model: Optional[str] = None
    ):
        self._api_key = api_key or OPENROUTER_API_KEY
        if not self._api_key:
            raise ValueError("OpenRouter API key required, this aint free")
        
        self._trace = trace_logger
        self._reasoning_model = reasoning_model or REASONING_MODEL
        self._agent_model = agent_model or AGENT_MODEL
        
        self._client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self._api_key
        )
        
        logger.info(
            f"LLMService initialized | "
            f"reasoning={self._reasoning_model} | "
            f"agent={self._agent_model}" 
        )
    
    @property
    def reasoning_model(self) -> str:
        """Model used for reasoning tasks"""
        return self._reasoning_model
    
    @property
    def agent_model(self) -> str:
        """Model used for agent"""
        return self._agent_model
    
    def update_models(
        self,
        reasoning_model: Optional[str] = None,
        agent_model: Optional[str] = None
    ) -> None:
        """Hot-swap models for this LLM service instance."""
        if reasoning_model:
            logger.info(f"LLMService: reasoning model {self._reasoning_model} → {reasoning_model}")
            self._reasoning_model = reasoning_model
        if agent_model:
            logger.info(f"LLMService: agent model {self._agent_model} → {agent_model}")
            self._agent_model = agent_model

    async def call_llm(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_retries: int = 5,
        reasoning: str = "low"
    ) -> Optional[str]:
        """Free-form reasoning, returns raw text. Returns None on failure."""
        model = model or self._reasoning_model

        
        if self._trace:
            self._trace.debug(
                f"[REASONING] Model: {model}\n"
                f"SYSTEM:\n{system}\n\n"
            )
        

        for attempt in range(max_retries):
            try:
                response = await self._client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ],
                    temperature=temperature,
                    extra_body={
                        "provider": {
                            "allow_fallbacks": True,
                            "data_collection": "deny",
                            # "zdr": True # zero data rentention(can uncomment if you want, might increase latency tho)
                        }, 
                        "reasoning": {"effort": reasoning}
                    }
                )
                
                content = response.choices[0].message.content
                
                # CHECK FOR "GHOST" RESPONSES
                if not content or not content.strip():
                    if attempt < max_retries - 1:
                        logger.warning(f"LLM returned empty content. Retrying ({attempt+1}/{max_retries})...")
                        continue
                    else:
                        logger.error("LLM returned empty content after max retries.")
                        return None

                if self._trace:
                    self._trace.debug(f"[REASONING] Response:\n{content}")
                
                return content
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"LLM call failed: {e}. Retrying ({attempt+1}/{max_retries})...")
                else:
                    logger.error(f"Reasoning LLM call failed after retries: {e}")
                    if self._trace:
                        self._trace.error(f"[REASONING] Failed: {e}")
                    return None
    
    async def call_llm_with_tools(
        self,
        system: str,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_retries: int = 3,
        xml_helper: bool = True
    ) -> Optional[Dict]:
        """Call with function tools. Returns parsed tool call."""
        model = model or self._agent_model
        
        if self._trace:
            self._trace.debug(f"[TOOLS SYNC] Model: {model}\nTools: {[t['function']['name'] for t in tools]}")
        
        for attempt in range(max_retries):
            try:
                response = await self._client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ],
                    tools=tools,
                    tool_choice="auto",
                    temperature=temperature,
                    extra_body={"provider": {
                            "allow_fallbacks": True,
                            "data_collection": "deny"
                            # "zdr": True
                            }
                        }
                )
                
                message = response.choices[0].message
                
                # VALIDATION: It must have either text OR a tool call.
                # If both are missing, it's a failed generation.
                has_content = message.content and message.content.strip()
                has_tools = message.tool_calls and len(message.tool_calls) > 0
                
                if not has_content and not has_tools:
                    if attempt < max_retries - 1:
                        logger.warning(f"Agent returned empty response (no text, no tools). Retrying ({attempt+1}/{max_retries})...")
                        continue
                    else:
                        logger.error("Agent returned empty response after max retries.")
                        return None
            
                
                # Process Tool Calls
                tool_calls = []
                if message.tool_calls:
                    tool_calls = [
                        {"name": tc.function.name, "arguments": tc.function.arguments}
                        for tc in message.tool_calls
                    ]
                
                if message.content:
                    logger.info(f"[AGENT THOUGHT]: {message.content[:200]}")
                    print(f"\n=== SCRATCHPAD ===\n{message.content}\n==================\n")
                
                if xml_helper:
                    if not has_tools and has_content and "<invoke" in message.content:
                        parsed = self._parse_xml_tool_calls(message.content)
                        if parsed:
                            tool_calls = parsed
                            logger.info(f"Parsed {len(parsed)} tool calls from XML fallback")
                
                if self._trace:
                    self._trace.info(f"[TOOLS SYNC] Response: {tool_calls} | Content: {message.content}")
                
                return {
                    "content": message.content,
                    "tool_calls": tool_calls
                }
                
            except Exception as e:
                if attempt < max_retries - 1:
                    logger.warning(f"Agent tool call failed: {e}. Retrying ({attempt+1}/{max_retries})...")
                    await asyncio.sleep(1) 
                else:
                    logger.error(f"Tool call failed after retries: {e}")
                    if self._trace:
                        self._trace.error(f"[TOOLS SYNC] Failed: {e}")
                    return None
                
    async def call_llm_with_tools_streaming(
        self,
        system: str,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0,
        reasoning: str = "low"
    ) -> AsyncGenerator[Dict, None]:
        """
        Streaming version of call_llm_with_tools.
        Yields tokens for text, accumulates tool calls silently.
        """
        model = model or self._agent_model
        
        content = ""
        tool_calls_by_index = {}
        tool_calls_detected = False
        usage = None
        generation_id = None
        try:
            response = await self._client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ],
                tools=tools,
                tool_choice="auto",
                temperature=temperature,
                stream=True,
                stream_options={"include_usage": True},
                extra_body={
                    "provider": {
                        "allow_fallbacks": True,
                        "data_collection": "deny",
                        # "zdr": True
                    },
                    "reasoning": {"effort": reasoning}
                }
            )
             
            async for chunk in response:
                if chunk.id and not generation_id:
                    generation_id = chunk.id
                
                if not chunk.choices:
                    continue
                    
                delta = chunk.choices[0].delta
                finish_reason = chunk.choices[0].finish_reason
                
                # Handle content delta
                if delta.content:
                    content += delta.content
                    if not tool_calls_detected:
                        yield {"type": "token", "content": delta.content}
                
                # Handle tool call deltas
                if delta.tool_calls:
                    tool_calls_detected = True
                    for tc in delta.tool_calls:
                        idx = tc.index
                        if idx not in tool_calls_by_index:
                            tool_calls_by_index[idx] = {"name": "", "arguments": ""}
                        if tc.function.name:
                            tool_calls_by_index[idx]["name"] = tc.function.name
                        if tc.function.arguments:
                            tool_calls_by_index[idx]["arguments"] += tc.function.arguments
                
                if hasattr(delta, 'reasoning_details') and delta.reasoning_details:
                    for rd in delta.reasoning_details:
                        if hasattr(rd, 'content') and rd.content:
                            yield {"type": "thinking", "content": rd.content}
                
                # Handle finish
                if finish_reason:
                    if tool_calls_by_index:
                        calls = [
                            {"name": tc["name"], "arguments": tc["arguments"]}
                            for tc in tool_calls_by_index.values()
                        ]
                        yield {"type": "tool_calls", "calls": calls, "content": content}

            if generation_id and not usage:
                import asyncio
                for attempt in range(3):
                    await asyncio.sleep(0.5)
                    usage = await self._fetch_generation_stats(generation_id)
                    if usage:
                        break
            
            yield {"type": "done", "content": content, "usage": usage}
                    
        except Exception as e:
            logger.error(f"Streaming call failed: {e}")
            yield {"type": "error", "message": str(e)}
    
    def _parse_xml_tool_calls(self, content: str) -> list:
        """Fallback parser for DeepSeek XML tool calls.(Or any model with xml tool calls)"""
        tools = []
        pattern = r'<invoke name="([^"]+)">(.*?)</invoke>'
        matches = re.findall(pattern, content, re.DOTALL)
        
        for name, params_block in matches:
            args = {}
            param_pattern = r'<parameter name="([^"]+)"[^>]*>([^<]*)</parameter>'
            for param_name, param_value in re.findall(param_pattern, params_block):
                param_value = param_value.strip()
                
                # Type conversion
                if param_value.isdigit():
                    args[param_name] = int(param_value)
                elif param_value.lower() in ('true', 'false'):
                    args[param_name] = param_value.lower() == 'true'
                else:
                    args[param_name] = param_value
                    
            tools.append({"name": name, "arguments": json.dumps(args)})
        
        return tools
    
    async def _fetch_generation_stats(self, generation_id: str) -> Optional[Dict]:
        """Fetch usage stats from OpenRouter generation endpoint"""
        try:
            import httpx
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://openrouter.ai/api/v1/generation?id={generation_id}",
                    headers={"Authorization": f"Bearer {self._api_key}"}
                )
                logger.info(f"Generation API status: {resp.status_code}")
                logger.info(f"Generation API response: {resp.text}")
                
                if resp.status_code == 200:
                    data = resp.json().get("data", {})
                    return {
                        "prompt_tokens": data.get("tokens_prompt", 0),
                        "completion_tokens": data.get("tokens_completion", 0),
                        "total_tokens": data.get("tokens_prompt", 0) + data.get("tokens_completion", 0),
                        "cost": data.get("total_cost", 0)
                    }
        except Exception as e:
            logger.warning(f"Failed to fetch generation stats: {e}")
        return None
    