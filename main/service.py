import asyncio
import json
import os
import re
from typing import Dict, List, Optional, TypeVar
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
        xml_helper: bool = False
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
    