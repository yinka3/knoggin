import os
from typing import Dict, List, Optional, Type, TypeVar
from openai import AsyncOpenAI, OpenAI
import instructor
from loguru import logger
from pydantic import BaseModel
from dotenv import load_dotenv

load_dotenv()

STRUCTURED_MODEL = os.environ.get("STRUCTURED_MODEL", "google/gemini-2.5-flash")
REASONING_MODEL = os.environ.get("REASONING_MODEL", "google/gemini-3-flash-preview")
AGENT_MODEL = os.environ.get("AGENT_MODEL", "anthropic/claude-sonnet-4.5")
OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY")

T = TypeVar('T', bound=BaseModel)

class LLMService:

    def __init__(
        self,
        api_key: Optional[str] = None,
        trace_logger=None,
        structured_model: Optional[str] = None,
        reasoning_model: Optional[str] = None,
        agent_model: Optional[str] = None
    ):
        self._api_key = api_key or OPENROUTER_API_KEY
        if not self._api_key:
            raise ValueError("OpenRouter API key required, this aint free")
        
        self._trace = trace_logger
        self._structured_model = structured_model or STRUCTURED_MODEL
        self._reasoning_model = reasoning_model or REASONING_MODEL
        self._agent_model = agent_model or AGENT_MODEL

        self._client_sync = OpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self._api_key
        )
        
        self._client = AsyncOpenAI(
            base_url="https://openrouter.ai/api/v1",
            api_key=self._api_key
        )
        
        self._client_instruct = instructor.from_openai(
            AsyncOpenAI(
                base_url="https://openrouter.ai/api/v1",
                api_key=self._api_key
            ),
            mode=instructor.Mode.JSON
        )
        
        logger.info(
            f"LLMService initialized | "
            f"structured={self._structured_model} | "
            f"reasoning={self._reasoning_model} | "
            f"agent={self._agent_model}" 
        )

    @property
    def structured_model(self) -> str:
        return self._structured_model
    
    @property
    def reasoning_model(self) -> str:
        return self._reasoning_model
    
    @property
    def agent_model(self) -> str:
        return self._agent_model

    async def call_structured(
        self,
        system: str,
        user: str,
        response_model: Type[T],
        model: Optional[str] = None,
        temperature: float = 0.0,
        max_retries: int = 2,
    ) -> Optional[T]:
        """Structured output parsed into Pydantic model. Returns None on failure."""
        model = model or self._structured_model
        
        if self._trace:
            self._trace.debug(
                f"[STRUCTURED] Model: {model}\n"
                f"Response Model: {response_model.__name__}\n"
                f"SYSTEM:\n{system}\n\n"
            )
        
        try:
            response = await self._client_instruct.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ],
                response_model=response_model,
                max_retries=max_retries,
                temperature=temperature,
                extra_body={"provider": {"allow_fallbacks": True}}
            )
            
            if self._trace:
                self._trace.debug(f"[STRUCTURED] Response:\n{response.model_dump_json(indent=2)}")
            
            return response
            
        except Exception as e:
            if self._trace:
                self._trace.error(f"[STRUCTURED] Failed: {e}")
            logger.error(f"Structured LLM call failed ({response_model.__name__}): {e}")
            return None


    async def call_reasoning(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0
    ) -> Optional[str]:
        """Free-form reasoning, returns raw text. Returns None on failure."""
        model = model or self._reasoning_model

        
        if self._trace:
            self._trace.debug(
                f"[REASONING] Model: {model}\n"
                f"SYSTEM:\n{system}\n\n"
            )
        
        try:
            response = await self._client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ],
                temperature=temperature,
                extra_body={"provider": {"allow_fallbacks": True}, "reasoning": {"effort": "minimal"}}
            )
            
            content = response.choices[0].message.content
            
            if self._trace:
                self._trace.debug(f"[REASONING] Response:\n{content}")
            
            return content
            
        except Exception as e:
            if self._trace:
                self._trace.error(f"[REASONING] Failed: {e}")
            logger.error(f"Reasoning LLM call failed: {e}")
            return None
    
    async def call_with_tools(
        self,
        system: str,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0,
    ) -> Optional[Dict]:
        """Sync call with function tools. Returns parsed tool call."""
        model = model or self._agent_model
        
        if self._trace:
            self._trace.debug(f"[TOOLS SYNC] Model: {model}\nTools: {[t['function']['name'] for t in tools]}")
        
        try:
            response = await self._client.chat.completions.create(
                model=model,
                messages=[
                    {"role": "system", "content": system},
                    {"role": "user", "content": user}
                ],
                tools=tools,
                tool_choice="required",
                temperature=temperature,
                extra_body={"provider": {"allow_fallbacks": True}}
            )
            
            message = response.choices[0].message
            
            tool_calls = []
            if message.tool_calls:
                tool_calls = [
                    {"name": tc.function.name, "arguments": tc.function.arguments}
                    for tc in message.tool_calls
                ]
            
            if self._trace:
                self._trace.debug(f"[TOOLS SYNC] Response: {tool_calls}")
            
            return {
                "content": message.content,
                "tool_calls": tool_calls
            }
            
        except Exception as e:
            if self._trace:
                self._trace.error(f"[TOOLS SYNC] Failed: {e}")
            logger.error(f"Tool call failed: {e}")
            return None
    