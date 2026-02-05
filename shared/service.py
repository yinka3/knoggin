import asyncio
import httpx
from typing import AsyncGenerator, Dict, List, Optional
from openai import AsyncOpenAI
from loguru import logger

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MAX_RETRIES = 3

class LLMService:

    def __init__(
        self,
        api_key: str = None,
        trace_logger=None,
        reasoning_model: str = "google/gemini-2.5-flash",
        agent_model: str = "google/gemini-3-flash-preview"
    ):
        self._api_key = api_key
        self._trace = trace_logger
        self._reasoning_model = reasoning_model
        self._agent_model = agent_model
        self._client = None
        
        if api_key:
            self._client = AsyncOpenAI(
                base_url=OPENROUTER_BASE_URL,
                api_key=self._api_key,
                timeout=60.0
            )
            logger.info(f"LLMService initialized | reasoning={reasoning_model} | agent={agent_model}")
        else:
            logger.warning("LLMService initialized without API key - calls will fail until key is configured")
    
    @property
    def reasoning_model(self) -> str:
        return self._reasoning_model
    
    @property
    def agent_model(self) -> str:
        return self._agent_model
    
    @property
    def is_configured(self) -> bool:
        """Check if the service has a valid API key."""
        return self._client is not None
    
    def _ensure_client(self):
        """Raise helpful error if client not configured."""
        if self._client is None:
            raise ValueError(
                "OpenRouter API key not configured. "
                "Please add your API key in Settings > Configuration."
            )
    
    def update_settings(
        self,
        api_key: str = None,
        reasoning_model: str = None,
        agent_model: str = None
    ):
        if api_key:
            self._api_key = api_key
            self._client = AsyncOpenAI(
                base_url=OPENROUTER_BASE_URL,
                api_key=self._api_key,
                timeout=60.0
            )
            logger.info("LLMService: API key updated")
        
        if reasoning_model:
            logger.info(f"LLMService: reasoning model {self._reasoning_model} -> {reasoning_model}")
            self._reasoning_model = reasoning_model
        
        if agent_model:
            logger.info(f"LLMService: agent model {self._agent_model} -> {agent_model}")
            self._agent_model = agent_model
    
    def _extra_body(self, reasoning: str = None) -> Dict:
        """OpenRouter-specific params."""
        body = {
            "provider": {
                "allow_fallbacks": True,
                "data_collection": "deny"
            }
        }
        if reasoning:
            body["reasoning"] = {"effort": reasoning}
        return body
    
    async def call_llm(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 0.0,
        reasoning: str = "low"
    ) -> Optional[str]:
        """Basic completion for reasoning tasks."""
        self._ensure_client()
        model = model or self._reasoning_model
        
        for attempt in range(MAX_RETRIES):
            try:
                response = await self._client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ],
                    temperature=temperature,
                    extra_body=self._extra_body(reasoning)
                )
                
                content = response.choices[0].message.content
                
                if not content or not content.strip():
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"Empty response, retrying ({attempt+1}/{MAX_RETRIES})")
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return None
                
                if self._trace:
                    self._trace.debug(
                        f"MODEL: {model}\n"
                        f"USER:\n{user}\n"
                        f"RESPONSE:\n{content}"
                    )
                
                return content
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    logger.warning(f"LLM call failed: {e}. Retrying ({attempt+1}/{MAX_RETRIES})")
                    await asyncio.sleep(0.5 * (attempt + 1))
                else:
                    logger.error(f"LLM call failed after retries: {e}")
                    return None


    async def call_llm_with_tools_streaming(
        self,
        system: str,
        user: str,
        tools: List[Dict],
        model: Optional[str] = None,
        temperature: float = 0.0
    ) -> AsyncGenerator[Dict, None]:
        """Streaming completion with tools."""
        model = model or self._agent_model
        
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
                extra_body=self._extra_body()
            )
            
            content = ""
            tool_calls_by_index = {}
            tool_calls_detected = False
            usage = None
            generation_id = None
            
            async for chunk in response:
                if chunk.id and not generation_id:
                    generation_id = chunk.id
                
                if not chunk.choices:
                    continue
                
                delta = chunk.choices[0].delta
                
                if delta.content:
                    content += delta.content
                    if not tool_calls_detected:
                        yield {"type": "token", "content": delta.content}
                
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
                
                if chunk.usage:
                    usage = {
                        "prompt_tokens": chunk.usage.prompt_tokens,
                        "completion_tokens": chunk.usage.completion_tokens,
                        "total_tokens": chunk.usage.total_tokens
                    }
            
            if tool_calls_by_index:
                calls = [tool_calls_by_index[i] for i in sorted(tool_calls_by_index.keys())]
                yield {"type": "tool_calls", "content": content, "calls": calls}
            
            if not usage and generation_id:
                usage = await self._fetch_generation_stats(generation_id)
            
            yield {"type": "done", "content": content, "usage": usage}
            
        except Exception as e:
            logger.error(f"Stream failed: {e}")
            yield {"type": "error", "message": str(e)}
    
    async def _fetch_generation_stats(self, generation_id: str) -> Optional[Dict]:
        """Fetch usage from OpenRouter's generation endpoint."""
        try:
            async with httpx.AsyncClient() as client:
                resp = await client.get(
                    f"https://openrouter.ai/api/v1/generation?id={generation_id}",
                    headers={"Authorization": f"Bearer {self._api_key}"}
                )
                
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
    
    # ================
    # Used in orchestrator which is used primarily
    # for benchmarking
    # ================
        # async def call_llm_with_tools(
    #     self,
    #     system: str,
    #     user: str,
    #     tools: List[Dict],
    #     model: Optional[str] = None,
    #     temperature: float = 0.0,
    #     max_retries: int = 3
    # ) -> Optional[Dict]:
    #     """Completion with tool calling."""
    #     model = model or self._agent_model
        
    #     for attempt in range(max_retries):
    #         try:
    #             response = await self._client.chat.completions.create(
    #                 model=model,
    #                 messages=[
    #                     {"role": "system", "content": system},
    #                     {"role": "user", "content": user}
    #                 ],
    #                 tools=tools,
    #                 tool_choice="auto",
    #                 temperature=temperature,
    #                 extra_body=self._extra_body()
    #             )
                
    #             message = response.choices[0].message
    #             has_content = message.content and message.content.strip()
    #             has_tools = message.tool_calls and len(message.tool_calls) > 0
                
    #             if not has_content and not has_tools:
    #                 if attempt < max_retries - 1:
    #                     logger.warning(f"Empty response, retrying ({attempt+1}/{max_retries})")
    #                     continue
    #                 return None
                
    #             tool_calls = []
    #             if message.tool_calls:
    #                 tool_calls = [
    #                     {"name": tc.function.name, "arguments": tc.function.arguments}
    #                     for tc in message.tool_calls
    #                 ]
                
    #             return {"content": message.content, "tool_calls": tool_calls}
                
    #         except Exception as e:
    #             if attempt < max_retries - 1:
    #                 logger.warning(f"Tool call failed: {e}. Retrying ({attempt+1}/{max_retries})")
    #             else:
    #                 logger.error(f"Tool call failed after retries: {e}")
    #                 return None