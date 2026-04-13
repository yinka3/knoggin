import asyncio
import httpx
from typing import Any, AsyncGenerator, Dict, List, Optional
from openai import AsyncOpenAI
from loguru import logger
import redis.asyncio as aioredis
import instructor
from transformers import AutoTokenizer
from common.infra.redis import RedisKeys
from common.errors.agent import ConfigurationError, DependencyError

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MAX_RETRIES = 3

# Fallback costs if API fetch fails (USD per 1M tokens)
FALLBACK_COSTS = {
    "google/gemini-3.1-pro": {"input": 2.00, "output": 12.00},
    "google/gemini-3.1-flash-lite": {"input": 0.25, "output": 1.50},
    "google/gemini-3-flash-preview": {"input": 0.50, "output": 3.00},
    "google/gemini-2.5-pro": {"input": 1.25, "output": 10.00},
    "google/gemini-2.5-flash": {"input": 0.30, "output": 1.50},
}


class LLMService:

    def __init__(
        self,
        api_key: str = None,
        base_url: str = None,
        trace_logger=None,
        agent_model: str = "google/gemini-3-flash-preview",
        extraction_model: str = "google/gemini-2.5-flash",
        merge_model: str = "google/gemini-2.5-pro",
        redis_client: aioredis.Redis = None
    ):
        self._api_key = api_key
        self._base_url = base_url or OPENROUTER_BASE_URL
        self._is_openrouter = "openrouter.ai" in self._base_url
        self._trace = trace_logger
        self._agent_model = agent_model
        self._extraction_model = extraction_model
        self._merge_model = merge_model
        self._redis = redis_client
        self._client = None
        self._raw_client = None
        self._http_client = httpx.AsyncClient(timeout=10.0)
        self._background_tasks: set = set()
        self._model_prices: Dict[str, Dict[str, float]] = FALLBACK_COSTS.copy()
        self._prices_fetched = False
        self._tokenizer = None

        if api_key:
            client = AsyncOpenAI(
                base_url=self._base_url,
                api_key=self._api_key,
                timeout=60.0
            )

            self._raw_client = client
            self._client = instructor.from_openai(client)
            
            provider_label = "OpenRouter" if self._is_openrouter else self._base_url
            logger.info(f"LLMService initialized ({provider_label}) | extraction={extraction_model} | merge={merge_model} | agent={agent_model}")
        else:
            logger.warning("LLMService initialized without API key")
    
    async def load_tokenizer(self):
        """Async loading of the heavy tokenizer."""
        if self._tokenizer:
            return
            
        try:
            loop = asyncio.get_running_loop()
            # Offload heavy loading to thread pool
            self._tokenizer = await loop.run_in_executor(
                None, 
                lambda: AutoTokenizer.from_pretrained("unsloth/gemma-2-2b")
            )
            logger.info("LLM tokenizer loaded")
        except Exception as e:
            logger.warning(f"Failed to load transformers tokenizer: {e}. Token estimation will be less accurate.")
    
    @property
    def agent_model(self) -> str:
        return self._agent_model
    
    @property
    def extraction_model(self) -> str:
        return self._extraction_model
    
    @property
    def merge_model(self) -> str:
        return self._merge_model
    
    @property
    def is_configured(self) -> bool:
        return self._client is not None
    
    def _ensure_client(self):
        if self._client is None:
            raise ConfigurationError(
                "LLM API key not configured. "
                "Please add your API key in Settings > Configuration."
            )
    
    def update_settings(self, api_key: str = None, base_url: str = None, agent_model: str = None, extraction_model: str = None, merge_model: str = None):
        if api_key and api_key != self._api_key:
            self._api_key = api_key
            if base_url:
                self._base_url = base_url
                self._is_openrouter = "openrouter.ai" in self._base_url
            client = AsyncOpenAI(
                base_url=self._base_url,
                api_key=self._api_key,
                timeout=60.0
            )
            self._raw_client = client
            self._client = instructor.from_openai(client)
            logger.info("LLMService: API key updated")
        
        if agent_model:
            logger.info(f"LLMService: agent model {self._agent_model} -> {agent_model}")
            self._agent_model = agent_model
        
        if extraction_model:
            logger.info(f"LLMService: extraction model {self._extraction_model} -> {extraction_model}")
            self._extraction_model = extraction_model

        if merge_model:
            logger.info(f"LLMService: merge model {self._merge_model} -> {merge_model}")
            self._merge_model = merge_model
    
    def _extra_body(self, reasoning: Optional[str] = "low") -> Dict[str, Any]:
        """Extra body parameters for OpenRouter."""
        body: Dict[str, Any] = {}
        if self._is_openrouter:
            body["provider"] = {
                "require_parameters": True,
            }
            # Add prompt caching for Anthropic/Gemini
            # By default OpenRouter uses 5m, we can keep it as is or specify
            body["cache_control"] = {"type": "ephemeral"} 
            
            if reasoning == "high":
                body["reasoning"] = {"max_tokens": 4096}
            elif reasoning == "medium":
                body["reasoning"] = {"max_tokens": 1024}
            elif reasoning == "low":
                # Default reasoning, no specific max_tokens
                pass
            elif reasoning: # For any other custom reasoning string
                body["reasoning"] = reasoning
        # Cast to ensure Pyre2 doesn't specialize the dict too early
        return dict(body)

    async def _fetch_model_prices(self):
        """Fetch live model pricing from OpenRouter."""
        if not self._is_openrouter:
            return
            
        try:
            async with httpx.AsyncClient() as client:
                response = await client.get(f"{OPENROUTER_BASE_URL}/models", timeout=10.0)
                if response.status_code == 200:
                    data = response.json().get("data", [])
                    for m in data:
                        m_id = m.get("id")
                        pricing = m.get("pricing", {})
                        if m_id and pricing:
                            # OpenRouter gives per-token. Convert to per-1M for internal table
                            self._model_prices[m_id] = {
                                "input": float(pricing.get("prompt", 0)) * 1_000_000,
                                "output": float(pricing.get("completion", 0)) * 1_000_000
                            }
                    logger.info(f"Refreshed pricing for {len(data)} OpenRouter models.")
        except Exception as e:
            logger.error(f"Failed to refresh model prices: {e}")

    def count_tokens(self, text: str) -> int:
        if not text:
            return 0
        if not self._tokenizer:
            # Fallback to rough estimation if tokenizer failed to load
            return len(text) // 4
        return len(self._tokenizer.encode(text))
    
    async def _ensure_prices(self):
        if not self._prices_fetched:
            self._prices_fetched = True

            async def _fetch_and_confirm():
                try:
                    await self._fetch_model_prices()
                except Exception:
                    self._prices_fetched = False

            task = asyncio.create_task(_fetch_and_confirm())
            self._background_tasks.add(task)
            task.add_done_callback(self._background_tasks.discard)
            
    
    async def _record_local_usage(self, model: str, prompt_tokens: int, completion_tokens: int):
        """Record usage stats locally based on dynamic cost table (No HTTP calls)."""
        if not self._redis:
            return
            
        costs = self._model_prices.get(model)
        if not costs:
            logger.warning(f"No cost data for model {model}. Usage not recorded.")
            return
            
        input_cost = (prompt_tokens / 1_000_000) * costs["input"]
        output_cost = (completion_tokens / 1_000_000) * costs["output"]
        total_cost = input_cost + output_cost
        total_tokens = prompt_tokens + completion_tokens
        
        try:
            stats_key = RedisKeys.global_stats()
            async with self._redis.pipeline() as pipe:
                if total_tokens > 0:
                    pipe.hincrby(stats_key, "total_tokens", total_tokens)
                if total_cost > 0:
                    pipe.hincrbyfloat(stats_key, "total_cost", total_cost)
                await pipe.execute()
                
            logger.debug(f"Recorded approx usage ({model}): {total_tokens} tokens, ${total_cost:.6f} (approx)")
        except Exception as e:
            logger.error(f"Failed to record approx usage: {e}")
    
    async def call_llm(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 1.0,
        response_model: Optional[type] = None,
        reasoning: Optional[str] = None,
        mode: Optional[instructor.Mode] = None
    ) -> Optional[object]:
        """
        Basic completion for pipeline tasks.
        Defaults to EXTRACTION_MODEL. Callers can override for specific use cases.
        """
        self._ensure_client()
        await self._ensure_prices()
        model = model or self._extraction_model
        
        # Default to Mode.JSON if response_model is provided but no mode specified.
        # This is generally more reliable across different OpenRouter models.
        if response_model and mode is None:
            mode = instructor.Mode.JSON
        for attempt in range(MAX_RETRIES):
            try:
                # Use Dict[str, Any] to avoid Pyre2 being overly restrictive with types here
                create_kwargs: Dict[str, Any] = {
                    "model": model,
                    "messages": [
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ],
                    "temperature": temperature,
                    "max_retries": MAX_RETRIES,
                }
                
                if self._is_openrouter:
                    create_kwargs["extra_body"] = self._extra_body(reasoning)
                    
                if response_model:
                    create_kwargs["response_model"] = response_model
                    if mode:
                        create_kwargs["mode"] = mode
                
                if response_model:
                    response, completion = await self._client.chat.completions.create_with_completion(**create_kwargs)
                    
                    if completion and completion.usage:
                        task = asyncio.create_task(self._record_local_usage(
                            model,
                            completion.usage.prompt_tokens,
                            completion.usage.completion_tokens
                        ))
                        self._background_tasks.add(task)
                        task.add_done_callback(self._background_tasks.discard)
                    
                    return response

                response = await self._client.chat.completions.create(**create_kwargs)

                if not response.choices:
                    return None
                    
                content = response.choices[0].message.content
                
                if not content or not content.strip():
                    return None
                
                if self._trace:
                    self._trace.debug(
                        f"MODEL: {model}\n"
                        f"USER:\n{user}\n"
                        f"RESPONSE:\n{content}"
                    )
                
                if response.usage:
                    prompt = response.usage.prompt_tokens
                    comp = response.usage.completion_tokens
                    task = asyncio.create_task(self._record_local_usage(model, prompt, comp))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                
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
        temperature: float = 0.0,
        reasoning: Optional[str] = "low"
    ) -> AsyncGenerator[Dict, None]:
        """Streaming completion with tools. Defaults to agent model."""
        self._ensure_client()
        await self._ensure_prices()
        model = model or self._agent_model
        
        for attempt in range(MAX_RETRIES):
            try:
                # Use Dict[str, Any] for flexibility
                create_kwargs: Dict[str, Any] = dict(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ],
                    tools=tools,
                    tool_choice="auto",
                    temperature=temperature,
                    stream=True,
                )
                if self._is_openrouter:
                    # Use update to avoid item assignment type issues
                    create_kwargs.update({
                        "stream_options": {"include_usage": True},
                        "extra_body": self._extra_body(reasoning=reasoning)
                    })
                
                response = await self._client.chat.completions.create(**create_kwargs)
                
                content: str = ""
                tool_calls_by_index: Dict[int, Any] = {}
                tool_calls_detected = False
                usage = None
                generation_id: Optional[str] = None
                
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
                
                if not usage:
                    # Final fallback: Estimate tokens if stream didn't provide them
                    p_tokens = self.count_tokens(f"{system}\n{user}")
                    c_tokens = self.count_tokens(content)
                    usage = {
                        "prompt_tokens": p_tokens,
                        "completion_tokens": c_tokens,
                        "total_tokens": p_tokens + c_tokens
                    }

                
                # Local usage recording (No background HTTP fetch needed anymore)
                if usage:
                    prompt = usage.get("prompt_tokens", 0)
                    comp = usage.get("completion_tokens", 0)
                    task = asyncio.create_task(self._record_local_usage(model, prompt, comp))
                    self._background_tasks.add(task)
                    task.add_done_callback(self._background_tasks.discard)
                
                yield {"type": "done", "content": content, "usage": usage}
                return
                
            except Exception as e:
                if attempt < MAX_RETRIES - 1:
                    if content or tool_calls_detected:
                        # Cannot retry safely if we already generated partial streaming content before crashing
                        logger.error(f"Stream failed ({model}) mid-generation: {e}. Cannot retry safely.")
                        yield {"type": "error", "message": f"Stream interrupted mid-generation: {str(e)}"}
                        return
                    logger.warning(f"Stream failed ({model}): {e}. Retrying in {0.5 * (attempt + 1)}s...")
                    await asyncio.sleep(0.5 * (attempt + 1))
                    continue
                logger.error(f"Stream failed ({model}) after {MAX_RETRIES} retries: {e}")
                yield {"type": "error", "message": str(e)}
    
    async def close(self):
        if self._background_tasks:
            # Wait with shield to protect crucial stats requests
            shielded = [asyncio.shield(t) for t in self._background_tasks]
            try:
                await asyncio.wait(shielded, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for LLM usage stats recording tasks")
        if self._raw_client:
            await self._raw_client.close()
        elif self._client and hasattr(self._client, 'close'):
            await self._client.close()
