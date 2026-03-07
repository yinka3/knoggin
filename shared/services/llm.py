
import asyncio
import httpx
from typing import AsyncGenerator, Dict, List, Optional
from openai import AsyncOpenAI
from loguru import logger
import redis.asyncio as aioredis
from shared.infra.redis import RedisKeys

OPENROUTER_BASE_URL = "https://openrouter.ai/api/v1"
MAX_RETRIES = 3


class LLMService:

    def __init__(
        self,
        api_key: str = None,
        base_url: str = None,
        trace_logger=None,
        agent_model: str = "google/gemini-3.1-pro-preview",
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
        self._http_client = httpx.AsyncClient(timeout=10.0)
        self._background_tasks: set = set()
        
        if api_key:
            self._client = AsyncOpenAI(
                base_url=self._base_url,
                api_key=self._api_key,
                timeout=60.0
            )
            provider_label = "OpenRouter" if self._is_openrouter else self._base_url
            logger.info(f"LLMService initialized ({provider_label}) | extraction={extraction_model} | merge={merge_model} | agent={agent_model}")
        else:
            logger.warning("LLMService initialized without API key")
    
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
            raise ValueError(
                "LLM API key not configured. "
                "Please add your API key in Settings > Configuration."
            )
    
    def update_settings(self, api_key: str = None, base_url: str = None, agent_model: str = None, extraction_model: str = None, merge_model: str = None):
        if api_key and api_key != self._api_key:
            self._api_key = api_key
            if base_url:
                self._base_url = base_url
                self._is_openrouter = "openrouter.ai" in self._base_url
            self._client = AsyncOpenAI(
                base_url=self._base_url,
                api_key=self._api_key,
                timeout=60.0
            )
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
    
    def _extra_body(self, reasoning: str = None) -> Dict:
        """Build provider-specific extra_body. Only sent for OpenRouter."""
        if not self._is_openrouter:
            return {}
        body = {
            "provider": {
                "allow_fallbacks": True,
                "data_collection": "deny"
            }
        }
        if reasoning:
            body["reasoning"] = {"effort": reasoning}
        return body
    
    async def _record_usage_stats(self, generation_id: str):
        """Fetch exact usage/cost from OpenRouter and increment global limits in Redis."""
        if not self._is_openrouter or not self._redis or not generation_id:
            return
            
        # OpenRouter may take time to finalize generation stats, retry with backoff
        delays = [3, 6, 10]
        for attempt, delay in enumerate(delays):
            await asyncio.sleep(delay)
            
            usage = await self._fetch_generation_stats(generation_id)
            if not usage:
                if attempt < len(delays) - 1:
                    logger.debug(f"Generation stats not ready for {generation_id}, retrying in {delays[attempt+1]}s...")
                    continue
                logger.warning(f"Could not fetch generation stats for {generation_id} after {len(delays)} attempts")
                return
                
            try:
                total_tokens = usage.get("total_tokens", 0)
                cost = usage.get("cost", 0.0)
                
                if total_tokens > 0 or (cost and cost > 0):
                    stats_key = RedisKeys.global_stats()
                    
                    async with self._redis.pipeline() as pipe:
                        if total_tokens > 0:
                            pipe.hincrby(stats_key, "total_tokens", total_tokens)
                        if cost and cost > 0:
                            pipe.hincrbyfloat(stats_key, "total_cost", cost)
                        await pipe.execute()
                        
                    logger.debug(f"Recorded usage for {generation_id}: {total_tokens} tokens, ${cost:.6f}")
                return
            except Exception as e:
                logger.error(f"Failed to record usage stats for {generation_id}: {e}")
                return
    
    async def _record_cost_only(self, generation_id: str):
        """Fetch cost from OpenRouter generation API and record it (tokens tracked separately)."""
        if not self._is_openrouter or not self._redis or not generation_id:
            return
        
        # OpenRouter may take time to finalize generation stats, retry with backoff
        delays = [3, 6, 10]
        for attempt, delay in enumerate(delays):
            await asyncio.sleep(delay)
            
            usage = await self._fetch_generation_stats(generation_id)
            if usage:
                try:
                    cost = usage.get("cost", 0.0)
                    if cost and cost > 0:
                        stats_key = RedisKeys.global_stats()
                        await self._redis.hincrbyfloat(stats_key, "total_cost", cost)
                        logger.debug(f"Recorded cost for {generation_id}: ${cost:.6f}")
                        return
                except Exception as e:
                    logger.error(f"Failed to record cost for {generation_id}: {e}")
                    return
            
            if attempt < len(delays) - 1:
                logger.debug(f"Generation stats not ready for {generation_id}, retrying in {delays[attempt+1]}s...")
        
        logger.warning(f"Could not fetch generation stats for {generation_id} after {len(delays)} attempts")
    
    async def call_llm(
        self,
        system: str,
        user: str,
        model: Optional[str] = None,
        temperature: float = 0.0,
        reasoning: str = "low"
    ) -> Optional[str]:
        """
        Basic completion for pipeline tasks.
        Defaults to EXTRACTION_MODEL. Callers can override for specific use cases.
        """
        self._ensure_client()
        model = model or self._extraction_model
        
        for attempt in range(MAX_RETRIES):
            try:
                response = await self._client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": system},
                        {"role": "user", "content": user}
                    ],
                    temperature=temperature,
                    **(dict(extra_body=self._extra_body(reasoning)) if self._is_openrouter else {})
                )
                
                if not response.choices:
                    if attempt < MAX_RETRIES - 1:
                        logger.warning(f"Empty response choices, retrying ({attempt+1}/{MAX_RETRIES})")
                        await asyncio.sleep(0.5 * (attempt + 1))
                        continue
                    return None
                    
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
                
                if response.id:
                    task = asyncio.create_task(self._record_usage_stats(response.id))
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
        temperature: float = 0.0
    ) -> AsyncGenerator[Dict, None]:
        """Streaming completion with tools. Defaults to agent model."""
        self._ensure_client()
        model = model or self._agent_model
        
        for attempt in range(MAX_RETRIES):
            try:
                create_kwargs = dict(
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
                    create_kwargs["stream_options"] = {"include_usage": True}
                    create_kwargs["extra_body"] = self._extra_body(reasoning="low")
                
                response = await self._client.chat.completions.create(**create_kwargs)
                
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
                
                # Immediately record stream usage tokens (reliable, doesn't need generation API)
                if self._redis and usage:
                    stream_tokens = usage.get("total_tokens", 0)
                    if stream_tokens > 0:
                        try:
                            stats_key = RedisKeys.global_stats()
                            await self._redis.hincrby(stats_key, "total_tokens", stream_tokens)
                        except Exception as e:
                            logger.warning(f"Failed to record stream tokens: {e}")
                
                # Background fetch for cost (needs OpenRouter generation API)
                if generation_id:
                    task = asyncio.create_task(self._record_cost_only(generation_id))
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
    
    async def _fetch_generation_stats(self, generation_id: str) -> Optional[Dict]:
        try:
            resp = await self._http_client.get(
                f"https://openrouter.ai/api/v1/generation?id={generation_id}",
                headers={"Authorization": f"Bearer {self._api_key}"}
            )
            
            if resp.status_code != 200:
                logger.warning(f"OpenRouter generation stats API returned {resp.status_code} for {generation_id}: {resp.text[:200]}")
                return None
            
            data = resp.json().get("data", {})
            cost = data.get("total_cost") or 0.0  # handle None
            tokens_prompt = data.get("tokens_prompt") or 0
            tokens_completion = data.get("tokens_completion") or 0
            
            logger.debug(f"Generation {generation_id}: {tokens_prompt}+{tokens_completion} tokens, cost=${cost}")
            
            return {
                "prompt_tokens": tokens_prompt,
                "completion_tokens": tokens_completion,
                "total_tokens": tokens_prompt + tokens_completion,
                "cost": float(cost)
            }
        except Exception as e:
            logger.warning(f"Failed to fetch generation stats for {generation_id}: {e}")
        return None

    async def close(self):
        if self._background_tasks:
            # Wait with shield to protect crucial stats requests
            shielded = [asyncio.shield(t) for t in self._background_tasks]
            try:
                await asyncio.wait(shielded, timeout=5.0)
            except asyncio.TimeoutError:
                logger.warning("Timeout waiting for LLM usage stats recording tasks")
        if self._client:
            await self._client.close()
