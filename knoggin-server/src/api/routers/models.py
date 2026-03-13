import asyncio
from datetime import datetime, timedelta

import httpx
from fastapi import APIRouter, HTTPException

from common.config.base import load_config

router = APIRouter()

# Module-level cache for OpenRouter models
_models_cache = None
_models_cache_expiry = None


@router.get("/curated")
async def get_curated_models():
    config = load_config()
    return {"models": config.get("curated_models", [])}


@router.get("/")
async def get_available_models():
    """
    Fetch models from OpenRouter, filtered for Knoggin's use cases.
    Returns separate lists for reasoning (with thinking) and agent (with tools) models.
    Results are cached in-memory for 1 hour.
    """
    global _models_cache, _models_cache_expiry

    if _models_cache and _models_cache_expiry and datetime.now() < _models_cache_expiry:
        return _models_cache

    try:
        async with httpx.AsyncClient() as client:
            reasoning_req = client.get(
                "https://openrouter.ai/api/v1/models",
                params={"supported_parameters": "reasoning"},
                timeout=15.0
            )
            
            tools_req = client.get(
                "https://openrouter.ai/api/v1/models",
                params={"supported_parameters": "tools"},
                timeout=15.0
            )
            
            reasoning_resp, tools_resp = await asyncio.gather(reasoning_req, tools_req)
            
            if reasoning_resp.status_code != 200 or tools_resp.status_code != 200:
                raise HTTPException(status_code=502, detail="Failed to fetch models from OpenRouter")
            
            reasoning_data = reasoning_resp.json().get("data", [])
            tools_data = tools_resp.json().get("data", [])
            
            def transform_model(m):
                pricing = m.get("pricing", {})
                prompt_price = float(pricing.get("prompt", 0)) * 1_000_000
                completion_price = float(pricing.get("completion", 0)) * 1_000_000
                
                return {
                    "id": m.get("id"),
                    "name": m.get("name"),
                    "context_length": m.get("context_length"),
                    "prompt_price": round(prompt_price, 2),
                    "completion_price": round(completion_price, 2),
                }
            
            result = {
                "reasoning": [transform_model(m) for m in reasoning_data],
                "agent": [transform_model(m) for m in tools_data],
                "cached_at": datetime.now().isoformat()
            }
            
            _models_cache = result
            _models_cache_expiry = datetime.now() + timedelta(hours=1)
            
            return result
            
    except httpx.TimeoutException:
        raise HTTPException(status_code=504, detail="OpenRouter request timed out")
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))
