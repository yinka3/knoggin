import os
import json
import secrets
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional
import httpx
from loguru import logger
from dotenv import load_dotenv

load_dotenv()

CONFIG_DIR = Path(os.getenv("VESTIGE_CONFIG_DIR", "./config"))
CONFIG_FILE = CONFIG_DIR / "vestige.json"

DEFAULT_REASONING_MODEL = "google/gemini-3-flash-preview"
DEFAULT_AGENT_MODEL = "google/gemini-3-flash-preview"
DEFAULT_TOPICS = ["General"]


def generate_password(length: int = 32) -> str:
    return secrets.token_urlsafe(length)[:length]


def get_default_config() -> dict:
    return {
        "user_name": "",
        "openrouter_api_key": "",
        "user_summary": None,
        "configured_at": None,
        "redis_password": generate_password(),
        "memgraph_user": "vestige",
        "memgraph_password": generate_password(),
        "reasoning_model": DEFAULT_REASONING_MODEL,
        "agent_model": DEFAULT_AGENT_MODEL,
        "topics": DEFAULT_TOPICS,
        "labels": []
    }


def load_config() -> Optional[dict]:
    if not CONFIG_FILE.exists():
        return None
    try:
        with open(CONFIG_FILE, "r") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load config: {e}")
        return None


def save_config(data: dict) -> bool:
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        
        existing = load_config() or get_default_config()
        existing.update(data)
        
        if not existing.get("configured_at"):
            existing["configured_at"] = datetime.now(timezone.utc).isoformat()
        
        with open(CONFIG_FILE, "w") as f:
            json.dump(existing, f, indent=2)
        
        logger.info(f"Config saved to {CONFIG_FILE}")
        return True
    except IOError as e:
        logger.error(f"Failed to save config: {e}")
        return False


def is_configured() -> bool:
    config = load_config()
    if config and config.get("user_name") and config.get("openrouter_api_key"):
        return True
    
    # Fall back to .env
    return bool(
        os.environ.get("VESTIGE_USER_NAME") and 
        os.environ.get("OPENROUTER_API_KEY")
    )


async def validate_openrouter_key(api_key: str) -> tuple[bool, str]:
    if not api_key or not api_key.strip():
        return False, "API key is required"
    
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            response = await client.get(
                "https://openrouter.ai/api/v1/models",
                headers={"Authorization": f"Bearer {api_key}"}
            )
            
            if response.status_code == 200:
                return True, "Valid"
            elif response.status_code == 401:
                return False, "Invalid API key"
            else:
                return False, f"OpenRouter returned {response.status_code}"
                
    except httpx.TimeoutException:
        return False, "OpenRouter request timed out"
    except httpx.RequestError as e:
        return False, f"Connection error: {str(e)}"


def get_config_value(key: str, default=None):
    config = load_config()
    if not config:
        return default
    return config.get(key, default)
