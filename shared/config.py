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

CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "./config"))
CONFIG_FILE = CONFIG_DIR / "knoggin.json"

DEFAULT_REASONING_MODEL = "google/gemini-2.5-flash"
DEFAULT_AGENT_MODEL = "google/gemini-3-flash-preview"
DEFAULT_TOPICS = ["General"]


def get_default_config() -> dict:
    return {
        "user_name": "",
        "user_summary": None,
        "configured_at": None,
        "reasoning_model": DEFAULT_REASONING_MODEL,
        "agent_model": DEFAULT_AGENT_MODEL,
        "default_topics": {
            "General": {
                "active": True, 
                "labels": [],
                "hierarchy": {}, 
                "aliases": [],
                "label_aliases": {},
            },
            "Identity": {
                "active": True,
                "labels": ["person"],
                "hierarchy": {},
                "aliases": [],
                "label_aliases": {}
            }
        },
        "agent_name": "STELLA",
"system_prompt": """You are {agent_name}, a personal knowledge management assistant. Your role is to help the user organize, recall, and connect information from their conversations.

Core behaviors:
- Be conversational and helpful
- Remember context from previous messages
- Surface relevant connections between topics
- Ask clarifying questions when needed
- Be concise unless detail is requested

You have access to a knowledge graph that stores entities, facts, and relationships extracted from conversations.""",
        "openrouter_api_key": "",
        "direct_provider": None,
        "direct_api_key": "",
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
    return bool(config and config.get("user_name"))


def get_required_config(key: str):
    value = get_config_value(key)
    if not value:
        raise RuntimeError(f"Config missing required key: {key}")
    return value

def get_config_value(key: str, default=None):
    config = load_config()
    if not config:
        return default
    return config.get(key, default)
