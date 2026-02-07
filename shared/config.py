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

_config_cache: Optional[dict] = None
_config_mtime: Optional[float] = None

def get_default_config() -> dict:
    return {
        "_warning": "This file is auto-generated. Use the UI to modify settings. Manual edits may be overwritten.",
        "user_name": "",
        "user_aliases": [],
        "user_facts": [],
        "configured_at": None,
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
        "curated_models": [
            {
                "id": "anthropic/claude-sonnet-4.5",
                "name": "Claude Sonnet 4.5",
                "input_price": 3.00,
                "output_price": 15.00
            },
            {
                "id": "anthropic/claude-opus-4.5",
                "name": "Claude Opus 4.5",
                "input_price": 5.00,
                "output_price": 25.00
            },
            {
                "id": "x-ai/grok-4.1-fast",
                "name": "Grok 4.1 Fast",
                "input_price": 0.20,
                "output_price": 0.50
            },
            {
                "id": "openai/gpt-5.1",
                "name": "GPT-5.1",
                "input_price": 1.25,
                "output_price": 10.00
            },
            {
                "id": "google/gemini-3-pro-preview",
                "name": "Gemini 3 Pro",
                "input_price": 2.00,
                "output_price": 12.00
            },
            {
                "id": "anthropic/claude-haiku-4.5",
                "name": "Claude Haiku 4.5",
                "input_price": 1.00,
                "output_price": 5.00
            },
            {
                "id": "google/gemini-2.5-flash-lite-preview-09-2025",
                "name": "Gemini 2.5 Flash Lite",
                "input_price": 0.10,
                "output_price": 0.40
            },
            {
                "id": "google/gemini-2.5-flash",
                "name": "Gemini 2.5 Flash",
                "input_price": 0.30,
                "output_price": 2.50
            },
            {
                "id": "deepseek/deepseek-v3.1",
                "name": "DeepSeek V3.1",
                "input_price": 0.60,
                "output_price": 1.70
            },
            {
                "id": "openai/gpt-oss-120b:free",
                "name": "GPT-OSS-120B",
                "input_price": 0,
                "output_price": 0
            }
        ],
        "llm": {
            "api_key": "",
            "reasoning_model": "google/gemini-2.5-flash",
            "agent_model": "google/gemini-3-flash-preview"
        },
        
        "developer_settings": {
            
            "ingestion": {
                "batch_size": 8,
                "batch_timeout": 300.0,
                # "checkpoint_interval": 32,  # Optional override (default: 4x batch)
                # "session_window": 24        # Optional override (default: 3x batch)
            },
            
            "jobs": {
                "cleaner": {
                    "interval_hours": 24,
                    "orphan_age_hours": 24,
                    "stale_junk_days": 30
                },
                "profile": {
                    "msg_window": 30,
                    "volume_threshold": 30,
                    "idle_threshold": 60,
                    "profile_batch_size": 8,
                    "contradiction_sim_low": 0.70,
                    "contradiction_sim_high": 0.95,
                    "contradiction_batch_size": 4
                },
                "merger": {
                    "auto_threshold": 0.93,
                    "hitl_threshold": 0.65,
                    "cosine_threshold": 0.65
                },
                "dlq": {
                    "interval_seconds": 60,
                    "batch_size": 50,
                    "max_attempts": 2
                },
                "archival": {
                    "retention_days": 14
                }
            },

            "search": {
                "vector_limit": 50,
                "fts_limit": 50,
                "rerank_candidates": 45,
                "default_message_limit": 8,
                "default_entity_limit": 5,
                "default_activity_hours": 24
            },
            
            "limits": {
                "agent_history_turns": 7,
                "max_tool_calls": 6,
                "max_attempts": 8,
                "max_consecutive_errors": 3,
                "max_accumulated_messages": 30,
                "conversation_context_turns": 10,
                "tool_limits": {
                    "search_messages": 2,
                    "get_connections": 4,
                    "search_entity": 4,
                    "get_activity": 5,
                    "find_path": 5,
                    "get_hierarchy": 5,
                    "save_memory": 2,
                    "forget_memory": 2
                }
            },
            
            "entity_resolution": {
                "fuzzy_substring_threshold": 75,
                "fuzzy_non_substring_threshold": 91,
                "generic_token_freq": 10,
                "candidate_fuzzy_threshold": 85,   
                "candidate_vector_threshold": 0.85
            },

            "nlp_pipeline": {
                "gliner_threshold": 0.85,
                "vp01_min_confidence": 0.8
            }

        }
    }

def _get_file_mtime() -> Optional[float]:
    """Get file modification time, or None if file doesn't exist."""
    try:
        return CONFIG_FILE.stat().st_mtime
    except FileNotFoundError:
        return None


def load_config(force_reload: bool = False) -> Optional[dict]:
    """Load config with caching. Reloads if file changed."""
    global _config_cache, _config_mtime
    
    if not CONFIG_FILE.exists():
        _config_cache = None
        _config_mtime = None
        return None
    
    current_mtime = _get_file_mtime()
    
    if not force_reload and _config_cache is not None and _config_mtime == current_mtime:
        return _config_cache
    
    try:
        with open(CONFIG_FILE, "r") as f:
            _config_cache = json.load(f)
            _config_mtime = current_mtime
            return _config_cache
    except (json.JSONDecodeError, IOError) as e:
        logger.error(f"Failed to load config: {e}")
        return None


def save_config(data: dict) -> bool:
    """Save config and invalidate cache."""
    global _config_cache, _config_mtime
    
    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        with open(CONFIG_FILE, "w") as f:
            json.dump(data, f, indent=2)
        
        CONFIG_FILE.chmod(0o600)
        
        _config_cache = data
        _config_mtime = _get_file_mtime()
        return True
    except IOError as e:
        logger.error(f"Failed to save config: {e}")
        return False


def invalidate_config_cache():
    """Force reload on next access. Call after external config changes."""
    global _config_cache, _config_mtime
    _config_cache = None
    _config_mtime = None


def is_configured() -> bool:
    config = load_config()
    return bool(config and config.get("user_name"))


def get_config_value(key: str, default=None):
    """Get a top-level config value. Uses cached config."""
    config = load_config()
    if not config:
        return default
    return config.get(key, default)
