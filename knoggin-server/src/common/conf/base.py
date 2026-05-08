import asyncio
import copy
import json
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Dict, Optional

from dotenv import load_dotenv
from loguru import logger

from common.schema.settings import RootConfig

load_dotenv()

CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "./config"))
CONFIG_FILE = CONFIG_DIR / "knoggin.json"

GLOBAL_CONFIG_LOCK = asyncio.Lock()

_config_lock = threading.Lock()
_config_cache: Optional[dict] = None
_config_mtime: Optional[float] = None

MCP_SERVER_PRESETS = [
    {
        "id": "google-workspace",
        "name": "Google Workspace",
        "description": "Gmail, Calendar, Drive & Docs",
        "command": "uvx",
        "args": ["google-workspace-mcp"],
        "env_vars": [
            {"key": "GOOGLE_CLIENT_ID", "label": "Client ID", "placeholder": "your-client-id.apps.googleusercontent.com"},
            {"key": "GOOGLE_CLIENT_SECRET", "label": "Client Secret", "placeholder": "GOCSPX-..."},
            {"key": "GOOGLE_REFRESH_TOKEN", "label": "Refresh Token", "placeholder": "1//0..."},
        ],
        "tags": ["gmail", "calendar", "drive", "docs", "google"],
        "risk": "moderate",
        "risk_note": "Can read emails, calendar events, and drive files. Write access depends on OAuth scopes granted.",
        "help_url": "https://console.cloud.google.com/apis/credentials",
        "help_label": "Google Cloud Console → Create OAuth credentials",
    },
    {
        "id": "google-maps",
        "name": "Google Maps",
        "description": "Location search, directions & geocoding",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-google-maps"],
        "env_vars": [
            {"key": "GOOGLE_MAPS_API_KEY", "label": "Maps API Key", "placeholder": "AIza..."},
        ],
        "tags": ["maps", "location", "directions", "google"],
        "risk": "safe",
        "risk_note": "Read-only location lookups and directions. No destructive operations.",
        "help_url": "https://console.cloud.google.com/apis/credentials",
        "help_label": "Google Cloud Console → Create API key with Maps enabled",
    },
    {
        "id": "github",
        "name": "GitHub",
        "description": "Repos, issues, PRs & code search",
        "command": "uvx",
        "args": ["mcp-server-github"],
        "env_vars": [
            {"key": "GITHUB_TOKEN", "label": "Personal Access Token", "placeholder": "ghp_..."},
        ],
        "tags": ["github", "git", "code", "repos"],
        "risk": "moderate",
        "risk_note": "Can read repos, issues, and PRs. Write access (creating issues, PRs) depends on token scopes.",
        "allowed_tools": [
            "search_repositories", "get_file_contents", "search_code",
            "list_issues", "get_issue", "list_commits",
            "get_pull_request", "list_pull_requests"
        ],
        "help_url": "https://github.com/settings/tokens",
        "help_label": "GitHub → Settings → Developer settings → Personal access tokens",
    },

    {
        "id": "filesystem",
        "name": "Filesystem",
        "description": "Read, write & search local files",
        "command": "npx",
        "args": ["-y", "@modelcontextprotocol/server-filesystem", "/home"],
        "env_vars": [],
        "tags": ["files", "filesystem", "local"],
        "risk": "destructive",
        "risk_note": "Can read, write, and delete files on your system. Restricted to read-only by default.",
        "allowed_tools": ["read_file", "list_directory", "search_files", "get_file_info"],
    },
    {
        "id": "slack",
        "name": "Slack",
        "description": "Channels, messages & users",
        "command": "uvx",
        "args": ["mcp-server-slack"],
        "env_vars": [
            {"key": "SLACK_BOT_TOKEN", "label": "Bot Token", "placeholder": "xoxb-..."},
        ],
        "tags": ["slack", "messaging", "chat"],
        "risk": "moderate",
        "risk_note": "Can read channels and messages. Write access depends on bot token scopes.",
        "allowed_tools": [
            "slack_list_channels", "slack_get_channel_history",
            "slack_get_users", "slack_get_thread_replies"
        ],
        "help_url": "https://api.slack.com/apps",
        "help_label": "Slack API → Create app → Bot token",
    },
]

def get_default_config() -> dict:
    """Get system default configuration from RootConfig model."""
    return RootConfig().model_dump()

def get_developer_mode_presets() -> list[dict]:
    return [
        {
            "id": "default",
            "name": "Default Knoggin",
            "description": "Balanced speed and deep context (current standard defaults).",
            "settings": get_default_config()["developer_settings"]
        },
        {
            "id": "speed",
            "name": "Speed & Lightweight",
            "description": "Optimized for fast responses by using smaller batch sizes and tighter limits. Suitable for simple chats.",
            "settings": {
                "ingestion": {
                    "batch_size": 4,
                    "batch_timeout": 30.0
                },
                "jobs": {
                    "cleaner": {"enabled": True, "interval_hours": 12, "orphan_age_hours": 12, "stale_junk_days": 15},
                    "profile": {"msg_window": 15, "volume_threshold": 8, "idle_threshold": 30, "profile_batch_size": 4, "max_facts_context": 30, "contradiction_sim_low": 0.70, "contradiction_sim_high": 0.95, "contradiction_batch_size": 2},
                    "merger": {"enabled": True, "auto_threshold": 0.95, "hitl_threshold": 0.75, "cosine_threshold": 0.65},
                    "dlq": {"interval_seconds": 120, "batch_size": 20, "max_attempts": 2},
                    "archival": {"enabled": True, "retention_days": 7, "fallback_interval_hours": 24},
                    "topic_config": {"enabled": False, "interval_msgs": 40, "conversation_window": 50}
                },
                "limits": {
                    "agent_history_turns": 4,
                    "max_tool_calls": 5,
                    "max_attempts": 6,
                    "max_consecutive_errors": 2,
                    "max_accumulated_messages": 10,
                    "conversation_context_turns": 4,
                    "max_conversation_history": 2000,
                    "tool_limits": {
                        "search_messages": 2, "get_connections": 2, "search_entity": 2, "get_activity": 2, "find_path": 2, "fact_check": 2,
                        "get_hierarchy": 2, "save_memory": 2, "forget_memory": 2, "search_files": 2, "web_search": 2, "news_search": 2
                    }
                },
                "search": {
                    "vector_limit": 10, "fts_limit": 10, "rerank_candidates": 10, "default_message_limit": 4, "default_entity_limit": 3, "default_activity_hours": 12
                },
                "community": {
                    "enabled": False, "interval_minutes": 60, "max_turns": 5, "seeding_agent_id": None, "agent_pool_ids": []
                }
            }
        },
        {
            "id": "deep",
            "name": "Deep Research",
            "description": "Large context windows and intensive thresholding for deep analysis. Suitable for extensive investigations. WARNING: Autonomous Agent Community (AAC) is active and agents will discuss automatically.",
            "settings": {
                "ingestion": {
                    "batch_size": 16,
                    "batch_timeout": 600.0
                },
                "jobs": {
                    "cleaner": {"enabled": False, "interval_hours": 48, "orphan_age_hours": 48, "stale_junk_days": 60},
                    "profile": {"msg_window": 60, "volume_threshold": 25, "idle_threshold": 120, "profile_batch_size": 16, "max_facts_context": 100, "contradiction_sim_low": 0.70, "contradiction_sim_high": 0.95, "contradiction_batch_size": 8},
                    "merger": {"enabled": True, "auto_threshold": 0.90, "hitl_threshold": 0.50, "cosine_threshold": 0.50},
                    "dlq": {"interval_seconds": 30, "batch_size": 100, "max_attempts": 3},
                    "archival": {"enabled": True, "retention_days": 30, "fallback_interval_hours": 12},
                    "topic_config": {"enabled": True, "interval_msgs": 20, "conversation_window": 100}
                },
                "limits": {
                    "agent_history_turns": 15,
                    "max_tool_calls": 25,
                    "max_attempts": 30,
                    "max_consecutive_errors": 5,
                    "max_accumulated_messages": 100,
                    "conversation_context_turns": 25,
                    "max_conversation_history": 50000,
                    "tool_limits": {
                        "search_messages": 15, "get_connections": 20, "search_entity": 20, "get_activity": 15, "find_path": 15, "fact_check": 10,
                        "get_hierarchy": 15, "save_memory": 10, "forget_memory": 10, "search_files": 10, "web_search": 15, "news_search": 15
                    }
                },
                "search": {
                    "vector_limit": 100, "fts_limit": 100, "rerank_candidates": 100, "default_message_limit": 20, "default_entity_limit": 15, "default_activity_hours": 72
                },
                "community": {
                    "enabled": True, "interval_minutes": 15, "max_turns": 25, "seeding_agent_id": None, "agent_pool_ids": []
                }
            }
        }
    ]


def _get_file_mtime() -> Optional[float]:
    """Get file modification time, or None if file doesn't exist."""
    try:
        return CONFIG_FILE.stat().st_mtime
    except FileNotFoundError:
        return None

def get_config() -> RootConfig:
    """Convenience helper to get validated RootConfig object."""
    data = load_config() or get_default_config()
    return RootConfig(**data)

def load_config(force_reload: bool = False) -> Optional[dict]:
    global _config_cache, _config_mtime

    with _config_lock:
        if not CONFIG_FILE.exists():
            _config_cache = None
            _config_mtime = None
            return None

        current_mtime = _get_file_mtime()

        if not force_reload and _config_cache is not None and _config_mtime == current_mtime:
            return _config_cache

        try:
            with open(CONFIG_FILE, "r") as f:
                data = json.load(f)
                RootConfig(**data)
                _config_cache = data
                _config_mtime = current_mtime
                return _config_cache
        except Exception as e:
            logger.error(f"Failed to load/validate config: {e}")
            raise


def save_config(data: dict) -> bool:
    global _config_cache, _config_mtime

    try:
        CONFIG_DIR.mkdir(parents=True, exist_ok=True)
        old_umask = os.umask(0o177)
        try:
            fd, temp_path = tempfile.mkstemp(dir=CONFIG_DIR, text=True)
            try:
                with os.fdopen(fd, "w") as f:
                    json.dump(data, f, indent=2)
                os.replace(temp_path, CONFIG_FILE)
            except Exception as write_err:
                os.unlink(temp_path)
                raise write_err
        finally:
            os.umask(old_umask)

        with _config_lock:
            _config_cache = data
            _config_mtime = _get_file_mtime()
        return True
    except (IOError, OSError, TypeError, ValueError) as e:
        logger.error(f"Failed to save config: {e}")
        return False

async def async_save_config(config: dict) -> bool:
    """Async wrapper for save_config with global locking."""
    async with GLOBAL_CONFIG_LOCK:
        loop = asyncio.get_running_loop()
        return await loop.run_in_executor(None, save_config, config)


def invalidate_config_cache():
    global _config_cache, _config_mtime
    with _config_lock:
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

def deep_merge(source: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge updates into source dict."""
    for key, value in updates.items():
        if isinstance(value, dict) and key in source and isinstance(source[key], dict):
            deep_merge(source[key], value)
        else:
            source[key] = value
    return source


def update_config_value(key: str, updates: dict) -> bool:
    """
    Update a top-level config key with deep merge.
    Preserves existing nested values not included in updates.
    """
    config = load_config() or get_default_config()

    if key not in config:
        config[key] = {}

    current = config[key]
    if isinstance(current, dict) and isinstance(updates, dict):
        new_val = copy.deepcopy(current)
        deep_merge(new_val, updates)
        config[key] = new_val
    else:
        config[key] = updates

    return save_config(config)

def redact_config(config: dict) -> dict:
    out = copy.deepcopy(config)

    if "llm" in out and isinstance(out["llm"], dict):
        api_key = out["llm"].get("api_key")
        if api_key:
            out["llm"]["api_key"] = f"...{api_key[-4:]}" if len(api_key) > 4 else "***"

    if "search" in out and isinstance(out["search"], dict):
        brave_key = out["search"].get("brave_api_key")
        if brave_key:
            out["search"]["brave_api_key"] = f"...{brave_key[-4:]}" if len(brave_key) > 4 else "***"
        tavily_key = out["search"].get("tavily_api_key")
        if tavily_key:
            out["search"]["tavily_api_key"] = f"...{tavily_key[-4:]}" if len(tavily_key) > 4 else "***"

    if "mcp" in out and isinstance(out["mcp"], dict):
        servers = out["mcp"].get("servers", {})
        for _, server_cfg in servers.items():
            if isinstance(server_cfg, dict) and "env" in server_cfg:
                env = server_cfg["env"]
                if isinstance(env, dict):
                    for key in env:
                        val = env[key]
                        if val:
                            env[key] = f"...{val[-4:]}" if len(val) > 4 else "***"

    return out
