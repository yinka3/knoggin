import asyncio
import os
import tempfile
import threading
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml
from loguru import logger
from pydantic import BaseModel, ValidationError

from common.schema.agent.settings import validate_tool_limit_overrides
from common.schema.agent.tool_names import get_configurable_tool_names
from common.schema.settings import RootConfig
from common.utils.core_utils import safe_update

CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "./config"))
CONFIG_FILE_YAML = CONFIG_DIR / "knoggin.yml"
CONFIG_FILE_NOTICE = (
    "# This configuration file is managed by Knoggin.\n"
    "# Manual edits may be overwritten by the app.\n\n"
)


def deep_merge(source: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge updates into source dict."""
    for key, value in updates.items():
        if isinstance(value, dict) and key in source and isinstance(source[key], dict):
            deep_merge(source[key], value)
        else:
            source[key] = value
    return source


class ConfigManager:
    """
    A unified, thread-safe Configuration Event Bus.
    Handles YAML I/O, Pydantic validation, and dispatches targeted config updates to subscribed services.
    """
    _instance: Optional["ConfigManager"] = None
    _lock = threading.Lock()

    def __init__(self):
        if ConfigManager._instance is not None:
            raise Exception("ConfigManager is a singleton. Use ConfigManager.get()")

        self.config: RootConfig = RootConfig()
        self.subscribers: List[Dict[str, Any]] = []
        self._async_lock = asyncio.Lock()

        self.load()

    @classmethod
    def get(cls) -> "ConfigManager":
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls()
        return cls._instance

    def load(self):
        """Loads configuration from YAML."""
        from common.utils.prompt_loader import validate_prompt_library

        validate_prompt_library()
        data = None
        load_failed = False
        config_exists = CONFIG_FILE_YAML.exists()
        if config_exists:
            try:
                with open(CONFIG_FILE_YAML, "r") as f:
                    data = yaml.safe_load(f)
            except Exception as exc:
                load_failed = True
                logger.error(
                    "Failed to load knoggin.yml; keeping the active "
                    f"configuration: {exc}"
                )

        if load_failed:
            return
        if data:
            try:
                new_config = RootConfig(**data)
                self._validate_runtime_config(new_config)
                self.config = new_config
            except ValidationError as exc:
                errors = "; ".join(
                    f"{'.'.join(str(part) for part in error['loc'])}: "
                    f"{error['msg']}"
                    for error in exc.errors(include_url=False)
                )
                logger.error(
                    "Configuration validation failed; keeping the active "
                    f"configuration: {errors}"
                )
            except Exception as exc:
                logger.error(
                    "Configuration load failed; keeping the active "
                    f"configuration: {exc}"
                )
        else:
            self.config = RootConfig()

        if not config_exists:
            self.save()

    def save(self) -> bool:
        """Saves current Pydantic RootConfig to the YAML file."""
        try:
            CONFIG_DIR.mkdir(parents=True, exist_ok=True)
            # Use model_dump(mode="json") to get YAML-compatible primitive types (e.g. str dates)
            data = self.config.model_dump(mode="json")

            old_umask = os.umask(0o177)
            try:
                fd, temp_path = tempfile.mkstemp(dir=CONFIG_DIR, text=True)
                try:
                    with os.fdopen(fd, "w") as f:
                        f.write(CONFIG_FILE_NOTICE)
                        yaml.dump(data, f, default_flow_style=False, sort_keys=False)
                    os.replace(temp_path, CONFIG_FILE_YAML)
                except Exception as write_err:
                    os.unlink(temp_path)
                    raise write_err
            finally:
                os.umask(old_umask)
            return True
        except Exception as e:
            logger.error(f"Failed to save configuration to YAML: {e}")
            return False

    async def async_save(self) -> bool:
        """Async wrapper for save()."""
        async with self._async_lock:
            loop = asyncio.get_running_loop()
            return await loop.run_in_executor(None, self.save)

    def subscribe(self, callback: Callable, path: Optional[str] = None) -> Callable[[], None]:
        """
        Subscribe a service callback to configuration updates.

        Args:
            callback: Method to call when config changes (e.g. `self.update_settings`).
            path: Pydantic attribute path (e.g. 'developer_settings.jobs.episode').
                  If provided, the callback is only triggered if this specific subtree changes.
        """
        subscription = {
            "callback": callback,
            "path": path
        }
        self.subscribers.append(subscription)
        # Immediately invoke the callback with the current settings so the service initializes correctly
        current_val = self._get_nested_model(self.config, path)
        if current_val is not None:
            safe_update(callback, current_val)

        def unsubscribe():
            try:
                self.subscribers.remove(subscription)
            except ValueError:
                pass

        return unsubscribe

    def _get_nested_model(self, model: BaseModel, path: Optional[str]) -> Any:
        if not path:
            return model
        parts = path.split('.')
        current = model
        for p in parts:
            if current is None:
                return None
            current = getattr(current, p, None)
        return current

    def update_settings(self, updates: Dict[str, Any]) -> bool:
        """
        Applies a partial dictionary update to the RootConfig.
        Validates the schema, saves to YAML, and fires all registered subscriber callbacks.
        """
        current_data = self.config.model_dump()
        updated_data = deep_merge(current_data, updates)

        try:
            new_config = RootConfig(**updated_data)
            self._validate_runtime_config(new_config)
        except Exception as e:
            logger.error(f"Failed to validate configuration updates: {e}")
            return False

        old_config = self.config
        self.config = new_config
        self.save()

        logger.info("Applying hot-reload of runtime settings via ConfigManager...")

        # Fire subscribers
        for sub in list(self.subscribers):
            cb = sub["callback"]
            path = sub["path"]

            old_val = self._get_nested_model(old_config, path)
            new_val = self._get_nested_model(new_config, path)

            # Only trigger callback if the specific path has changed
            if old_val != new_val:
                try:
                    safe_update(cb, new_val)
                except Exception as e:
                    logger.error(f"Error calling configuration subscriber {cb.__name__}: {e}")

        return True

    @staticmethod
    def _validate_runtime_config(config: RootConfig) -> None:
        """Validate configuration against portable tool-name contracts."""

        validate_tool_limit_overrides(
            config.developer_settings.limits,
            get_configurable_tool_names(),
        )
