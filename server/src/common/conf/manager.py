import asyncio
import inspect
import os
import tempfile
import threading
from copy import deepcopy
from dataclasses import dataclass
from functools import wraps
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

import yaml
from loguru import logger
from pydantic import BaseModel, ValidationError

from common.schema.agent.settings import validate_tool_limit_overrides
from common.schema.agent.tool_names import get_configurable_tool_names
from common.schema.settings import RootConfig
from common.utils.lifecycle import settle_owned_task

CONFIG_FILE_NOTICE = (
    "# This configuration file is managed by Knoggin.\n"
    "# Manual edits may be overwritten by the app.\n\n"
)


class ConfigurationLoadError(RuntimeError):
    """Raised when the configured YAML file cannot safely initialize runtime."""


class ConfigurationPersistenceError(RuntimeError):
    """Raised when initial configuration cannot be persisted."""


@dataclass(frozen=True)
class ConfigurationApplyStatus:
    """Value-free diagnostics for the latest publication attempt."""

    generation: int
    persisted: bool
    activated: bool
    failed_subscriptions: tuple[int, ...] = ()
    pending_subscriptions: tuple[int, ...] = ()

    @property
    def fully_applied(self) -> bool:
        return self.activated and not self.failed_subscriptions and not self.pending_subscriptions


def deep_merge(source: Dict[str, Any], updates: Dict[str, Any]) -> Dict[str, Any]:
    """Recursively merge updates into source dict."""
    for key, value in updates.items():
        if isinstance(value, dict) and key in source and isinstance(source[key], dict):
            deep_merge(source[key], value)
        else:
            source[key] = value
    return source


def _serialized(method):
    @wraps(method)
    def call(self, *args, **kwargs):
        with self._state_lock:
            return method(self, *args, **kwargs)
    return call


class ConfigManager:
    """
    Serialized configuration persistence and synchronous publication.

    Reads return isolated snapshots. Once services subscribe, publication must
    run on that thread (normally the application loop thread). async_save moves
    only save-current I/O to a worker; it never calls service subscribers there.
    Callbacks may reenter on the same thread; queued delivery uses latest state.
    """
    _instance: Optional["ConfigManager"] = None
    _lock = threading.Lock()

    def __init__(self, config_dir: Path):
        if ConfigManager._instance is not None:
            raise Exception("ConfigManager is a singleton. Use ConfigManager.get()")

        self.config_dir = config_dir.expanduser().resolve()
        self.config_file = self.config_dir / "knoggin.yml"

        self._state_lock = threading.RLock()
        self._config: RootConfig = RootConfig()
        self._callback_thread: int | None = None
        self.subscribers: List[Dict[str, Any]] = []
        self._async_lock = asyncio.Lock()
        self._loaded = False
        self._generation = 0
        self._next_subscription = 0
        self._pending_applies: dict[int, dict] = {}
        self._failed_applies: dict[int, dict] = {}
        self._dispatching = False
        self._last_apply_status = ConfigurationApplyStatus(0, False, False)

        self.load(require_valid=True)

    @property
    def config(self) -> RootConfig:
        """Return an isolated snapshot; use update_settings to change live state."""
        with self._state_lock:
            return self._config.model_copy(deep=True)

    def _require_callback_thread(self) -> None:
        if self.subscribers and self._callback_thread != threading.get_ident():
            raise RuntimeError("Configuration publication must run on the subscriber thread")

    @property
    def last_apply_status(self) -> ConfigurationApplyStatus:
        with self._state_lock:
            return self._last_apply_status

    def _record_status(self, *, persisted: bool, activated: bool) -> None:
        self._last_apply_status = ConfigurationApplyStatus(
            self._generation, persisted, activated,
            tuple(self._failed_applies), tuple(self._pending_applies),
        )

    def _validate_config(self, data: dict) -> RootConfig:
        candidate = RootConfig.model_validate(data)
        self._validate_runtime_config(candidate)
        return candidate

    def _validation_summary(self, error: ValidationError) -> str:
        """Report known field locations and error codes, never inputs/messages."""
        summaries = []
        for item in error.errors(include_input=False, include_context=False, include_url=False):
            current: Any = self._config
            location = []
            for part in item["loc"]:
                if isinstance(current, BaseModel) and part in type(current).model_fields:
                    location.append(str(part))
                    current = getattr(current, part)
                else:
                    location.append("<entry>")
                    current = None
            summaries.append(f"{'.'.join(location) or '<root>'}: {item['type']}")
        return "; ".join(summaries)

    @staticmethod
    def _invoke(callback: Callable, value: Any) -> None:
        # Reporting apply failures requires exceptions to remain visible here.
        if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(getattr(callback, "__call__", None)):
            raise TypeError("Configuration subscribers must be synchronous")
        result = callback(deepcopy(value))
        if inspect.isawaitable(result):
            if inspect.iscoroutine(result):
                result.close()
            raise TypeError("Configuration subscribers must not return awaitables")

    def _publish(self, candidate: RootConfig) -> None:
        previous = self._config
        self._config = candidate.model_copy(deep=True)
        self._generation += 1
        for sub in list(self.subscribers):
            token = sub["id"]
            if (self._get_nested_model(previous, sub["path"]) !=
                    self._get_nested_model(self._config, sub["path"]) or token in self._failed_applies):
                self._pending_applies[token] = sub
        self._drain_applies()

    def _drain_applies(self) -> None:
        if self._dispatching:
            self._record_status(persisted=True, activated=True)
            return
        self._dispatching = True
        try:
            while self._pending_applies:
                token = next(iter(self._pending_applies))
                sub = self._pending_applies.pop(token)
                if not any(item is sub for item in self.subscribers):
                    self._failed_applies.pop(token, None)
                    continue
                try:
                    # Resolve now: nested publications must not deliver stale values.
                    self._invoke(sub["callback"], self._get_nested_model(self._config, sub["path"]))
                except Exception:
                    if any(item is sub for item in self.subscribers):
                        self._failed_applies[token] = sub
                    logger.error("Configuration subscriber {} failed to apply", token)
                else:
                    self._failed_applies.pop(token, None)
        finally:
            self._dispatching = False
            self._record_status(persisted=True, activated=True)

    @_serialized
    def retry_failed_applies(self) -> ConfigurationApplyStatus:
        """Retry failed subscribers against current state without rewriting YAML."""
        self._require_callback_thread()
        previous_status = self._last_apply_status
        generation = self._generation
        self._pending_applies.update(self._failed_applies)
        self._drain_applies()
        if self._generation == generation:
            # Retrying service application is not a new successful disk write.
            self._record_status(persisted=previous_status.persisted, activated=self._loaded)
        return self._last_apply_status

    @classmethod
    def initialize(cls, config_dir: str | Path) -> "ConfigManager":
        """Initialize the process-wide configuration bus at one explicit path."""

        resolved = Path(config_dir).expanduser().resolve()
        with cls._lock:
            if cls._instance is None:
                cls._instance = cls(resolved)
            elif cls._instance.config_dir != resolved:
                raise RuntimeError(
                    "ConfigManager is already initialized for "
                    f"{cls._instance.config_dir}; cannot switch to {resolved}"
                )
        return cls._instance

    @classmethod
    def get(cls) -> "ConfigManager":
        with cls._lock:
            if cls._instance is None:
                raise RuntimeError(
                    "ConfigManager has not been initialized with a configuration directory"
                )
            return cls._instance

    def resolve_path(self, configured_path: str | Path) -> Path:
        """Resolve one config-owned path independently of the process cwd."""

        path = Path(configured_path).expanduser()
        if path.is_absolute():
            return path.resolve()
        return (self.config_dir / path).resolve()

    @_serialized
    def load(self, *, require_valid: bool = False) -> bool:
        """Validate and publish YAML; True means accepted, not every service applied.

        Consult last_apply_status for subscriber failures. Runtime load is an
        explicit reload, not a file watcher, and never rewrites the input file.
        """
        from common.utils.prompt_loader import validate_prompt_library

        validate_prompt_library()
        self._require_callback_thread()
        self._record_status(persisted=False, activated=False)
        data = None
        load_failed = False
        config_exists = self.config_file.exists()
        if not config_exists and self._loaded:
            message = f"Configuration file is missing: {self.config_file}"
            if require_valid:
                raise ConfigurationLoadError(message)
            logger.error(message)
            return False
        if config_exists:
            try:
                with self.config_file.open("r", encoding="utf-8") as f:
                    data = yaml.safe_load(f)
            except Exception:
                load_failed = True
                message = f"Failed to load {self.config_file}: unreadable file or invalid YAML"
                if require_valid:
                    raise ConfigurationLoadError(message) from None
                logger.error(f"{message}; keeping the active configuration")

        if load_failed:
            return False
        if config_exists or not self._loaded:
            try:
                if not config_exists:
                    data = {}
                if not isinstance(data, dict):
                    message = "Configuration root must be a mapping"
                    if require_valid:
                        raise ConfigurationLoadError(message)
                    logger.error(message)
                    return False
                new_config = self._validate_config(data)
            except ValidationError as exc:
                errors = self._validation_summary(exc)
                message = f"Configuration validation failed for {self.config_file}: {errors}"
                if require_valid:
                    raise ConfigurationLoadError(message) from None
                logger.error(f"{message}; keeping the active configuration")
                return False
            except ConfigurationLoadError:
                raise
            except Exception:
                message = f"Configuration load failed for {self.config_file}: runtime validation failed"
                if require_valid:
                    raise ConfigurationLoadError(message) from None
                logger.error(f"{message}; keeping the active configuration")
                return False
        if not config_exists and not self._persist_config(new_config):
            raise ConfigurationPersistenceError(
                f"Failed to create initial configuration at {self.config_file}"
            )
        self._loaded = True
        self._publish(new_config)
        return True

    @_serialized
    def save(self) -> bool:
        """Persist the current active snapshot, never an unrelated candidate."""
        return self._persist_config(self._config)

    @_serialized
    def _persist_config(self, config: RootConfig) -> bool:
        """Internal candidate write; callers own ordered activation under the lock."""
        try:
            self.config_dir.mkdir(parents=True, exist_ok=True)
            # Use model_dump(mode="json") to get YAML-compatible primitive types (e.g. str dates)
            data = config.model_dump(mode="json")

            # mkstemp creates an exclusive private file without changing process
            # permissions. Windows ACLs are separate from POSIX mode guarantees.
            fd, temp_path = tempfile.mkstemp(dir=self.config_dir, text=True)
            stream = None
            try:
                stream = os.fdopen(fd, "w", encoding="utf-8")
                with stream as f:
                    f.write(CONFIG_FILE_NOTICE)
                    yaml.dump(data, f, default_flow_style=False, sort_keys=False, allow_unicode=True)
                Path(temp_path).replace(self.config_file)
            except BaseException:
                if stream is None:
                    os.close(fd)
                Path(temp_path).unlink(missing_ok=True)
                raise
            return True
        except Exception:
            logger.error("Failed to save configuration to YAML")
            return False

    async def async_save(self) -> bool:
        """Async wrapper for save()."""
        async with self._async_lock:
            owned = asyncio.create_task(asyncio.to_thread(self.save))
            try:
                return await asyncio.shield(owned)
            except asyncio.CancelledError:
                await settle_owned_task(owned)
                raise

    @_serialized
    def subscribe(self, callback: Callable, path: Optional[str] = None) -> Callable[[], None]:
        """
        Subscribe a service callback to configuration updates.

        Args:
            callback: Method to call when config changes (e.g. `self.update_settings`).
            path: Pydantic attribute path (e.g. 'developer_settings.jobs.episode').
                  If provided, the callback is only triggered if this specific subtree changes.
        """
        self._require_callback_thread()
        self._validate_subscription(callback, path)
        self._callback_thread = threading.get_ident()
        self._next_subscription += 1
        subscription = {
            "id": self._next_subscription,
            "callback": callback,
            "path": path
        }
        self.subscribers.append(subscription)
        # Immediately invoke the callback with the current settings so the service initializes correctly
        current_val = self._get_nested_model(self.config, path)
        try:
            self._invoke(callback, current_val)
        except BaseException as exc:
            self.subscribers[:] = [sub for sub in self.subscribers if sub is not subscription]
            self._pending_applies.pop(subscription["id"], None)
            self._failed_applies.pop(subscription["id"], None)
            self._record_status(
                persisted=self._last_apply_status.persisted,
                activated=self._last_apply_status.activated,
            )
            if not isinstance(exc, Exception):
                raise
            raise RuntimeError("Initial configuration subscriber apply failed") from None

        def unsubscribe():
            with self._state_lock:
                self.subscribers[:] = [sub for sub in self.subscribers if sub is not subscription]
                self._pending_applies.pop(subscription["id"], None)
                self._failed_applies.pop(subscription["id"], None)
                self._record_status(
                    persisted=self._last_apply_status.persisted,
                    activated=self._last_apply_status.activated,
                )

        return unsubscribe

    def _validate_subscription(self, callback: Callable, path: Optional[str]) -> None:
        if not callable(callback):
            raise TypeError("Configuration subscriber must be callable")
        if inspect.iscoroutinefunction(callback) or inspect.iscoroutinefunction(getattr(callback, "__call__", None)):
            raise TypeError("Configuration subscribers must be synchronous")
        try:
            inspect.signature(callback).bind(object())
        except (TypeError, ValueError):
            raise TypeError("Configuration subscriber must accept one settings value") from None
        if path is None:
            return
        if not isinstance(path, str) or not path or path.strip() != path:
            raise ValueError("Invalid configuration subscription path")
        current = self._config
        for part in path.split("."):
            if not isinstance(current, BaseModel) or part not in type(current).model_fields:
                raise ValueError("Invalid configuration subscription path")
            current = getattr(current, part)

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

    @_serialized
    def update_settings(self, updates: Dict[str, Any]) -> bool:
        """
        Applies a partial dictionary update to the RootConfig.
        Validates the schema, saves to YAML, and fires all registered subscriber callbacks.
        True means persisted and activated. Inspect last_apply_status for apply
        failures; retry_failed_applies retries them without rolling back the file.
        """
        self._require_callback_thread()
        self._record_status(persisted=False, activated=False)
        try:
            if not isinstance(updates, dict):
                raise TypeError("Updates must be a mapping")
            updated_data = deep_merge(self._config.model_dump(), updates)
            new_config = self._validate_config(updated_data)
        except ValidationError as exc:
            logger.error("Failed to validate configuration updates: {}", self._validation_summary(exc))
            return False
        except Exception:
            logger.error("Failed to validate configuration updates")
            return False

        if not self._persist_config(new_config):
            logger.error("Configuration update was not applied because persistence failed")
            return False
        self._publish(new_config)
        return True

    @staticmethod
    def _validate_runtime_config(config: RootConfig) -> None:
        """Validate configuration against portable tool-name contracts."""

        validate_tool_limit_overrides(
            config.developer_settings.limits,
            get_configurable_tool_names(),
        )
