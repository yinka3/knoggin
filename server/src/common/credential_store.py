"""Saved logins for model providers and the services sandbox commands reach.

A value comes from the environment first, then from ``credentials.json`` in the
config folder. The file is separate from ``knoggin.yml`` so the settings API,
shared config files and instance backups never carry a secret, and it is
written readable by its owner only. Nothing here ever returns a value to the
API: callers outside the engine only learn whether a login is set.
"""

from __future__ import annotations

import json
import os
import threading
from dataclasses import dataclass
from pathlib import Path

CREDENTIALS_FILE = "credentials.json"
_MAX_VALUE_LENGTH = 4_000


@dataclass(frozen=True)
class Service:
    name: str
    label: str
    fields: tuple[str, ...]  # environment variable names, also the saved keys
    sandbox: bool  # usable by approved sandbox commands, allowed per project


SERVICES: dict[str, Service] = {
    service.name: service
    for service in (
        Service("openrouter", "OpenRouter", ("OPENROUTER_API_KEY",), sandbox=False),
        Service("jev", "JEV", ("JEV_API_KEY",), sandbox=False),
        Service("brave", "Brave Search", ("BRAVE_API_KEY",), sandbox=False),
        Service("tavily", "Tavily", ("TAVILY_API_KEY",), sandbox=False),
        Service("huggingface", "Hugging Face", ("HF_TOKEN",), sandbox=True),
        Service("kaggle", "Kaggle", ("KAGGLE_USERNAME", "KAGGLE_KEY"), sandbox=True),
        Service("github", "GitHub", ("GITHUB_TOKEN",), sandbox=True),
    )
}
SANDBOX_SERVICES = frozenset(name for name, service in SERVICES.items() if service.sandbox)
_FIELD_SERVICE = {field: service for service in SERVICES.values() for field in service.fields}

_lock = threading.Lock()
_cache: tuple[Path, int, dict[str, str]] | None = None


def _path() -> Path | None:
    from common.conf.manager import ConfigManager

    try:
        return ConfigManager.get().config_dir / CREDENTIALS_FILE
    except RuntimeError:  # not initialized: environment only
        return None


def _saved() -> dict[str, str]:
    """Saved values by field, re-read only when the file changes."""

    global _cache
    path = _path()
    if path is None:
        return {}
    with _lock:
        try:
            mtime = path.stat().st_mtime_ns
        except FileNotFoundError:
            _cache = None
            return {}
        if _cache is not None and _cache[0] == path and _cache[1] == mtime:
            return _cache[2]
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, ValueError):
            data = {}
        values = {
            key: value for key, value in (data if isinstance(data, dict) else {}).items()
            if key in _FIELD_SERVICE and isinstance(value, str) and value.strip()
        }
        _cache = (path, mtime, values)
        return values


def get(field: str) -> str:
    """One value: the environment wins, then the saved file, else ''."""

    return os.environ.get(field, "").strip() or _saved().get(field, "").strip()


def values(service: str) -> dict[str, str]:
    """Every field of a service, or {} unless all of them are set."""

    found = {field: get(field) for field in SERVICES[service].fields}
    return found if all(found.values()) else {}


def status() -> list[dict]:
    """Which logins are set and where from, never the values."""

    saved = _saved()
    result = []
    for service in SERVICES.values():
        sources = {
            "environment" if os.environ.get(field, "").strip()
            else "saved" if saved.get(field) else None
            for field in service.fields
        }
        result.append({
            "service": service.name,
            "label": service.label,
            "fields": list(service.fields),
            "sandbox": service.sandbox,
            "configured": None not in sources,
            "source": (
                None if None in sources else "environment" if "environment" in sources else "saved"
            ),
        })
    return result


def save(service: str, new_values: dict[str, str]) -> None:
    """Save every field of one service. ValueError for an unknown service,
    a missing or extra field, or an unusable value."""

    if service not in SERVICES:
        raise ValueError(f"Unknown service: {service}")
    fields = SERVICES[service].fields
    if not isinstance(new_values, dict) or set(new_values) != set(fields):
        raise ValueError(f"{service} needs exactly: {', '.join(fields)}")
    cleaned = {}
    for field, value in new_values.items():
        if not isinstance(value, str) or not value.strip() or len(value) > _MAX_VALUE_LENGTH or any(
            char in value for char in "\x00\r\n"
        ):
            raise ValueError(f"{field} must be a non-empty single-line value")
        cleaned[field] = value.strip()
    _write({**_saved(), **cleaned})


def remove(service: str) -> bool:
    """Forget the saved values of one service; False if none were saved."""

    if service not in SERVICES:
        raise ValueError(f"Unknown service: {service}")
    saved = _saved()
    if not any(field in saved for field in SERVICES[service].fields):
        return False
    _write({key: value for key, value in saved.items() if key not in SERVICES[service].fields})
    return True


def _write(data: dict[str, str]) -> None:
    global _cache
    path = _path()
    if path is None:
        raise RuntimeError("The config folder is not set, so logins cannot be saved")
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(f".{CREDENTIALS_FILE}.{os.getpid()}.tmp")
    with _lock:
        # Created owner-only (0600 on POSIX; on Windows the file inherits the
        # user-profile folder's access list).
        descriptor = os.open(temporary, os.O_WRONLY | os.O_CREAT | os.O_TRUNC, 0o600)
        try:
            with os.fdopen(descriptor, "w", encoding="utf-8") as stream:
                json.dump(data, stream, indent=2, sort_keys=True)
            os.replace(temporary, path)
        except BaseException:
            temporary.unlink(missing_ok=True)
            raise
        _cache = None
