from __future__ import annotations

import threading
from pathlib import Path
from typing import Any, Dict, Optional

from loguru import logger

from common.schema.settings import CoordinationLogSettings


class CoordinationLog:
    """Best-effort searchable sink for scrubbed coordination events."""

    def __init__(self) -> None:
        self._settings = CoordinationLogSettings()
        self._sink_id: Optional[int] = None
        self._lock = threading.Lock()

    def configure(self, settings: CoordinationLogSettings) -> None:
        with self._lock:
            if self._sink_id is not None:
                logger.remove(self._sink_id)
                self._sink_id = None

            self._settings = settings
            if not settings.enabled:
                return

            try:
                path = Path(settings.path)
                path.parent.mkdir(parents=True, exist_ok=True)
                self._sink_id = logger.add(
                    str(path),
                    format="{message}",
                    level="INFO",
                    filter=lambda record: (
                        record["extra"].get("coordination_log") is True
                    ),
                    rotation=f"{settings.rotation_mb} MB",
                    retention=f"{settings.retention_days} days",
                    backtrace=False,
                    diagnose=False,
                )
            except Exception as exc:
                logger.warning(
                    "Failed to configure coordination inspection log: "
                    f"{type(exc).__name__}"
                )

    def write(self, fields: Dict[str, Any]) -> None:
        if not self._settings.enabled or self._sink_id is None:
            return
        try:
            logger.bind(coordination_log=True).info(format_logfmt(fields))
        except Exception as exc:
            logger.warning(
                f"Failed to write coordination inspection event: {type(exc).__name__}"
            )


def format_logfmt(fields: Dict[str, Any]) -> str:
    return " ".join(
        f"{key}={_format_value(value)}"
        for key, value in fields.items()
        if value is not None
    )


def _format_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    text = str(value)
    if text == "":
        return '""'
    needs_quote = any(ch.isspace() or ch in {'"', "="} for ch in text)
    escaped = text.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"' if needs_quote else escaped


COORDINATION_LOG = CoordinationLog()


def configure_coordination_log(settings: CoordinationLogSettings) -> None:
    COORDINATION_LOG.configure(settings)


def write_coordination_event(fields: Dict[str, Any]) -> None:
    COORDINATION_LOG.write(fields)
