import json
from typing import Any

from loguru import logger


def safe_json_loads(data: str, default: Any = None) -> Any:
    """Safely parse JSON string, returning a default value on failure."""
    if not isinstance(data, str):
        return data if data is not None else default
    try:
        return json.loads(data)
    except json.JSONDecodeError as e:
        logger.warning(
            "Failed to parse JSON: {} at line {}, column {} ({} characters)",
            e.msg,
            e.lineno,
            e.colno,
            len(data),
        )
        return default
