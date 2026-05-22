from datetime import datetime, timezone
from typing import Union


def parse_iso_time(timestamp: Union[str, float, int]) -> datetime:
    """Safely parse ISO timestamps (including trailing Z) or Unix timestamps.
    Falls back to current UTC time if parsing fails or input is empty.
    """
    if not timestamp:
        return datetime.now(timezone.utc)

    if isinstance(timestamp, (int, float)):
        # Treat as Unix timestamp (seconds)
        # Note: if it's in milliseconds, this might give a year in the far future.
        # But for general use, assuming seconds is standard.
        return datetime.fromtimestamp(timestamp, timezone.utc)

    timestamp_str = str(timestamp).strip()
    if timestamp_str.endswith("Z"):
        timestamp_str = timestamp_str[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(timestamp_str)
    except ValueError:
        return datetime.now(timezone.utc)
