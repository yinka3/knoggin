from datetime import datetime, timezone
from typing import Optional, Union


def parse_iso_time(timestamp: Union[str, float, int]) -> Optional[datetime]:
    """Parse timestamp strictly. Returns None on failure — caller decides the fallback."""
    if not timestamp:
        return None

    if isinstance(timestamp, (int, float)):
        return datetime.fromtimestamp(timestamp, timezone.utc)

    timestamp_str = str(timestamp).strip()
    if timestamp_str.endswith("Z"):
        timestamp_str = timestamp_str[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(timestamp_str)
    except ValueError:
        return None


def parse_iso_time_or_now(timestamp: Union[str, float, int]) -> datetime:
    """Lenient wrapper — returns now() if parsing fails. Use for display-only contexts."""
    return parse_iso_time(timestamp) or datetime.now(timezone.utc)
