from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from typing import Iterator, Optional, Protocol, Union


class Clock(Protocol):
    def now(self) -> datetime:
        """Return a timezone-aware UTC datetime."""
        ...


class SystemClock:
    def now(self) -> datetime:
        return datetime.now(timezone.utc)


class TestClock:
    __test__ = False

    def __init__(self, frozen_at: Union[str, float, int, datetime]):
        self._now = _coerce_utc_datetime(frozen_at)

    def now(self) -> datetime:
        return self._now

    def set(self, frozen_at: Union[str, float, int, datetime]) -> None:
        self._now = _coerce_utc_datetime(frozen_at)

    def advance(self, delta: Optional[timedelta] = None, **kwargs) -> datetime:
        if delta is None:
            delta = timedelta(**kwargs)
        elif kwargs:
            delta += timedelta(**kwargs)
        self._now += delta
        return self._now


def _coerce_utc_datetime(value: Union[str, float, int, datetime]) -> datetime:
    if isinstance(value, datetime):
        dt = value
    elif isinstance(value, (int, float)):
        dt = datetime.fromtimestamp(value, timezone.utc)
    elif isinstance(value, str):
        parsed = parse_iso_time(value)
        if parsed is None:
            raise ValueError(f"Cannot parse datetime from string: {value}")
        dt = parsed
    else:
        raise TypeError(f"Cannot create clock time from {type(value)}: {value}")

    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


_SYSTEM_CLOCK = SystemClock()
_active_clock: Clock = _SYSTEM_CLOCK


def get_now() -> datetime:
    return _active_clock.now()


def get_now_iso() -> str:
    return get_now().isoformat()


def get_now_ms() -> int:
    return int(get_now().timestamp() * 1000)


def get_now_unix() -> float:
    return get_now().timestamp()


def set_test_clock(clock_or_time: Union[Clock, str, float, int, datetime]) -> Clock:
    global _active_clock
    if not isinstance(clock_or_time, (str, float, int, datetime)) and hasattr(
        clock_or_time, "now"
    ) and callable(clock_or_time.now):
        _active_clock = clock_or_time
    else:
        _active_clock = TestClock(clock_or_time)
    return _active_clock


def reset_clock() -> None:
    global _active_clock
    _active_clock = _SYSTEM_CLOCK


@contextmanager
def frozen_time(frozen_at: Union[Clock, str, float, int, datetime]) -> Iterator[Clock]:
    global _active_clock
    previous = _active_clock
    clock = set_test_clock(frozen_at)
    try:
        yield clock
    finally:
        _active_clock = previous


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
    return parse_iso_time(timestamp) or get_now()
