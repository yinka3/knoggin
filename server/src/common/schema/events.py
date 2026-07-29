"""Internal event contracts shared by debug and coordination adapters."""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True, slots=True)
class InternalEvent:
    """One internal event before debug and coordination adapters diverge."""

    ts: str
    scope_id: str
    component: str
    event: str
    data: Dict[str, Any]
    verbose_only: bool = False
