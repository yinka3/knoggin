"""Internal event contracts used by engine coordination adapters."""

from dataclasses import dataclass
from typing import Any, Dict


@dataclass(frozen=True, slots=True)
class InternalEvent:
    """One scoped engine event before coordination persistence."""

    ts: str
    scope_id: str
    component: str
    event: str
    data: Dict[str, Any]
    verbose_only: bool = False
