from dataclasses import dataclass, field
from typing import Dict, Optional
from uuid import uuid4


@dataclass
class ToolCall:
    name: str
    args: Dict = field(default_factory=dict)
    thinking: Optional[str] = None
    call_id: str = field(default_factory=lambda: str(uuid4()))
