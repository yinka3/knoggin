"""Agent-facing contract models."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Optional

from common.utils.time_utils import get_now, parse_iso_time


@dataclass
class AgentConfig:
    id: str
    name: str
    persona: str
    instructions: Optional[str] = None
    model: Optional[str] = None
    temperature: float = 0.7
    enabled_tools: Optional[List[str]] = None
    is_default: bool = False
    is_spawned: bool = False
    spawned_by: Optional[str] = None
    created_at: datetime = field(default_factory=get_now)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "persona": self.persona,
            "instructions": self.instructions,
            "model": self.model,
            "temperature": self.temperature,
            "enabled_tools": self.enabled_tools,
            "is_default": self.is_default,
            "is_spawned": self.is_spawned,
            "spawned_by": self.spawned_by,
            "created_at": self.created_at.isoformat(),
        }

    @classmethod
    def from_dict(cls, data: Dict) -> "AgentConfig":
        created = data.get("created_at")
        if isinstance(created, str):
            created = parse_iso_time(created)
        return cls(
            id=data["id"],
            name=data["name"],
            persona=data["persona"],
            instructions=data.get("instructions"),
            model=data.get("model"),
            temperature=data.get("temperature", 0.7),
            enabled_tools=data.get("enabled_tools"),
            is_default=data.get("is_default", False),
            is_spawned=data.get("is_spawned", False),
            spawned_by=data.get("spawned_by"),
            created_at=created or get_now(),
        )
