"""Agent-facing contract models."""

from dataclasses import dataclass, field
from datetime import datetime
from typing import Dict, List, Mapping, Optional, Union

from common.utils.agent_identity import parse_persona_profile, render_persona_profile
from common.utils.time_utils import get_now, parse_iso_time


@dataclass(frozen=True)
class PersonaProfile:
    attention_bias: str
    reasoning_style: str
    social_temperament: str
    communication_signature: str
    productive_flaw: str

    @classmethod
    def from_value(
        cls,
        value: Union["PersonaProfile", Mapping[str, str], str],
    ) -> "PersonaProfile":
        if isinstance(value, cls):
            return value
        data = (
            parse_persona_profile(value)
            if isinstance(value, str)
            else dict(value)
        )
        # Rendering performs the shared validation before construction.
        render_persona_profile(data)
        return cls(
            attention_bias=data["attention_bias"].strip(),
            reasoning_style=data["reasoning_style"].strip(),
            social_temperament=data["social_temperament"].strip(),
            communication_signature=data["communication_signature"].strip(),
            productive_flaw=data["productive_flaw"].strip(),
        )

    def to_dict(self) -> Dict[str, str]:
        return {
            "attention_bias": self.attention_bias,
            "reasoning_style": self.reasoning_style,
            "social_temperament": self.social_temperament,
            "communication_signature": self.communication_signature,
            "productive_flaw": self.productive_flaw,
        }

    def to_markdown(self) -> str:
        return render_persona_profile(self.to_dict())


@dataclass
class AgentConfig:
    id: str
    name: str
    # Stable, user-editable differentiation profile. The agent Brain stores
    # evolving context and lessons instead.
    persona: PersonaProfile
    instructions: Optional[str] = None
    model: Optional[str] = None
    temperature: float = 0.7
    enabled_tools: Optional[List[str]] = None
    is_default: bool = False
    is_spawned: bool = False
    spawned_by: Optional[str] = None
    brain_revision: int = 1
    created_at: datetime = field(default_factory=get_now)

    def __post_init__(self):
        self.persona = PersonaProfile.from_value(self.persona)

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "persona": self.persona.to_dict(),
            "persona_markdown": self.persona_markdown,
            "instructions": self.instructions,
            "model": self.model,
            "temperature": self.temperature,
            "enabled_tools": self.enabled_tools,
            "is_default": self.is_default,
            "is_spawned": self.is_spawned,
            "spawned_by": self.spawned_by,
            "brain_revision": self.brain_revision,
            "created_at": self.created_at.isoformat(),
        }

    @property
    def persona_profile(self) -> Dict[str, str]:
        """Structured representation used by agent settings forms."""
        return self.persona.to_dict()

    @property
    def persona_markdown(self) -> str:
        """Internal representation persisted in Postgres and sent to the model."""
        return self.persona.to_markdown()

    @classmethod
    def from_dict(cls, data: Dict) -> "AgentConfig":
        created = data.get("created_at")
        if isinstance(created, str):
            created = parse_iso_time(created)
        persona_value = data.get("persona_profile")
        if persona_value is None:
            persona_value = data["persona"]
        return cls(
            id=data["id"],
            name=data["name"],
            persona=persona_value,
            instructions=data.get("instructions"),
            model=data.get("model"),
            temperature=data.get("temperature", 0.7),
            enabled_tools=data.get("enabled_tools"),
            is_default=data.get("is_default", False),
            is_spawned=data.get("is_spawned", False),
            spawned_by=data.get("spawned_by"),
            brain_revision=data.get("brain_revision", 1),
            created_at=created or get_now(),
        )
