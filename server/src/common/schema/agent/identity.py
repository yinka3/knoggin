"""Agent identity and persona models."""

import math
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
    brain: Optional[str] = None
    model: Optional[str] = None
    temperature: float = 0.7
    enabled_tools: Optional[List[str]] = None
    is_default: bool = False
    # AAC participation is opt-in so creating an agent never begins autonomous
    # model work without the user's explicit choice.
    aac_enabled: bool = False
    spawned_by: Optional[str] = None
    brain_revision: int = 1
    created_at: datetime = field(default_factory=get_now)
    last_turn_at: Optional[datetime] = None

    def __post_init__(self):
        self.id = self._require_text(self.id, "id")
        self.persona = PersonaProfile.from_value(self.persona)
        self.name = self._require_text(self.name, "name")
        self.temperature = self._validate_temperature(self.temperature)
        self.brain_revision = self._validate_brain_revision(self.brain_revision)
        self.enabled_tools = self._normalize_enabled_tools(self.enabled_tools)
        if self.spawned_by is not None:
            self.spawned_by = self._require_text(self.spawned_by, "spawned_by")

    @staticmethod
    def _require_text(value: object, field_name: str) -> str:
        if not isinstance(value, str) or not value.strip():
            raise ValueError(f"AgentConfig {field_name} must not be blank")
        return value.strip()

    @staticmethod
    def _validate_temperature(value: object) -> float:
        if isinstance(value, bool) or not isinstance(value, (int, float)):
            raise ValueError("AgentConfig temperature must be a finite number")
        temperature = float(value)
        if not math.isfinite(temperature) or not 0.0 <= temperature <= 2.0:
            raise ValueError(
                "AgentConfig temperature must be finite and between 0.0 and 2.0"
            )
        return temperature

    @staticmethod
    def _validate_brain_revision(value: object) -> int:
        if isinstance(value, bool) or not isinstance(value, int) or value < 1:
            raise ValueError("AgentConfig brain_revision must be an integer >= 1")
        return value

    @staticmethod
    def _normalize_enabled_tools(
        tools: Optional[List[str]],
    ) -> Optional[List[str]]:
        if tools is None:
            return None
        if not isinstance(tools, list):
            raise ValueError("AgentConfig enabled_tools must be a list or None")

        normalized = []
        seen = set()
        for tool in tools:
            if not isinstance(tool, str) or not tool.strip():
                raise ValueError(
                    "AgentConfig enabled_tools must contain nonblank names"
                )
            name = tool.strip().lower()
            if name in seen:
                raise ValueError(
                    f"AgentConfig enabled_tools contains duplicate tool: {name}"
                )
            seen.add(name)
            normalized.append(name)
        return normalized

    def to_dict(self) -> Dict:
        return {
            "id": self.id,
            "name": self.name,
            "persona": self.persona.to_dict(),
            "persona_markdown": self.persona_markdown,
            "brain": self.brain,
            "model": self.model,
            "temperature": self.temperature,
            "enabled_tools": self.enabled_tools,
            "is_default": self.is_default,
            "aac_enabled": self.aac_enabled,
            "spawned_by": self.spawned_by,
            "brain_revision": self.brain_revision,
            "created_at": self.created_at.isoformat(),
            "last_turn_at": (
                self.last_turn_at.isoformat() if self.last_turn_at is not None else None
            ),
        }

    @property
    def persona_profile(self) -> Dict[str, str]:
        """Structured representation used by agent settings forms."""
        return self.persona.to_dict()

    @property
    def persona_markdown(self) -> str:
        """Internal representation persisted in Postgres and sent to the model."""
        return self.persona.to_markdown()

    @property
    def is_spawned(self) -> bool:
        """Whether this normal durable agent has a recorded AAC parent."""

        return self.spawned_by is not None

    @classmethod
    def from_dict(cls, data: Dict) -> "AgentConfig":
        created = data.get("created_at")
        if isinstance(created, str):
            created = parse_iso_time(created)
        last_turn_at = data.get("last_turn_at")
        if isinstance(last_turn_at, str):
            last_turn_at = parse_iso_time(last_turn_at)
        persona_value = data.get("persona_profile")
        if persona_value is None:
            persona_value = data["persona"]
        return cls(
            id=data["id"],
            name=data["name"],
            persona=persona_value,
            brain=data.get("brain"),
            model=data.get("model"),
            temperature=data.get("temperature", 0.7),
            enabled_tools=data.get("enabled_tools"),
            is_default=data.get("is_default", False),
            aac_enabled=data.get("aac_enabled", False),
            spawned_by=data.get("spawned_by"),
            brain_revision=data.get("brain_revision", 1),
            created_at=created or get_now(),
            last_turn_at=last_turn_at,
        )
