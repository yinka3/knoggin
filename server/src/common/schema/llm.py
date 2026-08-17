"""Shared strict primitives for data returned directly by an LLM."""

from typing import Optional

from pydantic import BaseModel, ConfigDict


def normalize_required_text(value: str, *, field_name: str) -> str:
    """Normalize a required model string without permitting blank values."""

    normalized = " ".join(value.split())
    if not normalized:
        raise ValueError(f"{field_name} must not be blank")
    return normalized


def normalize_optional_text(value: Optional[str], *, field_name: str) -> Optional[str]:
    """Normalize optional model text while preserving an omitted value."""

    if value is None:
        return None
    return normalize_required_text(value, field_name=field_name)


class StructuredLLMOutput(BaseModel):
    """Strict base schema for values returned directly by an LLM."""

    model_config = ConfigDict(extra="forbid")
