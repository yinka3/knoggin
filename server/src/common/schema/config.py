"""Shared base model for persisted application configuration."""

from pydantic import BaseModel, ConfigDict


class ConfigModel(BaseModel):
    """Reject unrecognized keys at persisted configuration boundaries."""

    model_config = ConfigDict(extra="forbid")
