"""Request shapes for project-owned settings operations."""

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, StrictInt

from common.schema.document import FolderScanSettings


class DomainActivationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_version: StrictInt = Field(ge=0)
    config: dict[str, Any]


class FolderScanSettingsRequest(FolderScanSettings):
    model_config = ConfigDict(extra="forbid", frozen=True)
