"""HTTP-only notebook request contracts."""

from pydantic import BaseModel, ConfigDict, Field


class DeleteNotebook(BaseModel):
    model_config = ConfigDict(extra="forbid")

    expected_revision: int = Field(ge=1)
