"""Project metadata request and response contracts."""

from datetime import datetime
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator


class WorkspaceView(BaseModel):
    name: str
    read_only: bool = False


def workspace_views(workspaces) -> list[dict]:
    """Stored or parsed workspaces as a name-ordered list for responses."""
    from core.project.project_files import parse_workspaces

    return [{"name": name, **settings.model_dump()}
            for name, settings in parse_workspaces(workspaces).items()]


class ProjectView(BaseModel):
    id: str
    name: str
    description: str | None = None
    status: Literal["active", "archived", "deleted"]
    session_count: int = 0

    @field_validator("workspaces", mode="before")
    @classmethod
    def list_workspaces(cls, value):
        if isinstance(value, (dict, str)) or value is None:
            return workspace_views(value or {})
        return value
    allowed_projects: list[str] = Field(default_factory=list)
    workspaces: list[WorkspaceView] = Field(default_factory=list)
    created_at: datetime | None = None
    updated_at: datetime | None = None


class CreateProject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=10_000)

    @field_validator("name")
    @classmethod
    def nonblank_name(cls, value: str) -> str:
        value = value.strip()
        if not value:
            raise ValueError("blank name")
        return value


class UpdateProject(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str | None = Field(default=None, min_length=1, max_length=200)
    description: str | None = Field(default=None, max_length=10_000)
    allowed_projects: list[str] | None = None

    @field_validator("name")
    @classmethod
    def nonblank_name(cls, value: str | None) -> str | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            raise ValueError("blank name")
        return value


class AddWorkspace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=100)
    read_only: bool = False


class UpdateWorkspace(BaseModel):
    model_config = ConfigDict(extra="forbid")

    read_only: bool | None = None
