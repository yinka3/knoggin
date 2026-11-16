"""Public project workspace mutation requests."""

from pydantic import BaseModel, ConfigDict, Field


class FileCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = Field(min_length=1)
    content: str = Field(min_length=1)


class FileReplace(FileCreate):
    expected_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class FileAppend(BaseModel):
    model_config = ConfigDict(extra="forbid")
    content: str = Field(min_length=1)
    expected_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class FileMove(BaseModel):
    model_config = ConfigDict(extra="forbid")
    source_path: str = Field(min_length=1)
    destination_path: str = Field(min_length=1)
    expected_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class FileDelete(BaseModel):
    model_config = ConfigDict(extra="forbid")
    expected_content_hash: str = Field(pattern=r"^[0-9a-f]{64}$")


class FolderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")
    path: str = Field(min_length=1)
