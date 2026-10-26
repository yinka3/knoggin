"""Document and source request boundaries."""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from common.document_limits import MAX_DOCUMENT_BASE64_LENGTH, MAX_SOURCE_BATCH_ITEMS
from common.schema.document import DocumentExtractionMode, DocumentExtractionStrategy


class UploadDocument(BaseModel):
    model_config = ConfigDict(extra="forbid")

    original_name: str = Field(min_length=1, max_length=512)
    content_base64: str = Field(min_length=1)
    relative_path: str | None = Field(default=None, min_length=1, max_length=2048)
    extraction_mode: DocumentExtractionMode = "standard"
    extraction_strategy: DocumentExtractionStrategy = "auto"


class SaveWebLink(BaseModel):
    model_config = ConfigDict(extra="forbid")

    url: str = Field(min_length=1, max_length=2048)
    title: str | None = Field(default=None, max_length=512)
    summary: str | None = Field(default=None, max_length=4000)


class UpdateWebLink(BaseModel):
    model_config = ConfigDict(extra="forbid")

    title: str | None = Field(default=None, max_length=512)
    summary: str | None = Field(default=None, max_length=4000)


class OcrPreflight(BaseModel):
    model_config = ConfigDict(extra="forbid")

    original_name: str = Field(min_length=1, max_length=512)
    content_base64: str = Field(min_length=1, max_length=MAX_DOCUMENT_BASE64_LENGTH)


class BatchDocument(UploadDocument):
    source_type: Literal["document"]


class BatchWebLink(SaveWebLink):
    source_type: Literal["web_link"]


class BatchSources(BaseModel):
    model_config = ConfigDict(extra="forbid")

    items: tuple[
        Annotated[Union[BatchDocument, BatchWebLink], Field(discriminator="source_type")], ...
    ] = Field(min_length=1, max_length=MAX_SOURCE_BATCH_ITEMS)

    @field_validator("items")
    @classmethod
    def bounded_content(cls, items):
        if sum(len(item.content_base64) for item in items if isinstance(item, BatchDocument)) > MAX_DOCUMENT_BASE64_LENGTH:
            raise ValueError("Batch content is too large")
        return items


class PromoteSource(BaseModel):
    model_config = ConfigDict(extra="forbid")

    session_id: str = Field(min_length=1)
    source_ref_id: str = Field(min_length=1)
    title: str | None = Field(default=None, max_length=512)
    summary: str | None = Field(default=None, max_length=4000)


class FolderFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relative_path: str = Field(min_length=1, max_length=2048)
    content_base64: str = Field(min_length=1)


class FolderIntake(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder_name: str = Field(min_length=1, max_length=512)
    files: list[FolderFile] = Field(min_length=1, max_length=1000)
    force_include_paths: list[str] = Field(default_factory=list, max_length=1000)


class StagedFolderFile(BaseModel):
    model_config = ConfigDict(extra="forbid")

    relative_path: str = Field(min_length=1, max_length=2048)
    size_bytes: int = Field(ge=1)


class StagedFolderCreate(BaseModel):
    model_config = ConfigDict(extra="forbid")

    folder_name: str = Field(min_length=1, max_length=512)
    files: list[StagedFolderFile] = Field(min_length=1, max_length=1000)
    force_include_paths: list[str] = Field(default_factory=list, max_length=1000)
