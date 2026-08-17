"""HTTP request contracts owned by the FastAPI application layer."""

from typing import Annotated, Any, Literal, Optional, Union

from pydantic import BaseModel, ConfigDict, Field

from knoggin import (
    DocumentFocus as SdkDocumentFocus,
)
from knoggin import (
    DocumentFocusDocument,
    DocumentFocusFolderUpload,
    DocumentFocusSubtree,
)


class _DocumentFocusRequest(BaseModel):
    """Browser input for the document scope of one message."""

    model_config = ConfigDict(populate_by_name=True, extra="forbid")


class DocumentFocusDocumentRequest(_DocumentFocusRequest):
    target_type: Literal["document"] = Field(alias="targetType")
    document_id: str = Field(alias="documentId", min_length=1)

    def to_sdk(self) -> DocumentFocusDocument:
        return DocumentFocusDocument(document_id=self.document_id)


class DocumentFocusSubtreeRequest(_DocumentFocusRequest):
    target_type: Literal["subtree"] = Field(alias="targetType")
    folder_root_id: str = Field(alias="folderRootId", min_length=1)
    path_prefix: str = Field(alias="pathPrefix", min_length=1)

    def to_sdk(self) -> DocumentFocusSubtree:
        return DocumentFocusSubtree(
            folder_root_id=self.folder_root_id,
            path_prefix=self.path_prefix,
        )


class DocumentFocusFolderUploadRequest(_DocumentFocusRequest):
    target_type: Literal["folder_upload"] = Field(alias="targetType")
    folder_root_id: str = Field(alias="folderRootId", min_length=1)

    def to_sdk(self) -> DocumentFocusFolderUpload:
        return DocumentFocusFolderUpload(folder_root_id=self.folder_root_id)


DocumentFocusRequest = Annotated[
    Union[
        DocumentFocusDocumentRequest,
        DocumentFocusSubtreeRequest,
        DocumentFocusFolderUploadRequest,
    ],
    Field(discriminator="target_type"),
]


def document_focus_to_sdk(
    focus: DocumentFocusRequest | None,
) -> SdkDocumentFocus | None:
    return focus.to_sdk() if focus is not None else None


class ProjectCreateRequest(BaseModel):
    """Create one project and its initial domain configuration."""

    model_config = ConfigDict(populate_by_name=True)

    name: str = Field(min_length=1, max_length=200)
    domain_config: dict[str, Any] = Field(alias="domainConfig")
    description: Optional[str] = Field(default=None, max_length=2_000)


class SessionCreateRequest(BaseModel):
    """Optional engine settings for a newly created session."""

    model_config = ConfigDict(populate_by_name=True)

    model: Optional[str] = Field(default=None, max_length=200)
    agent_id: Optional[str] = Field(default=None, alias="agentId", max_length=200)
    enabled_tools: Optional[list[str]] = Field(default=None, alias="enabledTools")


class MessageCreateRequest(BaseModel):
    """One user message to submit to an existing session."""

    model_config = ConfigDict(populate_by_name=True)

    content: str = Field(min_length=1, max_length=100_000)
    model: Optional[str] = Field(default=None, max_length=200)
    agent_id: Optional[str] = Field(default=None, alias="agentId", max_length=200)
    enabled_tools: Optional[list[str]] = Field(default=None, alias="enabledTools")
    document_focus: Optional[DocumentFocusRequest] = Field(
        default=None,
        alias="documentFocus",
    )
