"""Validated coordinates for document, message, and search-result sources."""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StrictLocator(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PdfPageLocator(_StrictLocator):
    """A one-based page locator for a PDF retrieval chunk."""

    kind: Literal["pdf_page"] = "pdf_page"
    page: int = Field(ge=1)


class TextLineLocator(_StrictLocator):
    """A one-based inclusive line range, optionally under a Markdown section."""

    kind: Literal["text_lines"] = "text_lines"
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=1)
    section_path: tuple[str, ...] | None = None

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line")
        return self

    @field_validator("section_path")
    @classmethod
    def _validate_section_path(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is not None and any(not part.strip() for part in value):
            raise ValueError("section_path must not contain blank headings")
        return value


class CsvRowLocator(_StrictLocator):
    """A one-based inclusive data-row range; the header is not a data row."""

    kind: Literal["csv_rows"] = "csv_rows"
    start_row: int = Field(ge=1)
    end_row: int = Field(ge=1)

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_row < self.start_row:
            raise ValueError("end_row must be greater than or equal to start_row")
        return self


class CodeLineLocator(_StrictLocator):
    """A one-based inclusive source-code line range with an optional symbol."""

    kind: Literal["code_lines"] = "code_lines"
    start_line: int = Field(ge=1)
    end_line: int = Field(ge=1)
    symbol_name: str | None = None

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_line < self.start_line:
            raise ValueError("end_line must be greater than or equal to start_line")
        return self

    @field_validator("symbol_name")
    @classmethod
    def _validate_symbol_name(cls, value: str | None) -> str | None:
        if value is not None and not value.strip():
            raise ValueError("symbol_name must not be blank")
        return value


class DocxParagraphLocator(_StrictLocator):
    """A one-based inclusive DOCX body-paragraph range under Word headings."""

    kind: Literal["docx_paragraphs"] = "docx_paragraphs"
    start_paragraph: int = Field(ge=1)
    end_paragraph: int = Field(ge=1)
    heading_path: tuple[str, ...] | None = None

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_paragraph < self.start_paragraph:
            raise ValueError(
                "end_paragraph must be greater than or equal to start_paragraph"
            )
        return self

    @field_validator("heading_path")
    @classmethod
    def _validate_heading_path(
        cls, value: tuple[str, ...] | None
    ) -> tuple[str, ...] | None:
        if value is not None and any(not part.strip() for part in value):
            raise ValueError("heading_path must not contain blank headings")
        return value


class PastedTextLocator(_StrictLocator):
    """A zero-based, end-exclusive span in the canonical user message."""

    kind: Literal["character_span"] = "character_span"
    start_char: int = Field(ge=0)
    end_char: int = Field(ge=1)

    @model_validator(mode="after")
    def _validate_range(self):
        if self.end_char <= self.start_char:
            raise ValueError("end_char must be greater than start_char")
        return self


class SearchResultLocator(_StrictLocator):
    """The provider result returned to the agent, not the linked web page."""

    kind: Literal["search_result"] = "search_result"
    provider: str = Field(min_length=1)
    query: str = Field(min_length=1)
    rank: int = Field(ge=1)

    @field_validator("provider", "query")
    @classmethod
    def _reject_blank_text(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("search locator fields must not be blank")
        return value


DocumentLocator = Annotated[
    Union[
        PdfPageLocator,
        TextLineLocator,
        CsvRowLocator,
        CodeLineLocator,
        DocxParagraphLocator,
    ],
    Field(discriminator="kind"),
]
SourceLocator = Annotated[
    Union[
        PdfPageLocator,
        TextLineLocator,
        CsvRowLocator,
        CodeLineLocator,
        DocxParagraphLocator,
        PastedTextLocator,
        SearchResultLocator,
    ],
    Field(discriminator="kind"),
]
