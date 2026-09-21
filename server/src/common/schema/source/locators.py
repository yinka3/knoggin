"""Validated coordinates for document, message, and search-result sources."""

from typing import Annotated, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator, model_validator


class _StrictLocator(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")


class PdfPageLocator(_StrictLocator):
    """A one-based page locator for a PDF retrieval chunk."""

    kind: Literal["pdf_page"] = "pdf_page"
    page: int = Field(ge=1)


class LayoutBoundingBox(_StrictLocator):
    """A PDF-point rectangle in a bottom-left coordinate system."""

    left: float
    bottom: float
    right: float
    top: float

    @model_validator(mode="after")
    def _validate_bounds(self):
        if self.right <= self.left or self.top <= self.bottom:
            raise ValueError("layout bounding boxes must have positive area")
        return self


class LayoutRegionLocator(_StrictLocator):
    """A retained region; mixed and unknown express page-level uncertainty."""

    kind: Literal["layout_region"] = "layout_region"
    page: int = Field(ge=1)
    element_type: str = Field(min_length=1)
    extraction_method: Literal[
        "native_text", "ocr", "model_interpretation", "mixed", "unknown"
    ]
    bbox: LayoutBoundingBox | None = None
    text_start: int | None = Field(default=None, ge=0)
    text_end: int | None = Field(default=None, ge=1)
    coordinate_unit: Literal["pdf_points"] = "pdf_points"
    coordinate_origin: Literal["bottom_left"] = "bottom_left"

    @field_validator("element_type")
    @classmethod
    def _require_element_type(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("element_type must not be blank")
        return value

    @model_validator(mode="after")
    def _validate_text_span(self):
        if (self.text_start is None) != (self.text_end is None):
            raise ValueError("layout text spans must include both boundaries")
        if self.text_start is not None and self.text_end <= self.text_start:
            raise ValueError("layout text_end must exceed text_start")
        return self


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
        LayoutRegionLocator,
        TextLineLocator,
        CsvRowLocator,
        CodeLineLocator,
    ],
    Field(discriminator="kind"),
]
SourceLocator = Annotated[
    Union[
        PdfPageLocator,
        LayoutRegionLocator,
        TextLineLocator,
        CsvRowLocator,
        CodeLineLocator,
        PastedTextLocator,
        SearchResultLocator,
    ],
    Field(discriminator="kind"),
]
