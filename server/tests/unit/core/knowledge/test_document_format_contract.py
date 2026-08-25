import pytest

from core.knowledge.documents.constants import ACCEPTED_EXTENSIONS, IMAGE_EXTENSIONS
from core.knowledge.documents.storage import is_accepted_extension
from tests.fixtures.documents import (
    build_docx_bytes,
    build_notebook_bytes,
    build_pdf_bytes,
    build_png_bytes,
)

DOCUMENT_PROCESSING_STRATEGIES = {
    "pdf": {".pdf"},
    "docx": {".docx"},
    "ocr": IMAGE_EXTENSIONS - {".svg"},
    "notebook": {".ipynb"},
    "csv": {".csv"},
    "markdown": {".md"},
    "code": {
        ".bash",
        ".c",
        ".cpp",
        ".cs",
        ".dockerfile",
        ".go",
        ".h",
        ".hpp",
        ".java",
        ".js",
        ".jsx",
        ".kt",
        ".php",
        ".py",
        ".rb",
        ".rs",
        ".sh",
        ".sql",
        ".swift",
        ".ts",
        ".tsx",
        ".vue",
        ".yaml",
        ".yml",
        ".zsh",
    },
    "generic_text": {
        ".cfg",
        ".coffee",
        ".conf",
        ".css",
        ".dart",
        ".env",
        ".ex",
        ".exs",
        ".groovy",
        ".hs",
        ".htm",
        ".html",
        ".ini",
        ".json",
        ".lua",
        ".m",
        ".pl",
        ".proto",
        ".r",
        ".rst",
        ".scala",
        ".tex",
        ".toml",
        ".txt",
        ".xml",
    },
}


@pytest.mark.unit
@pytest.mark.no_network
def test_every_accepted_extension_has_one_processing_strategy():
    categorized = [
        extension
        for extensions in DOCUMENT_PROCESSING_STRATEGIES.values()
        for extension in extensions
    ]

    assert set(categorized) == ACCEPTED_EXTENSIONS
    assert len(categorized) == len(set(categorized))


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("extension", sorted(ACCEPTED_EXTENSIONS))
def test_storage_accepts_every_extension_in_the_format_contract(extension):
    assert is_accepted_extension(extension)
    assert is_accepted_extension(extension.upper())


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("extension", ["", ".exe", ".svg", ".zip"])
def test_storage_rejects_extensions_outside_the_format_contract(extension):
    assert not is_accepted_extension(extension)


@pytest.mark.unit
@pytest.mark.no_network
def test_deterministic_binary_fixture_builders_have_expected_container_signatures():
    assert build_pdf_bytes("Launch ready.").startswith(b"%PDF-1.4")
    assert build_docx_bytes([("Launch", 1)]).startswith(b"PK")
    assert build_notebook_bytes().startswith(b'{"cells"')
    assert build_png_bytes().startswith(b"\x89PNG\r\n\x1a\n")
