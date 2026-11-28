import pytest

from core.knowledge.documents.extraction import extract_and_split_document
from core.knowledge.documents.lines import split_lines


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    "text", ["", "one", "one\n", "one\ntwo", "one\r\ntwo\r\n", "a\n\nb\n\n", "\n"],
)
def test_ordinary_text_splits_like_splitlines(text):
    assert split_lines(text) == text.splitlines()
    assert "".join(split_lines(text, keepends=True)) == text


@pytest.mark.unit
@pytest.mark.no_network
def test_only_newlines_end_a_line():
    text = "page one\fstill line one\nline two same line\vtoo\n"

    assert split_lines(text) == ["page one\fstill line one", "line two same line\vtoo"]
    assert len(text.splitlines()) == 5  # what drifted before


@pytest.mark.unit
@pytest.mark.no_network
def test_code_chunk_lines_match_the_file_despite_a_form_feed():
    source = (
        "def first():\n"
        "    return 1\n"
        "\f\n"  # a form feed on its own line, as in older C and Python files
        "def second():\n"
        "    return 2\n"
    )

    chunks = extract_and_split_document(source.encode(), ".py").chunks

    second = next(chunk for chunk in chunks if "def second" in chunk.content)
    assert second.start_line == 4
    assert source.split("\n")[second.start_line - 1] == "def second():"
