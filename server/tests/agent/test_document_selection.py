import pytest

from core.agent.document_selection import (
    DocumentSelectionError,
    parse_document_path_command,
)


@pytest.mark.parametrize(
    ("request", "path", "remaining"),
    [
        ("/docs/notes.md summarize this", "docs/notes.md", "summarize this"),
        (
            "Compare /src/main.py with the architecture",
            "src/main.py",
            "Compare with the architecture",
        ),
        ("Summarize /docs/notes.md.", "docs/notes.md", "Summarize."),
        (
            'Explain "/docs/Q2 notes.md", please!',
            "docs/Q2 notes.md",
            "Explain, please!",
        ),
        ("(/docs/notes.md) what changed?", "docs/notes.md", "() what changed?"),
    ],
)
def test_parse_document_path_command_preserves_remaining_request(
    request,
    path,
    remaining,
):
    command = parse_document_path_command(request)

    assert command.relative_path == path
    assert command.remaining_query == remaining


@pytest.mark.parametrize(
    "request",
    [
        "/docs/a.md and /docs/b.md",
        '"/docs/a.md" and "/docs/b.md"',
        "/docs/../private.md",
        "/docs//notes.md",
        '"/docs/notes.md',
        "/",
    ],
)
def test_parse_document_path_command_rejects_malformed_or_multiple_paths(request):
    with pytest.raises(DocumentSelectionError):
        parse_document_path_command(request)


def test_parse_document_path_command_does_not_treat_url_as_document_selector():
    request = "Read https://example.com/docs/notes.md"

    assert parse_document_path_command(request) is None
