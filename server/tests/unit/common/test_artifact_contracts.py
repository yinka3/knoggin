import pytest
from pydantic import ValidationError

from common.schema.agent.research import DEFAULT_RESEARCH_PROFILES
from common.schema.agent.tool_contracts import (
    TOOL_SCHEMAS_BY_NAME,
    validate_tool_arguments,
)
from common.schema.artifacts import (
    ArtifactDraft,
    ArtifactRevision,
    CalloutArtifactBlock,
    ChecklistArtifactBlock,
    ChecklistItem,
    MarkdownArtifactBlock,
    TableArtifactBlock,
    artifact_content_hash,
    render_artifact_markdown,
)


def test_artifact_blocks_render_to_deterministic_markdown():
    artifact = ArtifactDraft(
        kind="research_brief",
        title="Decision brief",
        blocks=(
            MarkdownArtifactBlock(content="A short finding."),
            CalloutArtifactBlock(
                tone="warning", title="Caveat", content="Verify this assumption."
            ),
            ChecklistArtifactBlock(
                title="Next steps",
                items=(
                    ChecklistItem(label="Review source", completed=True),
                    ChecklistItem(label="Ask for approval"),
                ),
            ),
            TableArtifactBlock(
                title="Options",
                columns=("Name", "Score"),
                rows=(("One", "1"), ("Two | alt", "2")),
            ),
        ),
    )

    markdown = render_artifact_markdown(artifact)
    assert "# Decision brief" in markdown
    assert "> **Caveat**" in markdown
    assert "- [x] Review source" in markdown
    assert "| Two \\| alt | 2 |" in markdown
    assert markdown == render_artifact_markdown(artifact)
    assert artifact_content_hash(artifact) == artifact_content_hash(artifact, markdown)


def test_artifact_contract_rejects_executable_markup_and_empty_blocks():
    with pytest.raises(ValidationError, match="executable"):
        ArtifactDraft(
            title="Unsafe",
            blocks=(MarkdownArtifactBlock(content="<script>alert(1)</script>"),),
        )

    with pytest.raises(ValidationError):
        ArtifactDraft(title="Empty", blocks=())


def test_research_profiles_make_mode_artifact_policy_explicit():
    assert DEFAULT_RESEARCH_PROFILES["normal"].artifact_policy == "none"
    assert DEFAULT_RESEARCH_PROFILES["research"].default_artifact_kind == "research_brief"
    assert DEFAULT_RESEARCH_PROFILES["research"].artifact_policy == "default"
    assert DEFAULT_RESEARCH_PROFILES["deep_research"].artifact_policy == "required"


def test_submit_answer_schema_accepts_optional_artifact():
    errors = validate_tool_arguments(
        TOOL_SCHEMAS_BY_NAME["submit_answer"],
        {
            "content": "Answer",
            "artifact": {
                "kind": "general",
                "title": "Reusable",
                "blocks": [{"kind": "markdown", "content": "Note"}],
            },
        },
    )
    assert errors == []


def test_artifact_revision_rejects_a_mismatched_content_hash():
    with pytest.raises(ValidationError, match="does not match"):
        ArtifactRevision(
            artifact_id="11111111-1111-1111-1111-111111111111",
            revision=1,
            schema_version=1,
            kind="general",
            title="Note",
            blocks=(MarkdownArtifactBlock(content="Durable"),),
            status="complete",
            markdown="# Note\n\nDurable\n",
            content_hash="0" * 64,
            created_at="2026-01-02T00:00:00Z",
        )
