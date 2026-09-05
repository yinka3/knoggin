from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextBlockRecord,
    ContextDelete,
    ContextReplace,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from core.knowledge.context.models import ContextProjectionConflictError
from core.knowledge.context.projection import _materialize_human_edit, _parse_markdown
from core.knowledge.context.render import (
    apply_context_edits,
    context_block_hash,
    render_context_markdown,
    render_context_model_input,
)


def _domain():
    return DomainConfig(version=1, topics=(), entity_types=()).compile()


def _block(*, section_key: str, markdown: str) -> ContextBlockRecord:
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key=section_key,
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=AssertionKind.AGENT_DERIVED,
    )


def _snapshot(*blocks: ContextBlockRecord) -> ContextSnapshot:
    return ContextSnapshot(
        revision_id=uuid4(),
        project_id="project-1",
        revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        content_hash="a" * 64,
        blocks=list(blocks),
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_context_markdown_is_stable_and_keeps_local_handles_out_of_canonical_file():
    active_work = _block(section_key="active_work", markdown="Build the renderer.")
    current_state = _block(section_key="current_state", markdown="Knoggin is local.  \n")
    snapshot = _snapshot(active_work, current_state)

    first = render_context_markdown(snapshot, _domain())
    second = render_context_markdown(snapshot, _domain())
    model_input = render_context_model_input(snapshot, _domain())

    assert first == second
    assert first.startswith("# Project Context\n\n## Current State\n")
    assert "<!-- knoggin-context-block:" in first
    assert "C1" not in first
    assert "C1" in model_input and "C2" in model_input
    assert first.index("## Current State") < first.index("## Active Work")


@pytest.mark.unit
@pytest.mark.no_network
def test_edit_applier_reuses_unchanged_blocks_tracks_lineage_and_impact():
    current_state = _block(section_key="current_state", markdown="Old state.")
    active_work = _block(section_key="active_work", markdown="Old work.")
    snapshot = _snapshot(current_state, active_work)

    materialization = apply_context_edits(
        snapshot,
        [
            ContextReplace(
                section_key="current_state",
                target={"handle": "C1"},
                markdown="New state.",
                dependencies=[{"handle": "C2"}],
            ),
            ContextAdd(
                section_key="current_state",
                markdown="A newly learned constraint.",
            ),
        ],
        _domain(),
    )

    assert materialization is not None
    assert [block.markdown for block in materialization.blocks] == [
        "New state.",
        "A newly learned constraint.",
        "Old work.",
    ]
    replacement = materialization.blocks[0]
    assert replacement.supersedes_block_id == current_state.block_id
    assert active_work.block_id not in materialization.new_block_ids
    assert materialization.impacted_block_ids == {
        current_state.block_id,
        active_work.block_id,
        replacement.block_id,
        materialization.blocks[1].block_id,
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_edit_applier_rejects_double_targets_and_elides_unchanged_replacement():
    current_state = _block(section_key="current_state", markdown="Current state.")
    snapshot = _snapshot(current_state)

    with pytest.raises(ValueError, match="same block twice"):
        apply_context_edits(
            snapshot,
            [
                ContextReplace(
                    section_key="current_state",
                    target={"handle": "C1"},
                    markdown="First change.",
                ),
                ContextDelete(section_key="current_state", target={"handle": "C1"}),
            ],
            _domain(),
        )

    assert (
        apply_context_edits(
            snapshot,
            [
                ContextReplace(
                    section_key="current_state",
                    target={"handle": "C1"},
                    markdown="Current state.",
                )
            ],
            _domain(),
        )
        is None
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_delete_keeps_immediate_current_neighbors_in_the_impact_closure():
    first = _block(section_key="current_state", markdown="First.")
    removed = _block(section_key="current_state", markdown="Removed.")
    last = _block(section_key="current_state", markdown="Last.")
    materialization = apply_context_edits(
        _snapshot(first, removed, last),
        [ContextDelete(section_key="current_state", target={"handle": "C2"})],
        _domain(),
    )

    assert materialization is not None
    assert materialization.impacted_block_ids == {
        first.block_id,
        removed.block_id,
        last.block_id,
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_context_import_preserves_unchanged_support_identity_and_humanizes_changes():
    state = _block(section_key="current_state", markdown="Current state.")
    work = _block(section_key="active_work", markdown="Active work.")
    preference = _block(section_key="preferences", markdown="Keep tests focused.")
    snapshot = _snapshot(state, work, preference)
    rendered = render_context_markdown(snapshot, _domain())
    edited = rendered.replace(
        "Current state.\n\n## Active Work",
        "Current state.\n\nAdded by the user.\n\n## Active Work",
    ).replace("Active work.", "Updated work.")
    edited = edited.replace(
        f"<!-- knoggin-context-block:{state.block_id} -->\nCurrent state.\n\n",
        "",
    )

    parsed = _parse_markdown(edited, _domain(), snapshot)
    materialization = _materialize_human_edit(snapshot, parsed, _domain(), "project-1")

    assert materialization is not None
    assert state.block_id not in {block.block_id for block in materialization.blocks}
    assert preference.block_id in {block.block_id for block in materialization.blocks}
    updated_work = next(block for block in materialization.blocks if block.markdown == "Updated work.")
    added = next(block for block in materialization.blocks if block.markdown == "Added by the user.")
    assert updated_work.assertion_kind is AssertionKind.HUMAN_ASSERTED
    assert updated_work.supersedes_block_id == work.block_id
    assert added.assertion_kind is AssertionKind.HUMAN_ASSERTED
    assert added.supersedes_block_id is None
    assert state.block_id in materialization.impacted_block_ids
    assert work.block_id in materialization.impacted_block_ids


@pytest.mark.unit
@pytest.mark.no_network
def test_context_import_fails_closed_for_unknown_marker_or_section():
    snapshot = _snapshot(_block(section_key="current_state", markdown="Current state."))
    rendered = render_context_markdown(snapshot, _domain())

    with pytest.raises(ContextProjectionConflictError, match="unknown or repeated"):
        _parse_markdown(
            rendered.replace(str(snapshot.blocks[0].block_id), str(uuid4())),
            _domain(),
            snapshot,
        )
    with pytest.raises(ContextProjectionConflictError, match="section"):
        _parse_markdown(rendered.replace("## Active Work", "## Work"), _domain(), snapshot)
