from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextBlockRecord,
    ContextReplace,
    ContextRevisionOrigin,
    ContextSnapshot,
    ContextSupportKind,
    LLMContextMaintenance,
)
from core.knowledge.context.maintainer import (
    ContextMaintenanceBuild,
    combine_maintenance,
    maintenance_snapshot,
)
from core.knowledge.context.models import ContextBlockSupport
from core.knowledge.context.render import apply_context_edits, context_block_hash


def _domain():
    return DomainConfig(version=1, topics=(), entity_types=()).compile()


def _block(markdown, *, kind=AssertionKind.USER_ASSERTED, time=None):
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=kind,
        source_time_ms=time,
    )


def _build(blocks, *, budget_tokens=40_000, share=0.3, supports=None):
    snapshot = ContextSnapshot(
        revision_id=uuid4(), project_id="project-1", revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION, domain_version=1,
        content_hash="a" * 64, blocks=list(blocks),
    )
    return ContextMaintenanceBuild(
        domain=_domain(), snapshot=snapshot, ages={blocks[0].block_id: 14},
        supports=supports or {}, budget_tokens=budget_tokens, max_removed_share=share,
    )


def _support(block, message_id):
    return ContextBlockSupport(
        block_id=block.block_id, message_id=message_id, session_id="s",
        support_kind=ContextSupportKind.USER_MESSAGE,
    )


def _edits(*edits):
    return LLMContextMaintenance.model_validate({"edits": list(edits)})


def _filler(count):
    return [_block(f"Filler fact number {index}.") for index in range(count)]


@pytest.mark.unit
@pytest.mark.no_network
def test_merge_keeps_one_block_with_the_evidence_of_both():
    first = _block("The kitchen renovation is complete.", time=10)
    second = _block("Alex has completed the kitchen renovation.", time=20)
    build = _build(
        [first, second, *_filler(5)],
        supports={first.block_id: [_support(first, 1)], second.block_id: [_support(second, 2)]},
    )

    result = build.apply(_edits({
        "operation": "merge", "targets": [{"handle": "C1"}, {"handle": "C2"}],
        "markdown": "The kitchen renovation is complete.",
    }))

    blocks = result.materialization.blocks
    assert len(blocks) == 6
    merged = blocks[0]
    assert merged.supersedes_block_id == first.block_id
    assert merged.assertion_kind is AssertionKind.AGENT_DERIVED
    assert merged.source_time_ms == 20
    assert {s.message_id for s in result.materialization.supports} == {1, 2}
    assert all(s.block_id == merged.block_id for s in result.materialization.supports)


@pytest.mark.unit
@pytest.mark.no_network
def test_compress_must_be_shorter_than_what_it_replaces():
    build = _build([_block("Demolition started.")])

    result = build.apply(_edits({
        "operation": "compress", "targets": [{"handle": "C1"}],
        "markdown": "Demolition started on the kitchen and the bathroom last spring.",
    }))

    assert result.materialization is None
    assert "shorter" in result.rejected_edits[0]


@pytest.mark.unit
@pytest.mark.no_network
def test_drop_only_when_context_is_over_budget():
    blocks = _filler(10)
    drop = {"operation": "drop", "targets": [{"handle": "C1"}]}

    under = _build(blocks).apply(_edits(drop))
    over = _build(blocks, budget_tokens=10).apply(_edits(drop))

    assert under.materialization is None
    assert "under budget" in under.rejected_edits[0]
    assert len(over.materialization.blocks) == 9


@pytest.mark.unit
@pytest.mark.no_network
def test_one_run_removes_at_most_its_share_of_blocks():
    build = _build(_filler(10), budget_tokens=10, share=0.3)

    result = build.apply(_edits(*(
        {"operation": "drop", "targets": [{"handle": f"C{index}"}]} for index in range(1, 5)
    )))

    assert len(result.materialization.blocks) == 7
    assert len(result.rejected_edits) == 1


@pytest.mark.unit
@pytest.mark.no_network
def test_blocks_the_user_wrote_are_never_changed():
    mine = _block("Alex wrote this note.", kind=AssertionKind.HUMAN_ASSERTED)
    build = _build([mine, *_filler(9)], budget_tokens=10)

    result = build.apply(_edits({"operation": "drop", "targets": [{"handle": "C1"}]}))

    assert result.materialization is None
    assert "written by the user" in result.rejected_edits[0]
    assert "[C1] section=Current State age=14_episodes locked=user_written" in build.brief()


@pytest.mark.unit
@pytest.mark.no_network
def test_maintaining_a_block_the_same_window_wrote_points_back_to_saved_history():
    saved = _block("Demolition started.")
    committed = ContextSnapshot(
        revision_id=uuid4(), project_id="project-1", revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION, domain_version=1,
        content_hash="a" * 64, blocks=[saved],
    )
    update = apply_context_edits(committed, [
        ContextReplace(section_key="current_state", target={"handle": "C1"},
                       markdown="The kitchen renovation finished in June at $38,500."),
        ContextAdd(section_key="current_state", markdown="Alex is planning a housewarming."),
    ], _domain(), project_id="project-1")
    after = maintenance_snapshot(update, project_id="project-1", domain=_domain())
    maintained = apply_context_edits(after, [
        ContextReplace(section_key="current_state", target={"handle": "C1"},
                       markdown="Renovation done, June, $38,500."),
        ContextReplace(section_key="current_state", target={"handle": "C2"},
                       markdown="Housewarming planned."),
    ], _domain(), project_id="project-1")

    combined = combine_maintenance(update, maintained)

    # Only blocks from the saved revision can be superseded; the window's own
    # unsaved blocks never existed in history.
    assert [block.supersedes_block_id for block in combined.blocks] == [saved.block_id, None]
