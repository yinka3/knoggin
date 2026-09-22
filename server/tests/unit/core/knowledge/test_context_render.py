import hashlib
from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextBlockRecord,
    ContextBlockSupportRecord,
    ContextDelete,
    ContextReplace,
    ContextRevisionOrigin,
    ContextSnapshot,
    ContextSupportKind,
)
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.policy import IngestionPolicy
from core.knowledge.context.models import (
    ContextProjectionConflictError,
    ContextUserEditSynchronizationError,
)
from core.knowledge.context.projection import (
    ContextProjection,
    _materialize_human_edit,
    _parse_markdown,
)
from core.knowledge.context.render import (
    apply_context_edits,
    context_block_hash,
    render_context_markdown,
    render_context_model_input,
)


def _domain():
    return DomainConfig(version=1, topics=(), entity_types=()).compile()


def _block(
    *,
    section_key: str,
    markdown: str,
    assertion_kind: AssertionKind = AssertionKind.AGENT_DERIVED,
) -> ContextBlockRecord:
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key=section_key,
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=assertion_kind,
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
async def test_detected_user_edit_failure_keeps_its_admission_meaning():
    snapshot = _snapshot()
    domain = _domain()
    generated = render_context_markdown(snapshot, domain).encode()
    failures = []

    class Reader:
        async def get_projection_state(self, **_kwargs):
            return SimpleNamespace(
                current_revision_id=snapshot.revision_id,
                projection_revision_id=snapshot.revision_id,
                projection_hash=hashlib.sha256(generated).hexdigest(),
                projection_pending_hash=None,
            )

        async def get_snapshot(self, *_args, **_kwargs):
            return snapshot

    class Writer:
        async def ensure_context(self, **_kwargs):
            return None

        async def record_projection_failure(self, **kwargs):
            failures.append(kwargs)

    class Filesystem:
        def read_bytes(self, _path):
            return b"not a valid Context document"

    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    projection = ContextProjection(
        reader=Reader(),
        writer=Writer(),
        filesystem=Filesystem(),
    )

    with pytest.raises(ContextUserEditSynchronizationError) as caught:
        await projection.synchronize(
            user_name="ada",
            project_id="project-1",
            ingestion_policy=policy,
            allow_user_edit=True,
        )

    assert isinstance(caught.value.__cause__, ContextProjectionConflictError)
    assert failures[0]["failure_code"] == "ContextUserEditSynchronizationError"


@pytest.mark.unit
@pytest.mark.no_network
async def test_known_stale_projection_is_repaired_without_user_edit_blocking():
    snapshot = _snapshot()
    domain = _domain()
    stale = b"previous generated projection"
    writes = []

    class Reader:
        async def get_projection_state(self, **_kwargs):
            return SimpleNamespace(
                current_revision_id=snapshot.revision_id,
                projection_revision_id=uuid4(),
                projection_hash=hashlib.sha256(stale).hexdigest(),
                projection_pending_hash=None,
            )

        async def get_snapshot(self, *_args, **_kwargs):
            return snapshot

    class Writer:
        async def ensure_context(self, **_kwargs):
            return None

        async def record_projection(self, **_kwargs):
            return True

        async def record_projection_failure(self, **_kwargs):
            raise AssertionError("known stale projection repair must not fail")

    class Filesystem:
        def read_bytes(self, _path):
            return stale

        def write_bytes(self, path, content, **kwargs):
            writes.append((path, content, kwargs))

    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    projection = ContextProjection(
        reader=Reader(),
        writer=Writer(),
        filesystem=Filesystem(),
    )

    result = await projection.synchronize(
        user_name="ada",
        project_id="project-1",
        ingestion_policy=policy,
        allow_user_edit=True,
    )

    assert result.changed is True
    assert writes[0][0] == "CONTEXT.md"
    assert writes[0][2]["expected_content_hash"] == hashlib.sha256(stale).hexdigest()


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
def test_model_context_keeps_assertion_kind_and_compact_support_handles_out_of_projection():
    user_block = _block(
        section_key="current_state",
        markdown="The user chose SQLite.",
        assertion_kind=AssertionKind.USER_ASSERTED,
    )
    source_block = ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="active_work",
        markdown="The migration guide supports a staged rollout.",
        content_hash=context_block_hash("The migration guide supports a staged rollout."),
        assertion_kind=AssertionKind.SOURCE_GROUNDED,
    )
    source_ref_id = uuid4()
    snapshot = _snapshot(user_block, source_block)
    supports = {
        user_block.block_id: (
            ContextBlockSupportRecord(
                block_id=user_block.block_id,
                project_id="project-1",
                message_id=17,
                session_id="session-1",
                support_kind=ContextSupportKind.USER_MESSAGE,
            ),
        ),
        source_block.block_id: (
            ContextBlockSupportRecord(
                block_id=source_block.block_id,
                project_id="project-1",
                message_id=18,
                session_id="session-1",
                support_kind=ContextSupportKind.ASSISTANT_SOURCE,
                source_ref_id=source_ref_id,
            ),
        ),
    }

    projection = render_context_markdown(snapshot, _domain())
    model_input = render_context_model_input(
        snapshot,
        _domain(),
        supports_by_block=supports,
    )

    assert "C1 [user_asserted; support: M1]" in model_input
    assert "C2 [source_grounded; support: S1]" in model_input
    assert "M1: user_message" in model_input
    assert "S1: assistant source" in model_input
    assert str(source_ref_id) not in model_input
    assert "user_asserted" not in projection
    assert "support: M1" not in projection


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
