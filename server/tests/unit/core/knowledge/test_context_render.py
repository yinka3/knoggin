import hashlib
from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.conf.domain_config import (
    DEFAULT_CONTEXT_SECTIONS,
    ContextSectionDefinition,
    DomainConfig,
)
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
    ContextRevisionConflictError,
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
async def test_user_note_commits_human_block_and_preserves_existing_context():
    original = _block(section_key="current_state", markdown="Agent observation")
    current = _snapshot(original)
    domain = _domain()
    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    initial_file = render_context_markdown(current, domain).encode()
    state = SimpleNamespace(
        current_revision_id=current.revision_id,
        projection_revision_id=current.revision_id,
        projection_hash=hashlib.sha256(initial_file).hexdigest(),
        projection_pending_hash=None,
    )
    captured = []

    class Reader:
        async def get_current_revision(self, **_scope):
            return current

        async def get_snapshot(self, revision_id, **_scope):
            assert revision_id == current.revision_id
            return current

        async def get_projection_state(self, **_scope):
            return state

    class Writer:
        async def ensure_context(self, **_scope):
            pass

        async def commit_revision(self, **kwargs):
            nonlocal current
            captured.append(kwargs)
            assert kwargs["expected_parent_revision_id"] == current.revision_id
            assert kwargs["new_human_edit_window"].policy_snapshot["kind"] == "context_user_note"
            current = ContextSnapshot(
                revision_id=uuid4(), project_id="project-1",
                revision_number=current.revision_number + 1,
                parent_revision_id=kwargs["expected_parent_revision_id"],
                origin=ContextRevisionOrigin.HUMAN_EDIT, domain_version=1,
                content_hash=kwargs["materialization"].content_hash,
                blocks=list(kwargs["materialization"].blocks),
            )
            state.current_revision_id = current.revision_id
            return current

        async def record_projection(self, *, revision_id, projection_hash, **_scope):
            state.projection_revision_id = revision_id
            state.projection_hash = projection_hash
            return True

    class Filesystem:
        content = initial_file

        def read_bytes(self, _path):
            return self.content

        def write_bytes(self, _path, content, *, expected_content_hash, **_kwargs):
            assert expected_content_hash == hashlib.sha256(self.content).hexdigest()
            self.content = content

    filesystem = Filesystem()
    projection = ContextProjection(reader=Reader(), writer=Writer(), filesystem=filesystem)
    original_revision_id = current.revision_id
    changed = await projection.change_user_note(
        user_name="ada", project_id="project-1", ingestion_policy=policy,
        expected_revision_id=original_revision_id,
        operation="add", section_key="preferences", markdown="Prefer concise answers",
    )
    assert changed.revision_id != original_revision_id
    assert changed.blocks[0].block_id == original.block_id
    assert changed.blocks[1].assertion_kind is AssertionKind.HUMAN_ASSERTED
    assert b"Prefer concise answers" in filesystem.content
    assert len(captured) == 1
    with pytest.raises(ContextRevisionConflictError):
        await projection.change_user_note(
            user_name="ada", project_id="project-1", ingestion_policy=policy,
            expected_revision_id=original_revision_id,
            operation="add", section_key="preferences", markdown="Stale note",
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
@pytest.mark.parametrize("bridge_revision", [False, True])
@pytest.mark.parametrize("edited", [False, True])
async def test_domain_title_change_repairs_clean_projection_before_or_after_bridge(
    bridge_revision, edited,
):
    original_domain = _domain()
    updated_domain = DomainConfig(
        version=2,
        topics=(),
        entity_types=(),
        context_sections=tuple(
            ContextSectionDefinition(
                key=section.key,
                title=(
                    "Current Work"
                    if section.key == "active_work"
                    else section.title
                ),
            )
            for section in DEFAULT_CONTEXT_SECTIONS
        ),
    ).compile()
    original_snapshot = _snapshot(
        _block(section_key="active_work", markdown="Ship the parser.")
    )
    original_projection = render_context_markdown(
        original_snapshot, original_domain
    ).encode()
    bridged_snapshot = original_snapshot.model_copy(
        update={
            "revision_id": uuid4(),
            "revision_number": original_snapshot.revision_number + 1,
            "domain_version": updated_domain.version,
        }
    )
    current_snapshot = bridged_snapshot if bridge_revision else original_snapshot
    file_content = original_projection + b"\nUser edit" if edited else original_projection
    writes = []
    failures = []

    class Reader:
        async def get_projection_state(self, **_kwargs):
            return SimpleNamespace(
                current_revision_id=current_snapshot.revision_id,
                projection_revision_id=original_snapshot.revision_id,
                projection_hash=hashlib.sha256(original_projection).hexdigest(),
                projection_pending_hash=None,
            )

        async def get_snapshot(self, *_args, **_kwargs):
            return current_snapshot

    class Writer:
        async def ensure_context(self, **_kwargs):
            return None

        async def record_projection(self, **_kwargs):
            return True

        async def record_projection_failure(self, **_kwargs):
            failures.append(_kwargs)

    class Filesystem:
        def read_bytes(self, _path):
            return file_content

        def write_bytes(self, path, content, **kwargs):
            writes.append((path, content, kwargs))

    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=updated_domain,
    )
    projection = ContextProjection(
        reader=Reader(),
        writer=Writer(),
        filesystem=Filesystem(),
    )

    if edited:
        with pytest.raises(ContextProjectionConflictError):
            await projection.synchronize(
                user_name="ada",
                project_id="project-1",
                ingestion_policy=policy,
                allow_user_edit=False,
            )
        assert writes == []
        assert failures[0]["failure_code"] == "ContextProjectionConflictError"
        return

    result = await projection.synchronize(
        user_name="ada",
        project_id="project-1",
        ingestion_policy=policy,
        allow_user_edit=False,
    )

    assert result.changed is True
    assert failures == []
    assert b"## Current Work" in writes[0][1]
    assert writes[0][2]["expected_content_hash"] == hashlib.sha256(
        original_projection
    ).hexdigest()


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


def _discard_harness(*, file_content, snapshot):
    domain = _domain()
    state = SimpleNamespace(
        current_revision_id=None if snapshot is None else snapshot.revision_id,
        # The local edit was made against an older projection, which is the
        # case synchronization refuses to import.
        projection_revision_id=uuid4(),
        projection_hash="0" * 64,
        projection_pending_hash=None,
    )

    class Reader:
        async def get_projection_state(self, **_kwargs):
            return state

        async def get_snapshot(self, *_args, **_kwargs):
            return snapshot

    class Writer:
        async def ensure_context(self, **_kwargs):
            return None

        async def record_projection(self, *, revision_id, projection_hash, **_kwargs):
            state.projection_revision_id = revision_id
            state.projection_hash = projection_hash
            return True

        async def record_projection_failure(self, **_kwargs):
            return None

    class Filesystem:
        content = file_content

        def read_bytes(self, _path):
            if self.content is None:
                raise FileNotFoundError(_path)
            return self.content

        def write_bytes(self, _path, content, *, expected_content_hash, **_kwargs):
            assert expected_content_hash == hashlib.sha256(self.content).hexdigest()
            self.content = content

        def delete_file(self, _path, *, expected_content_hash):
            assert expected_content_hash == hashlib.sha256(self.content).hexdigest()
            self.content = None

    filesystem = Filesystem()
    projection = ContextProjection(reader=Reader(), writer=Writer(), filesystem=filesystem)
    return projection, filesystem, domain


@pytest.mark.unit
@pytest.mark.no_network
async def test_discarding_a_stale_local_edit_restores_context_and_unblocks_sync():
    snapshot = _snapshot()
    edit = b"my edit against an older Context"
    projection, filesystem, domain = _discard_harness(file_content=edit, snapshot=snapshot)
    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    with pytest.raises(ContextUserEditSynchronizationError):
        await projection.synchronize(
            user_name="ada", project_id="project-1",
            ingestion_policy=policy, allow_user_edit=True,
        )

    discarded = await projection.discard_local_edits(
        user_name="ada", project_id="project-1", domain=domain,
        expected_file_hash=hashlib.sha256(edit).hexdigest(),
    )

    assert discarded == edit.decode()
    assert filesystem.content == render_context_markdown(snapshot, domain).encode()
    await projection.synchronize(
        user_name="ada", project_id="project-1",
        ingestion_policy=policy, allow_user_edit=True,
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_discard_refuses_edits_the_user_has_not_seen():
    projection, filesystem, domain = _discard_harness(
        file_content=b"a newer edit", snapshot=_snapshot()
    )

    with pytest.raises(ContextProjectionConflictError):
        await projection.discard_local_edits(
            user_name="ada", project_id="project-1", domain=domain,
            expected_file_hash=hashlib.sha256(b"the edit I reviewed").hexdigest(),
        )
    assert filesystem.content == b"a newer edit"


@pytest.mark.unit
@pytest.mark.no_network
async def test_discard_without_committed_context_removes_the_local_file():
    edit = b"a first Context that could not be imported"
    projection, filesystem, domain = _discard_harness(file_content=edit, snapshot=None)

    discarded = await projection.discard_local_edits(
        user_name="ada", project_id="project-1", domain=domain,
        expected_file_hash=hashlib.sha256(edit).hexdigest(),
    )

    assert discarded == edit.decode()
    assert filesystem.content is None
