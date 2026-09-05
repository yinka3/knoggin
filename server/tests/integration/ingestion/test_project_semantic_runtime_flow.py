"""Composed project-semantic job scenarios with model boundaries faked only."""

from types import SimpleNamespace
from uuid import UUID, uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from common.schema.ingestion.contracts import (
    ContextEntityResult,
    ContextRelationshipWrite,
)
from common.schema.semantic_window import (
    SemanticWindowClaimResult,
    SemanticWindowStage,
)
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_job import ProjectSemanticJob
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.context.render import (
    apply_context_edits,
    context_document_hash,
    render_context_markdown,
)
from core.knowledge.context.updater import ContextUpdateResult
from infrastructure.job.base import JobContext


def _domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {"Concept": {"topic": "Work", "labels": ["concept"]}},
        }
    ).compile()


def _row(message_id, *, session_id, timestamp_ms, content="x"):
    return {
        "user_message_id": message_id,
        "session_id": session_id,
        "user_content": content,
        "user_timestamp_ms": timestamp_ms,
        "user_lifecycle_state": "sealed",
        "user_exchange_state": "closed",
        "user_exchange_outcome": "assistant_final",
        "exchange_closed_at_ms": timestamp_ms + 1,
        "assistant_message_id": message_id + 10_000,
        "assistant_content": content,
        "assistant_timestamp_ms": timestamp_ms + 1,
        "assistant_lifecycle_state": "sealed",
        "session_status": "open",
        "already_claimed": False,
    }


class _FlowStore:
    def __init__(self, rows, *, assistant_source_refs=()):
        self.rows = rows
        self.assistant_source_refs = list(assistant_source_refs)
        self.window = None
        self.episodes = None
        self.snapshot = None
        self.knowledge_builds = []
        self.maintenance = []

    async def get_unclaimed_project_semantic_exchange_rows(self, **_kwargs):
        return self.rows

    async def claim_project_semantic_window(self, window, messages):
        if self.window is not None and self.window.stage is not SemanticWindowStage.COMPLETED:
            return SemanticWindowClaimResult(window=self.window, claimed=False)
        self.window = window
        self.messages = list(messages)
        for row in self.rows:
            row["already_claimed"] = True
        return SemanticWindowClaimResult(window=window, claimed=True)

    async def get_active_project_semantic_window(self, **_kwargs):
        if self.window is None or self.window.stage is SemanticWindowStage.COMPLETED:
            return None
        return self.window

    async def get_project_semantic_window_episode_result(self, _window_id, **_kwargs):
        return self.episodes

    async def write_project_semantic_window_episodes(self, *, episodes, **_kwargs):
        self.episodes = list(episodes)
        self.window = self.window.model_validate(
            self.window.model_dump() | {"episode_result_recorded": True}
        )
        return True

    async def get_project_semantic_window_evidence_messages(self, _window_id, **_kwargs):
        by_id = {}
        for row in self.rows:
            by_id[row["user_message_id"]] = {
                "message_id": row["user_message_id"],
                "session_id": row["session_id"],
                "role": "user",
                "content": row["user_content"],
                "timestamp_ms": row["user_timestamp_ms"],
            }
            by_id[row["assistant_message_id"]] = {
                "message_id": row["assistant_message_id"],
                "session_id": row["session_id"],
                "role": "assistant",
                "content": row["assistant_content"],
                "timestamp_ms": row["assistant_timestamp_ms"],
                "user_msg_id": row["user_message_id"],
            }
        return [by_id[member.message_id] for member in self.messages]

    async def get_project_semantic_window_assistant_source_refs(self, _window_id, **_kwargs):
        return self.assistant_source_refs

    async def get_project_semantic_window_context_snapshot(self, _window_id, **_kwargs):
        return self.snapshot if self.window.context_revision_id is not None else None

    async def get_current_project_context_revision(self, **_kwargs):
        return None if self.snapshot is None else SimpleNamespace(revision_id=self.snapshot.revision_id)

    async def get_project_context_snapshot(self, revision_id, **_kwargs):
        if self.snapshot is None or str(self.snapshot.revision_id) != str(revision_id):
            return None
        return self.snapshot

    async def commit_project_context_revision(self, *, materialization, window_id, edit_summary, **_kwargs):
        self.snapshot = ContextSnapshot(
            revision_id=uuid4(),
            project_id="project-1",
            revision_number=1,
            window_id=window_id,
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=1,
            edit_summary=edit_summary,
            content_hash=materialization.content_hash,
            blocks=list(materialization.blocks),
        )
        return self.snapshot

    async def advance_project_semantic_window_stage(
        self, *, expected_stage, next_stage, context_revision_id=None, **_kwargs
    ):
        assert self.window.stage is expected_stage
        self.window = self.window.model_validate(
            self.window.model_dump()
            | {
                "stage": next_stage,
                "context_revision_id": context_revision_id,
            }
        )
        return True

    async def get_project_context_revision_impact_block_ids(self, _revision_id, **_kwargs):
        return frozenset(block.block_id for block in self.snapshot.blocks)

    async def get_project_context_block_supports(self, block_ids, **_kwargs):
        return {UUID(str(block_id)): () for block_id in block_ids}

    async def commit_project_semantic_knowledge(self, build):
        self.knowledge_builds.append(build)
        self.window = self.window.model_validate(
            self.window.model_dump() | {"stage": SemanticWindowStage.KNOWLEDGE_COMMITTED}
        )
        return SimpleNamespace(resumed=False, relationships_written=len(build.relationship_writes))

    async def enrich_project_semantic_window_episodes(self, **_kwargs):
        return {"entities": 0, "relationships": len(self.knowledge_builds[-1].relationship_writes)}

    async def enqueue_project_semantic_window_maintenance(self, **kwargs):
        self.maintenance.append(kwargs)
        return True

    async def complete_project_semantic_window_maintenance(self, **_kwargs):
        return None

    async def record_project_semantic_window_failure(self, **kwargs):
        raise AssertionError(f"unexpected composed-flow failure: {kwargs}")


class _EpisodeGenerator:
    async def generate(self, **_kwargs):
        return SimpleNamespace(final_episodes=[])


class _Updater:
    def __init__(self, assertion_kind):
        self.assertion_kind = assertion_kind
        self.calls = []

    async def update(self, *, snapshot, domain, assistant_source_refs, **kwargs):
        self.calls.append({"assistant_source_refs": assistant_source_refs, **kwargs})
        materialization = apply_context_edits(
            snapshot,
            [
                ContextAdd(
                    section_key="current_state",
                    markdown="The durable Context update is active.",
                    assertion_kind=self.assertion_kind,
                    evidence=[{"handle": "M1"}],
                )
            ],
            domain,
            project_id="project-1",
        )
        return ContextUpdateResult(
            materialization=materialization,
            edit_summary="Recorded composed Context update",
            operation_count=1,
        )


class _EntityBuilder:
    def __init__(self):
        self.input_counts = []

    async def build(self, build):
        self.input_counts.append(len(build.knowledge_input_blocks))
        entity_ids = (10, 11) if build.knowledge_input_blocks else ()
        result = ContextEntityResult(
            entity_ids=entity_ids,
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            block_entity_associations=(),
            message_entity_refs=(),
        )
        build.set_entity_result(result)
        return result


class _Relationships:
    async def extract(self, build):
        if not build.knowledge_input_blocks:
            build.set_relationship_writes(())
            return ()
        write = ContextRelationshipWrite(
            support_block_ids=(build.knowledge_input_blocks[0].block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="supports",
        )
        build.set_relationship_writes((write,))
        return (write,)


def _job(store, updater, builder):
    domain = _domain()
    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(llm_ner=False),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    settings = IngestionSettings(semantic_window_tokens=3)
    admission = SemanticWindowAdmission(
        store,
        settings,
        token_counter=lambda text: text.count("x"),
    )

    async def capture_domain():
        return domain

    return ProjectSemanticJob(
        admission,
        store,
        _EpisodeGenerator(),
        settings=settings,
        capture_domain=capture_domain,
        capture_ingestion_policy=lambda: policy,
        context_updater=updater,
        context_entity_builder=builder,
        context_relationship_extractor=_Relationships(),
    )


async def _complete(job):
    context = JobContext(user_name="ada", project_id="project-1")
    results = [await job.execute(context) for _ in range(4)]
    assert all(result.success for result in results)


@pytest.mark.integration
@pytest.mark.no_network
async def test_project_semantic_runtime_composes_two_session_evidence_into_one_context_revision():
    source_ref_id = uuid4()
    store = _FlowStore(
        [
            _row(1, session_id="session-1", timestamp_ms=1),
            _row(2, session_id="session-2", timestamp_ms=2),
        ],
        assistant_source_refs=[
            {
                "source_ref_id": source_ref_id,
                "message_id": 10_001,
                "session_id": "session-1",
                "source_kind": "user_pasted_text",
            }
        ],
    )
    updater = _Updater(AssertionKind.SOURCE_GROUNDED)
    builder = _EntityBuilder()

    await _complete(_job(store, updater, builder))

    assert [member.session_id for member in store.messages] == [
        "session-1",
        "session-1",
        "session-2",
        "session-2",
    ]
    assert store.snapshot is not None
    assert store.snapshot.window_id == store.window.window_id
    assert updater.calls[0]["assistant_source_refs"][0]["source_ref_id"] == source_ref_id
    assert builder.input_counts == [1]
    assert len(store.knowledge_builds[0].relationship_writes) == 1
    assert store.window.stage is SemanticWindowStage.COMPLETED
    assert len(store.maintenance) == 1


@pytest.mark.integration
@pytest.mark.no_network
async def test_agent_derived_context_renders_but_does_not_enter_composed_knowledge():
    store = _FlowStore([_row(1, session_id="session-1", timestamp_ms=1)])
    updater = _Updater(AssertionKind.AGENT_DERIVED)
    builder = _EntityBuilder()

    await _complete(_job(store, updater, builder))

    assert "The durable Context update is active." in render_context_markdown(
        store.snapshot, _domain()
    )
    assert builder.input_counts == [0]
    assert store.knowledge_builds[0].relationship_writes == ()
    assert store.snapshot.content_hash == context_document_hash(store.snapshot.blocks, _domain())
