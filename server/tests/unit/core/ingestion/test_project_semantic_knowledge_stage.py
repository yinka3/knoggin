"""Batch 7 semantic Knowledge/finalization stage contracts."""

from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextBlockSupportRecord,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from common.schema.ingestion.contracts import ContextEntityResult
from common.schema.semantic_window import (
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_job import ProjectSemanticJob
from infrastructure.job.base import JobContext


def _domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {"Concept": {"topic": "Work", "labels": ["concept"]}},
        }
    ).compile()


def _policy():
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(llm_ner=False),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_domain(),
    )


class _Admission:
    def update_settings(self, _settings):
        pass


class _Builder:
    async def build(self, build):
        build.set_entity_result(
            ContextEntityResult(
                entity_ids=(),
                new_entity_ids=frozenset(),
                alias_updated_ids=frozenset(),
                alias_updates={},
                pending_entity_writes={},
                block_entity_associations=(),
                message_entity_refs=(),
            )
        )
        return build.entity_result


class _Relationships:
    async def extract(self, build):
        build.set_relationship_writes(())
        return ()


class _Store:
    def __init__(self):
        domain = _domain()
        self.block = ContextBlockRecord(
            block_id=uuid4(),
            project_id="project-1",
            section_key="current_state",
            markdown="The active Context is grounded.",
            content_hash="a" * 64,
            assertion_kind=AssertionKind.SOURCE_GROUNDED,
        )
        self.snapshot = ContextSnapshot(
            revision_id=uuid4(),
            project_id="project-1",
            revision_number=1,
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=domain.version,
            content_hash="b" * 64,
            blocks=[self.block],
        )
        self.window = SemanticWindowRecord(
            window_id=uuid4(),
            user_name="ada",
            project_id="project-1",
            origin=SemanticWindowOrigin.CONVERSATION,
            stage=SemanticWindowStage.CONTEXT_COMMITTED,
            domain_version=domain.version,
            policy_snapshot={"ingestion_policy": _policy().semantic_window_snapshot()},
            source_token_count=1,
            token_estimator="test",
            token_estimator_version="1",
            episode_result_recorded=True,
            context_revision_id=self.snapshot.revision_id,
        )
        self.commit_calls = []
        self.enrich_calls = 0
        self.enqueued = []
        self.maintenance_failures = []
        self.maintenance_completed = []

    async def get_active_project_semantic_window(self, **_kwargs):
        return None if self.window.stage is SemanticWindowStage.COMPLETED else self.window

    async def get_project_context_snapshot(self, _revision_id, **_kwargs):
        return self.snapshot

    async def get_project_context_revision_impact_block_ids(self, _revision_id, **_kwargs):
        return frozenset({self.block.block_id})

    async def get_project_context_block_supports(self, _block_ids, **_kwargs):
        return {
            self.block.block_id: (
                ContextBlockSupportRecord(
                    block_id=self.block.block_id,
                    project_id="project-1",
                    message_id=101,
                    session_id="session-1",
                    support_kind="user_message",
                ),
            )
        }

    async def get_project_semantic_window_evidence_messages(self, _window_id, **_kwargs):
        return [{"message_id": 101, "content": "The active Context is grounded."}]

    async def commit_project_semantic_knowledge(self, build):
        assert build.entity_result is not None
        self.commit_calls.append(build)
        self.window = self.window.model_validate(
            self.window.model_dump() | {"stage": SemanticWindowStage.KNOWLEDGE_COMMITTED}
        )
        return SimpleNamespace(resumed=False, relationships_written=0)

    async def enrich_project_semantic_window_episodes(self, **_kwargs):
        self.enrich_calls += 1
        return {"entities": 0, "relationships": 0}

    async def advance_project_semantic_window_stage(self, **kwargs):
        assert kwargs["expected_stage"] is SemanticWindowStage.KNOWLEDGE_COMMITTED
        assert kwargs["next_stage"] is SemanticWindowStage.COMPLETED
        self.window = self.window.model_validate(
            self.window.model_dump() | {"stage": SemanticWindowStage.COMPLETED}
        )
        return True

    async def enqueue_project_semantic_window_maintenance(self, **kwargs):
        self.enqueued.append(kwargs)
        return True

    async def record_project_semantic_window_maintenance_failure(self, **kwargs):
        self.maintenance_failures.append(kwargs)

    async def complete_project_semantic_window_maintenance(self, **kwargs):
        self.maintenance_completed.append(kwargs)

    async def record_project_semantic_window_failure(self, **_kwargs):
        raise AssertionError("successful Knowledge/finalization must not record failure")


@pytest.mark.unit
@pytest.mark.no_network
async def test_knowledge_commit_precedes_episode_enrichment_and_maintenance_failure_is_separate():
    store = _Store()

    async def failed_maintenance(_window):
        raise OSError("maintenance unavailable")

    async def capture_domain():
        return _domain()

    job = ProjectSemanticJob(
        _Admission(),
        store,
        object(),
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_domain=capture_domain,
        context_entity_builder=_Builder(),
        context_relationship_extractor=_Relationships(),
        post_completion_maintenance=failed_maintenance,
    )
    ctx = JobContext(user_name="ada", project_id="project-1")

    knowledge = await job.execute(ctx)
    completed = await job.execute(ctx)

    assert knowledge.success
    assert completed.success
    assert len(store.commit_calls) == 1
    assert store.enrich_calls == 1
    assert store.window.stage is SemanticWindowStage.COMPLETED
    assert len(store.enqueued) == 1
    assert len(store.maintenance_failures) == 1
    assert store.maintenance_completed == []
