"""Project semantic stages composed with real PostgreSQL storage.

Only model-result boundaries are faked here.  Window admission, revision
publication, stage checkpoints, Knowledge commit, finalization, and Context
projection all use their production implementations.
"""

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import AssertionKind, ContextAdd
from common.schema.ingestion.contracts import ContextEntityResult
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_job import ProjectSemanticJob
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.context.projection import ContextProjection
from core.knowledge.context.render import apply_context_edits, render_context_markdown
from core.knowledge.context.updater import ContextUpdateResult
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.documents.filesystem import ProjectFilesystem
from core.knowledge.store import KnowledgeStore
from infrastructure.job.base import JobContext


def _domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {"Concept": {"topic": "Work", "labels": ["concept"]}},
        }
    ).compile()


class _ZeroEpisodeGenerator:
    async def generate(self, **_kwargs):
        return type("EpisodeBuild", (), {"final_episodes": []})()


class _AgentDerivedUpdater:
    async def update(self, *, snapshot, domain, project_id, **_kwargs):
        return ContextUpdateResult(
            materialization=apply_context_edits(
                snapshot,
                [
                    ContextAdd(
                        section_key="current_state",
                        markdown="The local project Context is active.",
                        assertion_kind=AssertionKind.AGENT_DERIVED,
                        evidence=[{"handle": "M1"}],
                    )
                ],
                domain,
                project_id=project_id,
            ),
            edit_summary="Recorded composed Context update",
            operation_count=1,
        )


class _EmptyEntityBuilder:
    async def build(self, build):
        assert build.knowledge_input_blocks == ()
        result = ContextEntityResult(
            entity_ids=(),
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            block_entity_associations=(),
            message_entity_refs=(),
        )
        build.set_entity_result(result)
        return result


class _EmptyRelationships:
    async def extract(self, build):
        assert build.knowledge_input_blocks == ()
        build.set_relationship_writes(())
        return ()


class _HumanEditEntityBuilder:
    async def build(self, build):
        assert len(build.knowledge_input_blocks) == 1
        result = ContextEntityResult(
            entity_ids=(),
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            block_entity_associations=(),
            message_entity_refs=(),
        )
        build.set_entity_result(result)
        return result


class _HumanEditRelationships:
    async def extract(self, build):
        assert len(build.knowledge_input_blocks) == 1
        build.set_relationship_writes(())
        return ()


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_semantic_job_uses_real_storage_for_agent_derived_context(
    real_server_scope,
    tmp_path,
):
    scope = real_server_scope
    postgres = scope["postgres"]
    user_name = scope["user_name"]
    project_id = scope["project_id"]
    session_id = scope["session_id"]
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (%s, %s, 101, %s, 'user', 'Make the Context active.',
                  1, 'sealed', 'closed', 'user_only', 1)
        """,
        (user_name, session_id, project_id),
    )
    await postgres.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('second-semantic-session', %s, %s)
        """,
        (user_name, project_id),
    )
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (%s, 'second-semantic-session', 102, %s, 'user',
                  'Keep the same Context project-scoped.',
                  2, 'sealed', 'closed', 'user_only', 2)
        """,
        (user_name, project_id),
    )

    domain = _domain()
    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(llm_ner=False),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    store = KnowledgeStore(postgres, object())
    filesystem = ProjectFilesystem(tmp_path / project_id)
    projection = ContextProjection(
        reader=ProjectContextReader(postgres),
        writer=ProjectContextWriter(postgres),
        filesystem=filesystem,
        capture_ingestion_policy=lambda: policy,
    )

    async def capture_domain():
        return domain

    job = ProjectSemanticJob(
        SemanticWindowAdmission(
            store,
            IngestionSettings(semantic_window_tokens=100),
            token_counter=lambda text: max(1, len(text.split())),
            now_ms=lambda: 1_000_000,
        ),
        store,
        _ZeroEpisodeGenerator(),
        settings=IngestionSettings(semantic_window_tokens=100),
        capture_domain=capture_domain,
        capture_ingestion_policy=lambda: policy,
        context_updater=_AgentDerivedUpdater(),
        context_projection=projection,
        context_entity_builder=_EmptyEntityBuilder(),
        context_relationship_extractor=_EmptyRelationships(),
    )
    context = JobContext(user_name=user_name, project_id=project_id)

    results = [await job.execute(context) for _ in range(4)]
    assert all(result.success for result in results)

    revision = await ProjectContextReader(postgres).get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert revision is not None
    snapshot = await store.get_project_context_snapshot(
        str(revision.revision_id),
        user_name=user_name,
        project_id=project_id,
    )
    assert snapshot is not None
    assert "The local project Context is active." in render_context_markdown(snapshot, domain)
    assert await postgres.fetch_one(
        """
        SELECT stage, episode_result_recorded
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {"stage": "completed", "episode_result_recorded": True}
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM public.context_block_entities WHERE project_id = %s",
        (project_id,),
    ) == {"count": 0}
    assert await postgres.fetch_one(
        """
        SELECT count(DISTINCT session_id) AS count
        FROM public.project_semantic_window_messages
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {"count": 2}
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM public.relationship_observations WHERE project_id = %s",
        (project_id,),
    ) == {"count": 0}

    generated = filesystem.read_bytes("CONTEXT.md")
    filesystem.write_bytes(
        "CONTEXT.md",
        generated.replace(
            b"The local project Context is active.",
            b"The local project Context is human asserted.",
        ),
        overwrite=True,
    )
    human_edit_job = ProjectSemanticJob(
        SemanticWindowAdmission(
            store,
            IngestionSettings(semantic_window_tokens=100),
            token_counter=lambda text: max(1, len(text.split())),
            now_ms=lambda: 1_000_000,
        ),
        store,
        _ZeroEpisodeGenerator(),
        settings=IngestionSettings(semantic_window_tokens=100),
        capture_domain=capture_domain,
        capture_ingestion_policy=lambda: policy,
        context_updater=_AgentDerivedUpdater(),
        context_projection=projection,
        context_entity_builder=_HumanEditEntityBuilder(),
        context_relationship_extractor=_HumanEditRelationships(),
    )
    human_results = [await human_edit_job.execute(context) for _ in range(2)]
    assert all(result.success for result in human_results)
    human_revision = await ProjectContextReader(postgres).get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert human_revision is not None
    assert human_revision.origin.value == "human_edit"
    assert await postgres.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_semantic_windows
        WHERE project_id = %s AND origin = 'human_edit' AND stage = 'completed'
        """,
        (project_id,),
    ) == {"count": 1}
