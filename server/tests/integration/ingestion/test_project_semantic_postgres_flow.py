"""Project semantic stages composed with real PostgreSQL storage.

Only model-result boundaries are faked here.  Window admission, revision
publication, stage checkpoints, Knowledge commit, finalization, and Context
projection all use their production implementations.
"""

from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import AssertionKind, ContextAdd, LLMContextUpdate
from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextEntityResult,
    ContextRelationshipWrite,
    EntityWrite,
)
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
from core.knowledge.context.updater import ContextUpdater, ContextUpdateResult
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
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


def _relationship_domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                "Person": {"topic": "Work", "labels": ["person"]},
                "Company": {"topic": "Work", "labels": ["company"]},
            },
            "relationships": {
                "OWNS": {
                    "source_types": ["Person"],
                    "target_types": ["Company"],
                    "labels": ["owns"],
                }
            },
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


class _FailFirstWriteFilesystem:
    def __init__(self, filesystem):
        self.filesystem = filesystem
        self.write_attempts = 0

    def read_bytes(self, *args, **kwargs):
        return self.filesystem.read_bytes(*args, **kwargs)

    def write_bytes(self, *args, **kwargs):
        self.write_attempts += 1
        if self.write_attempts == 1:
            raise OSError("injected projection write failure")
        return self.filesystem.write_bytes(*args, **kwargs)


class _SourceGroundedContextModel:
    async def generate_structured(self, **_kwargs):
        return LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="Sarah owns Delta.",
                    assertion_kind=AssertionKind.SOURCE_GROUNDED,
                    evidence=[{"handle": "S1"}],
                )
            ],
            edit_summary="Recorded source-grounded ownership",
        )


class _RelationshipEntityBuilder:
    def __init__(self, store):
        self.store = store

    async def build(self, build):
        sarah_id = await self.store.allocate_entity_id()
        delta_id = await self.store.allocate_entity_id()
        block = build.knowledge_input_blocks[0]
        entities = {
            sarah_id: EntityWrite(
                entity_id=sarah_id,
                is_new=True,
                canonical_name="Sarah",
                entity_type="Person",
                topic="Work",
                embedding=None,
                aliases=("Sarah",),
            ),
            delta_id: EntityWrite(
                entity_id=delta_id,
                is_new=True,
                canonical_name="Delta",
                entity_type="Company",
                topic="Work",
                embedding=None,
                aliases=("Delta",),
            ),
        }
        result = ContextEntityResult(
            entity_ids=(sarah_id, delta_id),
            new_entity_ids=frozenset(entities),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes=entities,
            block_entity_associations=(
                ContextBlockEntityAssociation(
                    block_id=block.block_id,
                    entity_id=sarah_id,
                    mention_text="Sarah",
                ),
                ContextBlockEntityAssociation(
                    block_id=block.block_id,
                    entity_id=delta_id,
                    mention_text="Delta",
                ),
            ),
            message_entity_refs=(),
        )
        build.set_entity_result(result)
        return result


class _OwnershipRelationshipExtractor:
    async def extract(self, build):
        block = build.knowledge_input_blocks[0]
        sarah_id, delta_id = build.entity_result.entity_ids
        writes = (
            ContextRelationshipWrite(
                support_block_ids=(block.block_id,),
                entity_a_id=sarah_id,
                entity_b_id=delta_id,
                relationship_type="owns",
                canonical_type="OWNS",
                domain_status="recognized",
                source_type="Person",
                target_type="Company",
                domain_version=build.policy.domain.version,
            ),
        )
        build.set_relationship_writes(writes)
        return writes


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
    recovery_filesystem = _FailFirstWriteFilesystem(filesystem)
    recovery_projection = ContextProjection(
        reader=ProjectContextReader(postgres),
        writer=ProjectContextWriter(postgres),
        filesystem=recovery_filesystem,
        capture_ingestion_policy=lambda: policy,
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
        context_projection=recovery_projection,
        context_entity_builder=_HumanEditEntityBuilder(),
        context_relationship_extractor=_HumanEditRelationships(),
    )
    first_human_result = await human_edit_job.execute(context)
    assert first_human_result.success
    failed_state = await ProjectContextReader(postgres).get_projection_state(
        user_name=user_name,
        project_id=project_id,
    )
    assert failed_state is not None
    assert failed_state.projection_failure_code == "OSError"
    assert failed_state.projection_failure_at is not None

    second_human_result = await human_edit_job.execute(context)
    assert second_human_result.success
    human_revision = await ProjectContextReader(postgres).get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert human_revision is not None
    assert human_revision.origin.value == "human_edit"
    repaired_state = await ProjectContextReader(postgres).get_projection_state(
        user_name=user_name,
        project_id=project_id,
    )
    assert repaired_state is not None
    assert repaired_state.projection_revision_id == human_revision.revision_id
    assert repaired_state.projection_failure_code is None
    assert recovery_filesystem.write_attempts == 2
    assert await postgres.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_semantic_windows
        WHERE project_id = %s AND origin = 'human_edit' AND stage = 'completed'
        """,
        (project_id,),
    ) == {"count": 1}


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_semantic_job_commits_source_grounded_relationship_provenance(
    real_server_scope,
    tmp_path,
):
    scope = real_server_scope
    postgres = scope["postgres"]
    user_name = scope["user_name"]
    project_id = scope["project_id"]
    session_id = scope["session_id"]
    source_ref_id = uuid4()
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (%s, %s, 201, %s, 'user', 'Check the supplied ownership source.',
                  1, 'sealed', 'closed', 'assistant_final', 2),
                 (%s, %s, 202, %s, 'assistant', 'The source says Sarah owns Delta.',
                  2, 'sealed', 'open', NULL, NULL)
        """,
        (user_name, session_id, project_id, user_name, session_id, project_id),
    )
    await postgres.execute(
        """
        UPDATE public.messages
        SET user_msg_id = 201
        WHERE project_id = %s AND message_id = 202
        """,
        (project_id,),
    )
    await postgres.execute(
        """
        INSERT INTO public.message_source_refs (
            source_ref_id, project_id, session_id, message_id, source_kind,
            source_message_id, content_hash, locator, excerpt, metadata,
            encounter_kind, agent_run_id, result_position, idempotency_key
        ) VALUES (
            %s, %s, %s, 202, 'user_pasted_text', 201, %s,
            '{"kind":"character_span","start_char":0,"end_char":8}',
            'Sarah owns Delta.', '{}'::jsonb, 'user_pasted_text',
            'composed-run', 0, 'composed-source-provenance'
        )
        """,
        (source_ref_id, project_id, session_id, "0" * 64),
    )

    domain = _relationship_domain()
    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(llm_ner=False),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    store = KnowledgeStore(postgres, object())
    projection = ContextProjection(
        reader=ProjectContextReader(postgres),
        writer=ProjectContextWriter(postgres),
        filesystem=ProjectFilesystem(tmp_path / project_id),
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
        context_updater=ContextUpdater(llm=_SourceGroundedContextModel()),
        context_projection=projection,
        context_entity_builder=_RelationshipEntityBuilder(store),
        context_relationship_extractor=_OwnershipRelationshipExtractor(),
    )
    context = JobContext(user_name=user_name, project_id=project_id)

    results = [await job.execute(context) for _ in range(4)]
    assert all(result.success for result in results)
    provenance = await postgres.fetch_one(
        """
        SELECT
            observation.observed_relationship_label,
            support.support_kind,
            support.message_id,
            support.source_ref_id,
            source.message_id AS source_owner_message_id
        FROM public.relationship_observations AS observation
        JOIN public.relationship_observation_blocks AS observation_block
          ON observation_block.observation_id = observation.observation_id
         AND observation_block.project_id = observation.project_id
        JOIN public.project_context_block_supports AS support
          ON support.block_id = observation_block.block_id
         AND support.project_id = observation.project_id
        JOIN public.message_source_refs AS source
          ON source.source_ref_id = support.source_ref_id
         AND source.project_id = support.project_id
         AND source.session_id = support.session_id
         AND source.message_id = support.message_id
        WHERE observation.project_id = %s AND observation.retired_at IS NULL
        """,
        (project_id,),
    )
    assert provenance == {
        "observed_relationship_label": "owns",
        "support_kind": "assistant_source",
        "message_id": 202,
        "source_ref_id": source_ref_id,
        "source_owner_message_id": 202,
    }
    evidence = await store.get_relationship_observation_evidence(
        int(
            (
                await postgres.fetch_one(
                    """
                    SELECT observation_id
                    FROM public.relationship_observations
                    WHERE project_id = %s AND retired_at IS NULL
                    """,
                    (project_id,),
                )
            )["observation_id"]
        ),
        user_name=user_name,
        project_id=project_id,
    )
    assert [node.pointer.kind for node in evidence.nodes] == [
        "relationship_observation",
        "context_block",
        "message",
        "source_reference",
    ]
    assert {edge.relation for edge in evidence.edges} == {
        "source_owned_by_message",
        "supports_context_block",
        "supports_relationship_observation",
    }
    assert evidence.nodes_truncated is False
    assert evidence.edges_truncated is False
    repeated = await store.get_relationship_observation_evidence(
        int(evidence.subject.identifier),
        user_name=user_name,
        project_id=project_id,
    )
    assert repeated == evidence

    block_id = next(
        node.pointer.identifier
        for node in evidence.nodes
        if node.pointer.kind == "context_block"
    )
    block_evidence = await store.get_context_block_evidence(
        block_id,
        user_name=user_name,
        project_id=project_id,
    )
    assert {node.pointer.kind for node in block_evidence.nodes} == {
        "context_block",
        "message",
        "source_reference",
    }
    hidden = await store.get_relationship_observation_evidence(
        int(evidence.subject.identifier),
        user_name=user_name,
        project_id=f"{project_id}-outside-scope",
    )
    assert len(hidden.nodes) == 1
    assert hidden.nodes[0].status == "missing"
    assert hidden.edges == ()

    deleted = await ProjectDeletionWriter(postgres).delete_project(
        user_name=user_name,
        project_id=project_id,
    )
    assert deleted is not None
    after_deletion = await store.get_relationship_observation_evidence(
        int(evidence.subject.identifier),
        user_name=user_name,
        project_id=project_id,
    )
    assert len(after_deletion.nodes) == 1
    assert after_deletion.nodes[0].status == "missing"
    assert after_deletion.edges == ()
