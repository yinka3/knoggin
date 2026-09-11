"""Project semantic stages composed with real PostgreSQL storage.

Only model-result boundaries are faked here.  Window admission, revision
publication, stage checkpoints, Knowledge commit, finalization, and Context
projection all use their production implementations.
"""

from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextReplace,
    LLMContextUpdate,
)
from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextEntityResult,
    ContextRelationshipWrite,
    EntityWrite,
    ProjectEntityClassification,
)
from common.schema.semantic_window import SemanticWindowStage
from common.schema.settings import (
    EntityResolutionSettings,
    IngestionSettings,
    TextProcessorSettings,
)
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_job import ProjectSemanticJob
from core.ingestion.relationship_extractor import ContextRelationshipExtractor
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.context.projection import ContextProjection
from core.knowledge.context.render import apply_context_edits, render_context_markdown
from core.knowledge.context.updater import ContextUpdater, ContextUpdateResult
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.documents.filesystem import ProjectFilesystem
from core.knowledge.entity.resolver import EntityResolver
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


def _classification(entity_id, entity_type, *, membership):
    return ProjectEntityClassification(
        entity_id=entity_id,
        entity_type=entity_type,
        topic="Work",
        membership=membership,
    )


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
            project_classifications={},
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
            project_classifications={},
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
        self.calls = 0
        self.sarah_id: int | None = None
        self.delta_id: int | None = None

    async def build(self, build):
        self.calls += 1
        if self.calls > 1:
            raise AssertionError("Knowledge publication recovery must not rebuild entities")
        sarah_id = await self.store.allocate_entity_id()
        delta_id = await self.store.allocate_entity_id()
        self.sarah_id = sarah_id
        self.delta_id = delta_id
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
            project_classifications={
                sarah_id: _classification(
                    sarah_id,
                    "Person",
                    membership="missing",
                ),
                delta_id: _classification(
                    delta_id,
                    "Company",
                    membership="missing",
                ),
            },
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


class _PendingContextEntities:
    async def get_profile(self, _entity_id):
        raise AssertionError("new Context entities do not require profile hydration")

    def get_mentions_for_id(self, _entity_id):
        return []


class _FailingCommittedEntityPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[int, ...]] = []

    async def __call__(self, entity_ids: tuple[int, ...]) -> None:
        self.calls.append(tuple(entity_ids))
        raise OSError("injected resolver publication failure")


class _ContextRelationshipLLM:
    extraction_model = "test-vp02"

    def __init__(self, connections) -> None:
        self.connections = connections

    async def generate_structured(self, *, response_model, **_kwargs):
        return response_model.model_validate({"connections": self.connections})


class _HomonymSourceGroundedContextModel:
    async def generate_structured(self, **_kwargs):
        return LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="Alex owns Delta.",
                    assertion_kind=AssertionKind.SOURCE_GROUNDED,
                    evidence=[{"handle": "S1"}],
                )
            ],
            edit_summary="Recorded homonymous source-grounded ownership",
        )


class _HomonymRelationshipEntityBuilder:
    def __init__(self, store) -> None:
        self.store = store
        self.first_alex_id = None
        self.second_alex_id = None
        self.delta_id = None

    async def build(self, build):
        first_alex_id = await self.store.allocate_entity_id()
        second_alex_id = await self.store.allocate_entity_id()
        delta_id = await self.store.allocate_entity_id()
        self.first_alex_id = first_alex_id
        self.second_alex_id = second_alex_id
        self.delta_id = delta_id
        block = build.knowledge_input_blocks[0]
        entities = {
            first_alex_id: EntityWrite(
                entity_id=first_alex_id,
                is_new=True,
                canonical_name="Alex",
                entity_type="Person",
                topic="Work",
                embedding=None,
                aliases=("Alex",),
            ),
            second_alex_id: EntityWrite(
                entity_id=second_alex_id,
                is_new=True,
                canonical_name="Alex",
                entity_type="Person",
                topic="Work",
                embedding=None,
                aliases=("Alex",),
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
            entity_ids=(first_alex_id, second_alex_id, delta_id),
            new_entity_ids=frozenset(entities),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes=entities,
            project_classifications={
                first_alex_id: _classification(
                    first_alex_id,
                    "Person",
                    membership="missing",
                ),
                second_alex_id: _classification(
                    second_alex_id,
                    "Person",
                    membership="missing",
                ),
                delta_id: _classification(
                    delta_id,
                    "Company",
                    membership="missing",
                ),
            },
            block_entity_associations=tuple(
                ContextBlockEntityAssociation(
                    block_id=block.block_id,
                    entity_id=entity_id,
                    mention_text=mention_text,
                )
                for entity_id, mention_text in (
                    (first_alex_id, "Alex"),
                    (second_alex_id, "Alex"),
                    (delta_id, "Delta"),
                )
            ),
            message_entity_refs=(),
        )
        build.set_entity_result(result)
        return result


class _CorrectionContextModel:
    """Return one older claim, its newer correction, then a Context no-op."""

    def __init__(self) -> None:
        self.calls = 0

    async def generate_structured(self, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            return LLMContextUpdate(
                operations=[
                    ContextAdd(
                        section_key="current_state",
                        markdown="Sarah owns Delta.",
                        assertion_kind=AssertionKind.USER_ASSERTED,
                        evidence=[{"handle": "M1"}],
                    )
                ],
                edit_summary="Recorded Sarah ownership",
            )
        if self.calls == 2:
            return LLMContextUpdate(
                operations=[
                    ContextReplace(
                        section_key="current_state",
                        target={"handle": "C1"},
                        markdown="John owns Delta.",
                        assertion_kind=AssertionKind.USER_ASSERTED,
                        evidence=[{"handle": "M1"}],
                    )
                ],
                edit_summary="Corrected ownership",
            )
        if self.calls == 3:
            return LLMContextUpdate(
                operations=[],
                edit_summary="No Context changes",
            )
        raise AssertionError("unexpected Context update")


class _CorrectionEntityBuilder:
    """Use production persistence with deterministic entity-resolution output."""

    def __init__(self, store) -> None:
        self.store = store
        self.calls = 0
        self.delta_id: int | None = None

    async def build(self, build):
        self.calls += 1
        assert len(build.knowledge_input_blocks) == 1
        block = build.knowledge_input_blocks[0]
        if self.calls == 1:
            source_name = "Sarah"
            source_id = await self.store.allocate_entity_id()
            self.delta_id = await self.store.allocate_entity_id()
            writes = {
                source_id: EntityWrite(
                    entity_id=source_id,
                    is_new=True,
                    canonical_name=source_name,
                    entity_type="Person",
                    topic="Work",
                    embedding=None,
                    aliases=(source_name,),
                ),
                self.delta_id: EntityWrite(
                    entity_id=self.delta_id,
                    is_new=True,
                    canonical_name="Delta",
                    entity_type="Company",
                    topic="Work",
                    embedding=None,
                    aliases=("Delta",),
                ),
            }
        elif self.calls == 2:
            assert self.delta_id is not None
            source_name = "John"
            source_id = await self.store.allocate_entity_id()
            writes = {
                source_id: EntityWrite(
                    entity_id=source_id,
                    is_new=True,
                    canonical_name=source_name,
                    entity_type="Person",
                    topic="Work",
                    embedding=None,
                    aliases=(source_name,),
                )
            }
        else:
            raise AssertionError("no-op Context reuse must skip entity extraction")

        assert self.delta_id is not None
        result = ContextEntityResult(
            entity_ids=(source_id, self.delta_id),
            new_entity_ids=frozenset(writes),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes=writes,
            project_classifications={
                source_id: _classification(
                    source_id,
                    "Person",
                    membership="missing",
                ),
                self.delta_id: _classification(
                    self.delta_id,
                    "Company",
                    membership=(
                        "missing" if self.delta_id in writes else "existing"
                    ),
                ),
            },
            block_entity_associations=(
                ContextBlockEntityAssociation(
                    block_id=block.block_id,
                    entity_id=source_id,
                    mention_text=source_name,
                ),
                ContextBlockEntityAssociation(
                    block_id=block.block_id,
                    entity_id=self.delta_id,
                    mention_text="Delta",
                ),
            ),
            message_entity_refs=(),
        )
        build.set_entity_result(result)
        return result


class _CorrectionRelationshipExtractor:
    def __init__(self) -> None:
        self.calls = 0

    async def extract(self, build):
        self.calls += 1
        if self.calls > 2:
            raise AssertionError("no-op Context reuse must skip relationship extraction")
        assert len(build.knowledge_input_blocks) == 1
        block = build.knowledge_input_blocks[0]
        source_id, target_id = build.entity_result.entity_ids
        writes = (
            ContextRelationshipWrite(
                support_block_ids=(block.block_id,),
                entity_a_id=source_id,
                entity_b_id=target_id,
                relationship_type="owns",
                canonical_type="OWNS",
                domain_status="recognized",
                source_type="Person",
                target_type="Company",
                context=block.markdown,
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
async def test_project_semantic_job_recovers_resolver_publication_and_commits_source_grounded_relationship_provenance(
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

    entity_builder = _RelationshipEntityBuilder(store)
    relationship_extractor = ContextRelationshipExtractor(
        user_name=user_name,
        llm=_ContextRelationshipLLM(
            [
                {
                    "block_ids": ["b1"],
                    "entity_a": "e2",
                    "entity_b": "e3",
                    "relationship": "owns",
                    "context": "Sarah owns Delta.",
                }
            ]
        ),
        entities=_PendingContextEntities(),
    )
    now = [1_000_000]

    def new_job(publisher):
        return ProjectSemanticJob(
            SemanticWindowAdmission(
                store,
                IngestionSettings(semantic_window_tokens=100),
                token_counter=lambda text: max(1, len(text.split())),
                now_ms=lambda: now[0],
            ),
            store,
            _ZeroEpisodeGenerator(),
            settings=IngestionSettings(semantic_window_tokens=100),
            capture_domain=capture_domain,
            capture_ingestion_policy=lambda: policy,
            context_updater=ContextUpdater(llm=_SourceGroundedContextModel()),
            context_projection=projection,
            context_entity_builder=entity_builder,
            context_relationship_extractor=relationship_extractor,
            publish_committed_entity_ids=publisher,
            now_ms=lambda: now[0],
        )

    failed_publisher = _FailingCommittedEntityPublisher()
    job = new_job(failed_publisher)
    context = JobContext(user_name=user_name, project_id=project_id)

    results = [await job.execute(context) for _ in range(3)]
    assert all(result.success for result in results)
    committed_window = await store.get_active_project_semantic_window(
        user_name=user_name,
        project_id=project_id,
    )
    assert committed_window is not None
    assert committed_window.stage is SemanticWindowStage.KNOWLEDGE_COMMITTED
    failed_finalization = await job.execute(context)

    assert failed_finalization.success is False
    assert entity_builder.sarah_id is not None
    assert entity_builder.delta_id is not None
    assert failed_publisher.calls == [
        (entity_builder.sarah_id, entity_builder.delta_id)
    ]
    assert await postgres.fetch_one(
        """
        SELECT stage, last_failure_stage
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {
        "stage": "knowledge_committed",
        "last_failure_stage": "resolver_publication",
    }

    now[0] = 1_030_001
    recovered_resolver = EntityResolver(
        store,
        object(),
        project_id,
        [project_id],
    )
    recovered = await new_job(
        recovered_resolver.publish_committed_entity_ids
    ).execute(context)

    assert recovered.success
    assert entity_builder.calls == 1
    assert await recovered_resolver.get_id("Sarah") == entity_builder.sarah_id
    sarah = recovered_resolver.get_cached_profile(entity_builder.sarah_id)
    assert sarah is not None
    assert sarah.entity_type == "Person"
    assert sarah.topic == "Work"
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


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_vp02_persists_distinct_homonymous_handles_with_source_provenance(
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
        ) VALUES (%s, %s, 211, %s, 'user', 'Check the supplied ownership source.',
                  1, 'sealed', 'closed', 'assistant_final', 2),
                 (%s, %s, 212, %s, 'assistant', 'The source says Alex owns Delta.',
                  2, 'sealed', 'open', NULL, NULL)
        """,
        (user_name, session_id, project_id, user_name, session_id, project_id),
    )
    await postgres.execute(
        """
        UPDATE public.messages
        SET user_msg_id = 211
        WHERE project_id = %s AND message_id = 212
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
            %s, %s, %s, 212, 'user_pasted_text', 211, %s,
            '{"kind":"character_span","start_char":0,"end_char":8}',
            'Alex owns Delta.', '{}'::jsonb, 'user_pasted_text',
            'homonym-run', 0, 'homonym-source-provenance'
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

    entity_builder = _HomonymRelationshipEntityBuilder(store)
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
        context_updater=ContextUpdater(llm=_HomonymSourceGroundedContextModel()),
        context_projection=projection,
        context_entity_builder=entity_builder,
        context_relationship_extractor=ContextRelationshipExtractor(
            user_name=user_name,
            llm=_ContextRelationshipLLM(
                [
                    {
                        "block_ids": ["b1"],
                        "entity_a": "e2",
                        "entity_b": "e4",
                        "relationship": "owns",
                    },
                    {
                        "block_ids": ["b1"],
                        "entity_a": "e3",
                        "entity_b": "e4",
                        "relationship": "owns",
                    },
                ]
            ),
            entities=_PendingContextEntities(),
        ),
    )

    results = [
        await job.execute(JobContext(user_name=user_name, project_id=project_id))
        for _ in range(4)
    ]
    assert all(result.success for result in results)
    assert entity_builder.first_alex_id is not None
    assert entity_builder.second_alex_id is not None
    assert entity_builder.delta_id is not None

    observations = await postgres.fetch_all(
        """
        SELECT
            observation.source_entity_id,
            observation.target_entity_id,
            support.message_id,
            support.source_ref_id
        FROM public.relationship_observations AS observation
        JOIN public.relationship_observation_blocks AS observation_block
          ON observation_block.observation_id = observation.observation_id
         AND observation_block.project_id = observation.project_id
        JOIN public.project_context_block_supports AS support
          ON support.block_id = observation_block.block_id
         AND support.project_id = observation.project_id
        WHERE observation.project_id = %s AND observation.retired_at IS NULL
        ORDER BY observation.source_entity_id
        """,
        (project_id,),
    )
    assert observations == [
        {
            "source_entity_id": entity_builder.first_alex_id,
            "target_entity_id": entity_builder.delta_id,
            "message_id": 212,
            "source_ref_id": source_ref_id,
        },
        {
            "source_entity_id": entity_builder.second_alex_id,
            "target_entity_id": entity_builder.delta_id,
            "message_id": 212,
            "source_ref_id": source_ref_id,
        },
    ]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_semantic_job_preserves_correction_history_through_noop_restart(
    real_server_scope,
    tmp_path,
):
    """Compose correction, current/history reads, and no-op restart on real storage."""

    scope = real_server_scope
    postgres = scope["postgres"]
    user_name = scope["user_name"]
    project_id = scope["project_id"]
    session_id = scope["session_id"]

    async def insert_closed_exchange(message_id: int, content: str, timestamp_ms: int):
        await postgres.execute(
            """
            INSERT INTO public.messages (
                user_name, session_id, message_id, project_id, role, content,
                timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
                exchange_closed_at_ms
            ) VALUES (%s, %s, %s, %s, 'user', %s, %s, 'sealed', 'closed',
                      'user_only', %s)
            """,
            (
                user_name,
                session_id,
                message_id,
                project_id,
                content,
                timestamp_ms,
                timestamp_ms,
            ),
        )

    domain = _relationship_domain()
    settings = IngestionSettings(semantic_window_tokens=1)
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
    model = _CorrectionContextModel()
    entity_builder = _CorrectionEntityBuilder(store)
    relationship_extractor = _CorrectionRelationshipExtractor()

    async def capture_domain():
        return domain

    def new_job():
        return ProjectSemanticJob(
            SemanticWindowAdmission(
                store,
                settings,
                token_counter=lambda text: max(1, len(text.split())),
                now_ms=lambda: 1_000_000,
            ),
            store,
            _ZeroEpisodeGenerator(),
            settings=settings,
            capture_domain=capture_domain,
            capture_ingestion_policy=lambda: policy,
            context_updater=ContextUpdater(llm=model),
            context_projection=projection,
            context_entity_builder=entity_builder,
            context_relationship_extractor=relationship_extractor,
        )

    context = JobContext(user_name=user_name, project_id=project_id)

    async def complete_window(job):
        results = [await job.execute(context) for _ in range(4)]
        assert all(result.success for result in results)

    job = new_job()
    await insert_closed_exchange(301, "Sarah owns Delta.", 1_000)
    await complete_window(job)

    reader = ProjectContextReader(postgres)
    initial_revision = await reader.get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert initial_revision is not None
    old_observation = await postgres.fetch_one(
        """
        SELECT observation_id
        FROM public.relationship_observations
        WHERE project_id = %s AND retired_at IS NULL
        """,
        (project_id,),
    )
    assert old_observation is not None
    old_observation_id = int(old_observation["observation_id"])

    await insert_closed_exchange(302, "John owns Delta.", 2_000)
    await complete_window(job)

    correction_revision = await reader.get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert correction_revision is not None
    assert correction_revision.revision_number == initial_revision.revision_number + 1

    await insert_closed_exchange(303, "No change to the ownership.", 3_000)
    no_op_results = [await job.execute(context) for _ in range(2)]
    assert all(result.success for result in no_op_results)
    no_op_window = await store.get_active_project_semantic_window(
        user_name=user_name,
        project_id=project_id,
    )
    assert no_op_window is not None
    assert no_op_window.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert no_op_window.context_revision_id == correction_revision.revision_id

    restarted = new_job()
    restart_results = [await restarted.execute(context) for _ in range(2)]
    assert all(result.success for result in restart_results)

    assert model.calls == 3
    assert entity_builder.calls == 2
    assert relationship_extractor.calls == 2
    assert entity_builder.delta_id is not None

    observations = await postgres.fetch_all(
        """
        SELECT observation_id, source_entity_id, target_entity_id, observed_at_ms,
               retired_at, retired_reason
        FROM public.relationship_observations
        WHERE project_id = %s
        ORDER BY observation_id
        """,
        (project_id,),
    )
    assert len(observations) == 2
    retired = next(
        row for row in observations if int(row["observation_id"]) == old_observation_id
    )
    active = next(
        row for row in observations if int(row["observation_id"]) != old_observation_id
    )
    assert retired["retired_at"] is not None
    assert retired["retired_reason"] == "context_block_replaced_or_deleted"
    assert active["retired_at"] is None
    assert active["observed_at_ms"] == 2_000
    assert await postgres.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.relationship_observation_blocks
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {"count": 2}

    current = await EntityReader(postgres).get_related_entities(
        [entity_builder.delta_id],
        visible_project_ids=[project_id],
    )
    assert len(current) == 1
    assert current[0]["source"] == "John"
    assert current[0]["target"] == "Delta"
    assert current[0]["observation_count"] == 1
    assert current[0]["first_observed"] == 2_000
    assert current[0]["last_observed"] == 2_000
    assert current[0]["observation_refs"] == [
        {
            "observation_id": int(active["observation_id"]),
            "observed_relationship_label": "owns",
            "relationship_type": "owns",
            "observed_at_ms": 2_000,
            "context": "John owns Delta.",
        }
    ]
    assert current[0]["evidence_refs"] == [
        {
            "project_id": project_id,
            "user_name": user_name,
            "session_id": session_id,
            "message_id": 302,
        }
    ]

    old_evidence = await store.get_relationship_observation_evidence(
        old_observation_id,
        user_name=user_name,
        project_id=project_id,
    )
    historical_observation = next(
        node
        for node in old_evidence.nodes
        if node.pointer.kind == "relationship_observation"
    )
    assert historical_observation.status == "retired"
    old_support = await postgres.fetch_one(
        """
        SELECT block_id
        FROM public.relationship_observation_blocks
        WHERE observation_id = %s AND project_id = %s
        """,
        (old_observation_id, project_id),
    )
    assert old_support is not None
    assert {
        (node.pointer.kind, node.pointer.identifier) for node in old_evidence.nodes
    } == {
        ("relationship_observation", str(old_observation_id)),
        ("context_block", str(old_support["block_id"])),
        ("message", "301"),
    }

    recency_rows = await postgres.fetch_all(
        """
        SELECT entity.canonical_name, context.last_mentioned_ms
        FROM public.project_entity_contexts AS context
        JOIN public.entities AS entity ON entity.entity_id = context.entity_id
        WHERE context.project_id = %s
          AND entity.canonical_name = ANY(%s)
        """,
        (project_id, ["Sarah", "John", "Delta"]),
    )
    assert {
        row["canonical_name"]: row["last_mentioned_ms"] for row in recency_rows
    } == {"Sarah": 1_000, "John": 2_000, "Delta": 2_000}
    assert await postgres.fetch_one(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE stage = 'completed') AS completed,
               count(*) FILTER (WHERE episode_result_recorded) AS episode_results
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {"total": 3, "completed": 3, "episode_results": 3}
