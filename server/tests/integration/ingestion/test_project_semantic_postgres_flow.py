"""Project semantic stages composed with real PostgreSQL storage.

Only model-result boundaries are faked here.  Window admission, revision
publication, stage checkpoints, Knowledge commit, finalization, and Context
projection all use their production implementations.
"""

import asyncio
from types import SimpleNamespace

import pytest
import spacy

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextReplace,
    LLMContextUpdate,
)
from common.schema.episode.generation import (
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.ingestion.contracts import EntityResult
from common.schema.semantic_window import SemanticWindowStage
from common.schema.settings import (
    DeveloperSettings,
    EntityResolutionSettings,
    ExtractionSettings,
    IngestionSettings,
    RootConfig,
)
from core.ingestion.entity_build import EntityBuildService
from core.ingestion.policy import IngestionPolicy
from core.ingestion.project_semantic_processor import ProjectSemanticProcessor
from core.ingestion.relationship_extractor import RelationshipExtractor
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.ingestion.text_processor import TextProcessor
from core.knowledge.context.projection import ContextProjection
from core.knowledge.context.render import apply_context_edits, render_context_markdown
from core.knowledge.context.updater import ContextUpdater, ContextUpdateResult
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.documents.filesystem import ProjectFilesystem
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.episodes.build import ProjectEpisodeBuild
from core.knowledge.store import KnowledgeStore
from core.project.domain_config_store import DomainConfigStore
from core.project.project_manager import ProjectManager
from infrastructure.job.base import JobContext
from infrastructure.job.scheduler import Scheduler
from runtime.project_runtime import ProjectRuntime


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


class _BlockingEpisodeGenerator:
    def __init__(self) -> None:
        self.started = asyncio.Event()

    async def generate(self, **_kwargs):
        self.started.set()
        await asyncio.Event().wait()


class _NoopContextModel:
    async def generate_structured(self, **_kwargs):
        return LLMContextUpdate(operations=[], edit_summary="No Context changes")


class _UnexpectedKnowledgeBuilder:
    async def build(self, _build):
        raise AssertionError("a window without Episodes has no Knowledge input")


class _UnexpectedRelationshipExtractor:
    async def extract(self, _build):
        raise AssertionError("a window without Episodes has no relationships")


class _OneEpisodeGenerator:
    async def generate(self, *, project_id, messages, policy, **_kwargs):
        build = ProjectEpisodeBuild(
            project_id=project_id,
            policy=policy,
            messages=[dict(message) for message in messages],
        )
        build.prepare_local_references()
        build.apply_llm_output(
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="Both sessions asked for shared project Context.",
                        message_influences=list(build.message_ids_by_local),
                    )
                ]
            )
        )
        build.create_episodes()
        build.attach_embeddings([[0.25] * 768])
        return build


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


class _EpisodeAwareAgentDerivedUpdater(_AgentDerivedUpdater):
    async def update(self, *, earlier_episodes, **kwargs):
        # The window's own Episode is already in its messages; only Episodes
        # from earlier windows are background.
        assert earlier_episodes == []
        return await super().update(earlier_episodes=earlier_episodes, **kwargs)


class _EmptyEntityBuilder:
    async def build(self, build):
        assert len(build.episodes) == 1
        result = EntityResult(
            entity_ids=(),
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            project_classifications={},
        )
        build.set_entity_result(result)
        return result


class _EmptyRelationships:
    async def extract(self, build):
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


class _UserAssertedContextModel:
    """Each window records its own message as a Context block."""

    def __init__(self) -> None:
        self.calls = 0

    async def generate_structured(self, *, user, **_kwargs):
        self.calls += 1
        line = next(
            line for line in user.splitlines() if line.startswith("[M1]")
        )
        text = line.split(": ", 2)[-1].strip()
        return LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown=text,
                    assertion_kind=AssertionKind.USER_ASSERTED,
                    user_quote=text,
                    evidence=[{"handle": "M1"}],
                )
            ],
            edit_summary="Recorded the window's claim",
        )


class _NamesLLM:
    """VP-01 and VP-02 over a fixed name list: owner -> company in M1."""

    extraction_model = "test-extraction"

    def __init__(self, people, companies):
        self.people, self.companies = people, companies
        self.entity_calls = 0
        self.relationship_calls = 0

    async def generate_structured(self, *, response_model, user, **_kwargs):
        if response_model.__name__ == "EpisodeEntityExtraction":
            self.entity_calls += 1
            mentions = [
                {"messages": ["M1"], "name": name, "type": kind}
                for names, kind in ((self.people, "Person"), (self.companies, "Company"))
                for name in names
                if name in user.split("MESSAGES:", 1)[1]
            ]
            return response_model.model_validate({"mentions": mentions})
        self.relationship_calls += 1
        handles = {
            line.split(" = ")[1].split(" [")[0]: line.split(" = ")[0]
            for line in user.splitlines()
            if line.startswith("e") and " = " in line
        }
        connections = [
            {"messages": ["M1"], "entity_a": handles[person],
             "entity_b": handles[company], "relationship": "owns",
             "context": f"{person} owns {company}."}
            for person in self.people for company in self.companies
            if person in handles and company in handles
        ]
        return response_model.model_validate({"connections": connections})


def _extraction(resolver, llm, user_name, store):
    processor = TextProcessor(
        get_known_aliases=resolver.get_known_aliases,
        get_alias_version=resolver.get_alias_version,
        get_profile=resolver.get_profile,
        spacy=spacy.blank("en"),
        settings=ExtractionSettings(),
        llm=llm,
        user_name=user_name,
    )
    return (
        EntityBuildService(
            processor=processor, resolver=resolver,
            allocate_entity_id=store.allocate_entity_id,
        ),
        RelationshipExtractor(user_name=user_name, llm=llm, entities=resolver),
    )


class _FailingCommittedEntityPublisher:
    def __init__(self) -> None:
        self.calls: list[tuple[int, ...]] = []

    async def __call__(self, entity_ids: tuple[int, ...]) -> None:
        self.calls.append(tuple(entity_ids))
        raise OSError("injected resolver publication failure")


async def _publish_nothing(_entity_ids: tuple[int, ...]) -> None:
    return None


class _Stage2ContextModel:
    """Create one Context block, replace it, then leave Context unchanged."""

    def __init__(self) -> None:
        self.calls = 0

    async def generate_structured(self, **_kwargs):
        self.calls += 1
        if self.calls == 1:
            return LLMContextUpdate(
                operations=[
                    ContextAdd(
                        section_key="current_state",
                        markdown="Avery Stone owns Delta Corp.",
                        assertion_kind=AssertionKind.USER_ASSERTED,
                        user_quote="Avery Stone owns Delta Corp.",
                        evidence=[{"handle": "M1"}],
                    )
                ],
                edit_summary="Recorded Avery ownership",
            )
        if self.calls == 2:
            return LLMContextUpdate(
                operations=[
                    ContextReplace(
                        section_key="current_state",
                        target={"handle": "C1"},
                        markdown="Avery Stone continues to own Delta Corp.",
                        assertion_kind=AssertionKind.USER_ASSERTED,
                        user_quote="Avery Stone continues to own Delta Corp.",
                        evidence=[{"handle": "M1"}],
                    )
                ],
                edit_summary="Refreshed Avery ownership",
            )
        if self.calls == 3:
            return LLMContextUpdate(
                operations=[],
                edit_summary="No Context changes",
            )
        raise AssertionError("unexpected Context update")


class _Stage2Embedding:
    """Keep the composition test deterministic without loading local models."""

    def __init__(self) -> None:
        self.batch_calls: list[tuple[str, ...]] = []
        self.single_calls: list[str] = []

    async def encode(self, values, **_kwargs):
        materialized = tuple(values)
        self.batch_calls.append(materialized)
        return [[0.25] * 768 for _ in materialized]

    async def encode_single(self, value):
        self.single_calls.append(value)
        return [0.25] * 768


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_selection_retries_after_participation_toggle(
    real_server_scope,
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
        ) VALUES (%s, %s, 101, %s, 'user', 'Select this before the toggle.',
                  1, 'sealed', 'closed', 'user_only', 1)
        """,
        (user_name, session_id, project_id),
    )
    domain = _domain()
    store = KnowledgeStore(postgres, object())
    admission = SemanticWindowAdmission(
        store,
        IngestionSettings(semantic_window_tokens=100),
        token_counter=lambda text: max(1, len(text.split())),
        now_ms=lambda: 1_000_000,
    )
    proposal = await admission.select(
        user_name=user_name,
        project_id=project_id,
        domain=domain,
        force_flush=True,
    )
    assert proposal is not None

    await ProjectManager(
        SimpleNamespace(postgres=postgres), user_name=user_name
    ).set_session_semantic_participation(project_id, [])

    with pytest.raises(ValueError, match="no longer eligible"):
        await store.claim_project_semantic_window(
            proposal.window, list(proposal.messages)
        )

    assert await postgres.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {"count": 0}
    assert await admission.select(
        user_name=user_name,
        project_id=project_id,
        domain=domain,
        force_flush=True,
    ) is None


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_participation_claim_and_runtime_policy_are_coherent(
    real_server_scope,
    monkeypatch,
):
    """Exercise Stage 3 eligibility, claim ordering, and policy capture together."""

    scope = real_server_scope
    postgres = scope["postgres"]
    user_name = scope["user_name"]
    project_id = scope["project_id"]
    disabled_session_id = scope["session_id"]
    enabled_session_id = f"{disabled_session_id}-enabled"
    domain_store = DomainConfigStore(postgres)
    initial_domain = await domain_store.load(user_name, project_id)
    runtime = ProjectRuntime(
        project_id=project_id,
        entities=object(),
        knowledge_retrieval=object(),
        text_processor=SimpleNamespace(
            gliner_threshold=0.42,
            llm_ner_mode="fallback",
        ),
        scheduler=None,
        user_name=user_name,
        readable_project_ids=[project_id],
        domain_config=initial_domain,
        compiled_domain=initial_domain.compile(),
        document_service=SimpleNamespace(),
        domain_config_store=domain_store,
        config_manager=SimpleNamespace(
            config=RootConfig(developer_settings=DeveloperSettings()),
        ),
    )
    before_activation = await runtime.capture_semantic_policy()
    activation = await runtime.activate_domain_config(
        initial_domain.with_version(0),
        expected_version=initial_domain.version,
    )
    captured_policy = await runtime.capture_semantic_policy()

    assert before_activation.domain.version == initial_domain.version
    assert captured_policy.domain is activation.compiled
    assert captured_policy.domain.version == activation.config.version

    await postgres.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES (%s, %s, %s)
        """,
        (enabled_session_id, user_name, project_id),
    )
    manager = ProjectManager(SimpleNamespace(postgres=postgres), user_name=user_name)
    await manager.set_session_semantic_participation(project_id, [enabled_session_id])
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES
            (%s, %s, 101, %s, 'user', 'Excluded before re-enable.',
             1, 'sealed', 'closed', 'user_only', 1),
            (%s, %s, 201, %s, 'user', 'Always eligible.',
             2, 'sealed', 'closed', 'user_only', 2)
        """,
        (
            user_name,
            disabled_session_id,
            project_id,
            user_name,
            enabled_session_id,
            project_id,
        ),
    )
    await manager.set_session_semantic_participation(
        project_id,
        [disabled_session_id, enabled_session_id],
    )
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (%s, %s, 102, %s, 'user', 'Eligible after re-enable.',
                 3, 'sealed', 'closed', 'user_only', 3)
        """,
        (user_name, disabled_session_id, project_id),
    )

    store = KnowledgeStore(postgres, object())
    admission = SemanticWindowAdmission(
        store,
        IngestionSettings(semantic_window_tokens=100),
        token_counter=lambda text: max(1, len(text.split())),
        now_ms=lambda: 1_000_000,
    )
    stale_proposal = await admission.select(
        user_name=user_name,
        project_id=project_id,
        domain=captured_policy.domain,
        ingestion_policy=captured_policy,
        force_flush=True,
    )

    assert stale_proposal is not None
    assert [member.message_id for member in stale_proposal.messages] == [201, 102]
    assert (
        stale_proposal.window.policy_snapshot["compiled_domain"]
        == captured_policy.domain.to_dict()
    )
    assert (
        stale_proposal.window.policy_snapshot["ingestion_policy"]["compiled_domain"]
        == captured_policy.domain.to_dict()
    )

    await manager.set_session_semantic_participation(project_id, [enabled_session_id])
    with pytest.raises(ValueError, match="no longer eligible"):
        await store.claim_project_semantic_window(
            stale_proposal.window,
            list(stale_proposal.messages),
        )
    assert await postgres.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (project_id,),
    ) == {"count": 0}

    await manager.set_session_semantic_participation(
        project_id,
        [disabled_session_id, enabled_session_id],
    )
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (%s, %s, 103, %s, 'user', 'Frozen when claimed.',
                 4, 'sealed', 'closed', 'user_only', 4)
        """,
        (user_name, disabled_session_id, project_id),
    )
    proposal = await admission.select(
        user_name=user_name,
        project_id=project_id,
        domain=captured_policy.domain,
        ingestion_policy=captured_policy,
        force_flush=True,
    )

    assert proposal is not None
    assert [member.message_id for member in proposal.messages] == [201, 103]
    claimed = await store.claim_project_semantic_window(
        proposal.window,
        list(proposal.messages),
    )
    await manager.set_session_semantic_participation(project_id, [])
    replayed = await store.claim_project_semantic_window(
        proposal.window,
        list(proposal.messages),
    )

    assert claimed.claimed is True
    assert replayed.claimed is False
    assert replayed.window.window_id == claimed.window.window_id


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_semantic_processor_uses_real_storage_for_agent_derived_context(
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
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    store = KnowledgeStore(postgres, object())
    filesystem = ProjectFilesystem(tmp_path / project_id)
    projection = ContextProjection(
        reader=ProjectContextReader(postgres),
        writer=ProjectContextWriter(postgres),
        filesystem=filesystem,
    )

    async def capture_semantic_policy():
        return policy

    job = ProjectSemanticProcessor(
        SemanticWindowAdmission(
            store,
            IngestionSettings(semantic_window_tokens=100),
            token_counter=lambda text: max(1, len(text.split())),
            now_ms=lambda: 1_000_000,
        ),
        store,
        _OneEpisodeGenerator(),
        settings=IngestionSettings(semantic_window_tokens=100),
        capture_semantic_policy=capture_semantic_policy,
        context_updater=_EpisodeAwareAgentDerivedUpdater(),
        context_projection=projection,
        entity_builder=_EmptyEntityBuilder(),
        relationship_extractor=_EmptyRelationships(),
        publish_committed_entity_ids=_publish_nothing,
    )
    context = JobContext(user_name=user_name, project_id=project_id)

    result = await job.execute(context)
    assert result.success
    assert result.summary == (
        "Semantic processor completed durable stages: "
        "Episode -> Context -> Knowledge -> finalization"
    )

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
    episode_rows = await postgres.fetch_all(
        """
        SELECT episode_id, summary, source_message_count
        FROM public.episodes
        WHERE project_id = %s
        """,
        (project_id,),
    )
    assert len(episode_rows) == 1
    assert episode_rows[0]["summary"] == "Both sessions asked for shared project Context."
    assert episode_rows[0]["source_message_count"] == 2
    assert await postgres.fetch_all(
        """
        SELECT message_id, session_id
        FROM public.episode_messages
        WHERE episode_id = %s
        ORDER BY message_position
        """,
        (episode_rows[0]["episode_id"],),
    ) == [
        {"message_id": 101, "session_id": session_id},
        {"message_id": 102, "session_id": "second-semantic-session"},
    ]
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM public.message_entity_occurrences WHERE project_id = %s",
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
    )
    human_edit_job = ProjectSemanticProcessor(
        SemanticWindowAdmission(
            store,
            IngestionSettings(semantic_window_tokens=100),
            token_counter=lambda text: max(1, len(text.split())),
            now_ms=lambda: 1_000_000,
        ),
        store,
        _ZeroEpisodeGenerator(),
        settings=IngestionSettings(semantic_window_tokens=100),
        capture_semantic_policy=capture_semantic_policy,
        context_updater=_AgentDerivedUpdater(),
        context_projection=recovery_projection,
        entity_builder=_UnexpectedKnowledgeBuilder(),
        relationship_extractor=_UnexpectedRelationshipExtractor(),
        publish_committed_entity_ids=_publish_nothing,
    )
    first_human_result = await human_edit_job.execute(context)
    assert first_human_result.success
    failed_state = await ProjectContextReader(postgres).get_projection_state(
        user_name=user_name,
        project_id=project_id,
    )
    assert failed_state is not None
    assert (
        failed_state.projection_failure_code
        == "ContextUserEditSynchronizationError"
    )
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


async def _insert_closed_exchange(postgres, scope, message_id, content, timestamp_ms):
    await postgres.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
            exchange_closed_at_ms
        ) VALUES (%s, %s, %s, %s, 'user', %s, %s, 'sealed', 'closed',
                  'user_only', %s)
        """,
        (scope["user_name"], scope["session_id"], message_id, scope["project_id"],
         content, timestamp_ms, timestamp_ms),
    )


def _knowledge_job(store, postgres, tmp_path, project_id, policy, *, resolver, llm,
                   user_name, publisher, now_ms=lambda: 1_000_000, context_model=None):
    entity_builder, relationship_extractor = _extraction(resolver, llm, user_name, store)
    settings = IngestionSettings(semantic_window_tokens=1)

    async def capture_semantic_policy():
        return policy

    return ProjectSemanticProcessor(
        SemanticWindowAdmission(
            store, settings,
            token_counter=lambda text: max(1, len(text.split())), now_ms=now_ms,
        ),
        store,
        _OneEpisodeGenerator(),
        settings=settings,
        capture_semantic_policy=capture_semantic_policy,
        context_updater=ContextUpdater(llm=context_model or _UserAssertedContextModel()),
        context_projection=ContextProjection(
            reader=ProjectContextReader(postgres),
            writer=ProjectContextWriter(postgres),
            filesystem=ProjectFilesystem(tmp_path / project_id),
        ),
        entity_builder=entity_builder,
        relationship_extractor=relationship_extractor,
        publish_committed_entity_ids=publisher,
        now_ms=now_ms,
    )


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_knowledge_comes_from_episode_messages_and_survives_publication_failure(
    real_server_scope,
    tmp_path,
):
    scope = real_server_scope
    postgres, user_name, project_id = (
        scope["postgres"], scope["user_name"], scope["project_id"],
    )
    await _insert_closed_exchange(postgres, scope, 201, "Sarah owns Delta.", 1_000)
    policy = IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_relationship_domain(),
    )
    store = KnowledgeStore(postgres, object())
    llm = _NamesLLM(["Sarah"], ["Delta"])
    now = [1_000_000]

    def new_job(resolver, publisher):
        return _knowledge_job(
            store, postgres, tmp_path, project_id, policy, resolver=resolver, llm=llm,
            user_name=user_name, publisher=publisher, now_ms=lambda: now[0],
        )

    context = JobContext(user_name=user_name, project_id=project_id)
    failed_publisher = _FailingCommittedEntityPublisher()
    failed = await new_job(
        EntityResolver(store, project_id, [project_id]), failed_publisher,
    ).execute(context)

    assert failed.success is False
    assert await postgres.fetch_one(
        "SELECT stage, last_failure_stage FROM public.project_semantic_windows "
        "WHERE project_id = %s",
        (project_id,),
    ) == {"stage": "knowledge_committed", "last_failure_stage": "resolver_publication"}
    names = {
        row["canonical_name"]: int(row["entity_id"])
        for row in await postgres.fetch_all(
            "SELECT entity_id, canonical_name FROM public.entities "
            "WHERE user_name = %s AND canonical_name IN ('Sarah', 'Delta')",
            (user_name,),
        )
    }
    assert sorted(failed_publisher.calls[0]) == sorted(names.values())

    now[0] = 1_030_001
    recovered_resolver = EntityResolver(store, project_id, [project_id])
    recovered = await new_job(
        recovered_resolver, recovered_resolver.publish_committed_entity_ids,
    ).execute(context)

    assert recovered.success
    # Recovery publishes the committed graph; it does not extract again.
    assert (llm.entity_calls, llm.relationship_calls) == (1, 1)
    assert await recovered_resolver.get_id("Sarah") == names["Sarah"]
    assert await postgres.fetch_all(
        "SELECT message_id, entity_id FROM public.message_entity_refs "
        "WHERE message_id = 201 ORDER BY entity_id"
    ) == [{"message_id": 201, "entity_id": e} for e in sorted(names.values())]
    observation = await postgres.fetch_one(
        "SELECT observation_id, source_entity_id, target_entity_id, observed_at_ms "
        "FROM public.relationship_observations WHERE project_id = %s",
        (project_id,),
    )
    assert (observation["source_entity_id"], observation["target_entity_id"]) == (
        names["Sarah"], names["Delta"],
    )
    assert observation["observed_at_ms"] == 1_000
    evidence = await store.get_relationship_observation_evidence(
        int(observation["observation_id"]), user_name=user_name, project_id=project_id,
    )
    assert [(n.pointer.kind, n.pointer.identifier) for n in evidence.nodes] == [
        ("relationship_observation", str(observation["observation_id"])),
        ("message", "201"),
    ]
    assert {edge.relation for edge in evidence.edges} == {"supports_relationship_observation"}
    episode = await postgres.fetch_one(
        "SELECT episode_id FROM public.episodes WHERE project_id = %s", (project_id,),
    )
    assert await postgres.fetch_all(
        "SELECT entity_id FROM public.episode_entities WHERE episode_id = %s "
        "ORDER BY entity_id",
        (episode["episode_id"],),
    ) == [{"entity_id": e} for e in sorted(names.values())]
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM public.episode_relationships WHERE episode_id = %s",
        (episode["episode_id"],),
    ) == {"count": 1}

    assert await ProjectDeletionWriter(postgres).delete_project(
        user_name=user_name, project_id=project_id,
    ) is not None
    after_deletion = await store.get_relationship_observation_evidence(
        int(observation["observation_id"]), user_name=user_name, project_id=project_id,
    )
    assert [node.status for node in after_deletion.nodes] == ["missing"]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_newer_relationship_keeps_the_older_one_as_history(
    real_server_scope,
    tmp_path,
):
    """Context replaces the old claim; the graph keeps both, each with its time."""

    scope = real_server_scope
    postgres, user_name, project_id = (
        scope["postgres"], scope["user_name"], scope["project_id"],
    )
    policy = IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_relationship_domain(),
    )
    store = KnowledgeStore(postgres, object())
    resolver = EntityResolver(store, project_id, [project_id])
    job = _knowledge_job(
        store, postgres, tmp_path, project_id, policy, resolver=resolver,
        llm=_NamesLLM(["Sarah", "John"], ["Delta"]), user_name=user_name,
        publisher=resolver.publish_committed_entity_ids,
    )
    context = JobContext(user_name=user_name, project_id=project_id)

    await _insert_closed_exchange(postgres, scope, 301, "Sarah owns Delta.", 1_000)
    assert (await job.execute(context)).success
    await _insert_closed_exchange(postgres, scope, 302, "John owns Delta.", 2_000)
    assert (await job.execute(context)).success

    # One Delta: a one-word name written like the stored one is reused.
    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE canonical_name = 'Delta'"
    ) == {"count": 1}
    rows = await postgres.fetch_all(
        """SELECT source.canonical_name AS source, observation.observed_at_ms,
                  observation.retired_at
           FROM public.relationship_observations AS observation
           JOIN public.entities AS source ON source.entity_id = observation.source_entity_id
           WHERE observation.project_id = %s ORDER BY observation.observed_at_ms""",
        (project_id,),
    )
    assert rows == [
        {"source": "Sarah", "observed_at_ms": 1_000, "retired_at": None},
        {"source": "John", "observed_at_ms": 2_000, "retired_at": None},
    ]
    delta_id = int((await postgres.fetch_one(
        "SELECT entity_id FROM public.entities WHERE user_name = %s AND canonical_name = 'Delta'",
        (user_name,),
    ))["entity_id"])
    related = await EntityReader(postgres).get_related_entities(
        [delta_id], visible_project_ids=[project_id],
    )
    assert {(row["source"], row["last_observed"]) for row in related} == {
        ("Sarah", 1_000), ("John", 2_000),
    }
    assert {
        row["source"]: [ref["message_id"] for ref in row["evidence_refs"]]
        for row in related
    } == {"Sarah": [301], "John": [302]}
    recency = {
        row["canonical_name"]: row["last_mentioned_ms"]
        for row in await postgres.fetch_all(
            """SELECT entity.canonical_name, context.last_mentioned_ms
               FROM public.project_entity_contexts AS context
               JOIN public.entities AS entity ON entity.entity_id = context.entity_id
               WHERE context.project_id = %s""",
            (project_id,),
        )
    }
    assert recency == {"Sarah": 1_000, "John": 2_000, "Delta": 2_000}


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_timeout_retry_survives_processor_restart(
    real_server_scope,
    tmp_path,
):
    """A scheduler timeout must be durable before a fresh processor resumes it."""

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
        ) VALUES (%s, %s, 401, %s, 'user', 'Persist this retry.',
                  1, 'sealed', 'closed', 'user_only', 1)
        """,
        (user_name, session_id, project_id),
    )
    domain = _domain()
    settings = IngestionSettings.model_validate(
        {
            "semantic_window_tokens": 1,
            "semantic_window_retry": {
                "max_attempts": 2,
                "initial_backoff_seconds": 30,
                "max_backoff_seconds": 30,
            },
        }
    )
    policy = IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
    )
    store = KnowledgeStore(postgres, object())

    async def capture_semantic_policy():
        return policy

    def new_job(generator, now_ms):
        return ProjectSemanticProcessor(
            SemanticWindowAdmission(
                store,
                settings,
                token_counter=lambda text: max(1, len(text.split())),
                now_ms=now_ms,
            ),
            store,
            generator,
            settings=settings,
            capture_semantic_policy=capture_semantic_policy,
            context_updater=ContextUpdater(llm=_NoopContextModel()),
            context_projection=ContextProjection(
                reader=ProjectContextReader(postgres),
                writer=ProjectContextWriter(postgres),
                filesystem=ProjectFilesystem(tmp_path / project_id),
            ),
            entity_builder=_UnexpectedKnowledgeBuilder(),
            relationship_extractor=_UnexpectedRelationshipExtractor(),
            publish_committed_entity_ids=_publish_nothing,
            now_ms=now_ms,
        )

    now = [1_000]
    blocking = _BlockingEpisodeGenerator()
    scheduler = Scheduler(user_name, project_id)
    # Window claim and CONTEXT.md sync take ~10 ms before the episode stage;
    # the blocking generator never returns, so the timeout still always fires.
    scheduler.JOB_EXECUTION_TIMEOUT = 0.5
    context = JobContext(user_name=user_name, project_id=project_id)

    await scheduler._execute_job(new_job(blocking, lambda: now[0]), context)
    timed_out = await store.get_active_project_semantic_window(
        user_name=user_name,
        project_id=project_id,
    )
    assert blocking.started.is_set()
    assert timed_out is not None
    assert timed_out.stage is SemanticWindowStage.CLAIMED
    assert timed_out.attempt_count == 1
    assert timed_out.last_failure_stage == "episode"
    assert timed_out.last_failure_code == "TimeoutError"
    assert timed_out.next_retry_at_ms == 31_000

    now[0] = 31_000
    restarted = new_job(_ZeroEpisodeGenerator(), lambda: now[0])
    completed = await restarted.execute(context)

    assert completed.success is True
    assert await store.get_active_project_semantic_window(
        user_name=user_name,
        project_id=project_id,
    ) is None
    durable = await postgres.fetch_one(
        """
        SELECT stage, attempt_count, last_failure_stage, last_failure_code,
               next_retry_at_ms
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (project_id,),
    )
    assert durable == {
        "stage": "completed",
        "attempt_count": 0,
        "last_failure_stage": None,
        "last_failure_code": None,
        "next_retry_at_ms": None,
    }


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_domain_activation_noop_bridge_completes_knowledge_and_following_window(
    real_server_scope,
    tmp_path,
):
    """Compose the version bridge and its next window through real storage."""

    scope = real_server_scope
    postgres = scope["postgres"]
    user_name = scope["user_name"]
    project_id = scope["project_id"]
    session_id = scope["session_id"]
    settings = IngestionSettings(semantic_window_tokens=1)
    store = KnowledgeStore(postgres, object())
    domain_store = DomainConfigStore(postgres)
    initial_domain = await domain_store.load(user_name, project_id)
    runtime = ProjectRuntime(
        project_id=project_id,
        entities=object(),
        knowledge_retrieval=object(),
        text_processor=SimpleNamespace(
            gliner_threshold=0.42,
            llm_ner_mode="fallback",
        ),
        scheduler=None,
        user_name=user_name,
        readable_project_ids=[project_id],
        domain_config=initial_domain,
        compiled_domain=initial_domain.compile(),
        document_service=SimpleNamespace(),
        domain_config_store=domain_store,
        config_manager=SimpleNamespace(
            config=RootConfig(developer_settings=DeveloperSettings()),
        ),
    )
    projection = ContextProjection(
        reader=ProjectContextReader(postgres),
        writer=ProjectContextWriter(postgres),
        filesystem=ProjectFilesystem(tmp_path / project_id),
    )

    def new_job():
        return ProjectSemanticProcessor(
            SemanticWindowAdmission(
                store,
                settings,
                token_counter=lambda text: max(1, len(text.split())),
                now_ms=lambda: 1_000_000,
            ),
            store,
            _ZeroEpisodeGenerator(),
            settings=settings,
            capture_semantic_policy=runtime.capture_semantic_policy,
            context_updater=ContextUpdater(llm=_NoopContextModel()),
            context_projection=projection,
            entity_builder=_UnexpectedKnowledgeBuilder(),
            relationship_extractor=_UnexpectedRelationshipExtractor(),
            publish_committed_entity_ids=_publish_nothing,
        )

    async def add_and_complete(message_id: int, content: str):
        await postgres.execute(
            """
            INSERT INTO public.messages (
                user_name, session_id, message_id, project_id, role, content,
                timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
                exchange_closed_at_ms
            ) VALUES (%s, %s, %s, %s, 'user', %s, %s,
                      'sealed', 'closed', 'user_only', %s)
            """,
            (
                user_name,
                session_id,
                message_id,
                project_id,
                content,
                message_id,
                message_id,
            ),
        )
        result = await new_job().execute(
            JobContext(user_name=user_name, project_id=project_id)
        )
        assert result.success is True

    await add_and_complete(501, "Establish the original Context version.")
    before_activation = await ProjectContextReader(postgres).get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert before_activation is not None
    assert before_activation.domain_version == initial_domain.version

    activation = await runtime.activate_domain_config(
        initial_domain.with_version(0),
        expected_version=initial_domain.version,
    )
    await add_and_complete(502, "No Context change after activation.")
    bridge = await ProjectContextReader(postgres).get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert bridge is not None
    assert bridge.revision_id != before_activation.revision_id
    assert bridge.domain_version == activation.config.version
    assert bridge.content_hash == before_activation.content_hash

    await add_and_complete(503, "The following window is also a no-op.")
    following = await ProjectContextReader(postgres).get_current_revision(
        user_name=user_name,
        project_id=project_id,
    )
    assert following is not None
    assert following.revision_id == bridge.revision_id
    assert await postgres.fetch_one(
        """
        SELECT count(*) AS total,
               count(*) FILTER (WHERE stage = 'completed') AS completed,
               count(*) FILTER (WHERE context_revision_id = %s) AS bridged
        FROM public.project_semantic_windows
        WHERE project_id = %s
        """,
        (bridge.revision_id, project_id),
    ) == {"total": 3, "completed": 3, "bridged": 2}


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_episode_extraction_reuses_a_readable_foreign_identity(
    real_server_scope,
    tmp_path,
):
    """A name another readable project already knows resolves to that entity."""

    scope = real_server_scope
    postgres, user_name, project_id = (
        scope["postgres"], scope["user_name"], scope["project_id"],
    )
    shared_project_id = f"{project_id}-shared"
    policy = IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_relationship_domain(),
    )
    store = KnowledgeStore(postgres, object())
    await postgres.execute(
        """INSERT INTO public.projects (project_id, user_name, name, domain_config)
           VALUES (%s, %s, 'Shared identity source', %s::jsonb)""",
        (shared_project_id, user_name, "{}"),
    )
    await postgres.execute(
        """INSERT INTO public.project_read_scopes
           (user_name, project_id, readable_project_id) VALUES (%s, %s, %s)""",
        (user_name, project_id, shared_project_id),
    )
    target_id = await store.allocate_entity_id()
    homonym_id = await store.allocate_entity_id()
    await postgres.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (%s, %s, 'Avery Stone'), (%s, %s, 'Avery Quinn')""",
        (target_id, user_name, homonym_id, user_name),
    )
    await postgres.execute(
        "INSERT INTO public.entity_aliases (entity_id, alias) VALUES (%s, 'Avery'), (%s, 'Avery')",
        (target_id, homonym_id),
    )
    await postgres.execute(
        """INSERT INTO public.project_entity_contexts
           (project_id, entity_id, user_name, entity_type, topic)
           VALUES (%s, %s, %s, 'Person', 'Work'), (%s, %s, %s, 'Person', 'Work')""",
        (shared_project_id, target_id, user_name, shared_project_id, homonym_id, user_name),
    )
    resolver = EntityResolver(store, project_id, [project_id, shared_project_id])
    llm = _NamesLLM(["Avery Stone"], ["Delta Corp"])
    job = _knowledge_job(
        store, postgres, tmp_path, project_id, policy, resolver=resolver, llm=llm,
        user_name=user_name, publisher=resolver.publish_committed_entity_ids,
    )
    context = JobContext(user_name=user_name, project_id=project_id)

    await _insert_closed_exchange(postgres, scope, 401, "Avery Stone owns Delta Corp.", 1_000)
    assert (await job.execute(context)).success
    await _insert_closed_exchange(
        postgres, scope, 402, "Avery Stone continues to own Delta Corp.", 2_000,
    )
    assert (await job.execute(context)).success

    assert await postgres.fetch_one(
        "SELECT count(*) AS count FROM public.entities "
        "WHERE user_name = %s AND canonical_name = 'Avery Stone'",
        (user_name,),
    ) == {"count": 1}
    assert await postgres.fetch_one(
        "SELECT entity_type, topic, last_mentioned_ms FROM public.project_entity_contexts "
        "WHERE project_id = %s AND entity_id = %s",
        (project_id, target_id),
    ) == {"entity_type": "Person", "topic": "Work", "last_mentioned_ms": 2_000}
    observations = await postgres.fetch_all(
        "SELECT source_entity_id, observed_at_ms, retired_at "
        "FROM public.relationship_observations WHERE project_id = %s "
        "ORDER BY observed_at_ms",
        (project_id,),
    )
    assert observations == [
        {"source_entity_id": target_id, "observed_at_ms": 1_000, "retired_at": None},
        {"source_entity_id": target_id, "observed_at_ms": 2_000, "retired_at": None},
    ]
    assert (llm.entity_calls, llm.relationship_calls) == (2, 2)
    candidates = {
        candidate.entity_id: candidate
        for candidate in await EntityResolver(
            store, project_id, [project_id, shared_project_id],
        ).get_candidate_ids("Avery")
    }
    assert {target_id, homonym_id} <= set(candidates)
    assert "ambiguous_alias" in candidates[target_id].signals
