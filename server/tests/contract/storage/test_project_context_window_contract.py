import asyncio
import hashlib
from uuid import uuid4

import pytest
from psycopg.errors import CheckViolation, ForeignKeyViolation, UniqueViolation

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextRevisionOrigin,
    ContextSupportKind,
)
from common.schema.episode.models import Episode, MessageEpisode
from common.schema.semantic_window import (
    SemanticWindowMessage,
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.policy import IngestionPolicy
from core.knowledge.context.models import (
    ContextBlockSupport,
    ContextMaterialization,
    ContextProjectionConflictError,
    ContextRevisionConflictError,
)
from core.knowledge.context.projection import ContextProjection
from core.knowledge.context.render import context_block_hash, context_document_hash
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter
from core.knowledge.documents.filesystem import ProjectFilesystem
from core.knowledge.store import KnowledgeStore

_HASH = "a" * 64


def _domain():
    return DomainConfig(version=1, topics=(), entity_types=()).compile()


def _context_block(markdown: str, *, section_key: str = "current_state") -> ContextBlockRecord:
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key=section_key,
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=AssertionKind.AGENT_DERIVED,
    )


def _materialization(*blocks: ContextBlockRecord, supports=()) -> ContextMaterialization:
    return ContextMaterialization(
        blocks=blocks,
        content_hash=context_document_hash(blocks, _domain()),
        new_block_ids=frozenset(block.block_id for block in blocks),
        supports=tuple(supports),
    )


def _ingestion_policy() -> IngestionPolicy:
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(gliner_threshold=0.42, llm_ner=False),
        entity_resolution=EntityResolutionSettings(resolution_threshold=0.71),
        compiled_domain=_domain(),
    )


async def _seed_messages(client) -> None:
    await client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES
            ('session-1', 'ada', 'project-1'),
            ('session-2', 'ada', 'project-2');
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES
            ('ada', 'session-1', 101, 'project-1', 'user', 'First project message',
             'sealed', 'closed', 'user_only', 101),
            ('ada', 'session-2', 201, 'project-2', 'user', 'Second project message',
             'sealed', 'closed', 'user_only', 201);
        """
    )


def _window(*, project_id: str = "project-1") -> SemanticWindowRecord:
    return SemanticWindowRecord(
        window_id=uuid4(),
        user_name="ada",
        project_id=project_id,
        origin=SemanticWindowOrigin.CONVERSATION,
        stage=SemanticWindowStage.CLAIMED,
        domain_version=1,
        policy_snapshot={"semantic_window_tokens": 128_000},
        source_token_count=100,
        token_estimator="test-counter",
        token_estimator_version="1",
    )


def _membership(message_id: int = 101, session_id: str = "session-1") -> list[SemanticWindowMessage]:
    return [
        SemanticWindowMessage(
            message_id=message_id,
            session_id=session_id,
            exchange_user_message_id=message_id,
            role="user",
            ordinal=0,
        )
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_semantic_window_claim_is_atomic_and_membership_reloads_exactly(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    writer = SemanticWindowWriter(real_postgres_client)
    store = KnowledgeStore(real_postgres_client, object())
    proposed = _window()

    claimed = await store.claim_project_semantic_window(proposed, _membership())
    with pytest.raises(UniqueViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO public.project_semantic_windows (
                window_id, user_name, project_id, origin, stage, domain_version,
                policy_snapshot, source_token_count, token_estimator,
                token_estimator_version
            ) VALUES (
                %s, 'ada', 'project-1', 'conversation', 'claimed', 1,
                '{}'::jsonb, 0, 'test-counter', '1'
            )
            """,
            (uuid4(),),
        )
    duplicate = await writer.claim_window(_window(), _membership())

    assert claimed.claimed is True
    assert duplicate.claimed is False
    assert duplicate.window.window_id == proposed.window_id
    reloaded = await store.get_project_semantic_window(
        proposed.window_id,
        user_name="ada",
        project_id="project-1",
    )
    assert reloaded is not None
    assert reloaded.domain_version == proposed.domain_version
    assert reloaded.policy_snapshot == proposed.policy_snapshot
    assert await store.get_project_semantic_window_messages(
        proposed.window_id,
        user_name="ada",
        project_id="project-1",
    ) == _membership()

    await real_postgres_client.execute(
        """
        UPDATE public.project_semantic_windows
        SET stage = 'completed', completed_at = NOW()
        WHERE window_id = %s
        """,
        (proposed.window_id,),
    )
    second_window = _window()
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version
        ) VALUES (
            %s, 'ada', 'project-1', 'conversation', 'claimed', 1,
            '{}'::jsonb, 0, 'test-counter', '1'
        )
        """,
        (second_window.window_id,),
    )
    with pytest.raises(UniqueViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO public.project_semantic_window_messages (
                window_id, project_id, message_id, session_id,
                exchange_user_message_id, role, ordinal
            ) VALUES (%s, 'project-1', 101, 'session-1', 101, 'user', 1)
            """,
            (second_window.window_id,),
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_concurrent_semantic_window_claimers_return_one_project_owner(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    writer = SemanticWindowWriter(real_postgres_client)
    first, second = await asyncio.gather(
        writer.claim_window(_window(), _membership()),
        writer.claim_window(_window(), _membership()),
    )

    assert sum(result.claimed for result in (first, second)) == 1
    assert first.window.window_id == second.window.window_id
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_semantic_window_messages
        WHERE project_id = 'project-1' AND message_id = 101
        """
    ) == {"count": 1}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_source_catalog_reads_only_frozen_assistant_owned_refs(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            user_msg_id, lifecycle_state
        ) VALUES (
            'ada', 'session-1', 102, 'project-1', 'assistant',
            'The supplied source confirms the constraint.', 101, 'sealed'
        )
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.message_source_refs (
            source_ref_id, project_id, session_id, message_id, source_kind,
            source_message_id, content_hash, locator, excerpt, metadata,
            encounter_kind, agent_run_id, result_position, idempotency_key
        ) VALUES (
            %s, 'project-1', 'session-1', 102, 'user_pasted_text', 101,
            %s, '{"kind":"character_span","start_char":0,"end_char":8}',
            'source excerpt', '{}'::jsonb, 'user_pasted_text', 'run-1', 0,
            'context-source-catalog'
        )
        """,
        (uuid4(), _HASH),
    )
    window = _window()
    members = _membership() + [
        SemanticWindowMessage(
            message_id=102,
            session_id="session-1",
            exchange_user_message_id=101,
            role="assistant",
            ordinal=1,
        )
    ]
    assert (
        await SemanticWindowWriter(real_postgres_client).claim_window(window, members)
    ).claimed

    refs = await SemanticWindowReader(
        real_postgres_client
    ).get_window_assistant_source_refs(
        window.window_id,
        user_name="ada",
        project_id="project-1",
    )

    assert len(refs) == 1
    assert refs[0]["message_id"] == 102
    assert refs[0]["session_id"] == "session-1"
    assert refs[0]["excerpt"] == "source excerpt"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_episode_result_is_idempotent_and_has_no_legacy_side_effects(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    store = KnowledgeStore(real_postgres_client, object())
    window = _window()
    assert (await store.claim_project_semantic_window(window, _membership())).claimed
    messages = await store.get_project_semantic_window_evidence_messages(
        str(window.window_id),
        user_name="ada",
        project_id="project-1",
    )
    episode = Episode(
        episode_id="semantic-window-episode",
        project_id="project-1",
        summary="The semantic episode result is durable.",
        messages=[
            MessageEpisode(
                message_id=101,
                session_id="session-1",
                message_position=0,
            )
        ],
        generator_metadata={"decision_action": "create"},
    )

    assert await store.get_project_semantic_window_episode_result(
        str(window.window_id),
        user_name="ada",
        project_id="project-1",
    ) is None
    assert await store.write_project_semantic_window_episodes(
        window_id=str(window.window_id),
        episodes=[episode],
        window_messages=messages,
        user_name="ada",
        project_id="project-1",
    )
    assert not await store.write_project_semantic_window_episodes(
        window_id=str(window.window_id),
        episodes=[episode],
        window_messages=messages,
        user_name="ada",
        project_id="project-1",
    )
    result = await store.get_project_semantic_window_episode_result(
        str(window.window_id),
        user_name="ada",
        project_id="project-1",
    )
    assert result is not None
    assert [item.episode_id for item in result] == [episode.episode_id]
    assert result[0].entities == []
    assert result[0].relationships == []
    assert await real_postgres_client.fetch_one(
        "SELECT stage, episode_result_recorded FROM project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "claimed", "episode_result_recorded": True}

    zero_window = _window(project_id="project-2")
    assert (
        await store.claim_project_semantic_window(
            zero_window,
            _membership(message_id=201, session_id="session-2"),
        )
    ).claimed
    zero_messages = await store.get_project_semantic_window_evidence_messages(
        str(zero_window.window_id),
        user_name="ada",
        project_id="project-2",
    )
    assert await store.write_project_semantic_window_episodes(
        window_id=str(zero_window.window_id),
        episodes=[],
        window_messages=zero_messages,
        user_name="ada",
        project_id="project-2",
    )
    assert await store.get_project_semantic_window_episode_result(
        str(zero_window.window_id),
        user_name="ada",
        project_id="project-2",
    ) == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_episode_consolidation_keeps_complete_canonical_membership(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES (
            'ada', 'session-1', 102, 'project-1', 'user', 'Later project message',
            'sealed', 'closed', 'user_only', 102
        );
        INSERT INTO public.episodes (episode_id, project_id, summary)
        VALUES ('prior-semantic-episode', 'project-1', 'Earlier project memory');
        INSERT INTO public.episode_messages (
            episode_id, project_id, session_id, message_id, message_position
        ) VALUES ('prior-semantic-episode', 'project-1', 'session-1', 101, 0);
        """
    )
    store = KnowledgeStore(real_postgres_client, object())
    window = _window()
    assert (
        await store.claim_project_semantic_window(
            window,
            _membership(message_id=102),
        )
    ).claimed
    messages = await store.get_project_semantic_window_evidence_messages(
        str(window.window_id),
        user_name="ada",
        project_id="project-1",
    )
    consolidated = Episode(
        episode_id="prior-semantic-episode",
        project_id="project-1",
        summary="Earlier and later project memory are one episode.",
        messages=[
            MessageEpisode(message_id=101, session_id="session-1", message_position=0),
            MessageEpisode(message_id=102, session_id="session-1", message_position=1),
        ],
        generator_metadata={"decision_action": "consolidate"},
    )

    assert await store.write_project_semantic_window_episodes(
        window_id=str(window.window_id),
        episodes=[consolidated],
        window_messages=messages,
        user_name="ada",
        project_id="project-1",
    )
    result = await store.get_project_semantic_window_episode_result(
        str(window.window_id),
        user_name="ada",
        project_id="project-1",
    )
    assert result is not None
    assert [message.message_id for message in result[0].messages] == [101, 102]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_window_stage_cas_and_failures_keep_the_last_successful_stage(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    context_writer = ProjectContextWriter(real_postgres_client)
    window_writer = SemanticWindowWriter(real_postgres_client)
    window_reader = SemanticWindowReader(real_postgres_client)
    store = KnowledgeStore(real_postgres_client, object())
    await context_writer.ensure_context(user_name="ada", project_id="project-1")
    window = _window()
    assert (await window_writer.claim_window(window, _membership())).claimed
    revision_id = uuid4()
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_revisions (
            revision_id, project_id, revision_number, window_id, origin,
            domain_version, content_hash
        ) VALUES (%s, 'project-1', 1, %s, 'conversation', 1, %s)
        """,
        (revision_id, window.window_id, _HASH),
    )

    assert not await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        next_stage=SemanticWindowStage.KNOWLEDGE_COMMITTED,
    )
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=revision_id,
    )
    failed = await window_writer.record_failure(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        failure_stage="knowledge_reconciliation",
        failure_code="temporary_failure",
        error_summary="retry later",
        failed_at_ms=1_000,
        next_retry_at_ms=None,
    )

    assert failed is not None
    assert failed.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert failed.attempt_count == 1
    assert failed.last_failure_code == "temporary_failure"
    assert (
        await window_reader.get_window(
            window.window_id,
            user_name="ada",
            project_id="project-1",
        )
    ).stage is SemanticWindowStage.CONTEXT_COMMITTED
    exhausted_health = await store.get_semantic_window_health(
        user_name="ada",
        project_id="project-1",
    )
    assert exhausted_health["failed_count"] == 1
    assert exhausted_health["exhausted_count"] == 1

    retried = await window_writer.retry_window(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
    )

    assert retried is not None
    assert retried.window_id == window.window_id
    assert retried.stage is SemanticWindowStage.CONTEXT_COMMITTED
    assert retried.context_revision_id == revision_id
    assert retried.attempt_count == 0
    assert retried.last_failure_at_ms is None
    assert retried.next_retry_at_ms is None
    retried_health = await store.get_semantic_window_health(
        user_name="ada",
        project_id="project-1",
    )
    assert retried_health["pending_count"] == 1
    assert retried_health["claimed_count"] == 0
    assert retried_health["failed_count"] == 0
    assert retried_health["exhausted_count"] == 0
    assert retried_health["oldest_pending_ms"] is not None
    assert retried_health["last_processed_ms"] is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_scope_snapshot_and_project_deletion_cascade(real_postgres_client):
    await _seed_messages(real_postgres_client)
    context_writer = ProjectContextWriter(real_postgres_client)
    context_reader = ProjectContextReader(real_postgres_client)
    window_writer = SemanticWindowWriter(real_postgres_client)
    await context_writer.ensure_context(user_name="ada", project_id="project-1")
    revision_id = uuid4()
    block_id = uuid4()
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_revisions (
            revision_id, project_id, revision_number, origin, domain_version,
            content_hash
        ) VALUES (%s, 'project-1', 1, 'conversation', 1, %s)
        """,
        (revision_id, _HASH),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_blocks (
            block_id, project_id, section_key, markdown, content_hash,
            assertion_kind
        ) VALUES (
            %s, 'project-1', 'current_state', 'Knoggin is being refactored.',
            %s, 'user_asserted'
        )
        """,
        (block_id, _HASH),
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_revision_blocks (
            revision_id, project_id, block_id, ordinal
        ) VALUES (%s, 'project-1', %s, 0)
        """,
        (revision_id, block_id),
    )
    await real_postgres_client.execute(
        """
        UPDATE public.project_contexts
        SET current_revision_id = %s
        WHERE project_id = 'project-1' AND user_name = 'ada'
        """,
        (revision_id,),
    )

    snapshot = await context_reader.get_snapshot(
        revision_id,
        user_name="ada",
        project_id="project-1",
    )
    assert snapshot is not None
    assert snapshot.blocks[0].block_id == block_id

    project_two_revision_id = uuid4()
    await context_writer.ensure_context(user_name="ada", project_id="project-2")
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_context_revisions (
            revision_id, project_id, revision_number, origin, domain_version,
            content_hash
        ) VALUES (%s, 'project-2', 1, 'conversation', 1, %s)
        """,
        (project_two_revision_id, _HASH),
    )
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            UPDATE public.project_contexts
            SET current_revision_id = %s
            WHERE project_id = 'project-1' AND user_name = 'ada'
            """,
            (project_two_revision_id,),
        )
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            UPDATE public.project_contexts
            SET projection_revision_id = %s
            WHERE project_id = 'project-1' AND user_name = 'ada'
            """,
            (project_two_revision_id,),
        )
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            UPDATE public.project_contexts
            SET projection_pending_revision_id = %s
            WHERE project_id = 'project-1' AND user_name = 'ada'
            """,
            (project_two_revision_id,),
        )
    with pytest.raises(ForeignKeyViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO public.project_context_revision_blocks (
                revision_id, project_id, block_id, ordinal
            ) VALUES (%s, 'project-2', %s, 0)
            """,
            (project_two_revision_id, block_id),
        )
    with pytest.raises(UniqueViolation):
        await real_postgres_client.execute(
            """
            INSERT INTO public.project_context_block_supports (
                block_id, project_id, message_id, session_id, support_kind
            ) VALUES (%s, 'project-1', 101, 'session-1', 'user_message')
            """,
            (block_id,),
        )
        await real_postgres_client.execute(
            """
            INSERT INTO public.project_context_block_supports (
                block_id, project_id, message_id, session_id, support_kind
            ) VALUES (%s, 'project-1', 101, 'session-1', 'user_message')
            """,
            (block_id,),
        )

    claimed_window = _window()
    assert (
        await window_writer.claim_window(claimed_window, _membership())
    ).claimed is True
    assert await ProjectDeletionWriter(real_postgres_client).delete_project(
        user_name="ada", project_id="project-1"
    ) is not None
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_contexts WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_context_revisions WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_context_blocks WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_context_block_supports "
        "WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_semantic_windows WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_semantic_window_messages "
        "WHERE project_id = 'project-1'"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_revision_writer_serializes_children_and_rejects_invalid_supports(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    writer = ProjectContextWriter(real_postgres_client)
    reader = ProjectContextReader(real_postgres_client)
    await writer.ensure_context(user_name="ada", project_id="project-1")
    first_block = _context_block("Initial Context.")
    first = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Initial Context",
        materialization=_materialization(first_block),
    )

    async def commit_child(markdown: str):
        block = _context_block(markdown)
        return await writer.commit_revision(
            user_name="ada",
            project_id="project-1",
            expected_parent_revision_id=first.revision_id,
            window_id=None,
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=1,
            edit_summary="Competing child",
            materialization=ContextMaterialization(
                blocks=(first_block, block),
                content_hash=context_document_hash((first_block, block), _domain()),
                new_block_ids=frozenset({block.block_id}),
            ),
        )

    first_child, second_child = await asyncio.gather(
        commit_child("First child."),
        commit_child("Second child."),
        return_exceptions=True,
    )
    committed_children = [
        result
        for result in (first_child, second_child)
        if not isinstance(result, BaseException)
    ]
    conflicts = [
        result
        for result in (first_child, second_child)
        if isinstance(result, ContextRevisionConflictError)
    ]
    assert len(committed_children) == 1
    assert len(conflicts) == 1
    current = await reader.get_current_revision(user_name="ada", project_id="project-1")
    assert current is not None
    assert current.revision_id == committed_children[0].revision_id
    assert current.revision_number == 2

    window_writer = SemanticWindowWriter(real_postgres_client)
    window = _window()
    assert (await window_writer.claim_window(window, _membership())).claimed
    unsupported = _context_block("Unsupported evidence.")
    with pytest.raises(ValueError, match="outside this project"):
        await writer.commit_revision(
            user_name="ada",
            project_id="project-1",
            expected_parent_revision_id=current.revision_id,
            window_id=window.window_id,
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=1,
            edit_summary="Rejected support",
            materialization=ContextMaterialization(
                blocks=(*committed_children[0].blocks, unsupported),
                content_hash=context_document_hash(
                    (*committed_children[0].blocks, unsupported), _domain()
                ),
                new_block_ids=frozenset({unsupported.block_id}),
                supports=(
                    ContextBlockSupport(
                        block_id=unsupported.block_id,
                        message_id=201,
                        session_id="session-2",
                        support_kind="user_message",
                    ),
                ),
            ),
        )
    assert (
        await reader.get_current_revision(user_name="ada", project_id="project-1")
    ).revision_id == current.revision_id


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_stale_projection_checkpoint_keeps_the_newest_written_revision(
    real_postgres_client,
):
    """An older racing projection cannot hide a newer safely-stale file."""

    await _seed_messages(real_postgres_client)
    writer = ProjectContextWriter(real_postgres_client)
    reader = ProjectContextReader(real_postgres_client)
    initial_block = _context_block("Initial Context.")
    initial = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Initial Context",
        materialization=_materialization(initial_block),
    )
    assert await writer.record_projection(
        user_name="ada",
        project_id="project-1",
        revision_id=initial.revision_id,
        projection_hash="1" * 64,
    )

    async def commit_child(parent, markdown: str):
        block = _context_block(markdown)
        return await writer.commit_revision(
            user_name="ada",
            project_id="project-1",
            expected_parent_revision_id=parent.revision_id,
            window_id=None,
            origin=ContextRevisionOrigin.CONVERSATION,
            domain_version=1,
            edit_summary="Later Context",
            materialization=ContextMaterialization(
                blocks=(*parent.blocks, block),
                content_hash=context_document_hash((*parent.blocks, block), _domain()),
                new_block_ids=frozenset({block.block_id}),
            ),
        )

    second = await commit_child(initial, "Second Context.")
    third = await commit_child(second, "Third Context.")
    await commit_child(third, "Fourth Context.")

    assert await writer.record_stale_projection(
        user_name="ada",
        project_id="project-1",
        revision_id=second.revision_id,
        projection_hash="2" * 64,
    )
    assert await writer.record_stale_projection(
        user_name="ada",
        project_id="project-1",
        revision_id=third.revision_id,
        projection_hash="3" * 64,
    )
    assert await writer.record_stale_projection(
        user_name="ada",
        project_id="project-1",
        revision_id=second.revision_id,
        projection_hash="2" * 64,
    )

    state = await reader.get_projection_state(user_name="ada", project_id="project-1")
    assert state is not None
    assert state.projection_pending_revision_id == third.revision_id
    assert state.projection_pending_hash == "3" * 64


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_revision_persists_impact_closure_and_typed_support_reads(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    writer = ProjectContextWriter(real_postgres_client)
    reader = ProjectContextReader(real_postgres_client)
    window = _window()
    assert (
        await SemanticWindowWriter(real_postgres_client).claim_window(
            window,
            _membership(),
        )
    ).claimed
    tracked = _context_block("The selected provider is grounded in this message.")
    committed = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=window.window_id,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Persist impact evidence",
        materialization=ContextMaterialization(
            blocks=(tracked,),
            content_hash=context_document_hash((tracked,), _domain()),
            new_block_ids=frozenset({tracked.block_id}),
            impacted_block_ids=frozenset({tracked.block_id}),
            supports=(
                ContextBlockSupport(
                    block_id=tracked.block_id,
                    message_id=101,
                    session_id="session-1",
                    support_kind=ContextSupportKind.USER_MESSAGE,
                ),
            ),
        ),
    )

    assert await reader.get_revision_impact_block_ids(
        committed.revision_id,
        user_name="ada",
        project_id="project-1",
    ) == {tracked.block_id}
    supports = await reader.get_block_supports(
        [tracked.block_id],
        user_name="ada",
        project_id="project-1",
    )
    assert [(item.message_id, item.support_kind) for item in supports[tracked.block_id]] == [
        (101, ContextSupportKind.USER_MESSAGE)
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_revision_retry_and_projection_import_are_durable(
    real_postgres_client,
    tmp_path,
):
    await _seed_messages(real_postgres_client)
    writer = ProjectContextWriter(real_postgres_client)
    reader = ProjectContextReader(real_postgres_client)
    await writer.ensure_context(user_name="ada", project_id="project-1")
    initial_block = _context_block("Initial Context.")
    await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Initial Context",
        materialization=_materialization(initial_block),
    )
    filesystem = ProjectFilesystem(tmp_path / "project-1")
    projection = ContextProjection(
        reader=reader,
        writer=writer,
        filesystem=filesystem,
        capture_ingestion_policy=_ingestion_policy,
    )
    projected = await projection.reconcile(
        user_name="ada", project_id="project-1", domain=_domain()
    )
    assert projected.changed
    original_file = filesystem.read_bytes("CONTEXT.md")
    original_hash = hashlib.sha256(original_file).hexdigest()

    edited_file = original_file.replace(b"Initial Context.", b"Edited by a human.")
    filesystem.write_bytes(
        "CONTEXT.md",
        edited_file,
        overwrite=True,
        expected_content_hash=original_hash,
    )
    imported = await projection.import_user_edit(
        user_name="ada", project_id="project-1", domain=_domain()
    )
    assert imported.changed
    assert imported.snapshot is not None
    assert imported.snapshot.revision_number == 2
    assert imported.reconciliation_window_id is not None
    assert imported.snapshot.blocks[0].assertion_kind is AssertionKind.HUMAN_ASSERTED
    assert imported.snapshot.blocks[0].supersedes_block_id == initial_block.block_id
    assert await real_postgres_client.fetch_one(
        """
        SELECT origin, stage, context_revision_id
        FROM public.project_semantic_windows
        WHERE window_id = %s
        """,
        (imported.reconciliation_window_id,),
    ) == {
        "origin": "human_edit",
        "stage": "context_committed",
        "context_revision_id": imported.snapshot.revision_id,
    }
    policy_row = await real_postgres_client.fetch_one(
        """
        SELECT policy_snapshot
        FROM public.project_semantic_windows
        WHERE window_id = %s
        """,
        (imported.reconciliation_window_id,),
    )
    assert policy_row["policy_snapshot"]["ingestion_policy"]["gliner_threshold"] == 0.42

    stale_block = _context_block("Database moved ahead.")
    stale = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=imported.snapshot.revision_id,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Later database update",
        materialization=ContextMaterialization(
            blocks=(imported.snapshot.blocks[0], stale_block),
            content_hash=context_document_hash(
                (imported.snapshot.blocks[0], stale_block), _domain()
            ),
            new_block_ids=frozenset({stale_block.block_id}),
        ),
    )
    file_before_stale_import = filesystem.read_bytes("CONTEXT.md")
    filesystem.write_bytes(
        "CONTEXT.md",
        file_before_stale_import.replace(b"Edited by a human.", b"Stale local edit."),
        overwrite=True,
        expected_content_hash=hashlib.sha256(file_before_stale_import).hexdigest(),
    )
    with pytest.raises(ContextProjectionConflictError, match="stale"):
        await projection.import_user_edit(
            user_name="ada", project_id="project-1", domain=_domain()
        )
    assert (
        await reader.get_current_revision(user_name="ada", project_id="project-1")
    ).revision_id == stale.revision_id


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_revision_retry_by_window_returns_the_existing_snapshot(
    real_postgres_client,
):
    await _seed_messages(real_postgres_client)
    writer = ProjectContextWriter(real_postgres_client)
    window_writer = SemanticWindowWriter(real_postgres_client)
    await writer.ensure_context(user_name="ada", project_id="project-1")
    initial_block = _context_block("Initial Context.")
    initial = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Initial Context",
        materialization=_materialization(initial_block),
    )
    window = _window()
    assert (await window_writer.claim_window(window, _membership())).claimed
    new_block = _context_block("Window update.")
    materialization = ContextMaterialization(
        blocks=(initial_block, new_block),
        content_hash=context_document_hash((initial_block, new_block), _domain()),
        new_block_ids=frozenset({new_block.block_id}),
    )
    committed = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=initial.revision_id,
        window_id=window.window_id,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Window update",
        materialization=materialization,
    )
    retried = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=window.window_id,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Ignored retry payload",
        materialization=materialization,
    )

    assert retried.revision_id == committed.revision_id
    resumed = await ProjectContextReader(real_postgres_client).get_window_snapshot(
        window.window_id,
        user_name="ada",
        project_id="project-1",
    )
    assert resumed is not None
    assert resumed.revision_id == committed.revision_id
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_context_revisions WHERE project_id = 'project-1'"
    ) == {"count": 2}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_projection_write_failure_keeps_the_committed_context_revision(
    real_postgres_client,
    tmp_path,
):
    await _seed_messages(real_postgres_client)
    writer = ProjectContextWriter(real_postgres_client)
    reader = ProjectContextReader(real_postgres_client)
    initial_block = _context_block("Initial Context.")
    initial = await writer.commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Initial Context",
        materialization=_materialization(initial_block),
    )
    filesystem = ProjectFilesystem(tmp_path / "project-1")
    projection = ContextProjection(reader=reader, writer=writer, filesystem=filesystem)
    await projection.reconcile(user_name="ada", project_id="project-1", domain=_domain())
    previous_file = filesystem.read_bytes("CONTEXT.md")
    filesystem.write_bytes(
        "CONTEXT.md",
        previous_file.replace(b"Initial Context.", b"Human edit."),
        overwrite=True,
        expected_content_hash=hashlib.sha256(previous_file).hexdigest(),
    )

    class FailingFilesystem:
        def read_bytes(self, *args, **kwargs):
            return filesystem.read_bytes(*args, **kwargs)

        def write_bytes(self, *args, **kwargs):
            raise OSError("disk unavailable")

    failed_projection = ContextProjection(
        reader=reader,
        writer=writer,
        filesystem=FailingFilesystem(),
    )
    with pytest.raises(OSError, match="disk unavailable"):
        await failed_projection.synchronize(
            user_name="ada",
            project_id="project-1",
            domain=_domain(),
            allow_user_edit=True,
        )
    current = await reader.get_current_revision(user_name="ada", project_id="project-1")
    assert current is not None
    assert current.revision_number == 2
    assert current.parent_revision_id == initial.revision_id
    assert filesystem.read_bytes("CONTEXT.md") != previous_file
    failed_state = await reader.get_projection_state(user_name="ada", project_id="project-1")
    assert failed_state is not None
    assert failed_state.projection_failure_code == "OSError"
    assert failed_state.projection_pending_hash == hashlib.sha256(
        filesystem.read_bytes("CONTEXT.md")
    ).hexdigest()

    repaired = await ContextProjection(
        reader=reader,
        writer=writer,
        filesystem=filesystem,
    ).synchronize(
        user_name="ada",
        project_id="project-1",
        domain=_domain(),
        allow_user_edit=False,
    )
    assert repaired.changed
    repaired_state = await reader.get_projection_state(
        user_name="ada", project_id="project-1"
    )
    assert repaired_state is not None
    assert repaired_state.projection_revision_id == current.revision_id
    assert repaired_state.projection_pending_hash is None
    assert repaired_state.projection_failure_code is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_exchange_columns_enforce_user_only_closure_shape(real_postgres_client):
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES (
            'ada', 'session-1', 101, 'project-1', 'user', 'Closed manually',
            'closed', 'user_only', 1
        )
        """
    )
    with pytest.raises(CheckViolation, match="messages_exchange_user_shape_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO public.messages (
                user_name, session_id, message_id, project_id, role, content,
                exchange_state
            ) VALUES (
                'ada', 'session-1', 102, 'project-1', 'user', 'Bad closure',
                'closed'
            )
            """
        )
    with pytest.raises(CheckViolation, match="messages_exchange_user_shape_check"):
        await real_postgres_client.execute(
            """
            INSERT INTO public.messages (
                user_name, session_id, message_id, project_id, role, content,
                exchange_state, exchange_outcome, exchange_closed_at_ms
            ) VALUES (
                'ada', 'session-1', 103, 'project-1', 'assistant', 'Bad assistant',
                'closed', 'assistant_final', 2
            )
            """
        )
