import pytest

from common.conf.domain_config import DomainConfig
from common.schema.settings import IngestionSettings
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.store import KnowledgeStore


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_semantic_admission_filters_participation_before_session_fifo(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (
            session_id, user_name, project_id, status,
            semantic_participation_enabled,
            semantic_participation_after_message_id
        ) VALUES
            ('session-pre-frontier', 'ada', 'project-1', 'open', TRUE, 201),
            ('session-disabled', 'ada', 'project-1', 'open', FALSE, 0),
            ('session-deleted', 'ada', 'project-1', 'deleted', TRUE, 0),
            ('session-ready', 'ada', 'project-1', 'open', TRUE, 0),
            ('session-blocked', 'ada', 'project-1', 'open', TRUE, 0)
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES
            ('ada', 'session-pre-frontier', 201, 'project-1', 'user', 'Old.',
             'sealed', 'closed', 'user_only', 201),
            ('ada', 'session-pre-frontier', 202, 'project-1', 'user', 'New.',
             'sealed', 'closed', 'user_only', 202),
            ('ada', 'session-disabled', 203, 'project-1', 'user', 'Disabled.',
             'sealed', 'closed', 'user_only', 203),
            ('ada', 'session-deleted', 204, 'project-1', 'user', 'Deleted.',
             'sealed', 'closed', 'user_only', 204),
            ('ada', 'session-ready', 205, 'project-1', 'user', 'Ready.',
             'sealed', 'closed', 'user_only', 205),
            ('ada', 'session-blocked', 206, 'project-1', 'user', 'Editable.',
             'editable', 'open', NULL, NULL),
            ('ada', 'session-blocked', 207, 'project-1', 'user', 'Later.',
             'sealed', 'closed', 'user_only', 207)
        """
    )

    rows = await SemanticWindowReader(
        real_postgres_client
    ).get_unclaimed_project_exchange_rows(user_name="ada", project_id="project-1")

    assert {row["user_message_id"] for row in rows} == {202, 205, 206, 207}
    assert all("session_status" not in row for row in rows)

    admission = SemanticWindowAdmission(
        KnowledgeStore(real_postgres_client, object()),
        IngestionSettings(semantic_window_tokens=100),
        token_counter=lambda _text: 1,
    )
    selected = await admission.select(
        user_name="ada",
        project_id="project-1",
        domain=DomainConfig(version=1, topics=(), entity_types=()).compile(),
        force_flush=True,
    )

    assert selected is not None
    assert [member.message_id for member in selected.messages] == [202, 205]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_clarification_admission_preserves_its_unresolved_outcome_in_evidence(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-clarification', 'ada', 'project-1');
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            user_msg_id, timestamp_ms, lifecycle_state, exchange_state,
            exchange_outcome, exchange_closed_at_ms
        ) VALUES
            ('ada', 'session-clarification', 301, 'project-1', 'user',
             'I need help choosing a deployment profile.', 301, 301,
             'sealed', 'closed', 'clarification', 302),
            ('ada', 'session-clarification', 302, 'project-1', 'assistant',
             'Which deployment environment should I use?', 301, 302,
             'sealed', 'open', NULL, NULL)
        """
    )
    store = KnowledgeStore(real_postgres_client, object())
    admission = SemanticWindowAdmission(
        store,
        IngestionSettings(semantic_window_tokens=100),
        token_counter=lambda _text: 1,
    )

    claim = await admission.claim_next(
        user_name="ada",
        project_id="project-1",
        domain=DomainConfig(version=1, topics=(), entity_types=()).compile(),
        force_flush=True,
    )

    assert claim is not None
    messages = await store.get_project_semantic_window_evidence_messages(
        str(claim.window.window_id),
        user_name="ada",
        project_id="project-1",
    )
    assert [
        (message["message_id"], message["role"], message["exchange_outcome"])
        for message in messages
    ] == [
        (301, "user", "clarification"),
        (302, "assistant", "clarification"),
    ]
