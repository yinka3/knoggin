from contextlib import asynccontextmanager
from types import SimpleNamespace

import pytest

from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from common.schema.source.references import SourceReferenceCandidate
from core.knowledge.db.readers.source_reference_reader import SourceReferenceReader
from core.knowledge.db.writers.message_lifecycle_writer import ExchangeClosure
from core.knowledge.store import KnowledgeStore


class TransactionClient:
    def __init__(self):
        self.cursor = object()
        self.transaction_count = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_count += 1
        yield self.cursor


class MessageWriter:
    def __init__(self):
        self.calls = []

    async def save_message_logs(self, messages, *, cur=None):
        self.calls.append((messages, cur))
        return True


class SourceReferenceWriter:
    def __init__(self):
        self.calls = []

    async def write_for_assistant_message(self, message_id, candidates, **kwargs):
        self.calls.append((message_id, candidates, kwargs))
        return ["reference"]


class ArtifactWriter:
    def __init__(self):
        self.calls = []

    async def write_for_assistant_message(self, message_id, artifact, **kwargs):
        self.calls.append((message_id, artifact, kwargs))
        return "artifact"


class LifecycleWriter:
    def __init__(self):
        self.prepare_calls = []
        self.close_calls = []

    async def prepare_assistant_exchange_finalization(self, **kwargs):
        self.prepare_calls.append(kwargs)
        return None

    async def close_user_exchange(self, **kwargs):
        self.close_calls.append(kwargs)
        return ExchangeClosure(
            user_message_id=kwargs["user_message_id"],
            outcome=kwargs["outcome"],
            closed_at_ms=kwargs["closed_at_ms"],
        )


@pytest.mark.no_network
@pytest.mark.parametrize("reference_ids", [[], ["ref-2", "ref-1"]])
async def test_finalization_replay_reads_existing_sources_without_rewriting(
    reference_ids,
):
    class Cursor:
        async def execute(self, query, params):
            self.query, self.params = query, params

        async def fetchall(self):
            return [{"source_ref_id": value} for value in reference_ids]

    class ExistingLifecycle(LifecycleWriter):
        async def prepare_assistant_exchange_finalization(self, **kwargs):
            self.prepare_calls.append(kwargs)
            return SimpleNamespace(assistant_message_id=11)

    client = TransactionClient()
    client.cursor = Cursor()
    lifecycle = ExistingLifecycle()
    store = object.__new__(KnowledgeStore)
    store._postgres_client = client
    store._message_lifecycle_writer = lifecycle
    # No writers are installed: a replay must never write another response.
    store._source_reference_reader = SourceReferenceReader(client)

    result = await store.finalize_assistant_exchange(
        {
            "id": 12,
            "role": "assistant",
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "user_msg_id": 7,
        },
        [_candidate()],
        readable_project_ids=["project-1"],
    )

    assert result == (11, reference_ids, False)
    assert client.transaction_count == 1
    assert lifecycle.prepare_calls[0]["cur"] is client.cursor
    assert lifecycle.close_calls == []
    assert client.cursor.params == ("project-1", "session-1", 11, "ada")
    assert "message.user_name = %s" in client.cursor.query
    assert (
        "ORDER BY ref.created_at ASC, ref.result_position ASC, ref.source_ref_id ASC"
        in client.cursor.query
    )


def _candidate():
    return SourceReferenceCandidate(
        project_id="project-1",
        session_id="session-1",
        source_kind="user_pasted_text",
        source_message_id=5,
        content_hash="a" * 64,
        locator={"kind": "character_span", "start_char": 0, "end_char": 6},
        excerpt="pasted",
        metadata={"pasted_text": True},
        encounter_kind="user_pasted_text",
        agent_run_id="run-1",
        result_position=0,
    )


@pytest.mark.no_network
async def test_final_assistant_exchange_closes_the_user_in_the_same_transaction():
    client = TransactionClient()
    message_writer = MessageWriter()
    source_writer = SourceReferenceWriter()
    artifact_writer = ArtifactWriter()
    lifecycle = LifecycleWriter()
    store = object.__new__(KnowledgeStore)
    store._postgres_client = client
    store._message_writer = message_writer
    store._source_reference_writer = source_writer
    store._artifact_writer = artifact_writer
    store._message_lifecycle_writer = lifecycle
    message = {
        "id": 11,
        "role": "assistant",
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "user_msg_id": 7,
        "sealed_at_ms": 1_000,
    }

    final_message_id, source_ref_ids, created = await store.finalize_assistant_exchange(
        message,
        [_candidate()],
        readable_project_ids=["project-1"],
        artifact=ArtifactDraft(
            title="Saved artifact",
            blocks=(MarkdownArtifactBlock(content="Durable"),),
        ),
    )

    assert (final_message_id, source_ref_ids, created) == (11, [], True)
    assert client.transaction_count == 1
    assert lifecycle.prepare_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "user_message_id": 7,
            "outcome": "assistant_final",
            "cur": client.cursor,
        }
    ]
    assert message_writer.calls == [([message], client.cursor)]
    assert source_writer.calls[0][2]["cursor"] is client.cursor
    assert artifact_writer.calls[0][2]["cursor"] is client.cursor
    assert lifecycle.close_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "user_message_id": 7,
            "outcome": "assistant_final",
            "closed_at_ms": 1_000,
            "cur": client.cursor,
        }
    ]
