from contextlib import asynccontextmanager

import pytest

from common.schema.source_reference import SourceReferenceCandidate
from infrastructure.knowledge_store import KnowledgeStore


class TransactionClient:
    def __init__(self):
        self.cursor = object()
        self.transaction_count = 0

    @asynccontextmanager
    async def transaction(self):
        self.transaction_count += 1
        yield self.cursor


class GraphWriter:
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
async def test_assistant_message_and_source_refs_share_one_transaction():
    client = TransactionClient()
    graph_writer = GraphWriter()
    source_writer = SourceReferenceWriter()
    store = object.__new__(KnowledgeStore)
    store._postgres_client = client
    store._graph_writer = graph_writer
    store._source_reference_writer = source_writer
    message = {
        "id": 9,
        "role": "assistant",
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
    }

    references = await store.save_assistant_message_with_source_refs(
        message,
        [_candidate()],
    )

    assert references == ["reference"]
    assert client.transaction_count == 1
    assert graph_writer.calls == [([message], client.cursor)]
    assert source_writer.calls == [
        (
            9,
            [_candidate()],
            {
                "user_name": "ada",
                "project_id": "project-1",
                "session_id": "session-1",
                "cursor": client.cursor,
            },
        )
    ]
