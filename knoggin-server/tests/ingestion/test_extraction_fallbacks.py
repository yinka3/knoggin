import pytest

from common.schema.contracts import ConnectionsResult, MessageConnections
from common.schema.primitives import ConnectionRecord
from knoggin_server.ingestion.services.pipeline_service import BatchProcessor


class FakeLLM:
    def __init__(self, response):
        self.response = response

    async def call_llm(self, **kwargs):
        return self.response


class FakeEntities:
    def get_mentions_for_id(self, ent_id):
        return {1: ["Alice"], 2: ["Bob"]}.get(ent_id, [])

    async def get_profile(self, ent_id):
        profiles = {
            1: {"canonical_name": "Alice", "type": "person"},
            2: {"canonical_name": "Bob", "type": "person"},
        }
        return profiles.get(ent_id)


def make_processor(llm_response):
    return BatchProcessor(
        scope_id="session-1",
        redis_client=None,
        llm=FakeLLM(llm_response),
        entities=FakeEntities(),
        processor=None,
        cpu_executor=None,
        user_name="Ada",
        topic_config=None,
        get_next_ent_id=None,
        connection_prompt="Return JSON matching the requested schema.",
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connections_result_accepts_source_message_ids():
    result = ConnectionsResult.model_validate(
        {
            "connections": [
                {
                    "msg_id": 42,
                    "entity_a": "Alice",
                    "entity_b": "Bob",
                    "relationship": "met",
                    "confidence": 0.9,
                    "context": "Alice met Bob.",
                }
            ]
        }
    )

    assert result.connections[0].msg_id == 42


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connection_extraction_falls_back_to_empty_on_llm_failure():
    processor = make_processor(None)

    result = await processor._extract_connections(
        entity_ids=[1, 2],
        entity_msg_map={1: [7], 2: [7]},
        messages=[{"id": 7, "message": "Alice met Bob."}],
        session_text="",
    )

    assert result == ([], [])


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connection_extraction_keeps_valid_connections():
    processor = make_processor(
        ConnectionsResult(
            connections=[
                ConnectionRecord(
                    msg_id=7,
                    entity_a="Alice",
                    entity_b="Bob",
                    relationship="met",
                    confidence=0.9,
                    context="Alice met Bob.",
                )
            ]
        )
    )

    result = await processor._extract_connections(
        entity_ids=[1, 2],
        entity_msg_map={1: [7], 2: [7]},
        messages=[{"id": 7, "message": "Alice met Bob."}],
        session_text="",
    )

    assert result == ([
        MessageConnections(
            message_id=7,
            entity_pairs=[
                ConnectionRecord(
                    entity_a="Alice",
                    entity_b="Bob",
                    relationship="met",
                    confidence=0.9,
                    context="Alice met Bob.",
                    msg_id=7,
                )
            ],
        )
    ], [])
