import pytest
from pydantic import ValidationError

from common.schema.contracts import (
    ConnectionMention,
    ConnectionsResult,
    MessageConnections,
)
from common.schema.primitives import ConnectionRecord
from core.ingestion.services.pipeline_service import IngestionPipeline
from core.knowledge.entity.profile import EntityProfile


class FakeLLM:
    def __init__(self, response):
        self.response = response

    async def generate_structured(self, **kwargs):
        return self.response


class FakeEntities:
    def get_mentions_for_id(self, ent_id):
        return {1: ["Alice"], 2: ["Bob"]}.get(ent_id, [])

    async def get_profile(self, ent_id):
        profiles = {
            1: EntityProfile(canonical_name="Alice", entity_type="person"),
            2: EntityProfile(canonical_name="Bob", entity_type="person"),
        }
        return profiles.get(ent_id)


def make_processor(llm_response):
    return IngestionPipeline(
        project_id="project-1",
        redis_client=None,
        llm=FakeLLM(llm_response),
        entities=FakeEntities(),
        processor=None,
        cpu_executor=None,
        user_name="Ada",
        topic_config=None,
        get_next_ent_id=None,
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connections_result_accepts_source_message_ids():
    result = ConnectionsResult.model_validate(
        {
            "connections": [
                {
                    "msg_id": "m1",
                    "entity_a": "Alice",
                    "entity_b": "Bob",
                    "relationship": "met",
                    "confidence": 0.9,
                    "context": "Alice met Bob.",
                }
            ]
        }
    )

    assert result.connections[0].msg_id == "m1"


@pytest.mark.ingestion
@pytest.mark.no_network
def test_connections_result_requires_a_local_message_reference():
    with pytest.raises(ValidationError):
        ConnectionsResult.model_validate(
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


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connection_extraction_falls_back_to_empty_on_llm_failure():
    processor = make_processor(None)

    result = await processor._extract_connections(
        entity_ids=[1, 2],
        entity_msg_map={1: [7], 2: [7]},
        messages=[{"id": 7, "message": "Alice met Bob."}],
        session_text="",
        session_id="session-1",
    )

    assert result == ([], [])


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connection_extraction_keeps_valid_connections():
    processor = make_processor(
        ConnectionsResult(
            connections=[
                ConnectionMention(
                    msg_id="m1",
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
        session_id="session-1",
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
