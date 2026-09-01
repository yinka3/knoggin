import pytest
from pydantic import ValidationError

from common.exceptions import LLMResponseError
from common.schema.ingestion.contracts import (
    RelationshipObservation,
)
from common.schema.ingestion.extraction import (
    RelationshipExtraction,
    RelationshipMention,
)
from core.ingestion.batch import IngestionBatch
from core.ingestion.relationship_extractor import RelationshipExtractor
from core.knowledge.entity.profile import EntityProfile
from tests.fixtures.ingestion import ingestion_policy


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
    return RelationshipExtractor(
        llm=FakeLLM(llm_response),
        entities=FakeEntities(),
        user_name="Ada",
    )


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connections_result_accepts_source_message_ids():
    result = RelationshipExtraction.model_validate(
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
        RelationshipExtraction.model_validate(
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
async def test_connection_extraction_surfaces_invalid_model_response_for_retry():
    processor = make_processor(None)

    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Alice met Bob."}],
        session_text="",
        policy=ingestion_policy(),
    )
    batch.entity_ids = [1, 2]
    batch.entity_message_map = {1: [7], 2: [7]}

    with pytest.raises(LLMResponseError, match="returned no result"):
        await processor.extract(batch)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_connection_extraction_keeps_valid_connections():
    processor = make_processor(
        RelationshipExtraction(
            connections=[
                RelationshipMention(
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

    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=[{"id": 7, "message": "Alice met Bob."}],
        session_text="",
        policy=ingestion_policy(),
    )
    batch.entity_ids = [1, 2]
    batch.entity_message_map = {1: [7], 2: [7]}

    result = await processor.extract(batch)

    assert result == [
        RelationshipObservation(
            message_id=7,
            entity_a_name="Alice",
            entity_b_name="Bob",
            relationship_type="met",
            observed_label="met",
            domain_status="unrecognized",
            source_type="Identity",
            target_type="Identity",
            confidence=0.9,
            context="Alice met Bob.",
        )
    ]
