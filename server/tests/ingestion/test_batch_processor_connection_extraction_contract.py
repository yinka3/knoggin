import pytest

from common.conf.domain_config import DomainConfig
from common.exceptions import LLMProviderError
from common.schema.ingestion.contracts import (
    ExtractionTrace,
)
from common.schema.ingestion.extraction import (
    IdentityRelationshipMention,
    RelationshipExtraction,
    RelationshipMention,
)
from core.ingestion.batch import IngestionBatch
from core.ingestion.services.pipeline_service import IngestionPipeline
from tests.fixtures.ingestion import ingestion_policy
from tests.ingestion.test_batch_processor_entity_resolution_contract import (
    MESSAGES,
    make_harness,
    seed_entity,
)


class FakeConnectionLLM:
    extraction_model = "fake-connection-model"

    def __init__(self, response=None, *, raise_error=False):
        self.response = response if response is not None else RelationshipExtraction()
        self.raise_error = raise_error
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.raise_error:
            raise LLMProviderError("fake connection failure")
        return self.response


def make_processor(response=None, *, raise_error=False):
    processor, entities, _, _ = make_harness()
    processor.llm = FakeConnectionLLM(response, raise_error=raise_error)
    return processor, entities


async def seed_connection_entities(entities, *, include_user_entity=False):
    await seed_entity(entities, 101, "Alice")
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    await seed_entity(
        entities,
        103,
        "Knoggin",
        entity_type="project",
        topic="General",
    )
    if include_user_entity:
        await seed_entity(entities, 104, "ada")


def relationship(
    *,
    msg_id="m1",
    entity_a="Alice",
    entity_b="Robert Chen",
    name="works_with",
):
    return RelationshipMention(
        msg_id=msg_id,
        entity_a=entity_a,
        entity_b=entity_b,
        relationship=name,
        confidence=0.91,
        context=f"{entity_a} {name} {entity_b}.",
    )


def user_relationship(*, msg_id="m1", entity_name="Knoggin", name="works_on"):
    return IdentityRelationshipMention(
        msg_id=msg_id,
        entity_name=entity_name,
        relationship=name,
        confidence=0.88,
        context=f"ada {name} {entity_name}.",
    )


async def extract(
    processor: IngestionPipeline,
    *,
    entity_ids=None,
    entity_msg_map=None,
    messages=None,
    trace=None,
    issues=None,
    compiled_domain=None,
):
    if entity_ids is None:
        entity_ids = [101, 102, 103]
    if entity_msg_map is None:
        entity_msg_map = {101: [1], 102: [1], 103: [1]}

    batch = IngestionBatch.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        messages=MESSAGES if messages is None else messages,
        session_text="[USER]: prior context",
        policy=ingestion_policy(compiled_domain=compiled_domain),
    )
    batch.entity_ids = entity_ids
    batch.entity_message_map = entity_msg_map
    if trace is not None:
        batch.trace = trace
    if issues is not None:
        batch.issues = issues
    return await processor._extract_connections(batch)


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_no_entity_ids_skips_llm():
    processor, _ = make_processor()
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        entity_ids=[],
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert processor.llm.calls == []
    assert trace.relationship_model is None
    assert trace.relationship_prompt is None
    assert trace.relationships_seen == 0
    assert trace.user_relationships_seen == 0
    assert issues == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_normalizes_configured_and_unknown_relationships():
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {}},
            "entity_types": {
                "Project": {"topic": "Work", "labels": ["project"]},
                "Technology": {"topic": "Work", "labels": ["technology"]},
            },
            "relationships": {
                "USES": {
                    "source_types": ["Project"],
                    "target_types": ["Technology"],
                }
            },
        }
    ).compile()
    response = RelationshipExtraction(
        connections=[relationship(name="uses")],
    )
    processor, entities = make_processor(response)
    await seed_entity(
        entities,
        101,
        "Alice",
        entity_type="Project",
        topic="Work",
    )
    await seed_entity(
        entities,
        102,
        "Robert Chen",
        entity_type="Technology",
        topic="Work",
    )

    trace = ExtractionTrace()
    observations = await extract(
        processor,
        entity_ids=[101, 102],
        entity_msg_map={101: [1], 102: [1]},
        trace=trace,
        issues=[],
        compiled_domain=domain,
    )

    assert observations[0].canonical_type == "USES"
    assert observations[0].observed_label == "uses"
    assert observations[0].domain_status == "recognized"
    assert observations[0].relationship_type == "USES"
    assert trace.relationships_recognized == 1


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_happy_path_returns_graph_write_observations():
    response = RelationshipExtraction(
        connections=[relationship()],
        user_connections=[user_relationship()],
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations[0].message_id == 1
    assert observations[0].relationship_type == "works_with"
    assert observations[0].confidence == 0.91
    assert observations[0].identity_rooted is False
    assert observations[1].message_id == 1
    assert observations[1].relationship_type == "works_on"
    assert observations[1].entity_b_name == "Knoggin"
    assert observations[1].identity_rooted is True
    assert trace.relationship_model == "fake-connection-model"
    assert trace.relationship_prompt == "VEGAPUNK-02"
    assert trace.relationships_seen == 1
    assert trace.relationships_accepted == 1
    assert trace.user_relationships_seen == 1
    assert trace.user_relationships_accepted == 1
    assert issues == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_resolves_local_msg_id_to_real_message_id():
    response = RelationshipExtraction(connections=[relationship(msg_id="m2")])
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    messages = [
        {**MESSAGES[0], "id": 41},
        {**MESSAGES[1], "id": 99},
    ]

    observations = await extract(
        processor,
        entity_msg_map={101: [99], 102: [99], 103: [99]},
        messages=messages,
        trace=ExtractionTrace(),
        issues=[],
    )

    assert observations[0].message_id == 99
    assert observations[0].identity_rooted is False
    prompt = processor.llm.calls[0]["user"]
    assert "[MSG m1]" in prompt
    assert "[MSG m2]" in prompt
    assert "MSG 99" not in prompt
    assert "[MSG 99]" not in prompt


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_accepts_known_alias_names_from_llm():
    response = RelationshipExtraction(
        connections=[relationship(entity_b=" Bob ")],
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations[0].entity_b_name == "Robert Chen"
    assert trace.relationships_accepted == 1
    assert trace.relationships_rejected == 0
    assert issues == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_llm_failure_records_fallback_and_issue():
    processor, entities = make_processor(raise_error=True)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.fallbacks == [
        {
            "stage": "connections",
            "fallback": "empty_connections",
            "error_code": "llm_provider_error",
        }
    ]
    assert [issue.code for issue in issues] == ["llm_extraction_failed"]
    assert issues[0].metadata["error_code"] == "llm_provider_error"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_empty_llm_result_returns_empty_without_fallback():
    processor, entities = make_processor(RelationshipExtraction())
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.fallbacks == []
    assert trace.relationships_seen == 0
    assert trace.user_relationships_seen == 0
    assert issues == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_missing_profile_records_issue_and_skips_llm():
    processor, _ = make_processor(RelationshipExtraction(connections=[relationship()]))
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        entity_ids=[999],
        entity_msg_map={999: [1]},
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert processor.llm.calls == []
    assert [issue.code for issue in issues] == ["connection_candidate_profile_missing"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_invalid_relationship_msg_id():
    response = RelationshipExtraction(connections=[relationship(msg_id="m999")])
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.relationships_seen == 1
    assert trace.relationships_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_msg_id"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_unknown_relationship_entity():
    response = RelationshipExtraction(
        connections=[relationship(entity_b="Ghost Entity")],
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.relationships_seen == 1
    assert trace.relationships_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_entity_name"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_relationship_msg_id_without_entity_sources():
    response = RelationshipExtraction(connections=[relationship(msg_id="m2")])
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        entity_msg_map={101: [1], 102: [1], 103: [2]},
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.relationships_seen == 1
    assert trace.relationships_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_relationship_evidence_msg_id"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_self_relationship():
    response = RelationshipExtraction(
        connections=[relationship(entity_a="Robert Chen", entity_b="Bob")]
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.relationships_seen == 1
    assert trace.relationships_rejected == 1
    assert [issue.code for issue in issues] == ["self_relationship"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_duplicate_relationship():
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Identity": {}},
            "entity_types": {
                "Person": {"topic": "Identity", "labels": ["person"]},
            },
            "relationships": {
                "WORKS_WITH": {
                    "source_types": ["Person"],
                    "target_types": ["Person"],
                    "symmetric": True,
                }
            },
        }
    ).compile()
    response = RelationshipExtraction(
        connections=[
            relationship(),
            relationship(entity_a="Robert Chen", entity_b="Alice"),
        ],
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
        compiled_domain=domain,
    )

    assert observations[0].entity_a_name == "Alice"
    assert observations[0].entity_b_name == "Robert Chen"
    assert trace.relationships_seen == 2
    assert trace.relationships_accepted == 1
    assert trace.relationships_rejected == 1
    assert [issue.code for issue in issues] == ["duplicate_relationship"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_invalid_user_connection_msg_id():
    response = RelationshipExtraction(
        user_connections=[user_relationship(msg_id="m999")]
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.user_relationships_seen == 1
    assert trace.user_relationships_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_user_connection_msg_id"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_unknown_user_connection_entity():
    response = RelationshipExtraction(
        user_connections=[user_relationship(entity_name="Ghost Entity")]
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.user_relationships_seen == 1
    assert trace.user_relationships_rejected == 1
    assert [issue.code for issue in issues] == ["invalid_user_connection_entity"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_user_connection_without_entity_source():
    response = RelationshipExtraction(user_connections=[user_relationship(msg_id="m2")])
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        entity_msg_map={101: [1], 102: [1], 103: [1]},
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.user_relationships_seen == 1
    assert trace.user_relationships_rejected == 1
    assert [issue.code for issue in issues] == [
        "invalid_user_connection_evidence_msg_id"
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_user_connected_to_self():
    response = RelationshipExtraction(
        user_connections=[user_relationship(entity_name="ada")]
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities, include_user_entity=True)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        entity_ids=[101, 102, 103, 104],
        entity_msg_map={101: [1], 102: [1], 103: [1], 104: [1]},
        trace=trace,
        issues=issues,
    )

    assert observations == []
    assert trace.user_relationships_seen == 1
    assert trace.user_relationships_rejected == 1
    assert [issue.code for issue in issues] == ["self_user_connection"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_rejects_duplicate_user_connection():
    response = RelationshipExtraction(
        user_connections=[
            user_relationship(entity_name="Knoggin"),
            user_relationship(entity_name=" knoggin "),
        ],
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert observations[0].entity_b_name == "Knoggin"
    assert observations[0].identity_rooted is True
    assert trace.user_relationships_seen == 2
    assert trace.user_relationships_accepted == 1
    assert trace.user_relationships_rejected == 1
    assert [issue.code for issue in issues] == ["duplicate_user_connection"]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_keeps_valid_items_when_response_is_mixed():
    response = RelationshipExtraction(
        connections=[
            relationship(),
            relationship(msg_id="m999"),
            relationship(entity_b="Ghost Entity"),
        ],
        user_connections=[
            user_relationship(),
            user_relationship(msg_id="m999"),
            user_relationship(entity_name="Ghost Entity"),
        ],
    )
    processor, entities = make_processor(response)
    await seed_connection_entities(entities)
    trace = ExtractionTrace()
    issues = []

    observations = await extract(
        processor,
        trace=trace,
        issues=issues,
    )

    assert [item.relationship_type for item in observations] == [
        "works_with",
        "works_on",
    ]
    assert [item.identity_rooted for item in observations] == [False, True]
    assert trace.relationships_seen == 3
    assert trace.relationships_accepted == 1
    assert trace.relationships_rejected == 2
    assert trace.user_relationships_seen == 3
    assert trace.user_relationships_accepted == 1
    assert trace.user_relationships_rejected == 2
    assert [issue.code for issue in issues] == [
        "invalid_msg_id",
        "invalid_entity_name",
        "invalid_user_connection_msg_id",
        "invalid_user_connection_entity",
    ]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_extract_connections_uses_named_connection_prompt_with_user_name():
    processor, entities = make_processor(RelationshipExtraction())
    await seed_connection_entities(entities)

    await extract(processor)

    assert "VEGAPUNK-02" in processor.llm.calls[0]["system"]
    assert "ada" in processor.llm.calls[0]["system"]
    assert processor.llm.calls[0]["temperature"] == 0.0
    assert processor.llm.calls[0]["response_model"] is RelationshipExtraction
