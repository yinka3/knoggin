import pytest

from common.schema.contracts import (
    MessageConnections,
    MessageUserConnections,
    ResolutionResult,
    UserConnectionRecord,
    ValidationIssue,
)
from common.schema.primitives import ConnectionRecord
from knoggin_server.ingestion.services.pipeline_service import BatchProcessor
from knoggin_server.knowledge.services.entity_service import EntityManager
from tests.fixtures.factories import make_topic_config
from tests.ingestion.test_batch_processor_entity_resolution_contract import (
    FakeEmbeddingService,
    FakeGraphClient,
    FakeLLM,
    seed_entity,
)

FAKE_MESSAGES = [
    {
        "id": 1,
        "message": "Alice is working with Bob on the Knoggin project.",
        "timestamp": "2026-01-01T00:00:00+00:00",
        "role": "user",
    },
    {
        "id": 2,
        "message": "Bob uses Notion to organize the project notes.",
        "timestamp": "2026-01-01T00:01:00+00:00",
        "role": "user",
    },
]

FAKE_MENTIONS = [
    (1, "Alice", "person", "Identity"),
    (1, "Bob", "person", "Identity"),
    (1, "Knoggin", "project", "General"),
    (2, "Bob", "person", "Identity"),
    (2, "Notion", "tool", "General"),
]


class FakeEntities:
    project_id = "project-1"


def make_processor():
    return BatchProcessor(
        scope_id="session-1",
        redis_client=None,
        llm=None,
        entities=FakeEntities(),
        processor=None,
        cpu_executor=None,
        user_name="ada",
        topic_config=make_topic_config(),
        get_next_ent_id=lambda: 999,
    )


def fake_resolution_result():
    return ResolutionResult(
        entity_ids=[101, 102, 103, 104],
        new_ids={101, 103, 104},
        alias_ids={102},
        entity_msg_map={101: [1], 102: [1, 2], 103: [1], 104: [2]},
        alias_updates={102: ["Bob"]},
    )


def fake_relationships():
    return [
        MessageConnections(
            message_id=1,
            entity_pairs=[
                ConnectionRecord(
                    msg_id=1,
                    entity_a="Alice",
                    entity_b="Bob",
                    relationship="works_with",
                    confidence=0.9,
                    context="Alice is working with Bob.",
                )
            ],
        ),
        MessageConnections(
            message_id=2,
            entity_pairs=[
                ConnectionRecord(
                    msg_id=2,
                    entity_a="Bob",
                    entity_b="Notion",
                    relationship="uses",
                    confidence=0.85,
                    context="Bob uses Notion.",
                )
            ],
        ),
    ]


def fake_user_relationships():
    return [
        MessageUserConnections(
            message_id=1,
            user_connections=[
                UserConnectionRecord(
                    msg_id=1,
                    entity_name="Knoggin",
                    relationship="works_on",
                    confidence=0.9,
                    context="User is working on Knoggin.",
                )
            ],
        )
    ]


def fail_if_called(*args, **kwargs):
    raise AssertionError("This ingestion stage should not be called")


def make_processor_with_real_resolution():
    embedding = FakeEmbeddingService()
    graph = FakeGraphClient()
    entities = EntityManager(
        graph_client=graph,
        embedding_service=embedding,
        project_id="project-1",
        readable_project_ids=["project-1"],
    )
    next_ids = iter(range(1001, 1100))

    async def get_next_ent_id():
        return next(next_ids)

    processor = BatchProcessor(
        scope_id="session-1",
        redis_client=None,
        llm=FakeLLM(),
        entities=entities,
        processor=None,
        cpu_executor=None,
        user_name="ada",
        topic_config=make_topic_config(),
        get_next_ent_id=get_next_ent_id,
    )
    return processor, entities, embedding


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_happy_path_builds_graph_write_result():
    processor = make_processor()

    async def extract_mentions(messages, session_id, trace, issues):
        return list(FAKE_MENTIONS)

    async def resolve_mentions(mentions, messages):
        return fake_resolution_result()

    async def extract_connections(
        entity_ids, entity_msg_map, messages, session_text, trace=None, issues=None
    ):
        return fake_relationships(), fake_user_relationships()

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = resolve_mentions
    processor._extract_connections = extract_connections

    result = await processor.run(FAKE_MESSAGES, session_text="[USER]: prior context")

    assert result.success is True
    assert result.error is None
    assert result.scope.user_name == "ada"
    assert result.scope.session_id == "session-1"
    assert result.scope.project_id == "project-1"
    assert result.work_unit.kind == "message_batch"
    assert result.work_unit.status == "succeeded"
    assert result.work_unit.trace.summary == "4 entities, 3 relationships"
    assert result.trace.batch_size == 2
    assert result.trace.message_ids == [1, 2]
    assert result.entity_ids == [101, 102, 103, 104]
    assert result.new_entity_ids == {101, 103, 104}
    assert result.alias_updated_ids == {102}
    assert result.alias_updates == {102: ["Bob"]}
    assert [item.message_id for item in result.relationship_observations] == [1, 2]
    assert result.relationship_observations[0].entity_pairs[0].relationship == (
        "works_with"
    )
    assert result.relationship_observations[1].entity_pairs[0].relationship == "uses"
    assert [item.message_id for item in result.user_relationship_observations] == [1]
    assert (
        result.user_relationship_observations[0].user_connections[0].relationship
        == "works_on"
    )
    assert result.has_graph_writes() is True


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_with_real_resolution_builds_graph_write_result():
    processor, entities, embedding = make_processor_with_real_resolution()
    await seed_entity(entities, 102, "Robert Chen", aliases=["Bob"])
    seen_entity_msg_map = {}

    async def extract_mentions(messages, session_id, trace, issues):
        return list(FAKE_MENTIONS)

    async def extract_connections(
        entity_ids, entity_msg_map, messages, session_text, trace=None, issues=None
    ):
        seen_entity_msg_map.update(entity_msg_map)
        return fake_relationships(), fake_user_relationships()

    processor._extract_mentions = extract_mentions
    processor._extract_connections = extract_connections

    result = await processor.run(FAKE_MESSAGES, session_text="[USER]: prior context")

    assert result.success is True
    assert result.error is None
    assert result.scope.user_name == "ada"
    assert result.scope.session_id == "session-1"
    assert result.scope.project_id == "project-1"
    assert result.entity_ids == [1001, 102, 1002, 1003]
    assert result.new_entity_ids == {1001, 1002, 1003}
    assert result.alias_updated_ids == set()
    assert result.alias_updates == {}
    assert seen_entity_msg_map == {
        1001: [1],
        102: [1, 2],
        1002: [1],
        1003: [2],
    }
    assert result.work_unit.status == "succeeded"
    assert result.work_unit.trace.summary == "4 entities, 3 relationships"
    assert result.has_graph_writes() is True
    assert (await entities.get_profile(1001))["canonical_name"] == "Alice"
    assert (await entities.get_profile(102))["canonical_name"] == "Robert Chen"
    assert (await entities.get_profile(1002))["canonical_name"] == "Knoggin"
    assert (await entities.get_profile(1003))["canonical_name"] == "Notion"
    assert set(embedding.batch_calls[0]) == {"Alice", "Bob", "Knoggin", "Notion"}


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_empty_batch_marks_work_unit_skipped():
    processor = make_processor()
    processor._extract_mentions = fail_if_called
    processor._resolve_mentions = fail_if_called
    processor._extract_connections = fail_if_called

    result = await processor.run([], session_text="")

    assert result.success is True
    assert result.entity_ids == []
    assert result.relationship_observations == []
    assert result.user_relationship_observations == []
    assert result.trace.batch_size == 0
    assert result.trace.message_ids == []
    assert result.work_unit.status == "skipped"
    assert result.work_unit.trace.summary == "No messages"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_no_mentions_skips_resolution():
    processor = make_processor()

    async def extract_mentions(messages, session_id, trace, issues):
        return []

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = fail_if_called
    processor._extract_connections = fail_if_called

    result = await processor.run(FAKE_MESSAGES, session_text="")

    assert result.success is True
    assert result.entity_ids == []
    assert result.relationship_observations == []
    assert result.user_relationship_observations == []
    assert result.has_graph_writes() is False
    assert result.trace.batch_size == 2
    assert result.trace.message_ids == [1, 2]
    assert result.work_unit.status == "succeeded"
    assert result.work_unit.trace.summary == "No mentions found"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_filters_blank_mentions_before_resolution():
    processor = make_processor()
    seen_mentions = []

    async def extract_mentions(messages, session_id, trace, issues):
        return [
            (1, "", "person", "Identity"),
            (1, "Alice", "person", "Identity"),
        ]

    async def resolve_mentions(mentions, messages):
        seen_mentions.extend(mentions)
        return ResolutionResult(
            entity_ids=[101],
            new_ids={101},
            alias_ids=set(),
            entity_msg_map={101: [1]},
            alias_updates={},
        )

    async def extract_connections(
        entity_ids, entity_msg_map, messages, session_text, trace=None, issues=None
    ):
        return [], []

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = resolve_mentions
    processor._extract_connections = extract_connections

    result = await processor.run(FAKE_MESSAGES[:1], session_text="")

    assert result.success is True
    assert seen_mentions == [(1, "Alice", "person", "Identity")]
    assert result.entity_ids == [101]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_marks_failed_when_connections_fail():
    processor = make_processor()

    async def extract_mentions(messages, session_id, trace, issues):
        return list(FAKE_MENTIONS)

    async def resolve_mentions(mentions, messages):
        return fake_resolution_result()

    async def extract_connections(
        entity_ids, entity_msg_map, messages, session_text, trace=None, issues=None
    ):
        return None, None

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = resolve_mentions
    processor._extract_connections = extract_connections

    result = await processor.run(FAKE_MESSAGES, session_text="")

    assert result.success is False
    assert result.error == "Connection extraction failed (VP-03)"
    assert result.work_unit.status == "failed"
    assert result.work_unit.trace.summary == result.error
    assert result.entity_ids == [101, 102, 103, 104]
    assert result.relationship_observations == []
    assert result.user_relationship_observations == []


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_marks_failed_when_extraction_raises():
    processor = make_processor()

    async def extract_mentions(messages, session_id, trace, issues):
        raise RuntimeError("mention boom")

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = fail_if_called
    processor._extract_connections = fail_if_called

    result = await processor.run(FAKE_MESSAGES, session_text="")

    assert result.success is False
    assert "mention boom" in result.error
    assert result.work_unit.status == "failed"
    assert result.scope.user_name == "ada"
    assert result.scope.session_id == "session-1"
    assert result.scope.project_id == "project-1"
    assert result.trace.batch_size == 2
    assert result.trace.message_ids == [1, 2]


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_attaches_validation_issues_to_work_unit():
    processor = make_processor()

    async def extract_mentions(messages, session_id, trace, issues):
        issues.append(
            ValidationIssue(
                stage="ner",
                code="ambiguous_topic",
                message="Topic label matched multiple topics",
                severity="warning",
            )
        )
        return []

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = fail_if_called
    processor._extract_connections = fail_if_called

    result = await processor.run(FAKE_MESSAGES, session_text="")

    assert len(result.issues) == 1
    assert result.work_unit.issues == result.issues
    issue = result.issues[0]
    assert issue.stage == "ner"
    assert issue.code == "ambiguous_topic"
    assert issue.severity == "warning"
    assert issue.message == "Topic label matched multiple topics"


@pytest.mark.ingestion
@pytest.mark.no_network
async def test_batch_processor_run_preserves_trace_fields():
    processor = make_processor()

    async def extract_mentions(messages, session_id, trace, issues):
        trace.entity_model = "fake-ner-model"
        trace.entity_prompt = "VEGAPUNK-01"
        trace.llm_mentions_seen = 3
        trace.llm_mentions_accepted = 2
        return list(FAKE_MENTIONS[:2])

    async def resolve_mentions(mentions, messages):
        return ResolutionResult(
            entity_ids=[101, 102],
            new_ids={101, 102},
            alias_ids=set(),
            entity_msg_map={101: [1], 102: [1]},
            alias_updates={},
        )

    async def extract_connections(
        entity_ids, entity_msg_map, messages, session_text, trace=None, issues=None
    ):
        trace.relationship_model = "fake-relationship-model"
        trace.relationship_prompt = "VEGAPUNK-02"
        trace.relationships_seen = 2
        trace.relationships_accepted = 2
        return [], []

    processor._extract_mentions = extract_mentions
    processor._resolve_mentions = resolve_mentions
    processor._extract_connections = extract_connections

    result = await processor.run(FAKE_MESSAGES, session_text="")

    assert result.success is True
    assert result.trace.entity_model == "fake-ner-model"
    assert result.trace.entity_prompt == "VEGAPUNK-01"
    assert result.trace.llm_mentions_seen == 3
    assert result.trace.llm_mentions_accepted == 2
    assert result.trace.relationship_model == "fake-relationship-model"
    assert result.trace.relationship_prompt == "VEGAPUNK-02"
    assert result.trace.relationships_seen == 2
    assert result.trace.relationships_accepted == 2
    assert result.trace.fallbacks == []
    assert result.work_unit.status == "succeeded"
