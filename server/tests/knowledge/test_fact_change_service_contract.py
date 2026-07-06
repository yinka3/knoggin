import pytest

from common.schema.primitives import FactRecord
from infrastructure.redis_client import RedisKeys
from core.knowledge.services import fact_change_service as service_module
from core.knowledge.services.fact_change_service import FactChangeService
from tests.fixtures.fakes import FakeRedis


class FakeKnowledgeStore:
    def __init__(self):
        self.remove_calls = []
        self.replace_calls = []

    async def remove_fact_with_audit(self, **kwargs):
        self.remove_calls.append(kwargs)
        return {
            "fact_change_id": kwargs["fact_change_id"],
            "entity_id": kwargs["entity_id"],
            "invalidated_fact_ids": [kwargs["fact_id"]],
            "created_fact_ids": [],
        }

    async def replace_facts_with_audit(self, **kwargs):
        self.replace_calls.append(kwargs)
        return {
            "fact_change_id": kwargs["fact_change_id"],
            "entity_id": kwargs["entity_id"],
            "invalidated_fact_ids": list(kwargs["fact_ids"]),
            "created_fact_ids": [kwargs["replacement_fact"].id],
        }


class FakeEmbedding:
    def __init__(self):
        self.calls = []

    async def encode_single(self, text):
        self.calls.append(text)
        return [float(len(text)), 0.5]


class FailingRedis:
    async def sadd(self, *args):
        raise RuntimeError("redis down")


@pytest.mark.no_network
async def test_fact_change_service_remove_validates_required_scope():
    service = FactChangeService(FakeKnowledgeStore(), FakeEmbedding())

    with pytest.raises(ValueError, match="requires user_name scope"):
        await service.remove_fact(
            user_name="",
            project_id="project-1",
            entity_id=2,
            fact_id="fact-1",
            actor="ada",
            reason="not_true",
        )

    with pytest.raises(ValueError, match="requires reason scope"):
        await service.remove_fact(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_id="fact-1",
            actor="ada",
            reason="",
        )


@pytest.mark.no_network
async def test_fact_change_service_remove_calls_store_and_marks_dirty(monkeypatch):
    events = []

    async def fake_emit(*args, **kwargs):
        events.append((args, kwargs))

    monkeypatch.setattr(service_module, "emit", fake_emit)
    store = FakeKnowledgeStore()
    redis = FakeRedis()
    profile_key = RedisKeys.project_profile_complete("ada", "project-1")
    await redis.set(profile_key, "done")
    service = FactChangeService(store, FakeEmbedding(), redis=redis)

    result = await service.remove_fact(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_id="fact-1",
        actor="ada",
        reason="not_true",
        session_id="session-1",
    )

    call = store.remove_calls[0]
    assert call["change_type"] == "manual_remove"
    assert call["fact_id"] == "fact-1"
    assert call["session_id"] == "session-1"
    assert result["dirty_marked"] is True
    dirty_key = RedisKeys.dirty_entities("ada", "project-1")
    assert await redis.smembers(dirty_key) == {"2"}
    assert profile_key in redis.deleted_keys
    assert events[0][0][2] == "dirty_entities_marked"
    assert events[0][0][3]["reason"] == "fact_change"


@pytest.mark.no_network
async def test_fact_change_service_replace_embeds_replacement_and_passes_fact_record():
    store = FakeKnowledgeStore()
    embedding = FakeEmbedding()
    service = FactChangeService(store, embedding)

    result = await service.replace_facts(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_ids=["fact-1"],
        replacement_content=" Ada uses Linear. ",
        actor="ada",
        reason="wrong_tool",
    )

    call = store.replace_calls[0]
    replacement = call["replacement_fact"]
    assert isinstance(replacement, FactRecord)
    assert embedding.calls == ["Ada uses Linear."]
    assert replacement.content == "Ada uses Linear."
    assert replacement.embedding == [16.0, 0.5]
    assert replacement.source_entity_id == 2
    assert replacement.source == "user"
    assert call["change_type"] == "manual_correction"
    assert call["replacement_content"] == "Ada uses Linear."
    assert result["dirty_marked"] is False


@pytest.mark.no_network
async def test_fact_change_service_replace_supports_fact_merge_change_type():
    store = FakeKnowledgeStore()
    service = FactChangeService(store, FakeEmbedding())

    await service.replace_facts(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_ids=["fact-1", "fact-2"],
        replacement_content="Ada uses both Linear and Notion.",
        actor="ada",
        reason="duplicate",
        change_type="fact_merge",
    )

    assert store.replace_calls[0]["change_type"] == "fact_merge"


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("operation", "change_type", "expected_call"),
    [
        ("remove", "manual_remove", "remove"),
        ("replace", "manual_correction", "replace"),
        ("replace", "fact_merge", "replace"),
        ("remove", "bad_extraction_report", "remove"),
        ("replace", "bad_extraction_report", "replace"),
        ("remove", "admin_recovery", "remove"),
        ("replace", "admin_recovery", "replace"),
    ],
)
async def test_fact_change_service_operation_change_type_mapping(
    operation, change_type, expected_call
):
    store = FakeKnowledgeStore()
    service = FactChangeService(store, FakeEmbedding())

    if operation == "remove":
        await service.remove_fact(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_id="fact-1",
            actor="ada",
            reason="manual review",
            change_type=change_type,
        )
    else:
        await service.replace_facts(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1"],
            replacement_content="Ada uses Linear.",
            actor="ada",
            reason="manual review",
            change_type=change_type,
        )

    calls = store.remove_calls if expected_call == "remove" else store.replace_calls
    assert calls[0]["change_type"] == change_type


@pytest.mark.no_network
async def test_fact_change_service_supports_bad_extraction_change_type():
    store = FakeKnowledgeStore()
    service = FactChangeService(store, FakeEmbedding())

    await service.remove_fact(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_id="fact-1",
        actor="ada",
        reason="misread_source",
        change_type="bad_extraction_report",
    )
    await service.replace_facts(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_ids=["fact-1"],
        replacement_content="Ada uses Linear.",
        actor="ada",
        reason="misread_source",
        change_type="bad_extraction_report",
    )

    assert store.remove_calls[0]["change_type"] == "bad_extraction_report"
    assert store.replace_calls[0]["change_type"] == "bad_extraction_report"


@pytest.mark.no_network
async def test_fact_change_service_rejects_invalid_entity_and_empty_inputs():
    service = FactChangeService(FakeKnowledgeStore(), FakeEmbedding())

    with pytest.raises(ValueError, match="positive entity_id"):
        await service.remove_fact(
            user_name="ada",
            project_id="project-1",
            entity_id=0,
            fact_id="fact-1",
            actor="ada",
            reason="not_true",
        )

    with pytest.raises(ValueError, match="requires fact_id scope"):
        await service.remove_fact(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_id="",
            actor="ada",
            reason="not_true",
        )

    with pytest.raises(ValueError, match="requires fact_ids"):
        await service.replace_facts(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=[],
            replacement_content="Ada uses Linear.",
            actor="ada",
            reason="wrong_tool",
        )

    with pytest.raises(ValueError, match="requires replacement_content scope"):
        await service.replace_facts(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1"],
            replacement_content="   ",
            actor="ada",
            reason="wrong_tool",
        )


@pytest.mark.no_network
async def test_fact_change_service_rejects_unsupported_change_types_and_duplicates():
    service = FactChangeService(FakeKnowledgeStore(), FakeEmbedding())

    with pytest.raises(ValueError, match="does not support change_type"):
        await service.remove_fact(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_id="fact-1",
            actor="ada",
            reason="not_true",
            change_type="fact_merge",
        )

    with pytest.raises(ValueError, match="duplicate fact_ids"):
        await service.replace_facts(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1", "fact-1"],
            replacement_content="Ada uses Linear.",
            actor="ada",
            reason="wrong_tool",
        )

    with pytest.raises(ValueError, match="does not support change_type"):
        await service.replace_facts(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1"],
            replacement_content="Ada uses Linear.",
            actor="ada",
            reason="wrong_tool",
            change_type="manual_remove",
        )

    with pytest.raises(ValueError, match="does not support change_type"):
        await service.remove_fact(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_id="fact-1",
            actor="ada",
            reason="not_true",
            change_type="user_remove",
        )

    with pytest.raises(ValueError, match="does not support change_type"):
        await service.replace_facts(
            user_name="ada",
            project_id="project-1",
            entity_id=2,
            fact_ids=["fact-1"],
            replacement_content="Ada uses Linear.",
            actor="ada",
            reason="wrong_tool",
            change_type="user_correction",
        )


@pytest.mark.no_network
async def test_fact_change_service_redis_failure_does_not_fail_remove():
    service = FactChangeService(
        FakeKnowledgeStore(), FakeEmbedding(), redis=FailingRedis()
    )

    result = await service.remove_fact(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_id="fact-1",
        actor="ada",
        reason="not_true",
    )

    assert result["dirty_marked"] is False


@pytest.mark.no_network
async def test_fact_change_service_redis_failure_does_not_fail_replace():
    service = FactChangeService(
        FakeKnowledgeStore(), FakeEmbedding(), redis=FailingRedis()
    )

    result = await service.replace_facts(
        user_name="ada",
        project_id="project-1",
        entity_id=2,
        fact_ids=["fact-1"],
        replacement_content="Ada uses Linear.",
        actor="ada",
        reason="wrong_tool",
    )

    assert result["dirty_marked"] is False
