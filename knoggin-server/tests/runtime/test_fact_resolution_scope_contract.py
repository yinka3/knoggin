import pytest

from common.schema.contracts import FactMergeResult, SkippedFactChange
from common.schema.primitives import Fact, FactRecord
from common.utils.time_utils import get_now
from knoggin_server.knowledge.services.fact_resolution import FactResolver


class RecordingKnowledgeStore:
    def __init__(self, *, fail_create=False, fail_audit=False):
        self.create_calls = []
        self.invalidate_calls = []
        self.audit_calls = []
        self.fail_create = fail_create
        self.fail_audit = fail_audit

    async def create_facts_batch(
        self,
        entity_id,
        facts,
        *,
        user_name,
        project_id,
        session_id=None,
    ):
        if self.fail_create:
            raise RuntimeError("write failed")
        self.create_calls.append(
            {
                "entity_id": entity_id,
                "facts": list(facts),
                "user_name": user_name,
                "session_id": session_id,
                "project_id": project_id,
            }
        )
        return len(facts)

    async def invalidate_fact(self, fact_id, invalid_at, *, project_id):
        self.invalidate_calls.append(
            {
                "fact_id": fact_id,
                "invalid_at": invalid_at,
                "project_id": project_id,
            }
        )
        return True

    async def create_applied_fact_change_audit(self, **kwargs):
        if self.fail_audit:
            raise RuntimeError("audit failed")
        self.audit_calls.append(kwargs)


class FakeEmbedding:
    async def encode_single(self, content):
        return [float(len(content)), 0.1]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_uses_source_session_map_for_project_context():
    knowledge_store = RecordingKnowledgeStore()
    merge_result = FactMergeResult(
        new_contents=[
            Fact(content="Alice uses Linear.", source_msg_id=1),
            Fact(content="Bob uses Notion.", source_msg_id=2),
        ]
    )

    summary = await FactResolver.apply_fact_changes(
        101,
        merge_result,
        existing_facts=[],
        valid_msg_ids={1, 2},
        session_id="project-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        source_session_by_msg_id={1: "session-a", 2: "session-b"},
    )

    call = knowledge_store.create_calls[0]
    facts = call["facts"]

    assert call["session_id"] is None
    assert call["project_id"] == "project-1"
    assert [fact.source_session_id for fact in facts] == ["session-a", "session-b"]
    assert [fact.source_user_name for fact in facts] == ["ada", "ada"]
    assert [fact.source_msg_id for fact in summary.created_facts] == [1, 2]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_audits_profile_extraction_created_facts():
    knowledge_store = RecordingKnowledgeStore()
    merge_result = FactMergeResult(
        new_contents=[
            Fact(content="Alice uses Linear.", source_msg_id=1),
        ],
        skipped=[
            SkippedFactChange(
                content="duplicate", reason="duplicate", metadata={"x": True}
            )
        ],
    )

    await FactResolver.apply_fact_changes(
        101,
        merge_result,
        existing_facts=[],
        valid_msg_ids={1},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        source_session_by_msg_id={1: "session-1"},
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
        fact_change_id="change-1",
    )

    audit = knowledge_store.audit_calls[0]
    assert audit["fact_change_id"] == "change-1"
    assert audit["change_type"] == "profile_extraction"
    assert audit["actor"] == "profile_refinement"
    assert audit["source_msg_ids"] == [1]
    assert len(audit["created_fact_ids"]) == 1
    assert audit["invalidated_fact_ids"] == []
    assert audit["invalidated_fact_snapshots"] == []
    assert audit["metadata"]["skipped"][0]["reason"] == "duplicate"


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_audits_profile_extraction_invalidated_facts():
    knowledge_store = RecordingKnowledgeStore()
    existing = FactRecord(
        id="fact-old",
        content="Alice uses Trello.",
        source_entity_id=101,
        source_msg_id=7,
        source_user_name="ada",
        source_session_id="session-1",
        valid_at=get_now(),
    )
    merge_result = FactMergeResult(
        to_invalidate=["fact-old"],
        missing_targets=[
            SkippedFactChange(content="missing", reason="invalidates_target_not_found")
        ],
    )

    await FactResolver.apply_fact_changes(
        101,
        merge_result,
        existing_facts=[existing],
        valid_msg_ids={7},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    audit = knowledge_store.audit_calls[0]
    assert audit["created_fact_ids"] == []
    assert audit["invalidated_fact_ids"] == ["fact-old"]
    assert audit["source_msg_ids"] == [7]
    assert audit["invalidated_fact_snapshots"] == [
        {
            "fact_id": "fact-old",
            "entity_id": 101,
            "user_name": "ada",
            "project_id": "project-1",
            "content": "Alice uses Trello.",
            "valid_at": existing.valid_at,
            "invalid_at": None,
            "confidence": 1.0,
            "source_msg_id": 7,
            "source_user_name": "ada",
            "source_session_id": "session-1",
            "source": "user",
        }
    ]
    assert audit["metadata"]["missing_targets"][0]["reason"] == (
        "invalidates_target_not_found"
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_does_not_audit_noop_profile_extraction():
    knowledge_store = RecordingKnowledgeStore()

    await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(),
        existing_facts=[],
        valid_msg_ids=set(),
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    assert knowledge_store.audit_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_write_failure_skips_invalidations_and_audit():
    knowledge_store = RecordingKnowledgeStore(fail_create=True)

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(
            to_invalidate=["fact-old"],
            new_contents=[Fact(content="Alice uses Linear.", source_msg_id=1)],
        ),
        existing_facts=[],
        valid_msg_ids={1},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    assert summary.write_failed is True
    assert knowledge_store.invalidate_calls == []
    assert knowledge_store.audit_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_audit_failure_does_not_fail_profile_extraction():
    knowledge_store = RecordingKnowledgeStore(fail_audit=True)

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(
            new_contents=[Fact(content="Alice uses Linear.", source_msg_id=1)]
        ),
        existing_facts=[],
        valid_msg_ids={1},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        source_session_by_msg_id={1: "session-1"},
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    assert summary.write_failed is False
    assert len(summary.created_facts) == 1
    assert len(knowledge_store.create_calls) == 1
    assert knowledge_store.audit_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_rejects_missing_scope_before_noop():
    merge_result = FactMergeResult()

    with pytest.raises(ValueError, match="requires user_name scope"):
        await FactResolver.apply_fact_changes(
            101,
            merge_result,
            existing_facts=[],
            valid_msg_ids=set(),
            session_id="session-1",
            knowledge_store=RecordingKnowledgeStore(),
            embedding_service=FakeEmbedding(),
            llm=object(),
            user_name="",
            project_id="project-1",
        )

    with pytest.raises(ValueError, match="requires project_id scope"):
        await FactResolver.apply_fact_changes(
            101,
            merge_result,
            existing_facts=[],
            valid_msg_ids=set(),
            session_id="session-1",
            knowledge_store=RecordingKnowledgeStore(),
            embedding_service=FakeEmbedding(),
            llm=object(),
            user_name="ada",
            project_id="",
        )
