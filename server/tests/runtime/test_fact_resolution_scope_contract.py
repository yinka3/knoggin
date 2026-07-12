import pytest

from common.schema.contracts import (
    BulkContradictionResult,
    ContradictionJudgment,
    FactMergeResult,
    SkippedFactChange,
)
from common.schema.primitives import Fact, FactRecord
from common.utils.time_utils import get_now
from core.knowledge.services.embedding_service import TextPairClassification
from core.knowledge.services.fact_resolution import FactResolver


class RecordingKnowledgeStore:
    def __init__(self, *, fail_create=False, fail_audit=False):
        self.create_calls = []
        self.invalidate_calls = []
        self.audit_calls = []
        self.transactional_change_calls = []
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

    async def apply_fact_changes_with_audit(self, **kwargs):
        if self.fail_create:
            raise RuntimeError("write failed")
        if self.fail_audit:
            raise RuntimeError("audit failed")
        self.transactional_change_calls.append(kwargs)
        return {
            "fact_change_id": kwargs["fact_change_id"],
            "entity_id": kwargs["entity_id"],
            "invalidated_fact_ids": kwargs["fact_ids_to_invalidate"],
            "created_fact_ids": [fact.id for fact in kwargs["facts_to_create"]],
        }


class FakeEmbedding:
    async def encode_single(self, content):
        return [float(len(content)), 0.1]

    async def classify_text_pairs(self, pairs, batch_size=None):
        return [
            TextPairClassification(
                premise=premise,
                hypothesis=hypothesis,
                label="neutral",
                scores={"neutral": 1.0},
            )
            for premise, hypothesis in pairs
        ]


class FakeContradictionLLM:
    async def generate_structured(self, **kwargs):
        return BulkContradictionResult(
            judgments=[ContradictionJudgment(index=1, is_contradiction=True)]
        )


class FakeNoContradictionLLM:
    async def generate_structured(self, **kwargs):
        return BulkContradictionResult(judgments=[])


class NLIContradictionEmbedding(FakeEmbedding):
    async def encode_single(self, content):
        return [1.0, 0.0]

    async def classify_text_pairs(self, pairs, batch_size=None):
        return [
            TextPairClassification(
                premise=premise,
                hypothesis=hypothesis,
                label="contradiction",
                scores={"contradiction": 0.99, "neutral": 0.01},
            )
            for premise, hypothesis in pairs
        ]


class HighSimilarityEmbedding(FakeEmbedding):
    async def encode_single(self, content):
        return [1.0, 0.0]


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
        llm=FakeNoContradictionLLM(),
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
async def test_fact_resolution_skips_new_fact_with_invalid_source_msg_id():
    knowledge_store = RecordingKnowledgeStore()

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(
            new_contents=[Fact(content="Alice uses Linear.", source_msg_id=99)]
        ),
        existing_facts=[],
        valid_msg_ids={1},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
    )

    assert summary.created_facts == []
    assert summary.invalid_source_msg_ids == [99]
    assert knowledge_store.create_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_skips_new_fact_without_source_msg_id():
    knowledge_store = RecordingKnowledgeStore()

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(new_contents=[Fact(content="Alice uses Linear.")]),
        existing_facts=[],
        valid_msg_ids={1},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
    )

    assert summary.created_facts == []
    assert summary.invalid_source_msg_ids == []
    assert knowledge_store.create_calls == []


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
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
        source_session_by_msg_id={1: "session-1"},
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
        fact_change_id="change-1",
    )

    change = knowledge_store.transactional_change_calls[0]
    assert change["fact_change_id"] == "change-1"
    assert change["change_type"] == "profile_extraction"
    assert change["actor"] == "profile_refinement"
    assert len(change["facts_to_create"]) == 1
    assert change["fact_ids_to_invalidate"] == []
    assert change["metadata"]["skipped"][0]["reason"] == "duplicate"


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
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    change = knowledge_store.transactional_change_calls[0]
    assert change["facts_to_create"] == []
    assert change["fact_ids_to_invalidate"] == ["fact-old"]
    assert change["metadata"]["missing_targets"][0]["reason"] == (
        "invalidates_target_not_found"
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_filters_invalidation_ids_to_active_existing_facts():
    knowledge_store = RecordingKnowledgeStore()
    existing = FactRecord(
        id="fact-old",
        content="Alice uses Trello.",
        source_entity_id=101,
        source_msg_id=7,
        valid_at=get_now(),
    )

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(to_invalidate=["fact-old", "foreign-fact"]),
        existing_facts=[existing],
        valid_msg_ids={7},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=FakeEmbedding(),
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
    )

    assert summary.invalidated_fact_ids == ["fact-old"]
    assert [call["fact_id"] for call in knowledge_store.invalidate_calls] == [
        "fact-old"
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_nli_candidate_reaches_llm_when_embedding_is_low():
    knowledge_store = RecordingKnowledgeStore()
    existing = FactRecord(
        id="fact-old",
        content="Alice uses Notion.",
        source_entity_id=101,
        source_msg_id=1,
        valid_at=get_now(),
        embedding=[0.0, 1.0],
    )

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(
            new_contents=[Fact(content="Alice does not use Notion.", source_msg_id=2)]
        ),
        existing_facts=[existing],
        valid_msg_ids={2},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=NLIContradictionEmbedding(),
        llm=FakeContradictionLLM(),
        user_name="ada",
        project_id="project-1",
    )

    assert summary.contradicted_fact_ids == ["fact-old"]
    assert summary.invalidated_fact_ids == ["fact-old"]
    assert summary.contradiction_candidate_diagnostics == [
        {
            "new_content": "Alice does not use Notion.",
            "candidate_fact_id": "fact-old",
            "candidate_content": "Alice uses Notion.",
            "sources": ["nli"],
            "embedding_similarity": 0.0,
            "nli_label": "contradiction",
            "nli_scores": {"contradiction": 0.99, "neutral": 0.01},
        }
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_high_similarity_still_reaches_llm():
    knowledge_store = RecordingKnowledgeStore()
    existing = FactRecord(
        id="fact-old",
        content="Alice uses Notion.",
        source_entity_id=101,
        source_msg_id=1,
        valid_at=get_now(),
        embedding=[1.0, 0.0],
    )

    summary = await FactResolver.apply_fact_changes(
        101,
        FactMergeResult(
            new_contents=[Fact(content="Alice does not use Notion.", source_msg_id=2)]
        ),
        existing_facts=[existing],
        valid_msg_ids={2},
        session_id="session-1",
        knowledge_store=knowledge_store,
        embedding_service=HighSimilarityEmbedding(),
        llm=FakeContradictionLLM(),
        user_name="ada",
        project_id="project-1",
    )

    assert summary.contradicted_fact_ids == ["fact-old"]
    assert summary.invalidated_fact_ids == ["fact-old"]
    assert summary.contradiction_candidate_diagnostics[0]["sources"] == [
        "embedding"
    ]
    assert summary.contradiction_candidate_diagnostics[0][
        "embedding_similarity"
    ] == 1.0


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
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    assert knowledge_store.transactional_change_calls == []


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
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    assert summary.write_failed is True
    assert knowledge_store.invalidate_calls == []
    assert knowledge_store.transactional_change_calls == []


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_audit_failure_rolls_back_profile_extraction():
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
        llm=FakeNoContradictionLLM(),
        user_name="ada",
        project_id="project-1",
        source_session_by_msg_id={1: "session-1"},
        audit_change_type="profile_extraction",
        actor="profile_refinement",
        reason="profile_extraction",
    )

    assert summary.write_failed is True
    assert summary.created_facts == []
    assert knowledge_store.create_calls == []
    assert knowledge_store.transactional_change_calls == []


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
            llm=FakeNoContradictionLLM(),
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
            llm=FakeNoContradictionLLM(),
            user_name="ada",
            project_id="",
        )
