from datetime import timedelta

import pytest

from common.utils.time_utils import get_now
from core.knowledge.entity.merge_service import EntityMergeService
from tests.fixtures.fakes import RecordingPostgresClient


def entity(
    entity_id,
    *,
    entity_type="person",
    name=None,
    aliases=None,
    user_name="ada",
    project_id="project-1",
):
    return {
        "entity_id": entity_id,
        "user_name": user_name,
        "project_id": project_id,
        "canonical_name": name or f"Entity {entity_id}",
        "type": entity_type,
        "aliases": aliases or [],
    }


def fact(
    fact_id,
    entity_id,
    content="Shared evidence",
    *,
    user_name="ada",
    project_id="project-1",
):
    return {
        "fact_id": fact_id,
        "entity_id": entity_id,
        "user_name": user_name,
        "project_id": project_id,
        "content": content,
        "invalid_at": None,
    }


class RecordingPostgres:
    def __init__(self, fetch_results=None, execute_results=None):
        self.fetch_results = list(fetch_results or [])
        self.execute_results = list(execute_results or [])
        self.calls = []

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        return self.fetch_results.pop(0) if self.fetch_results else []

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        return self.execute_results.pop(0) if self.execute_results else 1


class RecordingKnowledgeStore:
    def __init__(self, merge_result=True):
        self.merge_result = merge_result
        self.merges = []
        self.projection_rebuilds = []

    async def merge_entities(
        self, primary_id, duplicate_id, *, project_id
    ):
        self.merges.append((primary_id, duplicate_id, project_id))
        return self.merge_result

    async def rebuild_project_projection(self, project_id, user_name):
        self.projection_rebuilds.append((project_id, user_name))
        return {"entities": 2}


def snapshot_results(
    *,
    entities=None,
    facts=None,
    relationships=None,
    hierarchy=None,
):
    return [
        entities or [entity(2), entity(3)],
        facts or [fact("fact-2", 2), fact("fact-3", 3)],
        relationships or [],
        hierarchy or [],
    ]


@pytest.mark.no_network
@pytest.mark.parametrize(
    ("primary_id", "duplicate_id", "reason"),
    [
        (2, 2, "An entity cannot be merged into itself."),
        (1, 2, "The protected identity entity cannot be merged."),
        (2, 3, "Merge reasoning is required."),
    ],
)
async def test_merge_proposal_rejects_basic_guardrails_without_db_access(
    primary_id,
    duplicate_id,
    reason,
):
    postgres = RecordingPostgres()
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=primary_id,
        duplicate_id=duplicate_id,
        evidence_fact_ids=["fact-2"],
        reasoning="" if reason == "Merge reasoning is required." else "same",
    )

    assert result["policy_result"] == "rejected"
    assert result["reason"] == reason
    assert postgres.calls == []


@pytest.mark.no_network
async def test_merge_proposal_rejects_type_conflict():
    postgres = RecordingPostgres(
        snapshot_results(entities=[entity(2), entity(3, entity_type="company")])
    )
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2"],
        reasoning="Names look similar.",
    )

    assert result["policy_result"] == "rejected"
    assert "types conflict" in result["reason"]
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_merge_proposal_rejects_missing_or_invisible_candidates():
    postgres = RecordingPostgres(snapshot_results(entities=[entity(2)]))
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2"],
        reasoning="Possible duplicate.",
    )

    assert result["policy_result"] == "rejected"
    assert result["policy_checks"] == {
        "entities_exist_in_project": False,
        "entities_visible_in_authorized_scope": False,
    }
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_merge_proposal_rejects_wrong_user_or_project_scope():
    postgres = RecordingPostgres(
        snapshot_results(
            entities=[entity(2), entity(3, user_name="grace")],
            facts=[fact("fact-2", 2), fact("fact-3", 3, user_name="grace")],
        )
    )
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2", "fact-3"],
        reasoning="Possible duplicate.",
    )

    assert result["policy_result"] == "rejected"
    assert result["policy_checks"]["entities_exist_in_project"] is True
    assert result["policy_checks"]["entities_visible_in_authorized_scope"] is False
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_merge_proposal_requires_candidate_owned_evidence():
    postgres = RecordingPostgres(snapshot_results())
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-from-another-entity"],
        reasoning="Possible duplicate.",
    )

    assert result["policy_result"] == "rejected"
    assert result["policy_checks"]["missing_evidence_fact_ids"] == [
        "fact-from-another-entity"
    ]


@pytest.mark.no_network
async def test_merge_proposal_rejects_conflicting_stable_identifiers():
    postgres = RecordingPostgres(
        snapshot_results(
            facts=[
                fact("fact-2", 2, "Email ada@example.com"),
                fact("fact-3", 3, "Email grace@example.com"),
            ]
        )
    )
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2", "fact-3"],
        reasoning="Names overlap.",
    )

    assert result["policy_result"] == "rejected"
    assert "email" in result["policy_checks"]["stable_identifier_conflicts"]


@pytest.mark.no_network
async def test_merge_proposal_rejects_conflicting_stable_identifiers_from_aliases():
    postgres = RecordingPostgres(
        snapshot_results(
            entities=[
                entity(2, aliases=["ada@example.com"]),
                entity(3, aliases=["grace@example.com"]),
            ]
        )
    )
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2", "fact-3"],
        reasoning="Names overlap.",
    )

    assert result["policy_result"] == "rejected"
    assert "email" in result["policy_checks"]["stable_identifier_conflicts"]


@pytest.mark.no_network
async def test_merge_proposal_allows_alias_mismatch_without_identifier_conflict():
    postgres = RecordingPostgres(
        snapshot_results(
            entities=[
                entity(2, aliases=["Ada", "A. Lovelace"]),
                entity(3, aliases=["Countess Lovelace"]),
            ]
        )
    )
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2", "fact-3"],
        reasoning="Aliases are different but compatible.",
        model_confidence=1.0,
    )

    assert result["policy_result"] == "confirmation_required"
    assert result["policy_checks"]["stable_identifiers_compatible"] is True
    assert result["policy_checks"]["important_facts_and_timelines"] == (
        "confirmation_required"
    )
    assert result["policy_checks"]["model_confidence_is_advisory"] is True


@pytest.mark.no_network
async def test_valid_merge_proposal_is_persisted_for_manual_confirmation():
    postgres = RecordingPostgres(snapshot_results())
    service = EntityMergeService(postgres, RecordingKnowledgeStore())

    result = await service.propose(
        user_name="ada",
        project_id="project-1",
        primary_id=2,
        duplicate_id=3,
        evidence_fact_ids=["fact-2", "fact-2", "fact-3"],
        reasoning="Both profiles describe the same person.",
        model_confidence=0.94,
    )

    assert result["policy_result"] == "confirmation_required"
    assert result["confirmation_token"]
    assert "automatic_execution_enabled" not in result["policy_checks"]
    assert result["policy_checks"]["important_facts_and_timelines"] == (
        "confirmation_required"
    )
    insert = next(call for call in postgres.calls if call[0] == "execute")
    assert "INSERT INTO entity_merge_proposals" in insert[1]
    assert insert[2][5] == '["fact-2", "fact-3"]'


def rollback_audit(*, before_state=None, after_state=None, **overrides):
    audit = {
        "audit_id": "audit-1",
        "proposal_id": "proposal-1",
        "user_name": "ada",
        "project_id": "project-1",
        "primary_entity_id": 2,
        "duplicate_entity_id": 3,
        "evidence_fact_ids": ["fact-2"],
        "reasoning": "Same person.",
        "confirmed_by": "ada",
        "before_state": before_state or rollback_before_state(),
        "after_state": after_state or rollback_after_state(),
        "status": "executed",
        "failure_reason": None,
        "rollback_status": "available",
        "rollback_expires_at": get_now() + timedelta(hours=1),
        "rolled_back_at": None,
        "rolled_back_by": None,
        "rollback_failure_reason": None,
    }
    audit.update(overrides)
    return audit


def rollback_before_state():
    return {
        "entities": [
            {
                **entity(2, name="Ada Lovelace", aliases=["Ada"]),
                "session_id": "session-1",
                "topic": "People",
                "confidence": 0.8,
                "last_mentioned_ms": 100,
                "last_updated_ms": 120,
                "last_profiled_msg_id": 10,
            },
            {
                **entity(3, name="Countess Lovelace", aliases=["Augusta"]),
                "session_id": "session-1",
                "topic": "People",
                "confidence": 0.7,
                "last_mentioned_ms": 90,
                "last_updated_ms": 110,
                "last_profiled_msg_id": 9,
            },
        ],
        "facts": [
            fact("fact-2", 2, "Ada fact"),
            fact("fact-3", 3, "Duplicate fact"),
        ],
        "relationships": [
            {
                "relationship_id": "project-1:3:9",
                "user_name": "ada",
                "project_id": "project-1",
                "entity_a_id": 3,
                "entity_b_id": 9,
                "weight": 1,
                "confidence": 0.9,
                "context": "worked with",
                "last_seen_ms": 100,
                "evidence_refs": [
                    {
                        "user_name": "ada",
                        "session_id": "session-1",
                        "message_id": 11,
                    }
                ],
            }
        ],
        "hierarchy": [
            {
                "project_id": "project-1",
                "parent_id": 9,
                "child_id": 3,
                "created_at_ms": 100,
            }
        ],
    }


def rollback_after_state():
    return {
        "entities": [
            {
                **entity(2, name="Ada Lovelace", aliases=["Ada", "Augusta"]),
                "session_id": "session-1",
                "topic": "People",
                "confidence": 0.8,
                "last_mentioned_ms": 100,
                "last_updated_ms": 120,
                "last_profiled_msg_id": 10,
            }
        ],
        "facts": [
            fact("fact-2", 2, "Ada fact"),
            fact("fact-3", 2, "Duplicate fact"),
        ],
        "relationships": [],
        "hierarchy": [],
    }


@pytest.mark.no_network
async def test_rollback_restores_audited_before_state_and_rebuilds_projection():
    after_state = rollback_after_state()
    postgres = RecordingPostgresClient(
        fetch_all_results=[
            [rollback_audit(after_state=after_state)],
            *snapshot_results(
                entities=after_state["entities"],
                facts=after_state["facts"],
                relationships=[],
                hierarchy=[],
            ),
        ]
    )
    store = RecordingKnowledgeStore()
    service = EntityMergeService(postgres, store)

    result = await service.rollback("audit-1", "admin")

    assert result["policy_result"] == "rolled_back"
    assert result["search_rebuild_required"] is True
    assert store.projection_rebuilds == [("project-1", "ada")]
    executed_sql = "\n".join(call[1] for call in postgres.calls)
    assert "INSERT INTO entities" in executed_sql
    assert "INSERT INTO facts" in executed_sql
    assert "INSERT INTO relationships" in executed_sql
    assert "INSERT INTO relationship_evidence_refs" in executed_sql
    assert "INSERT INTO hierarchy_edges" in executed_sql
    assert "rollback_status = 'rolled_back'" in executed_sql


@pytest.mark.no_network
async def test_rollback_rejects_non_executed_audit_without_writes():
    postgres = RecordingPostgresClient(
        fetch_all_results=[[rollback_audit(status="failed")]]
    )
    result = await EntityMergeService(
        postgres,
        RecordingKnowledgeStore(),
    ).rollback("audit-1", "admin")

    assert result["policy_result"] == "rejected"
    assert "executed" in result["reason"]
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_rollback_rejects_expired_or_missing_state():
    postgres = RecordingPostgresClient(
        fetch_all_results=[
            [
                rollback_audit(
                    rollback_status="expired",
                    before_state=None,
                    after_state=None,
                )
            ]
        ]
    )
    result = await EntityMergeService(
        postgres,
        RecordingKnowledgeStore(),
    ).rollback("audit-1", "admin")

    assert result["policy_result"] == "rejected"
    assert "expired" in result["reason"]
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_rollback_rejects_available_audit_with_missing_state():
    audit = rollback_audit()
    audit["before_state"] = None
    postgres = RecordingPostgresClient(fetch_all_results=[[audit]])

    result = await EntityMergeService(
        postgres,
        RecordingKnowledgeStore(),
    ).rollback("audit-1", "admin")

    assert result["policy_result"] == "rejected"
    assert "missing" in result["reason"]
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_rollback_rejects_already_rolled_back_audit_idempotently():
    postgres = RecordingPostgresClient(
        fetch_all_results=[[rollback_audit(rollback_status="rolled_back")]]
    )
    result = await EntityMergeService(
        postgres,
        RecordingKnowledgeStore(),
    ).rollback("audit-1", "admin")

    assert result["policy_result"] == "rejected"
    assert "already" in result["reason"]
    assert not any(call[0] == "execute" for call in postgres.calls)


@pytest.mark.no_network
async def test_rollback_rejects_duplicate_entity_id_reuse_and_records_failure():
    after_state = rollback_after_state()
    reused_entities = [*after_state["entities"], rollback_before_state()["entities"][1]]
    postgres = RecordingPostgresClient(
        fetch_all_results=[
            [rollback_audit(after_state=after_state)],
            *snapshot_results(
                entities=reused_entities,
                facts=after_state["facts"],
                relationships=[],
                hierarchy=[],
            ),
        ]
    )

    result = await EntityMergeService(
        postgres,
        RecordingKnowledgeStore(),
    ).rollback("audit-1", "admin")

    assert result["policy_result"] == "rejected"
    assert "reused" in result["reason"]
    assert any("rollback_status = 'failed'" in call[1] for call in postgres.calls)


@pytest.mark.no_network
async def test_rollback_rejects_changed_post_merge_state_and_records_failure():
    after_state = rollback_after_state()
    changed_facts = [fact("fact-2", 2, "Changed after merge")]
    postgres = RecordingPostgresClient(
        fetch_all_results=[
            [rollback_audit(after_state=after_state)],
            *snapshot_results(
                entities=after_state["entities"],
                facts=changed_facts,
                relationships=[],
                hierarchy=[],
            ),
        ]
    )

    result = await EntityMergeService(
        postgres,
        RecordingKnowledgeStore(),
    ).rollback("audit-1", "admin")

    assert result["policy_result"] == "rejected"
    assert "differs" in result["reason"]
    assert any("rollback_status = 'failed'" in call[1] for call in postgres.calls)
