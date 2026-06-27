import pytest

from knoggin_server.knowledge.services.entity_merge_service import EntityMergeService


def entity(entity_id, *, entity_type="person", name=None, aliases=None):
    return {
        "entity_id": entity_id,
        "canonical_name": name or f"Entity {entity_id}",
        "type": entity_type,
        "aliases": aliases or [],
    }


def fact(fact_id, entity_id, content="Shared evidence"):
    return {
        "fact_id": fact_id,
        "entity_id": entity_id,
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

    async def merge_entities(
        self, primary_id, duplicate_id, *, project_id
    ):
        self.merges.append((primary_id, duplicate_id, project_id))
        return self.merge_result


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
    assert result["policy_checks"]["automatic_execution_enabled"] is False
    insert = next(call for call in postgres.calls if call[0] == "execute")
    assert "INSERT INTO entity_merge_proposals" in insert[1]
    assert insert[2][5] == '["fact-2", "fact-3"]'
