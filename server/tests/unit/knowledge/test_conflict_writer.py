from contextlib import asynccontextmanager

import pytest

from core.knowledge.db.writers.conflict_writer import ConflictWriter


def _group() -> dict:
    return {
        "conflict_id": "conflict-1",
        "user_name": "ada",
        "project_id": "project-1",
        "status": "open",
        "origin": "background_discovery",
        "kind": "possible_contradiction",
        "rationale": "The two observations disagree.",
        "confidence": 0.8,
        "evidence_signature": "signature",
        "metadata": {},
        "resolution_kind": None,
        "resolution_note": None,
    }


def _observation(observation_id: int) -> dict:
    return {
        "observation_id": observation_id,
        "relationship_id": f"rel-{observation_id}",
        "message_id": observation_id,
        "session_id": "session-1",
        "source_entity_id": 1,
        "source_entity_name": "Ada",
        "target_entity_id": 2,
        "target_entity_name": "Acme",
        "observed_relationship_label": "works at",
        "canonical_relationship_type": None,
        "domain_status": "unrecognized",
        "confidence": 0.9,
        "context": "Ada works at Acme.",
        "observed_at_ms": observation_id,
    }


class Cursor:
    def __init__(self, client) -> None:
        self.client = client
        self.rowcount = 0

    async def execute(self, query, params=None):
        self.client.calls.append((query, params))
        if "INSERT INTO public.conflict_evidence_refs" in query:
            self.rowcount = self.client.evidence_rowcounts.pop(0)
        else:
            self.rowcount = 1

    async def fetchone(self):
        return self.client.one.pop(0) if self.client.one else None

    async def fetchall(self):
        return self.client.all.pop(0) if self.client.all else []


class Client:
    def __init__(self, *, one, all, evidence_rowcounts):
        self.one = list(one)
        self.all = list(all)
        self.evidence_rowcounts = list(evidence_rowcounts)
        self.calls = []

    @asynccontextmanager
    async def transaction(self):
        yield Cursor(self)


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_writer_creates_a_durable_group_evidence_and_human_review():
    client = Client(
        one=[_group()],
        all=[[_observation(101), _observation(104)], []],
        evidence_rowcounts=[1, 1],
    )

    result = await ConflictWriter(client).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="background_discovery",
        kind="possible_contradiction",
        rationale="The two observations disagree.",
        confidence=0.8,
        evidence_ids=[101, 104],
    )

    assert result.created
    assert result.evidence_added == 2
    assert result.group.conflict_id == "conflict-1"
    queries = [query for query, _ in client.calls]
    assert any("INSERT INTO public.conflict_groups" in query for query in queries)
    assert sum("INSERT INTO public.conflict_evidence_refs" in query for query in queries) == 2
    assert any("INSERT INTO public.human_reviews" in query for query in queries)


@pytest.mark.unit
@pytest.mark.no_network
async def test_conflict_writer_extends_only_one_unambiguously_contained_group():
    existing = _group()
    updated = {**existing, "rationale": "The new observation supports it."}
    client = Client(
        one=[existing, updated],
        all=[
            [_observation(101), _observation(104), _observation(224)],
            [existing],
            [
                {"observation_id": 101},
                {"observation_id": 104},
                {"observation_id": 224},
            ],
        ],
        evidence_rowcounts=[0, 0, 1],
    )

    result = await ConflictWriter(client).record_detection(
        user_name="ada",
        project_id="project-1",
        origin="agent_discovery",
        kind="possible_contradiction",
        rationale="The new observation supports it.",
        confidence=0.8,
        evidence_ids=[101, 104, 224],
    )

    assert not result.created
    assert result.evidence_added == 1
    queries = [query for query, _ in client.calls]
    assert any("FROM public.conflict_groups conflict" in query for query in queries)
    assert any("INSERT INTO public.conflict_evidence_refs" in query for query in queries)
    assert not any("INSERT INTO public.conflict_groups" in query for query in queries)
    review_params = next(
        params
        for query, params in client.calls
        if "INSERT INTO public.human_reviews" in query
    )
    assert '"evidence_ids": [101, 104, 224]' in review_params[-1]
