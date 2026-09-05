from uuid import uuid4

import pytest

from common.schema.evidence import EvidenceTraversalLimits
from core.knowledge.db.readers.evidence_traversal_reader import (
    EvidenceTraversalReader,
)
from core.knowledge.evidence_service import EvidenceService


class EvidenceClient:
    def __init__(self, rows):
        self.rows = rows
        self.calls = []

    async def fetch_all(self, query, params):
        self.calls.append((query, params))
        return list(self.rows)


def _row(
    *,
    observation_id=7,
    message_id=11,
    source_ref_id=None,
    support_kind="user_message",
    message_role="user",
    retired_at=None,
    block_id=None,
):
    return {
        "observation_id": observation_id,
        "observed_relationship_label": "owns",
        "observed_at_ms": 100,
        "retired_at": retired_at,
        "block_id": block_id or uuid4(),
        "block_content_hash": "1" * 64,
        "block_markdown": "Sarah owns Delta.",
        "support_kind": support_kind,
        "message_id": message_id,
        "message_role": message_role,
        "message_timestamp_ms": 90,
        "source_ref_id": source_ref_id,
        "source_kind": None,
        "source_content_hash": None,
        "source_locator": None,
        "source_excerpt": None,
    }


@pytest.mark.unit
@pytest.mark.no_network
async def test_evidence_service_builds_deterministic_bundle_with_one_query():
    rows = [_row(message_id=12), _row(message_id=11)]
    client = EvidenceClient(rows)
    service = EvidenceService(EvidenceTraversalReader(client))

    first = await service.for_relationship_observation(
        7,
        user_name="ada",
        project_id="project-1",
    )
    second = await service.for_relationship_observation(
        7,
        user_name="ada",
        project_id="project-1",
    )

    assert first == second
    assert first.state_token == second.state_token
    assert [node.pointer.kind for node in first.nodes] == [
        "relationship_observation",
        "context_block",
        "context_block",
        "message",
        "message",
    ]
    assert len(client.calls) == 2
    assert all("LIMIT %s" in query for query, _ in client.calls)


@pytest.mark.unit
@pytest.mark.no_network
async def test_evidence_service_reports_leaf_truncation_without_dangling_edges():
    client = EvidenceClient([_row(message_id=11), _row(message_id=12)])
    bundle = await EvidenceService(
        EvidenceTraversalReader(client)
    ).for_relationship_observation(
        7,
        user_name="ada",
        project_id="project-1",
        limits=EvidenceTraversalLimits(max_leaf_evidence=1),
    )

    assert bundle.nodes_truncated is True
    assert bundle.edges_truncated is True
    returned = {
        (node.pointer.kind, node.pointer.identifier) for node in bundle.nodes
    }
    assert all(
        (edge.source.kind, edge.source.identifier) in returned
        and (edge.target.kind, edge.target.identifier) in returned
        for edge in bundle.edges
    )


@pytest.mark.unit
@pytest.mark.no_network
async def test_evidence_reader_rejects_unbounded_or_invalid_requests_before_sql():
    client = EvidenceClient([])
    reader = EvidenceTraversalReader(client)

    with pytest.raises(ValueError, match="at most 128"):
        await reader.get_relationship_rows(
            list(range(1, 130)),
            user_name="ada",
            project_id="project-1",
            row_limit=10,
        )
    with pytest.raises(ValueError, match="positive integers"):
        await reader.get_relationship_rows(
            [0],
            user_name="ada",
            project_id="project-1",
            row_limit=10,
        )
    assert client.calls == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_evidence_reader_uses_one_query_for_a_bounded_observation_list():
    client = EvidenceClient([])
    reader = EvidenceTraversalReader(client)

    await reader.get_relationship_rows(
        [9, 7, 9],
        user_name="ada",
        project_id="project-1",
        row_limit=25,
    )

    assert len(client.calls) == 1
    _, params = client.calls[0]
    assert params == ("ada", "project-1", [7, 9], 25)


@pytest.mark.unit
@pytest.mark.no_network
async def test_evidence_service_handles_assistant_support_and_deduplicates_paths():
    block_id = uuid4()
    row = _row(
        message_id=12,
        support_kind="assistant_message",
        message_role="assistant",
        retired_at="2026-09-05T00:00:00Z",
        block_id=block_id,
    )
    client = EvidenceClient([row, row])

    bundle = await EvidenceService(
        EvidenceTraversalReader(client)
    ).for_relationship_observation(
        7,
        user_name="ada",
        project_id="project-1",
    )

    assert [node.pointer.kind for node in bundle.nodes] == [
        "relationship_observation",
        "context_block",
        "message",
    ]
    assert bundle.nodes[0].status == "retired"
    assert bundle.nodes[-1].role == "assistant"
    assert len(bundle.edges) == 2


@pytest.mark.unit
@pytest.mark.no_network
async def test_evidence_service_batches_observations_and_builds_typed_snapshot():
    client = EvidenceClient([_row(observation_id=7), _row(observation_id=9)])
    service = EvidenceService(EvidenceTraversalReader(client))

    bundles = await service.for_relationship_observations(
        [9, 7, 9], user_name="ada", project_id="project-1"
    )
    snapshot = service.snapshot(bundles)

    assert [bundle.subject.identifier for bundle in bundles] == ["7", "9"]
    assert len(client.calls) == 1
    assert snapshot.total_edges == 4
    assert {pointer.kind for pointer in snapshot.pointers} == {
        "relationship_observation",
        "context_block",
        "message",
    }
