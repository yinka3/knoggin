import pytest

from knoggin_server.knowledge.db.readers.merge_audit_reader import MergeAuditReader
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_reader_reads_candidate_snapshot():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{"entity_id": 2}],
            [{"fact_id": "fact-1"}],
            [{"relationship_id": "rel-1"}],
            [{"parent_id": 9, "child_id": 2}],
        ]
    )
    reader = MergeAuditReader(client)

    result = await reader.snapshot("ada", "project-1", 2, 3)

    assert result == {
        "entities": [{"entity_id": 2}],
        "facts": [{"fact_id": "fact-1"}],
        "relationships": [{"relationship_id": "rel-1"}],
        "hierarchy": [{"parent_id": 9, "child_id": 2}],
    }
    assert len(client.calls) == 4
    assert "FROM entities e" in client.calls[0][1]
    assert "FROM facts" in client.calls[1][1]
    assert "FROM relationships r" in client.calls[2][1]
    assert "FROM hierarchy_edges h" in client.calls[3][1]


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_reader_gets_proposal_by_id():
    client = RecordingPostgresClient(fetch_all_results=[[{"proposal_id": "p1"}]])
    reader = MergeAuditReader(client)

    result = await reader.get_proposal("p1")

    assert result == {"proposal_id": "p1"}
    assert "FROM entity_merge_proposals" in client.calls[0][1]
    assert client.calls[0][2] == ("p1",)


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_reader_gets_audit_by_id():
    client = RecordingPostgresClient(fetch_all_results=[[{"audit_id": "audit-1"}]])
    reader = MergeAuditReader(client)

    result = await reader.get_audit("audit-1")

    assert result == {"audit_id": "audit-1"}
    assert client.calls[0][0] == "fetch_all"
    assert "FROM entity_merge_audits" in client.calls[0][1]
    assert client.calls[0][2] == ("audit-1",)
