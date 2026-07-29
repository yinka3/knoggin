import pytest

from core.knowledge.db.readers.merge_audit_reader import MergeAuditReader
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_merge_audit_reader_reads_candidate_snapshot():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [{"entity_id": 2}],
            [{"message_id": 7, "entity_id": 2}],
            [{"episode_id": "episode-1", "entity_id": 2}],
            [{"relationship_id": "rel-1"}],
            [{"episode_id": "episode-1", "relationship_id": "rel-1"}],
            [{"parent_id": 9, "child_id": 2}],
        ]
    )
    reader = MergeAuditReader(client)

    result = await reader.snapshot("ada", "project-1", 2, 3)

    assert result == {
        "entities": [{"entity_id": 2}],
        "message_refs": [{"message_id": 7, "entity_id": 2}],
        "episode_entities": [{"episode_id": "episode-1", "entity_id": 2}],
        "relationships": [{"relationship_id": "rel-1"}],
        "episode_relationships": [
            {"episode_id": "episode-1", "relationship_id": "rel-1"}
        ],
        "hierarchy": [{"parent_id": 9, "child_id": 2}],
    }
    assert client.transaction_enters == 1
    assert client.transaction_exits == 1
    assert len(client.calls) == 7
    assert (
        "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY"
        in client.calls[0][1]
    )
    assert "FROM entities e" in client.calls[1][1]
    assert "FROM message_entity_refs" in client.calls[2][1]
    assert "FROM episode_entities" in client.calls[3][1]
    assert "FROM relationships r" in client.calls[4][1]
    assert "FROM episode_relationships" in client.calls[5][1]
    assert "FROM hierarchy_edges h" in client.calls[6][1]


class MutatingMergeAuditReader(MergeAuditReader):
    """Commit a relationship after the snapshot's first query."""

    def __init__(self, client):
        super().__init__(client)
        self._query_count = 0

    async def _fetch_all(self, cur, query, params):
        rows = await super()._fetch_all(cur, query, params)
        self._query_count += 1
        if self._query_count == 1:
            await self.client.execute(
                """
                INSERT INTO relationships (
                    relationship_id,
                    user_name,
                    project_id,
                    entity_a_id,
                    entity_b_id,
                    relationship_type
                )
                VALUES (
                    'project-1:2:3:related', 'ada', 'project-1', 2, 3,
                    'related'
                )
                """
            )
        return rows


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_merge_snapshot_excludes_a_commit_after_its_first_query(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO entities (
            entity_id, user_name, project_id, canonical_name, topic
        )
        VALUES
            (2, 'ada', 'project-1', 'Primary', 'People'),
            (3, 'ada', 'project-1', 'Duplicate', 'People')
        """
    )
    reader = MutatingMergeAuditReader(real_postgres_client)

    snapshot = await reader.snapshot("ada", "project-1", 2, 3)

    assert snapshot["relationships"] == []
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM relationships"
    ) == {"count": 1}


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
