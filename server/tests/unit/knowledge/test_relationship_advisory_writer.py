import pytest

from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.unit
@pytest.mark.no_network
async def test_writer_persists_current_state_and_audit_for_acceptance():
    client = RecordingPostgresClient(fetch_one_results=[None])

    decision = await RelationshipAdvisoryWriter(client).apply_action(
        user_name="ada",
        project_id="project-1",
        pattern_key="deploys to|project|technology",
        action="accept",
        relationship_type="DEPLOYS_TO",
        decided_by="ada",
    )

    assert decision.disposition == "accepted"
    assert decision.revision == 1
    assert client.transaction_enters == 1
    executed = [call for call in client.calls if call[0] == "execute"]
    assert len(executed) == 3
    assert "FOR UPDATE" in executed[0][1]
    assert "INSERT INTO relationship_advisories" in executed[1][1]
    assert "INSERT INTO relationship_advisory_decisions" in executed[2][1]
