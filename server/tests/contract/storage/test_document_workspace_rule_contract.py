import pytest

from core.knowledge.db.writers.document_writer import DocumentWriter
from tests.fixtures.fakes import RecordingPostgresClient


@pytest.mark.storage
@pytest.mark.no_network
async def test_the_workspace_rule_skips_and_requeues_in_one_transaction():
    client = RecordingPostgresClient()

    requeued = await DocumentWriter(client, "project-1").apply_workspace_index_rule(
        extensions={".py"}
    )

    assert client.transaction_enters == 1
    statements = [query for kind, query, _params in client.calls if kind == "execute"]
    assert len(statements) == 2
    assert "SET status = 'skipped'" in statements[0]
    assert "SET status = 'queued'" in statements[1]
    assert requeued == 1  # the fake cursor's rowcount
