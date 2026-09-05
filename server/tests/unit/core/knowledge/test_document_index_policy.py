import pytest

from core.knowledge.documents import DocumentIndexPolicy


@pytest.mark.unit
@pytest.mark.no_network
def test_document_index_policy_contains_execution_limits_without_a_version_hash():
    policy = DocumentIndexPolicy.capture(
        inline_index_max_bytes=128,
        embedding_chunk_batch_size=4,
    )

    assert policy.inline_index_max_bytes == 128
    assert policy.embedding_chunk_batch_size == 4
    assert not hasattr(policy, "version")
