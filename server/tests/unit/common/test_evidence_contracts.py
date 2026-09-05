from uuid import uuid4

import pytest
from pydantic import ValidationError

from common.schema.evidence import (
    EvidenceBundle,
    EvidenceEdge,
    EvidencePointer,
    EvidenceSnapshot,
    EvidenceSubject,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_evidence_pointers_validate_closed_identifier_shapes():
    block_id = str(uuid4())
    assert EvidencePointer(
        kind="context_block", identifier=block_id
    ).identifier == block_id
    assert EvidencePointer.for_observation(7).identifier == "7"

    for payload in (
        {"kind": "observation", "identifier": "7"},
        {"kind": "relationship_observation", "identifier": "0"},
        {"kind": "message", "identifier": "message-7"},
        {"kind": "source_reference", "identifier": "not-a-uuid"},
        {"kind": "episode", "identifier": " episode-1"},
    ):
        with pytest.raises(ValidationError):
            EvidencePointer.model_validate(payload)


@pytest.mark.unit
@pytest.mark.no_network
def test_evidence_edges_reject_semantically_invalid_endpoints():
    block = EvidencePointer(kind="context_block", identifier=str(uuid4()))
    observation = EvidencePointer.for_observation(9)
    assert EvidenceEdge(
        source=block,
        target=observation,
        relation="supports_relationship_observation",
    ).target == observation

    with pytest.raises(ValidationError, match="endpoints"):
        EvidenceEdge(
            source=observation,
            target=block,
            relation="supports_relationship_observation",
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_evidence_bundle_requires_exact_truncation_metadata():
    subject = EvidenceSubject(kind="relationship_observation", identifier="9")
    with pytest.raises(ValidationError, match="nodes_truncated"):
        EvidenceBundle(
            subject=subject,
            nodes=(),
            edges=(),
            total_nodes=1,
            total_edges=0,
            nodes_truncated=False,
            state_token="0" * 64,
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_evidence_snapshot_rejects_arbitrary_or_unbounded_payloads():
    with pytest.raises(ValidationError, match="Extra inputs"):
        EvidenceSnapshot.model_validate({"raw_database_rows": [{"secret": "value"}]})
    with pytest.raises(ValidationError):
        EvidenceSnapshot(
            pointers=tuple(
                EvidencePointer(kind="message", identifier=str(index))
                for index in range(1, 514)
            )
        )
