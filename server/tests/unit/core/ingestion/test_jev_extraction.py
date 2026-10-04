"""Bounded extraction request and acceptance contracts."""

from uuid import uuid4

from common.conf.domain_config import DomainConfig
from common.schema.jev import JevPolicy, JevResponse, JevResult
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.jev_extraction import (
    ExtractionCandidate,
    accepted_extraction_type,
    prepare_extraction_request,
)
from core.ingestion.policy import IngestionPolicy


def test_truncated_type_choices_cannot_be_accepted():
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {}},
            "entity_types": {
                name: {"topic": "Work", "labels": [name.lower()]}
                for name in ("Company", "Person", "Project")
            },
        }
    ).compile()
    policy = IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=JevPolicy(extraction_mode="active", max_options_per_choice=3),
    )
    candidate = ExtractionCandidate(
        block_id=uuid4(),
        name="Delta",
        evidence_origin="known_alias_gap",
        support_text="Delta is part of the work.",
        proposed_type=None,
        source_start=0,
        source_end=5,
    )

    _state, questions, mapping, truncated = prepare_extraction_request(
        candidate, policy
    )
    result = JevResult(
        outcome="available",
        response=JevResponse.model_validate(
            {
                "model": policy.jev.model,
                "answers": {
                    "type_choice": {
                        "type": "choice",
                        "choice": "type_1",
                        "probabilities": {
                            handle: float(handle == "type_1")
                            for handle in questions["type_choice"].criteria
                        },
                        "confidence": 1.0,
                    },
                    "entity_evidence": {"type": "noul", "noul": 1.0},
                },
                "usage": {"input_tokens": 10, "output_tokens": 0},
            }
        ),
    )

    assert truncated is True
    assert mapping == {"type_1": "Company"}
    assert (
        accepted_extraction_type(
            result,
            candidate,
            mapping,
            policy.jev,
            candidate_set_truncated=truncated,
        )
        is None
    )
