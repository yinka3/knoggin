import pytest

from core.knowledge.relationship_advisories import (
    AdvisoryThresholds,
    RelationshipAdvisoryDecisionError,
    apply_advisory_action,
    build_relationship_advisories,
)


def unknown_observation(
    message_id,
    source_entity_id,
    target_entity_id,
    *,
    label="deploys to",
    source_type="Project",
    target_type="Technology",
    observed_at_ms=None,
    domain_status="unrecognized",
):
    return {
        "message_id": message_id,
        "source_entity_id": source_entity_id,
        "target_entity_id": target_entity_id,
        "source_type": source_type,
        "target_type": target_type,
        "observed_relationship_label": label,
        "domain_status": domain_status,
        "observed_at_ms": observed_at_ms,
    }


@pytest.mark.unit
@pytest.mark.no_network
def test_advisories_group_repeated_unknown_evidence_and_keep_provenance():
    observations = [
        unknown_observation(12, 101, 201, observed_at_ms=300),
        unknown_observation(10, 100, 201, observed_at_ms=100, label="deploys   to"),
        unknown_observation(11, 101, 202, observed_at_ms=200),
        unknown_observation(
            99,
            100,
            201,
            label="USES",
            domain_status="recognized",
        ),
    ]

    advisories = build_relationship_advisories(observations)

    assert len(advisories) == 1
    advisory = advisories[0]
    assert advisory.pattern_key == "deploys to|project|technology"
    assert advisory.observed_label == "deploys to"
    assert advisory.occurrence_count == 3
    assert advisory.distinct_source_entities == 2
    assert advisory.distinct_target_entities == 2
    assert advisory.distinct_entities == 4
    assert advisory.message_ids == (10, 11, 12)
    assert advisory.first_observed_ms == 100
    assert advisory.last_observed_ms == 300
    assert advisory.to_dict()["disposition"] == "pending"


@pytest.mark.unit
@pytest.mark.no_network
def test_advisories_are_directional_and_require_thresholds():
    rows = [
        unknown_observation(1, 1, 2),
        unknown_observation(2, 1, 2),
        unknown_observation(
            3,
            2,
            1,
            source_type="Technology",
            target_type="Project",
        ),
    ]

    assert build_relationship_advisories(rows) == []

    advisories = build_relationship_advisories(
        rows,
        thresholds=AdvisoryThresholds(min_occurrences=2, min_distinct_entities=2),
    )

    assert len(advisories) == 1
    assert advisories[0].occurrence_count == 2
    assert advisories[0].pattern_key == "deploys to|project|technology"


@pytest.mark.unit
@pytest.mark.no_network
def test_advisory_thresholds_reject_non_positive_values():
    with pytest.raises(ValueError, match="min_occurrences"):
        AdvisoryThresholds(min_occurrences=0)
    with pytest.raises(ValueError, match="min_distinct_entities"):
        AdvisoryThresholds(min_distinct_entities=0)
    with pytest.raises(ValueError, match="min_distinct_messages"):
        AdvisoryThresholds(min_distinct_messages=0)


@pytest.mark.unit
@pytest.mark.no_network
def test_advisories_do_not_promote_repetition_within_one_message():
    rows = [
        unknown_observation(1, 1, 2),
        unknown_observation(1, 1, 3),
        unknown_observation(1, 2, 3),
    ]

    assert build_relationship_advisories(rows) == []


@pytest.mark.unit
@pytest.mark.no_network
def test_advisory_decisions_follow_explicit_lifecycle_without_domain_mutation():
    pattern_key = "deploys to|project|technology"

    edited = apply_advisory_action(
        None,
        pattern_key=pattern_key,
        action="edit",
        relationship_type="DEPLOYS_TO",
        decided_by="ada",
    )
    accepted = apply_advisory_action(
        edited,
        pattern_key=pattern_key,
        action="accept",
    )
    reopened = apply_advisory_action(
        accepted,
        pattern_key=pattern_key,
        action="reopen",
    )
    dismissed = apply_advisory_action(
        reopened,
        pattern_key=pattern_key,
        action="dismiss",
        note="Not useful for this project",
    )
    suppressed = apply_advisory_action(
        dismissed,
        pattern_key=pattern_key,
        action="suppress",
    )

    assert edited.disposition == "pending"
    assert accepted.disposition == "accepted"
    assert accepted.proposed_relationship_type == "DEPLOYS_TO"
    assert dismissed.decision_note == "Not useful for this project"
    assert suppressed.disposition == "suppressed"
    assert suppressed.revision == 5


@pytest.mark.unit
@pytest.mark.no_network
def test_advisory_decision_rejects_invalid_transition_and_missing_type():
    pattern_key = "deploys to|project|technology"

    with pytest.raises(RelationshipAdvisoryDecisionError, match="requires"):
        apply_advisory_action(
            None,
            pattern_key=pattern_key,
            action="accept",
        )

    accepted = apply_advisory_action(
        None,
        pattern_key=pattern_key,
        action="accept",
        relationship_type="DEPLOYS_TO",
    )
    with pytest.raises(RelationshipAdvisoryDecisionError, match="Cannot accept"):
        apply_advisory_action(
            accepted,
            pattern_key=pattern_key,
            action="accept",
            relationship_type="DEPLOYS_TO",
        )
