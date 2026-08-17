import pytest

from core.knowledge.db.writers.human_review_writer import HumanReviewWriter
from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.relationship_advisories import RelationshipAdvisory


@pytest.mark.storage
@pytest.mark.requires_postgres
async def test_relationship_advisory_owns_its_decision_while_review_tracks_open_state(
    real_postgres_client,
):
    reviews = HumanReviewWriter(real_postgres_client)
    writer = RelationshipAdvisoryWriter(real_postgres_client, reviews=reviews)
    advisory = RelationshipAdvisory(
        pattern_key="deploys to|project|technology",
        observed_label="deploys to",
        source_type="Project",
        target_type="Technology",
        occurrence_count=3,
        distinct_source_entities=2,
        distinct_target_entities=2,
        message_ids=(1, 2, 3),
        first_observed_ms=1,
        last_observed_ms=3,
    )

    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory
    )
    open_reviews = await reviews.list_open(user_name="ada", project_id="project-1")
    assert [(review.kind, review.subject_id) for review in open_reviews] == [
        ("relationship_advisory", advisory.pattern_key)
    ]

    decision = await writer.apply_action(
        user_name="ada",
        project_id="project-1",
        pattern_key=advisory.pattern_key,
        action="accept",
        relationship_type="DEPLOYS_TO",
        decided_by="ada",
    )

    assert decision.disposition == "accepted"
    assert await reviews.list_open(user_name="ada", project_id="project-1") == []
