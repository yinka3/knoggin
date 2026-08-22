import pytest

from core.knowledge.human_reviews import HumanReview


@pytest.mark.unit
@pytest.mark.no_network
def test_human_review_is_a_workflow_neutral_subject_pointer():
    review = HumanReview(
        review_id="review-1",
        user_name="ada",
        project_id="project-1",
        kind="relationship_advisory",
        subject_type="relationship_advisory",
        subject_id="advisory-1",
        status="open",
        priority="high",
        title="Relationship advisory",
    )

    assert review.subject_id == "advisory-1"
    assert review.status == "open"

    with pytest.raises(ValueError, match="status"):
        HumanReview(
            review_id="review-2",
            user_name="ada",
            project_id="project-1",
            kind="relationship_advisory",
            subject_type="relationship_advisory",
            subject_id="advisory-2",
            status="pending",  # type: ignore[arg-type]
            priority="high",
            title="Invalid state",
        )
