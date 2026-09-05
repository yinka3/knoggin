import pytest

from core.knowledge.entity.profile import EntityIdentity, ProjectEntityContext


@pytest.mark.unit
@pytest.mark.no_network
def test_global_identity_and_project_context_are_separate_records():
    identity = EntityIdentity(
        entity_id=7,
        user_name="ada",
        canonical_name="Sarah Johnson",
        aliases=("Sarah J.",),
    )
    work_context = ProjectEntityContext(
        project_id="work",
        entity_id=identity.entity_id,
        user_name="ada",
        entity_type="coworker",
        topic="Career",
    )
    personal_context = ProjectEntityContext(
        project_id="personal",
        entity_id=identity.entity_id,
        user_name="ada",
        entity_type="friend",
        topic="Relationships",
    )

    assert identity.canonical_name == "Sarah Johnson"
    assert {work_context.topic, personal_context.topic} == {"Career", "Relationships"}
