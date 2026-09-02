from types import SimpleNamespace

import pytest

from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.maintenance_reviews import (
    RelationshipInterpretationChange,
    RelationshipInterpretationPlan,
)
from core.project.maintenance_service import ProjectMaintenanceService


async def _project(_project_id):
    return {"project_id": "project-1", "status": "active"}


async def _seed_entities(client):
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada Lovelace'), (3, 'ada', 'Augusta Ada King')
        """
    )
    await client.execute(
        """
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        )
        VALUES
            ('project-1', 2, 'ada', 'Concept', 'General'),
            ('project-1', 3, 'ada', 'Concept', 'General')
        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_confirmed_global_merge_and_rollback_repair_durable_state(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    service = EntityMaintenanceService(
        postgres=real_postgres_client,
        user_name="ada",
    )
    preview = await service.preview_merge(
        survivor_entity_id=2,
        retired_entity_id=3,
    )
    review = await service.review_writer.open(
        user_name="ada",
        scope="user-global",
        project_id=None,
        kind="entity_merge",
        reasoning="Both names identify the same person.",
        proposed_plan=preview["plan"],
        expected_state={
            "state_hash": preview["state_hash"],
            "frontiers": preview["frontiers"],
            "definition_versions": preview["definition_versions"],
        },
    )

    merged = await service.apply_merge_review(
        review.review_id,
        expected_state=review.expected_state,
    )

    assert merged["projection_errors"] == []
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, redirect_entity_id
        FROM public.entities
        WHERE entity_id = 3
        """
    ) == {"status": "redirected", "redirect_entity_id": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (review.review_id,),
    ) == {"status": "applied"}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1'
        ORDER BY entity_id
        """
    ) == [{"entity_id": 2}]

    rolled_back = await service.rollback(merged["merge_id"])

    assert rolled_back["rolled_back"] is True, rolled_back
    assert rolled_back["projection_errors"] == []
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, redirect_entity_id
        FROM public.entities
        WHERE entity_id = 3
        """
    ) == {"status": "active", "redirect_entity_id": None}
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1'
        ORDER BY entity_id
        """
    ) == [{"entity_id": 2}, {"entity_id": 3}]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_review_application_reinterprets_before_marking_applied(
    real_postgres_client,
):
    await _seed_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            timestamp_ms, lifecycle_state, ingestion_state
        )
        VALUES (
            'ada', 'session-1', 101, 'project-1', 'user', 'Ada met Augusta.',
            1000, 'sealed', 'processed'
        )
        """
    )
    relationship_id = "project-1:2:3:related_to"
    await real_postgres_client.execute(
        """
        INSERT INTO public.relationships (
            relationship_id, user_name, project_id, entity_a_id, entity_b_id,
            relationship_type, "symmetric"
        )
        VALUES (%s, 'ada', 'project-1', 2, 3, 'RELATED_TO', false)
        """,
        (relationship_id,),
    )
    observation = await real_postgres_client.fetch_one(
        """
        INSERT INTO public.relationship_observations (
            relationship_id, project_id, user_name, session_id, message_id,
            source_entity_id, target_entity_id, observed_relationship_label,
            observed_at_ms
        )
        VALUES (
            %s, 'project-1', 'ada', 'session-1', 101,
            2, 3, 'related to', 1000
        )
        RETURNING observation_id
        """,
        (relationship_id,),
    )
    resources = SimpleNamespace(
        postgres=real_postgres_client,
        knowledge_store=object(),
    )
    service = ProjectMaintenanceService(
        resources=resources,
        user_name="ada",
        project_lookup=_project,
        active_projects={},
        project_leases={},
    )
    review = await service._maintenance_reviews.open(
        user_name="ada",
        scope="project",
        project_id="project-1",
        kind="relationship_interpretation",
        reasoning="This extracted edge should remain evidence but leave the graph.",
        proposed_plan=RelationshipInterpretationPlan(
            changes=[
                RelationshipInterpretationChange(
                    observation_id=observation["observation_id"],
                    expected_relationship_id=relationship_id,
                    target_relationship_type=None,
                    interpretation_source="review",
                )
            ]
        ),
        expected_state={"domain_version": 1},
    )

    applied = await service.transition_maintenance_review(
        "project-1",
        review.review_id,
        status="applied",
        expected_state=review.expected_state,
    )

    assert applied.status == "applied"
    assert await real_postgres_client.fetch_one(
        """
        SELECT relationship_id, interpretation_source
        FROM public.relationship_observations
        WHERE observation_id = %s
        """,
        (observation["observation_id"],),
    ) == {"relationship_id": None, "interpretation_source": "review"}
    assert await real_postgres_client.fetch_one(
        "SELECT relationship_id FROM public.relationships WHERE relationship_id = %s",
        (relationship_id,),
    ) is None
