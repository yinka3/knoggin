"""Advisory review replacement must commit or roll back as one decision."""

from types import SimpleNamespace

import pytest

from common.conf.domain_config import DomainConfig
from core.knowledge.db.readers.evidence_traversal_reader import EvidenceTraversalReader
from core.knowledge.db.readers.relationship_observation_reader import (
    RelationshipObservationReader,
)
from core.knowledge.db.writers.entity_reclassification_writer import (
    EntityReclassificationWriter,
)
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.evidence_service import EvidenceService
from core.knowledge.jobs.relationship_advisory_job import RelationshipAdvisoryJob
from core.knowledge.maintenance.maintenance_reviews import RelationshipAdvisoryPlan
from core.knowledge.relationship_advisories import RelationshipAdvisory
from core.project.domain_config_store import DomainConfigStore
from core.project.maintenance_service import ProjectMaintenanceService
from infrastructure.job.base import JobContext


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_advisory_action_rolls_back_stale_transition_if_successor_fails(
    real_postgres_client, monkeypatch
):
    writer = RelationshipAdvisoryWriter(real_postgres_client)
    pattern_key = "deploys to|project|technology"
    original = await writer.reviews.open(
        user_name="ada",
        scope="project",
        project_id="project-1",
        kind="relationship_advisory",
        dedupe_key=pattern_key,
        reasoning="Three observations need a decision.",
        proposed_plan=RelationshipAdvisoryPlan(
            pattern_key=pattern_key, observed_label="deploys to"
        ),
    )

    async def fail_open(**_kwargs):
        raise RuntimeError("successor insert failed")

    monkeypatch.setattr(writer.reviews, "open", fail_open)
    with pytest.raises(RuntimeError, match="successor insert failed"):
        await writer.apply_action(
            user_name="ada",
            project_id="project-1",
            pattern_key=pattern_key,
            action="accept",
            relationship_type="DEPLOYS_TO",
        )

    assert await real_postgres_client.fetch_one(
        "SELECT status FROM public.maintenance_reviews WHERE review_id = %s",
        (original.review_id,),
    ) == {"status": "open"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.maintenance_reviews",
    ) == {"count": 1}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_stale_advisory_refresh_creates_open_successor(real_postgres_client):
    writer = RelationshipAdvisoryWriter(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada'), (3, 'ada', 'Acme')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_semantic_windows (
            window_id, user_name, project_id, origin, stage, domain_version,
            policy_snapshot, source_token_count, token_estimator,
            token_estimator_version, completed_at
        ) VALUES
            ('11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
             'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now())
        """
    )
    rows = await real_postgres_client.fetch_all(
        """
        INSERT INTO public.relationship_observations (
            project_id, user_name, semantic_window_id, source_entity_id,
            target_entity_id, observed_relationship_label, support_key,
            observed_at_ms
        ) VALUES
            ('project-1', 'ada', '11111111-1111-4111-8111-111111111111',
             2, 3, 'deploys to', md5('first'), 100),
            ('project-1', 'ada', '11111111-1111-4111-8111-111111111111',
             2, 3, 'deploys to', md5('second'), 200)
        RETURNING observation_id
        """
    )
    observation_ids = tuple(int(row["observation_id"]) for row in rows)
    advisory = RelationshipAdvisory(
        pattern_key="deploys to|project|technology",
        observed_label="deploys to",
        source_type="Project",
        target_type="Technology",
        occurrence_count=2,
        distinct_source_entities=2,
        distinct_target_entities=2,
        semantic_window_ids=("window-1", "window-2"),
        observation_ids=observation_ids,
        first_observed_ms=1,
        last_observed_ms=2,
    )
    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory, domain_version=4
    )
    original = await writer.reviews.get_by_key(
        user_name="ada", project_id="project-1", kind="relationship_advisory",
        dedupe_key=advisory.pattern_key,
    )
    await writer.reviews.transition(
        original.review_id, user_name="ada", project_id="project-1",
        status="stale", actor="ada", reason="evidence identity changed",
    )

    await writer.materialize_pending(
        user_name="ada", project_id="project-1", advisory=advisory, domain_version=4
    )

    successor = await writer.reviews.get_by_key(
        user_name="ada", project_id="project-1", kind="relationship_advisory",
        dedupe_key=advisory.pattern_key,
    )
    assert successor.review_id != original.review_id
    assert successor.status == "open"
    assert successor.expected_state["supersedes_review_id"] == original.review_id


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_context_observations_become_reviewable_advisory(real_postgres_client):
    client = real_postgres_client
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Project A'), (3, 'ada', 'Project B'),
               (4, 'ada', 'Tool A'), (5, 'ada', 'Tool B');
        INSERT INTO public.project_entity_contexts
            (project_id, entity_id, user_name, entity_type, topic)
        VALUES ('project-1', 2, 'ada', 'Concept', 'General'),
               ('project-1', 3, 'ada', 'Concept', 'General'),
               ('project-1', 4, 'ada', 'Concept', 'General'),
               ('project-1', 5, 'ada', 'Concept', 'General');
        INSERT INTO public.project_semantic_windows
            (window_id, user_name, project_id, origin, stage, domain_version,
             policy_snapshot, source_token_count, token_estimator,
             token_estimator_version, completed_at)
        VALUES ('11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
                'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now()),
               ('22222222-2222-4222-8222-222222222222', 'ada', 'project-1',
                'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now());
        INSERT INTO public.project_context_blocks
            (block_id, project_id, section_key, markdown, content_hash,
             assertion_kind, source_time_ms)
        VALUES ('33333333-3333-4333-8333-333333333333', 'project-1',
                'current_state', 'Project A deploys to Tool A.', repeat('a', 64),
                'source_grounded', 100),
               ('44444444-4444-4444-8444-444444444444', 'project-1',
                'current_state', 'Project B deploys to Tool A.', repeat('b', 64),
                'source_grounded', 200),
               ('55555555-5555-4555-8555-555555555555', 'project-1',
                'current_state', 'Project A deploys to Tool B.', repeat('c', 64),
                'source_grounded', 300);
        """
    )
    observations = await client.fetch_all(
        """
        INSERT INTO public.relationship_observations
            (project_id, user_name, semantic_window_id, source_entity_id,
             target_entity_id, observed_relationship_label, support_key,
             observed_at_ms)
        VALUES ('project-1', 'ada', '11111111-1111-4111-8111-111111111111',
                2, 4, 'deploys to', md5('a'), 100),
               ('project-1', 'ada', '11111111-1111-4111-8111-111111111111',
                3, 4, 'deploys to', md5('b'), 200),
               ('project-1', 'ada', '22222222-2222-4222-8222-222222222222',
                2, 5, 'deploys to', md5('c'), 300)
        RETURNING observation_id
        """
    )
    for observation, block_id in zip(
        observations,
        (
            '33333333-3333-4333-8333-333333333333',
            '44444444-4444-4444-8444-444444444444',
            '55555555-5555-4555-8555-555555555555',
        ),
        strict=True,
    ):
        await client.execute(
            """INSERT INTO public.relationship_observation_blocks
               (observation_id, project_id, block_id) VALUES (%s, 'project-1', %s)""",
            (observation['observation_id'], block_id),
        )

    reader = RelationshipObservationReader(client)
    rows = await reader.get_unrecognized_observations(
        user_name='ada', project_id='project-1'
    )
    assert len(rows) == 3
    assert {row['evidence_origin'] for row in rows} == {'context'}

    evidence = EvidenceService(EvidenceTraversalReader(client))

    async def load_evidence(observation_ids, *, user_name, project_id):
        return await evidence.for_relationship_observations(
            observation_ids, user_name=user_name, project_id=project_id
        )

    async def get_project(project_id):
        return {'status': 'active'} if project_id == 'project-1' else None

    service = ProjectMaintenanceService(
        resources=SimpleNamespace(
            postgres=client,
            knowledge_store=SimpleNamespace(
                get_relationship_observations_evidence=load_evidence
            ),
        ),
        user_name='ada',
        project_lookup=get_project,
        active_projects={},
        project_leases={},
    )
    job = RelationshipAdvisoryJob(service)
    result = await job.execute(JobContext(user_name='ada', project_id='project-1'))
    assert result.success

    reviews = await service.list_maintenance_reviews('project-1')
    assert len(reviews) == 1
    review = reviews[0]
    assert review.status == 'open'
    assert review.proposed_plan.pattern_key == 'deploys to|concept|concept'
    assert len(review.evidence_refs) == 3

    await job.execute(JobContext(user_name='ada', project_id='project-1'))
    assert len(await service.list_maintenance_reviews('project-1')) == 1
    with pytest.raises(ValueError, match='relationship advisory action'):
        await service.transition_maintenance_review(
            'project-1', review.review_id, status='dismissed'
        )

    edited = await service.apply_relationship_advisory_action(
        'project-1', review.review_id, 'edit', relationship_type='DEPLOYS_TO'
    )
    assert edited.disposition == 'pending'
    with pytest.raises(ValueError, match='stale'):
        await service.apply_relationship_advisory_action(
            'project-1', review.review_id, 'accept'
        )
    latest = (await service.list_maintenance_reviews('project-1'))[-1]
    accepted = await service.apply_relationship_advisory_action(
        'project-1', latest.review_id, 'accept'
    )
    assert accepted.disposition == 'accepted'
    await job.execute(JobContext(user_name='ada', project_id='project-1'))
    assert len(await service.list_maintenance_reviews('project-1')) == 3
    accepted_review = (await service.list_maintenance_reviews('project-1'))[-1]
    reopened = await service.apply_relationship_advisory_action(
        'project-1', accepted_review.review_id, 'reopen'
    )
    assert reopened.disposition == 'pending'
    reopened_review = (await service.list_maintenance_reviews('project-1'))[-1]
    dismissed = await service.apply_relationship_advisory_action(
        'project-1', reopened_review.review_id, 'dismiss', note='Too broad'
    )
    assert dismissed.disposition == 'dismissed'
    dismissed_review = (await service.list_maintenance_reviews('project-1'))[-1]
    suppressed = await service.apply_relationship_advisory_action(
        'project-1', dismissed_review.review_id, 'suppress'
    )
    assert suppressed.disposition == 'suppressed'
    await job.execute(JobContext(user_name='ada', project_id='project-1'))
    assert len(await service.list_maintenance_reviews('project-1')) == 6
    assert await client.fetch_one(
        "SELECT domain_config -> 'relationships' AS relationships "
        "FROM public.projects WHERE project_id = 'project-1'"
    ) == {'relationships': {}}
    suppressed_review = (await service.list_maintenance_reviews('project-1'))[-1]
    reopened = await service.apply_relationship_advisory_action(
        'project-1', suppressed_review.review_id, 'reopen'
    )
    assert reopened.disposition == 'pending'
    domain_store = DomainConfigStore(client)
    current_domain = await domain_store.load('ada', 'project-1')
    candidate = DomainConfig.from_mapping({
        **current_domain.to_dict(),
        'relationships': {
            'DEPLOYS_TO': {
                'source_types': ['Concept'],
                'target_types': ['Concept'],
                'labels': ['deploys to'],
            },
        },
    })
    await domain_store.activate(
        user_name='ada', project_id='project-1', candidate=candidate,
        expected_version=current_domain.version,
    )
    assert (await service.list_maintenance_reviews('project-1'))[-1].status == 'stale'
    assert await service.refresh_relationship_advisories('project-1') == []
    assert len(await service.list_maintenance_reviews('project-1')) == 7
    assert await client.fetch_one(
        "SELECT domain_config -> 'relationships' AS relationships "
        "FROM public.projects WHERE project_id = 'project-1'"
    ) == {'relationships': candidate.to_dict()['relationships']}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_endpoint_type_reclassification_stales_cited_advisory(
    real_postgres_client, monkeypatch
):
    client = real_postgres_client
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Project A'), (3, 'ada', 'Tool A');
        INSERT INTO public.project_entity_contexts
            (project_id, entity_id, user_name, entity_type, topic)
        VALUES ('project-1', 2, 'ada', 'Legacy', 'General'),
               ('project-1', 3, 'ada', 'Concept', 'General');
        INSERT INTO public.project_semantic_windows
            (window_id, user_name, project_id, origin, stage, domain_version,
             policy_snapshot, source_token_count, token_estimator,
             token_estimator_version, completed_at)
        VALUES ('11111111-1111-4111-8111-111111111111', 'ada', 'project-1',
                'conversation', 'completed', 1, '{}'::jsonb, 0, 'test', 'v1', now());
        """
    )
    observation = await client.fetch_one(
        """
        INSERT INTO public.relationship_observations
            (project_id, user_name, semantic_window_id, source_entity_id,
             target_entity_id, observed_relationship_label, support_key,
             observed_at_ms)
        VALUES ('project-1', 'ada', '11111111-1111-4111-8111-111111111111',
                2, 3, 'deploys to', md5('one'), 100)
        RETURNING observation_id
        """
    )
    advisory = RelationshipAdvisory(
        pattern_key='deploys to|legacy|concept',
        observed_label='deploys to',
        source_type='Legacy', target_type='Concept',
        occurrence_count=1, distinct_source_entities=1,
        distinct_target_entities=1,
        semantic_window_ids=('11111111-1111-4111-8111-111111111111',),
        observation_ids=(observation['observation_id'],),
        first_observed_ms=100, last_observed_ms=100,
    )
    advisory_writer = RelationshipAdvisoryWriter(client)
    await advisory_writer.materialize_pending(
        user_name='ada', project_id='project-1', advisory=advisory,
        domain_version=1,
    )
    review = await advisory_writer.reviews.get_by_key(
        user_name='ada', project_id='project-1', kind='relationship_advisory',
        dedupe_key=advisory.pattern_key,
    )
    assert review.status == 'open'

    stored = await DomainConfigStore(client).load('ada', 'project-1')
    definition = stored.to_dict()
    definition['entity_types']['Concept']['labels'].append('Legacy')
    compiled = DomainConfig.from_mapping(definition).compile()
    reclassifier = EntityReclassificationWriter(client)
    plan = await reclassifier.preview(
        user_name='ada', project_id='project-1', domain=compiled,
    )
    assert [change.entity_id for change in plan.changes] == [2]
    original_stale = MaintenanceReviewWriter.mark_stale_for_observations

    async def fail_stale(_self, **_kwargs):
        raise RuntimeError('stale review update failed')

    monkeypatch.setattr(
        'core.knowledge.db.writers.entity_reclassification_writer.'
        'MaintenanceReviewWriter.mark_stale_for_observations', fail_stale
    )
    with pytest.raises(RuntimeError, match='stale review update failed'):
        await reclassifier.apply(
            user_name='ada', project_id='project-1', domain=compiled,
            changes=plan.changes,
        )
    assert await client.fetch_one(
        "SELECT entity_type FROM public.project_entity_contexts "
        "WHERE project_id = 'project-1' AND entity_id = 2"
    ) == {'entity_type': 'Legacy'}
    assert (await advisory_writer.reviews.get(
        review.review_id, user_name='ada', project_id='project-1'
    )).status == 'open'
    monkeypatch.setattr(
        'core.knowledge.db.writers.entity_reclassification_writer.'
        'MaintenanceReviewWriter.mark_stale_for_observations', original_stale
    )
    result = await reclassifier.apply(
        user_name='ada', project_id='project-1', domain=compiled,
        changes=plan.changes,
    )

    assert result.updated == 1
    assert (await advisory_writer.reviews.get(
        review.review_id, user_name='ada', project_id='project-1'
    )).status == 'stale'
