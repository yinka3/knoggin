"""Durable source contracts for entity names used by project deletion."""

from uuid import uuid4

import pytest

from core.knowledge.db.writers.global_entity_merge_writer import (
    GlobalEntityMergeWriter,
)
from core.knowledge.db.writers.graph_writer import GraphWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter


async def _seed_mergeable_entities(client) -> None:
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Ada Lovelace'), (3, 'ada', 'Augusta Ada King');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'Concept', 'General'),
            ('project-1', 3, 'ada', 'Concept', 'General');
        INSERT INTO public.entity_aliases (entity_id, alias)
        VALUES (2, 'Ada'), (3, 'Augusta');
        """
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_explicit_user_names_have_global_support(real_postgres_client):
    await GraphWriter(real_postgres_client).ensure_identity_entity(
        "ada", ["Ada Lovelace", "A. Lovelace"]
    )
    await real_postgres_client.execute(
        """
        INSERT INTO public.entity_name_supports (
            entity_id, name, project_id, source_kind, source_key
        ) VALUES
            (1, 'Ada Lovelace', 'project-1', 'project', 'project-1'),
            (1, 'A. Lovelace', 'project-1', 'project', 'project-1')
        """
    )
    await GraphWriter(real_postgres_client).ensure_identity_entity(
        "ada", ["Ada Lovelace"]
    )

    rows = await real_postgres_client.fetch_all(
        """
        SELECT entity_id, name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 1
        """
    )
    assert {
        (
            row["entity_id"],
            row["name"],
            row["project_id"],
            row["source_kind"],
            row["source_key"],
        )
        for row in rows
    } == {
        (1, "Ada Lovelace", None, "user", "ada"),
        (1, "Ada Lovelace", "project-1", "project", "project-1"),
        (1, "ada", None, "user", "ada"),
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_sources_are_distinct_from_copied_episode_and_merge_history(
    real_postgres_client,
):
    await _seed_mergeable_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-2', 2, 'ada', 'Concept', 'General');
        INSERT INTO public.episodes (
            episode_id, project_id, summary, source_message_count
        ) VALUES (
            'episode-copy', 'project-1',
            'Copied summary calls Ada the first programmer.', 0
        );
        INSERT INTO public.entity_global_merge_audits (
            merge_id, user_name, survivor_entity_id, retired_entity_id, plan
        ) VALUES (
            'merge-copy', 'ada', 2, 3,
            '{"copied_name":"Ada the first programmer"}'::jsonb
        );
        """
    )
    writer = GraphWriter(real_postgres_client)
    await writer.update_entity_aliases({2: ["Ada"]}, project_id="project-1")
    await writer.update_entity_aliases(
        {2: ["Ada in project two"]}, project_id="project-2"
    )

    assert await real_postgres_client.fetch_all(
        """
        SELECT name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 2
        ORDER BY project_id, name
        """
    ) == [
        {
            "name": "Ada",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
        {
            "name": "Ada in project two",
            "project_id": "project-2",
            "source_kind": "project",
            "source_key": "project-2",
        },
    ]

    await ProjectDeletionWriter(real_postgres_client).delete_project(
        user_name="ada",
        project_id="project-1",
    )

    assert await real_postgres_client.fetch_all(
        """
        SELECT name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 2
        """
    ) == [
        {
            "name": "Ada in project two",
            "project_id": "project-2",
            "source_kind": "project",
            "source_key": "project-2",
        }
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_project_alias_writer_records_its_source(real_postgres_client):
    await _seed_mergeable_entities(real_postgres_client)

    await GraphWriter(real_postgres_client).update_entity_aliases(
        {2: ["Lady Ada"]},
        project_id="project-1",
    )

    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 2
        """
    ) == [
        {
            "entity_id": 2,
            "name": "Lady Ada",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        }
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_global_merge_moves_name_supports_and_rollback_restores_them(
    real_postgres_client,
):
    await _seed_mergeable_entities(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entity_name_supports (
            entity_id, name, project_id, source_kind, source_key
        ) VALUES
            (2, 'Ada', 'project-1', 'project', 'project-1'),
            (3, 'Augusta', 'project-1', 'project', 'project-1'),
            (3, 'Augusta Ada King', 'project-1', 'project', 'project-1');
        """
    )
    writer = GlobalEntityMergeWriter(real_postgres_client)
    merge_id = str(uuid4())

    await writer.merge(
        user_name="ada",
        survivor_id=2,
        retired_id=3,
        merge_id=merge_id,
    )

    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id IN (2, 3)
        ORDER BY entity_id, name
        """
    ) == [
        {
            "entity_id": 2,
            "name": "Ada",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
        {
            "entity_id": 2,
            "name": "Augusta",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
        {
            "entity_id": 2,
            "name": "Augusta Ada King",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
    ]

    rollback = await writer.plan_rollback(merge_id=merge_id, user_name="ada")
    result = await writer.rollback_safe(
        merge_id=merge_id,
        user_name="ada",
        safe_mutation_ids=rollback["safe_mutation_ids"],
    )

    assert result["rolled_back"] is True
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id IN (2, 3)
        ORDER BY entity_id, name
        """
    ) == [
        {
            "entity_id": 2,
            "name": "Ada",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
        {
            "entity_id": 3,
            "name": "Augusta",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
        {
            "entity_id": 3,
            "name": "Augusta Ada King",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        },
    ]
