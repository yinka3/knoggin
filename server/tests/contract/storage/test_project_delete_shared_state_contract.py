"""Project deletion contracts for shared entity state and merge history."""

import pytest
from psycopg.errors import RaiseException

from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.writers.global_entity_merge_writer import (
    GlobalEntityMergeWriter,
)
from core.knowledge.db.writers.project_deletion_writer import (
    ProjectDeletionWriter,
)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_delete_removes_unsupported_names_and_invalidates_merge_rollback(
    real_postgres_client,
):
    """A surviving project keeps only the names it independently supports."""

    await real_postgres_client.execute(
        """
        INSERT INTO public.projects (project_id, user_name, name, domain_config)
        VALUES ('project-3', 'ada', 'Project 3', '{}'::jsonb);
        INSERT INTO public.project_read_scopes (
            user_name, project_id, readable_project_id
        ) VALUES ('ada', 'project-3', 'project-1');
        INSERT INTO public.entities (entity_id, user_name, canonical_name) VALUES
            (2, 'ada', 'Project One Name'),
            (3, 'ada', 'Project One Only');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'concept', 'General'),
            ('project-2', 2, 'ada', 'concept', 'General'),
            ('project-1', 3, 'ada', 'concept', 'General');
        INSERT INTO public.entity_aliases (entity_id, alias) VALUES
            (2, 'Project One Alias'),
            (2, 'Project Two Name'),
            (2, 'User Preferred Name'),
            (3, 'Project One Only Alias');
        INSERT INTO public.entity_name_supports (
            entity_id, name, project_id, source_kind, source_key
        ) VALUES
            (2, 'Project One Name', 'project-1', 'project', 'project-1'),
            (2, 'Project One Alias', 'project-1', 'project', 'project-1'),
            (2, 'Project Two Name', 'project-2', 'project', 'project-2'),
            (2, 'User Preferred Name', NULL, 'user', 'ada'),
            (3, 'Project One Only', 'project-1', 'project', 'project-1'),
            (3, 'Project One Only Alias', 'project-1', 'project', 'project-1');
        INSERT INTO public.entity_global_merge_audits (
            merge_id, user_name, survivor_entity_id, retired_entity_id,
            plan, affected_project_ids, status
        ) VALUES (
            'merge-project-1', 'ada', 2, 3,
            '{"project_payload":"remove this"}'::jsonb,
            '["project-1", "project-2"]'::jsonb,
            'executed'
        );
        INSERT INTO public.entity_global_merge_mutations (
            merge_id, object_kind, object_key, before_value, after_value
        ) VALUES (
            'merge-project-1', 'entity_aliases', '2',
            '{"project_payload":"remove this"}'::jsonb,
            '{"project_payload":"remove this"}'::jsonb
        );
        """
    )

    with pytest.raises(RaiseException, match="canonical_name is immutable"):
        await real_postgres_client.execute(
            """
            UPDATE public.entities
            SET canonical_name = 'Ordinary Rename'
            WHERE entity_id = 2
            """
        )

    deleted = await ProjectDeletionWriter(real_postgres_client).delete_project(
        user_name="ada",
        project_id="project-1",
    )

    assert deleted is not None
    assert deleted["projects"] == 1
    assert deleted["cache_project_ids"] == ["project-2", "project-3"]
    assert await real_postgres_client.fetch_one(
        """
        SELECT canonical_name FROM public.entities WHERE entity_id = 2
        """
    ) == {"canonical_name": "User Preferred Name"}
    with pytest.raises(RaiseException, match="canonical_name is immutable"):
        await real_postgres_client.execute(
            """
            UPDATE public.entities
            SET canonical_name = 'Second Ordinary Rename'
            WHERE entity_id = 2
            """
        )
    assert await real_postgres_client.fetch_all(
        """
        SELECT alias FROM public.entity_aliases
        WHERE entity_id = 2
        ORDER BY alias
        """
    ) == [
        {"alias": "Project Two Name"},
        {"alias": "User Preferred Name"},
    ]
    assert await real_postgres_client.fetch_all(
        """
        SELECT name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 2
        ORDER BY name, project_id
        """
    ) == [
        {
            "name": "Project Two Name",
            "project_id": "project-2",
            "source_kind": "project",
            "source_key": "project-2",
        },
        {
            "name": "User Preferred Name",
            "project_id": None,
            "source_kind": "user",
            "source_key": "ada",
        }
    ]
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id = 3"
    ) == {"count": 0}

    reader = EntityReader(real_postgres_client)
    assert await reader.get_entities_by_names(
        ["Project One Alias"],
        visible_project_ids=["project-2"],
    ) == []
    assert [entity["id"] for entity in await reader.get_entities_by_names(
        ["Project Two Name"],
        visible_project_ids=["project-2"],
    )] == [2]

    # Delete leaves no fact tombstone: later source-grounded evidence may
    # introduce the same name through another surviving project.
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (4, 'ada', 'Project One Only');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-3', 4, 'ada', 'concept', 'General');
        INSERT INTO public.entity_name_supports (
            entity_id, name, project_id, source_kind, source_key
        ) VALUES (4, 'Project One Only', 'project-3', 'project', 'project-3');
        """
    )
    assert [entity["id"] for entity in await reader.get_entities_by_names(
        ["Project One Only"],
        visible_project_ids=["project-3"],
    )] == [4]

    assert await real_postgres_client.fetch_one(
        """
        SELECT status, plan, affected_project_ids, failure_reason
        FROM public.entity_global_merge_audits
        WHERE merge_id = 'merge-project-1'
        """
    ) == {
        "status": "failed",
        "plan": {},
        "affected_project_ids": ["project-2"],
        "failure_reason": "project_deleted",
    }
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.entity_global_merge_mutations
        WHERE merge_id = 'merge-project-1'
        """
    ) == {"count": 0}
    with pytest.raises(ValueError, match="already failed"):
        await GlobalEntityMergeWriter(real_postgres_client).plan_rollback(
            merge_id="merge-project-1",
            user_name="ada",
        )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_delete_removes_redirected_entity_with_its_unsupported_merge(
    real_postgres_client,
):
    """A retired merge identity cannot block deletion of its unsupported survivor."""

    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name) VALUES
            (2, 'ada', 'First Name'),
            (3, 'ada', 'Second Name');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'concept', 'General'),
            ('project-1', 3, 'ada', 'concept', 'General');
        INSERT INTO public.entity_aliases (entity_id, alias) VALUES
            (2, 'First Alias'),
            (3, 'Second Alias');
        INSERT INTO public.entity_name_supports (
            entity_id, name, project_id, source_kind, source_key
        ) VALUES
            (2, 'First Name', 'project-1', 'project', 'project-1'),
            (2, 'First Alias', 'project-1', 'project', 'project-1'),
            (3, 'Second Name', 'project-1', 'project', 'project-1'),
            (3, 'Second Alias', 'project-1', 'project', 'project-1');
        """
    )
    await GlobalEntityMergeWriter(real_postgres_client).merge(
        user_name="ada",
        survivor_id=2,
        retired_id=3,
        merge_id="merge-redirect",
    )

    deleted = await ProjectDeletionWriter(real_postgres_client).delete_project(
        user_name="ada",
        project_id="project-1",
    )

    assert deleted is not None
    assert deleted["entities"] == 2
    assert deleted["affected_entity_ids"] == [2, 3]
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id IN (2, 3)"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, plan, failure_reason
        FROM public.entity_global_merge_audits
        WHERE merge_id = 'merge-redirect'
        """
    ) == {
        "status": "failed",
        "plan": {},
        "failure_reason": "project_deleted",
    }
