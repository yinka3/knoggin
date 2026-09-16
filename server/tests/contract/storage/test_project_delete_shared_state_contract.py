"""Project deletion contracts for shared entity state and merge history."""

import pytest
from psycopg.errors import RaiseException

from core.knowledge.db.projection_rebuilder import GraphBuilder
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.writers.global_entity_merge_writer import (
    GlobalEntityMergeWriter,
)
from core.knowledge.db.writers.project_deletion_writer import (
    ProjectDeletionWriter,
)
from core.knowledge.entity.maintenance_service import EntityMaintenanceService


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

    await GraphBuilder(real_postgres_client).rebuild_project_projection(
        "project-2",
        "ada",
    )
    graph_before = await real_postgres_client.fetch_one(
        real_postgres_client.build_cypher(
            """
            MATCH (entity:Entity {id: 2})
            RETURN entity.canonical_name AS canonical_name, entity.aliases AS aliases
            """,
            "canonical_name agtype, aliases agtype",
        ),
        ("{}",),
    )
    assert graph_before is not None
    assert set(graph_before["aliases"]) == {
        "Project One Alias",
        "Project Two Name",
        "User Preferred Name",
    }

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

    graph_after = await real_postgres_client.fetch_one(
        real_postgres_client.build_cypher(
            """
            MATCH (entity:Entity {id: 2})
            RETURN entity.canonical_name AS canonical_name, entity.aliases AS aliases
            """,
            "canonical_name agtype, aliases agtype",
        ),
        ("{}",),
    )
    assert graph_after is not None
    assert graph_after["canonical_name"] == "User Preferred Name"
    assert set(graph_after["aliases"]) == {
        "Project Two Name",
        "User Preferred Name",
    }

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
    with pytest.raises(ValueError, match="invalidated merge"):
        await EntityMaintenanceService(
            postgres=real_postgres_client,
            user_name="ada",
        ).repair_merge_projections("merge-project-1")


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_project_delete_rolls_back_shared_cleanup_when_projection_rebuild_fails(
    real_postgres_client,
    monkeypatch,
):
    """A failed derived rebuild leaves the project and its sources retryable."""

    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (2, 'ada', 'Project One Name');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'concept', 'General'),
            ('project-2', 2, 'ada', 'concept', 'General');
        INSERT INTO public.entity_aliases (entity_id, alias)
        VALUES (2, 'Project Two Name');
        INSERT INTO public.entity_name_supports (
            entity_id, name, project_id, source_kind, source_key
        ) VALUES
            (2, 'Project One Name', 'project-1', 'project', 'project-1'),
            (2, 'Project Two Name', 'project-2', 'project', 'project-2');
        INSERT INTO public.entity_global_merge_audits (
            merge_id, user_name, survivor_entity_id, retired_entity_id,
            plan, affected_project_ids, status
        ) VALUES (
            'merge-retry', 'ada', 2, 99,
            '{"project_payload":"retain until commit"}'::jsonb,
            '["project-1", "project-2"]'::jsonb,
            'executed'
        );
        INSERT INTO public.entity_global_merge_mutations (
            merge_id, object_kind, object_key, before_value, after_value
        ) VALUES (
            'merge-retry', 'entity_aliases', '2',
            '{"project_payload":"retain until commit"}'::jsonb,
            '{"project_payload":"retain until commit"}'::jsonb
        );
        """
    )
    writer = ProjectDeletionWriter(real_postgres_client)
    original_rebuild = writer.projection_rebuilder.rebuild_project_projection

    async def fail_rebuild(*_args, **_kwargs):
        raise RuntimeError("injected projection rebuild failure")

    monkeypatch.setattr(
        writer.projection_rebuilder,
        "rebuild_project_projection",
        fail_rebuild,
    )
    with pytest.raises(RuntimeError, match="injected projection rebuild failure"):
        await writer.delete_project(user_name="ada", project_id="project-1")

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.projects WHERE project_id = 'project-1'"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_file_cleanup_tasks
        WHERE project_id = 'project-1'
        """
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM public.entities WHERE entity_id = 2"
    ) == {"canonical_name": "Project One Name"}
    assert await real_postgres_client.fetch_all(
        "SELECT alias FROM public.entity_aliases WHERE entity_id = 2"
    ) == [{"alias": "Project Two Name"}]
    assert await real_postgres_client.fetch_all(
        """
        SELECT name, project_id
        FROM public.entity_name_supports
        WHERE entity_id = 2
        ORDER BY project_id, name
        """
    ) == [
        {"name": "Project One Name", "project_id": "project-1"},
        {"name": "Project Two Name", "project_id": "project-2"},
    ]
    assert await real_postgres_client.fetch_one(
        """
        SELECT status, plan, failure_reason
        FROM public.entity_global_merge_audits
        WHERE merge_id = 'merge-retry'
        """
    ) == {
        "status": "executed",
        "plan": {"project_payload": "retain until commit"},
        "failure_reason": None,
    }
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.entity_global_merge_mutations
        WHERE merge_id = 'merge-retry'
        """
    ) == {"count": 1}

    monkeypatch.setattr(
        writer.projection_rebuilder,
        "rebuild_project_projection",
        original_rebuild,
    )
    deleted = await writer.delete_project(user_name="ada", project_id="project-1")

    assert deleted is not None
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.projects WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM public.entities WHERE entity_id = 2"
    ) == {"canonical_name": "Project Two Name"}


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
