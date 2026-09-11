"""Historical review probes plus active reproductions for unresolved findings.

F1, F2, F3, and F7 are skipped historical records after Stages 1 and 2:
normal regressions now assert the desired behavior. The remaining probes still
document active findings rather than desired acceptance contracts.
"""

from uuid import uuid4

import pytest

from common.exceptions import StorageWriteError
from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextRelationshipWrite,
)
from common.schema.semantic_window import SemanticWindowStage
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter
from tests.contract.storage.test_semantic_commit_contract import (
    _block,
    _build,
    _commit_context,
    _entity,
    _membership,
    _seed_message,
    _window,
    _WindowContext,
)

pytest_plugins = ["tests.contract.storage.conftest"]


def relationship(block_id):
    return ContextRelationshipWrite(
        support_block_ids=(block_id,),
        entity_a_id=10,
        entity_b_id=11,
        relationship_type="owns",
        canonical_type="OWNS",
        source_type="Person",
        target_type="Company",
        domain_version=1,
    )


async def advance(client, window, expected, next_stage, revision=None):
    return await SemanticWindowWriter(client).advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=expected,
        next_stage=next_stage,
        **({"context_revision_id": revision} if revision else {}),
    )


async def initial_context(client):
    await _seed_message(client)
    window = _window()
    assert (
        await SemanticWindowWriter(client).claim_window(window, _membership())
    ).claimed
    block = _block("Sarah owns Delta.")
    context = await _commit_context(client, window, (block,))
    await advance(
        client,
        window,
        SemanticWindowStage.CLAIMED,
        SemanticWindowStage.CONTEXT_COMMITTED,
        context.revision_id,
    )
    return window, block, context


def build_for(window, block, context, *, new_entities):
    return _build(
        _WindowContext(window.window_id, context),
        impact=(block.block_id,),
        entity_ids=(10, 11),
        entities=new_entities,
        associations=tuple(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=i,
                mention_text=name,
            )
            for i, name in ((10, "Sarah"), (11, "Delta"))
        ),
        relationship=relationship(block.block_id),
    )


@pytest.mark.skip(
    reason=(
        "Historical F1 reproduction resolved by 98414ff; desired behavior is "
        "covered by the Stage 1 "
        "ingestion and semantic-commit regressions"
    ),
)
async def test_noop_context_reuses_previous_impact_in_next_stage():
    from common.schema.settings import IngestionSettings
    from core.ingestion.project_semantic_job import ProjectSemanticJob
    from infrastructure.job.base import JobContext
    from tests.unit.core.ingestion.test_project_semantic_context_stage import (
        _block as unit_block,
    )
    from tests.unit.core.ingestion.test_project_semantic_context_stage import (
        _ContextStore,
        _job,
        _NoopUpdater,
        _snapshot,
    )
    from tests.unit.core.ingestion.test_project_semantic_context_stage import (
        _window as unit_window,
    )
    from tests.unit.core.ingestion.test_project_semantic_knowledge_stage import (
        _Admission,
        _Builder,
        _domain,
        _Relationships,
        _Store,
    )

    old = _snapshot(unit_block("Earlier established knowledge."))
    store = _ContextStore(unit_window(), current_snapshot=old)
    assert (
        await _job(store, _NoopUpdater()).execute(
            JobContext(user_name="ada", project_id="project-1")
        )
    ).success
    assert store.window.context_revision_id == old.revision_id
    assert store.commit_calls == []

    knowledge_store = _Store()

    async def capture_domain():
        return _domain()

    job = ProjectSemanticJob(
        _Admission(),
        knowledge_store,
        object(),
        settings=IngestionSettings(semantic_window_tokens=1),
        capture_domain=capture_domain,
        context_entity_builder=_Builder(),
        context_relationship_extractor=_Relationships(),
    )
    assert (
        await job.execute(JobContext(user_name="ada", project_id="project-1"))
    ).success
    assert knowledge_store.commit_calls[0].impact_block_ids == {
        knowledge_store.block.block_id
    }


@pytest.mark.skip(
    reason=(
        "Historical F7 reproduction resolved by 71112d0; desired behavior is "
        "covered by current-read "
        "and Stage 1 combined regressions"
    ),
)
async def test_two_windows_same_context_create_two_active_observations(
    real_postgres_client,
):
    client = real_postgres_client
    window, block, context = await initial_context(client)
    writer = SemanticCommitWriter(client)
    await writer.commit(
        build_for(
            window,
            block,
            context,
            new_entities={
                10: _entity(10, "Sarah", "Person"),
                11: _entity(11, "Delta", "Company"),
            },
        )
    )
    await advance(
        client,
        window,
        SemanticWindowStage.KNOWLEDGE_COMMITTED,
        SemanticWindowStage.COMPLETED,
    )
    second = _window()
    assert (
        await SemanticWindowWriter(client).claim_window(second, _membership(102))
    ).claimed
    await advance(
        client,
        second,
        SemanticWindowStage.CLAIMED,
        SemanticWindowStage.CONTEXT_COMMITTED,
        context.revision_id,
    )
    await writer.commit(build_for(second, block, context, new_entities={}))
    counts = await client.fetch_one("""
        SELECT count(*) AS observations,
               count(DISTINCT semantic_window_id) AS windows
        FROM relationship_observations WHERE retired_at IS NULL
    """)
    assert counts == {"observations": 2, "windows": 2}
    assert await client.fetch_one(
        "SELECT count(DISTINCT block_id) AS blocks FROM relationship_observation_blocks"
    ) == {"blocks": 1}
    # Current path evidence still includes a retired observation while another
    # active observation keeps the same aggregate relationship alive.
    await client.execute(
        "UPDATE relationship_observations SET retired_at=NOW(), retired_reason='review_probe' WHERE semantic_window_id=%s",
        (window.window_id,),
    )
    from core.knowledge.db.readers.graph_reader import GraphReader

    row = await client.fetch_one("SELECT relationship_id FROM relationships")
    refs = await GraphReader(client)._relationship_observation_refs(
        [row["relationship_id"]], ["project-1"]
    )
    assert len(refs[0]) == 2


@pytest.mark.skip(
    reason=(
        "Historical F2 reproduction resolved by 4eac23e; desired behavior is "
        "covered by test_semantic_commit_admits_a_currently_readable_foreign_identity "
        "and the Stage 2 composition regression"
    ),
)
async def test_linked_entity_without_local_classification_rejected(
    real_postgres_client,
):
    client = real_postgres_client
    window, block, context = await initial_context(client)
    await client.execute("""
        INSERT INTO entities (entity_id, user_name, canonical_name)
        VALUES (10, 'ada', 'Sarah');
        INSERT INTO project_entity_contexts (project_id, entity_id, user_name, entity_type, topic)
        VALUES ('project-2', 10, 'ada', 'Person', 'Work');
    """)
    build = build_for(
        window, block, context, new_entities={11: _entity(11, "Delta", "Company")}
    )
    with pytest.raises(StorageWriteError) as error:
        await SemanticCommitWriter(client).commit(build)
    assert "context block entity must be visible" in str(error.value.__cause__)
    assert await client.fetch_one(
        "SELECT count(*) AS n FROM project_entity_contexts WHERE project_id='project-1' AND entity_id=10"
    ) == {"n": 0}


@pytest.mark.skip(
    reason=(
        "Historical F2 cache-only reproduction resolved by 4eac23e; desired "
        "behavior is covered by the storage and Stage 2 composition regressions"
    ),
)
async def test_resolver_accepts_entity_from_readable_project():
    from common.schema.ingestion.contracts import ContextBlockMention
    from core.knowledge.entity.resolver import EntityResolver
    from tests.contract.storage.test_semantic_commit_contract import _policy
    from tests.unit.core.knowledge.conftest import (
        FakeEmbeddingService,
        FakeEntityKnowledgeStore,
    )

    store = FakeEntityKnowledgeStore()
    store.add_entity(
        10, "Sarah Chen", entity_type="Person", topic="Work", project_id="project-2"
    )
    resolver = EntityResolver(
        store, FakeEmbeddingService(), "project-1", ["project-1", "project-2"]
    )
    # A prior entity lookup legitimately warms the resolver's visible cache.
    await resolver.get_profile(10)
    block_id = uuid4()

    async def allocate():
        raise AssertionError("Should reuse the readable identity")

    result = await resolver.resolve_context_block_mentions(
        [
            ContextBlockMention(
                name="Sarah Chen",
                entity_type="Person",
                topic="Work",
                block_ids=(block_id,),
                origin="vp01",
            )
        ],
        block_text_by_id={block_id: "Sarah Chen owns Delta."},
        policy=_policy(),
        allocate_entity_id=allocate,
    )
    assert result["entity_ids"] == (10,)
    assert result["pending_entity_writes"] == {}


@pytest.mark.skip(
    reason=(
        "Historical F3 reproduction resolved by a0b49af and beb68b6; desired "
        "behavior is covered by test_cold_resolver_hydrates_a_durable_exact_alias "
        "and the Stage 2 composition regression"
    ),
)
async def test_cold_resolver_misses_durable_exact_alias_when_vector_does_not_match():
    from core.knowledge.entity.resolver import EntityResolver
    from tests.unit.core.knowledge.conftest import (
        FakeEmbeddingService,
        FakeEntityKnowledgeStore,
    )

    store = FakeEntityKnowledgeStore()
    store.add_entity(
        10,
        "International Business Machines",
        aliases=["IBM"],
        entity_type="Company",
        topic="Work",
    )
    resolver = EntityResolver(store, FakeEmbeddingService(), "project-1", ["project-1"])
    assert await store.get_entities_by_names(["IBM"], visible_project_ids=["project-1"])
    store.name_lookups.clear()
    assert await resolver.get_candidate_ids("IBM", strict=True) == []
    assert store.name_lookups == []


async def test_document_index_accepts_changed_bytes_under_old_hash(
    real_postgres_client, tmp_path
):
    import hashlib

    from core.knowledge.documents import DocumentService, ProjectFilesystemFactory
    from tests.unit.core.knowledge.test_document_service import (
        FakeEmbeddingService,
        run_inline,
    )

    client = real_postgres_client
    service = DocumentService(
        project_id="project-1",
        postgres_client=client,
        embedding_service=FakeEmbeddingService(),
        blocking_runner=run_inline,
        document_rerank_enabled=False,
        filesystem_factory=ProjectFilesystemFactory(tmp_path / "projects"),
    )
    old = b"Launch is Friday."
    new = b"Launch is Monday."
    metadata = await service.add_document(content=old, original_name="launch.txt")
    files = list(tmp_path.rglob("launch.txt"))
    assert len(files) == 1
    files[0].write_bytes(new)
    indexed = await service.index_document(document_id=metadata["document_id"])
    assert indexed["status"] == "indexed"
    assert indexed["content_hash"] == hashlib.sha256(old).hexdigest()
    extraction = await client.fetch_one(
        "SELECT extracted_text, extracted_content_hash FROM document_extractions WHERE document_id=%s",
        (metadata["document_id"],),
    )
    assert extraction["extracted_text"] == new.decode()
    assert extraction["extracted_content_hash"] == hashlib.sha256(old).hexdigest()
