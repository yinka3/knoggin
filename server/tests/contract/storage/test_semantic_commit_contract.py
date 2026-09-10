"""Fresh-schema contracts for Context-first semantic Knowledge commits."""

from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextBlockSupportRecord,
    ContextRevisionOrigin,
    ContextSupportKind,
)
from common.schema.ingestion.contracts import (
    ContextBlockEntityAssociation,
    ContextEntityResult,
    ContextRelationshipWrite,
    EntityWrite,
)
from common.schema.semantic_window import (
    SemanticWindowMessage,
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.policy import IngestionPolicy
from core.knowledge.conflicts import ConflictDiscoveryCursor
from core.knowledge.context.models import ContextBlockSupport, ContextMaterialization
from core.knowledge.context.render import context_block_hash, context_document_hash
from core.knowledge.db.readers.conflict_discovery_reader import ConflictDiscoveryReader
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter


def _domain():
    return DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                "Person": {"topic": "Work", "labels": ["person"]},
                "Company": {"topic": "Work", "labels": ["company"]},
            },
            "relationships": {
                "OWNS": {
                    "source_types": ["Person"],
                    "target_types": ["Company"],
                    "labels": ["owns"],
                }
            },
        }
    ).compile()


def _policy():
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(llm_ner=False),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_domain(),
    )


def _block(markdown: str, *, supersedes_block_id=None, source_time_ms=1_000):
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=AssertionKind.SOURCE_GROUNDED,
        supersedes_block_id=supersedes_block_id,
        source_time_ms=source_time_ms,
    )


def _window():
    return SemanticWindowRecord(
        window_id=uuid4(),
        user_name="ada",
        project_id="project-1",
        origin=SemanticWindowOrigin.CONVERSATION,
        stage=SemanticWindowStage.CLAIMED,
        domain_version=1,
        policy_snapshot={"ingestion_policy": _policy().semantic_window_snapshot()},
        source_token_count=1,
        token_estimator="test",
        token_estimator_version="1",
    )


def _membership(message_id=101):
    return [
        SemanticWindowMessage(
            message_id=message_id,
            session_id="session-1",
            exchange_user_message_id=message_id,
            role="user",
            ordinal=0,
        )
    ]


async def _seed_message(client):
    await client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1');
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, exchange_state, exchange_outcome, exchange_closed_at_ms
        ) VALUES (
            'ada', 'session-1', 101, 'project-1', 'user', 'Sarah owns Delta.',
            'sealed', 'closed', 'user_only', 101
        ), (
            'ada', 'session-1', 102, 'project-1', 'user', 'John owns Delta.',
            'sealed', 'closed', 'user_only', 102
        );
        """
    )


async def _commit_context(
    client, window, blocks, *, parent=None, impact=(), support_message_id=101
):
    materialization = ContextMaterialization(
        blocks=tuple(blocks),
        content_hash=context_document_hash(tuple(blocks), _domain()),
        new_block_ids=frozenset(
            block.block_id for block in blocks if block.block_id not in {parent_block.block_id for parent_block in (parent.blocks if parent else [])}
        ),
        impacted_block_ids=frozenset(impact or [block.block_id for block in blocks]),
        supports=tuple(
            ContextBlockSupport(
                block_id=block.block_id,
                message_id=support_message_id,
                session_id="session-1",
                support_kind=ContextSupportKind.USER_MESSAGE,
            )
            for block in blocks
            if parent is None or block.block_id not in {item.block_id for item in parent.blocks}
        ),
    )
    return await ProjectContextWriter(client).commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None if parent is None else parent.revision_id,
        window_id=window.window_id,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Context changed",
        materialization=materialization,
    )


def _entity(entity_id, name, entity_type):
    return EntityWrite(
        entity_id=entity_id,
        is_new=True,
        canonical_name=name,
        entity_type=entity_type,
        topic="Work",
        embedding=None,
        aliases=(name,),
    )


def _build(
    snapshot,
    *,
    impact,
    entity_ids,
    entities,
    associations,
    relationship=None,
    relationship_writes=None,
    support_message_id=101,
):
    result = ContextEntityResult(
        entity_ids=tuple(entity_ids),
        new_entity_ids=frozenset(entities),
        alias_updated_ids=frozenset(),
        alias_updates={},
        pending_entity_writes={item.entity_id: item for item in entities.values()},
        block_entity_associations=tuple(associations),
        message_entity_refs=(),
    )
    build = SemanticWindowBuild(
        window_id=snapshot.window_id,
        user_name="ada",
        project_id="project-1",
        context=snapshot.context,
        impact_block_ids=frozenset(impact),
        policy=_policy(),
        policy_snapshot={"ingestion_policy": _policy().semantic_window_snapshot()},
        block_supports={
            block.block_id: (
                ContextBlockSupportRecord(
                    block_id=block.block_id,
                    project_id="project-1",
                    message_id=support_message_id,
                    session_id="session-1",
                    support_kind=ContextSupportKind.USER_MESSAGE,
                ),
            )
            for block in snapshot.context.blocks
        },
        message_text_by_id={support_message_id: "Sarah owns Delta."},
    )
    build.set_entity_result(result)
    if relationship is not None and relationship_writes is not None:
        raise ValueError("test build accepts either relationship or relationship_writes")
    if relationship_writes is None:
        relationship_writes = () if relationship is None else (relationship,)
    build.set_relationship_writes(relationship_writes)
    return build


def _empty_build(window_id, context):
    build = SemanticWindowBuild(
        window_id=window_id,
        user_name="ada",
        project_id="project-1",
        context=context,
        impact_block_ids=frozenset(),
        policy=_policy(),
        policy_snapshot={"ingestion_policy": _policy().semantic_window_snapshot()},
        block_supports={},
        message_text_by_id={},
    )
    build.set_empty_knowledge_result()
    return build


class _WindowContext:
    def __init__(self, window_id, context):
        self.window_id = window_id
        self.context = context


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_is_atomic_idempotent_and_retracts_replaced_support(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window_writer = SemanticWindowWriter(real_postgres_client)
    first_window = _window()
    assert (await window_writer.claim_window(first_window, _membership())).claimed
    old_block = _block("Sarah owns Delta.")
    adjacent_block = _block("The ownership remains active.")
    first_context = await _commit_context(
        real_postgres_client, first_window, (old_block, adjacent_block)
    )
    assert await window_writer.advance_stage(
        window_id=first_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=first_context.revision_id,
    )
    first_build = _build(
        _WindowContext(first_window.window_id, first_context),
        impact=(old_block.block_id, adjacent_block.block_id),
        entity_ids=(10, 11),
        entities={10: _entity(10, "Sarah", "Person"), 11: _entity(11, "Delta", "Company")},
        associations=(
            ContextBlockEntityAssociation(block_id=old_block.block_id, entity_id=10, mention_text="Sarah"),
            ContextBlockEntityAssociation(block_id=old_block.block_id, entity_id=11, mention_text="Delta"),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(old_block.block_id, adjacent_block.block_id),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )
    writer = SemanticCommitWriter(real_postgres_client)
    first = await writer.commit(first_build)
    resumed = await writer.commit(first_build)

    assert first.resumed is False
    assert first.relationships_written == 1
    assert resumed.resumed is True
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationship_observation_blocks"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (first_window.window_id,),
    ) == {"stage": "knowledge_committed"}
    assert await window_writer.advance_stage(
        window_id=first_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.KNOWLEDGE_COMMITTED,
        next_stage=SemanticWindowStage.COMPLETED,
    )
    old_observation = await real_postgres_client.fetch_one(
        """
        SELECT observation_id, relationship_id
        FROM public.relationship_observations
        WHERE source_entity_id = 10
        """
    )

    second_window = _window()
    assert (await window_writer.claim_window(second_window, _membership(102))).claimed
    replacement = _block("John owns Delta.", supersedes_block_id=old_block.block_id)
    second_context = await _commit_context(
        real_postgres_client,
        second_window,
        (replacement,),
        parent=first_context,
        impact=(old_block.block_id, adjacent_block.block_id, replacement.block_id),
        support_message_id=102,
    )
    assert await window_writer.advance_stage(
        window_id=second_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=second_context.revision_id,
    )
    second_build = _build(
        _WindowContext(second_window.window_id, second_context),
        impact=(old_block.block_id, adjacent_block.block_id, replacement.block_id),
        entity_ids=(12, 11),
        entities={12: _entity(12, "John", "Person")},
        associations=(
            ContextBlockEntityAssociation(block_id=replacement.block_id, entity_id=12, mention_text="John"),
            ContextBlockEntityAssociation(block_id=replacement.block_id, entity_id=11, mention_text="Delta"),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(replacement.block_id,),
            entity_a_id=12,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
        support_message_id=102,
    )
    second = await writer.commit(second_build)

    assert second.observations_retired == 1
    assert second.relationships_removed == 1
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationships WHERE entity_a_id = 10"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        """
        SELECT relationship_id, retired_reason
        FROM public.relationship_observations
        WHERE source_entity_id = 10
        """
    ) == {
        "relationship_id": None,
        "retired_reason": "context_block_replaced_or_deleted",
    }
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationships WHERE entity_a_id = 12"
    ) == {"count": 1}
    audit = await real_postgres_client.fetch_one(
        """
        SELECT observation_ids, changes
        FROM public.maintenance_reinterpretation_audits
        WHERE project_id = 'project-1'
        """
    )
    assert audit["observation_ids"] == [old_observation["observation_id"]]
    assert audit["changes"] == [
        {
            "interpretation_source": "context_reconciliation",
            "new_relationship_id": None,
            "observation_id": old_observation["observation_id"],
            "old_relationship_id": old_observation["relationship_id"],
            "reason": "context_block_replaced_or_deleted",
        }
    ]
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.maintenance_reviews"
    ) == {"count": 0}
    seeds = await ConflictDiscoveryReader(real_postgres_client).get_seed_observations(
        ConflictDiscoveryCursor("ada", "project-1", 0),
        max_span_days=60,
    )
    assert [row["source_entity_id"] for row in seeds] == [12]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_reuses_published_context_without_replaying_its_impact(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window_writer = SemanticWindowWriter(real_postgres_client)
    owner_window = _window()
    assert (await window_writer.claim_window(owner_window, _membership())).claimed
    block = _block("Sarah owns Delta.")
    context = await _commit_context(real_postgres_client, owner_window, (block,))
    assert await window_writer.advance_stage(
        window_id=owner_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    writer = SemanticCommitWriter(real_postgres_client)
    owner_build = _build(
        _WindowContext(owner_window.window_id, context),
        impact=(block.block_id,),
        entity_ids=(10, 11),
        entities={10: _entity(10, "Sarah", "Person"), 11: _entity(11, "Delta", "Company")},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id, entity_id=10, mention_text="Sarah"
            ),
            ContextBlockEntityAssociation(
                block_id=block.block_id, entity_id=11, mention_text="Delta"
            ),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(block.block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )
    assert (await writer.commit(owner_build)).relationships_written == 1
    assert await window_writer.advance_stage(
        window_id=owner_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.KNOWLEDGE_COMMITTED,
        next_stage=SemanticWindowStage.COMPLETED,
    )

    no_op_window = _window()
    assert (await window_writer.claim_window(no_op_window, _membership(102))).claimed
    assert await window_writer.advance_stage(
        window_id=no_op_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )

    replayed_build = _build(
        _WindowContext(no_op_window.window_id, context),
        impact=(block.block_id,),
        entity_ids=(10, 11),
        entities={10: _entity(10, "Sarah", "Person"), 11: _entity(11, "Delta", "Company")},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id, entity_id=10, mention_text="Sarah"
            ),
            ContextBlockEntityAssociation(
                block_id=block.block_id, entity_id=11, mention_text="Delta"
            ),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(block.block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )
    with pytest.raises(ValueError, match="impact closure"):
        await writer.commit(replayed_build)

    malformed_empty_build = _empty_build(no_op_window.window_id, context)
    malformed_empty_build.set_entity_result(
        ContextEntityResult(
            entity_ids=(10,),
            new_entity_ids=frozenset(),
            alias_updated_ids=frozenset(),
            alias_updates={},
            pending_entity_writes={},
            block_entity_associations=(),
            message_entity_refs=(),
        )
    )
    with pytest.raises(ValueError, match="empty Knowledge build"):
        await writer.commit(malformed_empty_build)

    no_op_build = _empty_build(no_op_window.window_id, context)
    committed = await writer.commit(no_op_build)
    resumed = await writer.commit(no_op_build)

    assert committed.resumed is False
    assert committed.relationships_written == 0
    assert resumed.resumed is True
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationship_observations"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationship_observation_blocks"
    ) == {"count": 1}
    assert await window_writer.advance_stage(
        window_id=no_op_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.KNOWLEDGE_COMMITTED,
        next_stage=SemanticWindowStage.COMPLETED,
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_rejects_an_unowned_context_revision(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    context = await ProjectContextWriter(real_postgres_client).commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=None,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Unowned initial Context",
        materialization=ContextMaterialization(
            blocks=(),
            content_hash=context_document_hash((), _domain()),
            new_block_ids=frozenset(),
            impacted_block_ids=frozenset(),
        ),
    )
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )

    with pytest.raises(ValueError, match="has no owning window"):
        await SemanticCommitWriter(real_postgres_client).commit(
            _empty_build(window.window_id, context)
        )

    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "context_committed"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_accepts_an_owned_empty_context_revision(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    context = await ProjectContextWriter(real_postgres_client).commit_revision(
        user_name="ada",
        project_id="project-1",
        expected_parent_revision_id=None,
        window_id=window.window_id,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        edit_summary="Initial empty Context",
        materialization=ContextMaterialization(
            blocks=(),
            content_hash=context_document_hash((), _domain()),
            new_block_ids=frozenset(),
            impacted_block_ids=frozenset(),
        ),
    )
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )

    summary = await SemanticCommitWriter(real_postgres_client).commit(
        _empty_build(window.window_id, context)
    )

    assert summary.resumed is False
    assert summary.relationships_written == 0
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "knowledge_committed"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_rolls_back_every_write_when_endpoint_validation_fails(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Sarah owns Delta.")
    context = await _commit_context(real_postgres_client, window, (block,))
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    build = _build(
        _WindowContext(window.window_id, context),
        impact=(block.block_id,),
        entity_ids=(10, 11),
        entities={10: _entity(10, "Sarah", "Person")},
        associations=(
            ContextBlockEntityAssociation(block_id=block.block_id, entity_id=10, mention_text="Sarah"),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(block.block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )

    with pytest.raises(ValueError, match="endpoints"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id = 10"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "context_committed"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_uses_persisted_context_source_times(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    early_block = _block("Sarah owns EarlyCo.", source_time_ms=600_000)
    later_block = _block("John owns LaterCo.", source_time_ms=602_000)
    unrelated_block = _block("IgnoreCo is mentioned separately.", source_time_ms=604_000)
    context = await _commit_context(
        real_postgres_client,
        window,
        (early_block, later_block, unrelated_block),
    )
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    forged_context = context.model_copy(
        update={
            "blocks": [
                block.model_copy(update={"source_time_ms": 999_999})
                for block in context.blocks
            ]
        }
    )
    build = _build(
        _WindowContext(window.window_id, forged_context),
        impact=(early_block.block_id, later_block.block_id, unrelated_block.block_id),
        entity_ids=(10, 11, 12, 13, 14),
        entities={
            10: _entity(10, "Sarah", "Person"),
            11: _entity(11, "EarlyCo", "Company"),
            12: _entity(12, "John", "Person"),
            13: _entity(13, "LaterCo", "Company"),
            14: _entity(14, "IgnoreCo", "Company"),
        },
        associations=(
            ContextBlockEntityAssociation(
                block_id=early_block.block_id, entity_id=10, mention_text="Sarah"
            ),
            ContextBlockEntityAssociation(
                block_id=early_block.block_id, entity_id=11, mention_text="EarlyCo"
            ),
            ContextBlockEntityAssociation(
                block_id=later_block.block_id, entity_id=12, mention_text="John"
            ),
            ContextBlockEntityAssociation(
                block_id=later_block.block_id, entity_id=13, mention_text="LaterCo"
            ),
            ContextBlockEntityAssociation(
                block_id=unrelated_block.block_id,
                entity_id=14,
                mention_text="IgnoreCo",
            ),
        ),
        relationship_writes=(
            ContextRelationshipWrite(
                support_block_ids=(early_block.block_id,),
                entity_a_id=10,
                entity_b_id=11,
                relationship_type="owns",
                canonical_type="OWNS",
                source_type="Person",
                target_type="Company",
                domain_version=1,
            ),
            ContextRelationshipWrite(
                support_block_ids=(early_block.block_id, later_block.block_id),
                entity_a_id=12,
                entity_b_id=13,
                relationship_type="owns",
                canonical_type="OWNS",
                source_type="Person",
                target_type="Company",
                domain_version=1,
            ),
        ),
    )

    summary = await SemanticCommitWriter(real_postgres_client).commit(build)

    assert summary.relationships_written == 2
    assert await real_postgres_client.fetch_all(
        """
        SELECT source_entity_id, target_entity_id, observed_at_ms
        FROM public.relationship_observations
        ORDER BY source_entity_id, target_entity_id
        """
    ) == [
        {"source_entity_id": 10, "target_entity_id": 11, "observed_at_ms": 600_000},
        {"source_entity_id": 12, "target_entity_id": 13, "observed_at_ms": 602_000},
    ]
    entity_rows = await real_postgres_client.fetch_all(
        """
        SELECT entity_id, last_mentioned_ms
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1'
        ORDER BY entity_id
        """
    )
    assert {row["entity_id"]: row["last_mentioned_ms"] for row in entity_rows} == {
        10: 600_000,
        11: 600_000,
        12: 602_000,
        13: 602_000,
        14: 604_000,
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_updates_existing_entity_recency_monotonically(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (10, 'ada', 'Sarah');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic, last_mentioned_ms
        ) VALUES ('project-1', 10, 'ada', 'Person', 'Work', 700000);
        """
    )
    window_writer = SemanticWindowWriter(real_postgres_client)
    first_window = _window()
    assert (await window_writer.claim_window(first_window, _membership())).claimed
    older_block = _block("Sarah was mentioned earlier.", source_time_ms=600_000)
    first_context = await _commit_context(
        real_postgres_client, first_window, (older_block,)
    )
    assert await window_writer.advance_stage(
        window_id=first_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=first_context.revision_id,
    )
    older_build = _build(
        _WindowContext(first_window.window_id, first_context),
        impact=(older_block.block_id,),
        entity_ids=(10,),
        entities={},
        associations=(
            ContextBlockEntityAssociation(
                block_id=older_block.block_id, entity_id=10, mention_text="Sarah"
            ),
        ),
    )
    await SemanticCommitWriter(real_postgres_client).commit(older_build)
    assert await real_postgres_client.fetch_one(
        """
        SELECT last_mentioned_ms
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1' AND entity_id = 10
        """
    ) == {"last_mentioned_ms": 700_000}
    assert await window_writer.advance_stage(
        window_id=first_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.KNOWLEDGE_COMMITTED,
        next_stage=SemanticWindowStage.COMPLETED,
    )

    second_window = _window()
    assert (await window_writer.claim_window(second_window, _membership(102))).claimed
    newer_block = _block("Sarah was mentioned later.", source_time_ms=800_000)
    second_context = await _commit_context(
        real_postgres_client,
        second_window,
        (older_block, newer_block),
        parent=first_context,
        impact=(newer_block.block_id,),
        support_message_id=102,
    )
    assert await window_writer.advance_stage(
        window_id=second_window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=second_context.revision_id,
    )
    newer_build = _build(
        _WindowContext(second_window.window_id, second_context),
        impact=(newer_block.block_id,),
        entity_ids=(10,),
        entities={},
        associations=(
            ContextBlockEntityAssociation(
                block_id=newer_block.block_id, entity_id=10, mention_text="Sarah"
            ),
        ),
    )
    await SemanticCommitWriter(real_postgres_client).commit(newer_build)

    assert await real_postgres_client.fetch_one(
        """
        SELECT canonical_name
        FROM public.entities
        WHERE entity_id = 10
        """
    ) == {"canonical_name": "Sarah"}
    assert await real_postgres_client.fetch_one(
        """
        SELECT last_mentioned_ms
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1' AND entity_id = 10
        """
    ) == {"last_mentioned_ms": 800_000}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id = 10"
    ) == {"count": 1}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_rejects_wholly_untimed_context_support(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    timed_block = _block("Sarah owns Delta.", source_time_ms=1_000)
    untimed_block = _block("Sarah owns Delta without a timestamp.", source_time_ms=None)
    context = await _commit_context(
        real_postgres_client, window, (timed_block, untimed_block)
    )
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    entities = {
        10: _entity(10, "Sarah", "Person"),
        11: _entity(11, "Delta", "Company"),
    }
    relationship_build = _build(
        _WindowContext(window.window_id, context),
        impact=(timed_block.block_id, untimed_block.block_id),
        entity_ids=(10, 11),
        entities=entities,
        associations=(
            ContextBlockEntityAssociation(
                block_id=timed_block.block_id, entity_id=10, mention_text="Sarah"
            ),
            ContextBlockEntityAssociation(
                block_id=timed_block.block_id, entity_id=11, mention_text="Delta"
            ),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(untimed_block.block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )
    with pytest.raises(ValueError, match="source time"):
        await SemanticCommitWriter(real_postgres_client).commit(relationship_build)

    entity_build = _build(
        _WindowContext(window.window_id, context),
        impact=(timed_block.block_id, untimed_block.block_id),
        entity_ids=(10, 11),
        entities=entities,
        associations=(
            ContextBlockEntityAssociation(
                block_id=untimed_block.block_id, entity_id=10, mention_text="Sarah"
            ),
            ContextBlockEntityAssociation(
                block_id=untimed_block.block_id, entity_id=11, mention_text="Delta"
            ),
        ),
    )
    with pytest.raises(ValueError, match="source time"):
        await SemanticCommitWriter(real_postgres_client).commit(entity_build)

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id IN (10, 11)"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_entity_contexts WHERE project_id = 'project-1'"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.context_block_entities"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationship_observations"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "context_committed"}
