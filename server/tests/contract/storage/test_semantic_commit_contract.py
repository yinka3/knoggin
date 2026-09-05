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


def _block(markdown: str, *, supersedes_block_id=None):
    return ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown=markdown,
        content_hash=context_block_hash(markdown),
        assertion_kind=AssertionKind.SOURCE_GROUNDED,
        supersedes_block_id=supersedes_block_id,
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
    relationship,
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
    build.set_relationship_writes((relationship,))
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
