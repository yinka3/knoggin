"""Fresh-schema contracts for Context-first semantic Knowledge commits."""

import asyncio
from dataclasses import replace
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
    ContextBlockMention,
    ContextEntityResult,
    ContextRelationshipWrite,
    EntityWrite,
    ProjectEntityClassification,
)
from common.schema.jev import JevDecisionRecord, JevPolicy, JevResponse, JevResult
from common.schema.semantic_window import (
    SemanticWindowMessage,
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.context_entity_build import assemble_context_entity_result
from core.ingestion.policy import IngestionPolicy
from core.knowledge.conflict.conflicts import ConflictDiscoveryCursor
from core.knowledge.context.models import ContextBlockSupport, ContextMaterialization
from core.knowledge.context.render import context_block_hash, context_document_hash
from core.knowledge.db.readers.conflict_discovery_reader import ConflictDiscoveryReader
from core.knowledge.db.readers.entity_reader import EntityReader
from core.knowledge.db.readers.project_context_reader import ProjectContextReader
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.db.writers.global_entity_merge_writer import GlobalEntityMergeWriter
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.jev_client import JevWorkBudget


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


def _policy(jev=None):
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_domain(),
        jev=jev,
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


def _classification_policy():
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {
                "Work": {"active": True},
                "Finance": {"active": True},
            },
            "entity_types": {
                "Company": {
                    "topic": "Work",
                    "allowed_topics": ["Work", "Finance"],
                    "labels": ["company"],
                }
            },
        }
    ).compile()
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=JevPolicy(classification_mode="observe"),
    )


def _add_jev_observation(build, block_id, *, evidence_block_id=None):
    record = JevDecisionRecord(
        capability="identity",
        mode="observe",
        project_id=build.project_id,
        window_id=build.window_id,
        pass_number=1,
        occurrence_key=f"0:{block_id}",
        evidence_block_ids=(evidence_block_id or block_id,),
        domain_version=build.policy.domain.version,
        question_version="identity-v1",
        acceptance_policy_version="observe-v1",
        pinned_model="jev-1.13.0",
        option_mapping={"candidate_1": "42"},
        result=JevResult(outcome="unavailable", reason="missing_api_key"),
        baseline_outcome="deterministic_abstention",
        acceptance_status="unavailable",
    )
    build.trace.identity_decisions.append(
        {
            "outcome": "abstained",
            "jev": {
                "record": record.model_dump(mode="json"),
                "baseline_entity_id": None,
                "suggested_entity_id": None,
                "eligible_candidate_count": 1,
                "eligible_candidate_ids": [42],
                "candidate_catalog_truncated": False,
                "offered_candidate_count": 1,
                "candidate_set_truncated": False,
            },
        }
    )


def _add_jev_extraction_observation(build, block_id, *, evidence_block_id=None):
    record = JevDecisionRecord(
        capability="extraction",
        mode="active",
        project_id=build.project_id,
        window_id=build.window_id,
        pass_number=1,
        occurrence_key=f"1:{block_id}:delta",
        evidence_block_ids=(evidence_block_id or block_id,),
        domain_version=build.policy.domain.version,
        question_version="extraction-v1",
        acceptance_policy_version="positive-v1",
        pinned_model="jev-1.13.0",
        option_mapping={},
        result=JevResult(
            outcome="available",
            response=JevResponse.model_validate({
                "model": "jev-1.13.0",
                "answers": {"entity_evidence": {"type": "noul", "noul": 0.9}},
                "usage": {"input_tokens": 20, "output_tokens": 0},
            }),
        ),
        baseline_outcome="unknown_relationship_endpoint",
        acceptance_status="accepted",
    )
    build.trace.extraction_decisions.append(
        {
            "record": record.model_dump(mode="json"),
            "candidate_name": "Delta",
            "evidence_origin": "unknown_relationship_endpoint",
            "proposed_entity_type": "Company",
            "suggested_entity_type": "Company",
            "accepted_entity_type": "Company",
            "candidate_set_truncated": False,
            "has_context_offsets": True,
        }
    )


def _add_jev_classification_observation(
    build, block_id, *, evidence_block_id=None
):
    result = JevResult(
        outcome="available",
        response=JevResponse.model_validate(
            {
                "model": "jev-1.13.0",
                "answers": {
                    "topic_choice": {
                        "type": "choice",
                        "choice": "topic_2",
                        "probabilities": {
                            "topic_1": 0.05,
                            "topic_2": 0.9,
                            "insufficient_evidence": 0.05,
                        },
                        "confidence": 0.9,
                    },
                    "topic_evidence": {"type": "noul", "noul": 0.9},
                },
                "usage": {"input_tokens": 20, "output_tokens": 0},
            }
        ),
    )
    occurrence_key = f"0:{block_id}:topic"
    aggregate = {
        "entity_id": 10,
        "entity_type": "Company",
        "membership": "missing",
        "status": "consistent_proposal",
        "proposed_topics": ["Finance"],
        "operational_topic": "Work",
        "observation_count": 1,
    }
    record = JevDecisionRecord(
        capability="classification",
        mode="observe",
        project_id=build.project_id,
        window_id=build.window_id,
        pass_number=1,
        occurrence_key=occurrence_key,
        evidence_block_ids=(evidence_block_id or block_id,),
        domain_version=build.policy.domain.version,
        question_version=build.policy.jev.classification_question_version,
        acceptance_policy_version=(
            build.policy.jev.classification_acceptance_policy_version
        ),
        pinned_model=build.policy.jev.model,
        option_mapping={"topic_1": "Work", "topic_2": "Finance"},
        result=result,
        baseline_outcome="default_topic",
        acceptance_status="proposed",
    )
    build.trace.classification_decisions.append(
        {
            "record": record.model_dump(mode="json"),
            "entity_id": 10,
            "entity_type": "Company",
            "baseline_topic": "Work",
            "suggested_topic": "Finance",
            "accepted_topic": None,
            "allowed_topics": ["Work", "Finance"],
            "option_set_truncated": False,
            "aggregation": aggregate,
        }
    )
    build.trace.classification_aggregates.append(aggregate)


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
        aliases=(name,),
    )


def _classification(entity_id, entity_type, *, membership):
    return ProjectEntityClassification(
        entity_id=entity_id,
        entity_type=entity_type,
        topic="Work",
        membership=membership,
    )


def _build(
    snapshot,
    *,
    impact,
    entity_ids,
    entities,
    associations,
    existing_classifications=None,
    missing_classifications=None,
    relationship=None,
    relationship_writes=None,
    alias_updates=None,
    support_message_id=101,
    ingestion_policy=None,
):
    existing_classifications = existing_classifications or {}
    missing_classifications = missing_classifications or {}
    alias_updates = alias_updates or {}
    project_classifications = {
        entity_id: _classification(
            entity_id,
            entity.entity_type,
            membership="missing",
        )
        for entity_id, entity in entities.items()
    }
    for entity_id in entity_ids:
        if entity_id in project_classifications:
            continue
        if entity_id in missing_classifications:
            project_classifications[entity_id] = _classification(
                entity_id,
                missing_classifications[entity_id],
                membership="missing",
            )
            continue
        try:
            entity_type = existing_classifications[entity_id]
        except KeyError as exc:
            raise ValueError(
                "test build requires an existing local classification"
            ) from exc
        project_classifications[entity_id] = _classification(
            entity_id,
            entity_type,
            membership="existing",
        )
    result = ContextEntityResult(
        entity_ids=tuple(entity_ids),
        new_entity_ids=frozenset(entities),
        alias_updated_ids=frozenset(alias_updates),
        alias_updates=alias_updates,
        pending_entity_writes={item.entity_id: item for item in entities.values()},
        project_classifications=project_classifications,
        block_entity_associations=tuple(associations),
        message_entity_refs=(),
    )
    ingestion_policy = ingestion_policy or _policy()
    build = SemanticWindowBuild(
        window_id=snapshot.window_id,
        user_name="ada",
        project_id="project-1",
        context=snapshot.context,
        impact_block_ids=frozenset(impact),
        policy=ingestion_policy,
        policy_snapshot={
            "ingestion_policy": ingestion_policy.semantic_window_snapshot()
        },
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


class _PausedMergeWriter(GlobalEntityMergeWriter):
    """Pause after the merge has locked its identity rows."""

    def __init__(self, client, locked: asyncio.Event, release: asyncio.Event):
        super().__init__(client)
        self._locked = locked
        self._release = release

    async def snapshot(
        self,
        user_name: str,
        survivor_id: int,
        retired_id: int,
        *,
        cur=None,
    ) -> dict:
        snapshot = await super().snapshot(
            user_name,
            survivor_id,
            retired_id,
            cur=cur,
        )
        if cur is not None and not self._locked.is_set():
            self._locked.set()
            await self._release.wait()
        return snapshot


class _PausedSemanticCommitWriter(SemanticCommitWriter):
    """Pause after reused identities have been durably locked for publication."""

    def __init__(self, client, locked: asyncio.Event, release: asyncio.Event):
        super().__init__(client)
        self._locked = locked
        self._release = release

    async def _write_aliases(
        self,
        cur,
        result: ContextEntityResult,
        *,
        user_name: str,
        project_id: str,
    ) -> int:
        if not self._locked.is_set():
            self._locked.set()
            await self._release.wait()
        return await super()._write_aliases(
            cur,
            result,
            user_name=user_name,
            project_id=project_id,
        )


async def _seed_merge_entities(client):
    await client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES
            (2, 'ada', 'Ada Lovelace'),
            (3, 'ada', 'Augusta Ada King'),
            (4, 'ada', 'Analytical Engine');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES
            ('project-1', 2, 'ada', 'Person', 'Work'),
            ('project-1', 3, 'ada', 'Person', 'Work'),
            ('project-1', 4, 'ada', 'Company', 'Work');
        """
    )


async def _prepare_merge_commit_interleaving(client):
    await _seed_merge_entities(client)
    await _seed_message(client)
    window = _window()
    window_writer = SemanticWindowWriter(client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Augusta Ada King is active in this project.")
    context = await _commit_context(client, window, (block,))
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
        entity_ids=(3, 4),
        entities={},
        existing_classifications={3: "Person", 4: "Company"},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=3,
                mention_text="Augusta Ada King",
            ),
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=4,
                mention_text="Analytical Engine",
            ),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(block.block_id,),
            entity_a_id=3,
            entity_b_id=4,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Person",
            target_type="Company",
            domain_version=1,
        ),
    )
    return build, block.block_id


async def _assert_no_retired_identity_references(client):
    assert await client.fetch_one(
        """
        SELECT
            (SELECT count(*) FROM public.context_block_entities
             WHERE entity_id = 3) AS association_count,
            (SELECT count(*) FROM public.message_entity_refs
             WHERE entity_id = 3) AS message_ref_count,
            (SELECT count(*) FROM public.project_entity_contexts
             WHERE entity_id = 3) AS classification_count,
            (SELECT count(*) FROM public.relationships
             WHERE entity_a_id = 3 OR entity_b_id = 3) AS relationship_count,
            (SELECT count(*) FROM public.relationship_observations
             WHERE source_entity_id = 3 OR target_entity_id = 3) AS observation_count
        """
    ) == {
        "association_count": 0,
        "message_ref_count": 0,
        "classification_count": 0,
        "relationship_count": 0,
        "observation_count": 0,
    }


async def _assert_waiting(task):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(task), timeout=0.1)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_jev_identity_decision_commits_once_and_is_owner_scoped(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Bob joined the meeting.")
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
        entity_ids=(),
        entities={},
        associations=(),
        ingestion_policy=_policy(JevPolicy(identity_mode="observe")),
    )
    _add_jev_observation(build, block.block_id)
    writer = SemanticCommitWriter(real_postgres_client)
    assert (await writer.commit(build)).resumed is False
    assert (await writer.commit(build)).resumed is True

    reader = SemanticWindowReader(real_postgres_client)
    records = await reader.list_jev_identity_decisions(
        window.window_id, user_name="ada", project_id="project-1"
    )
    assert len(records) == 1
    assert records[0]["record"]["occurrence_key"] == f"0:{block.block_id}"
    assert (
        await reader.list_jev_identity_decisions(
            window.window_id, user_name="other", project_id="project-1"
        )
        == []
    )
    await real_postgres_client.execute(
        "DELETE FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_jev_identity_decisions WHERE window_id = %s",
        (window.window_id,),
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
async def test_carried_active_identity_persists_once_after_second_pass(real_postgres_client):
    await _seed_message(real_postgres_client)
    await _seed_merge_entities(real_postgres_client)
    await real_postgres_client.execute(
        "INSERT INTO public.entity_aliases (entity_id, alias) VALUES (2, 'Bob'), (3, 'Bob')"
    )
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Bob joined the meeting.")
    context = await _commit_context(real_postgres_client, window, (block,))
    assert await window_writer.advance_stage(
        window_id=window.window_id, user_name="ada", project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )

    class BudgetedJev:
        async def evaluate(self, **kwargs):
            assert kwargs["work_budget"].take(), "Second pass must not call the provider"
            handles = kwargs["questions"]["identity_choice"].criteria
            return JevResult(outcome="available", response=JevResponse.model_validate({
                "model": "jev-1.13.0",
                "answers": {
                    "identity_choice": {"type": "choice", "choice": "candidate_1", "confidence": 0.95,
                        "probabilities": {handle: float(handle == "candidate_1") for handle in handles}},
                    "identity_evidence": {"type": "noul", "noul": 0.95},
                }, "usage": {"input_tokens": 20, "output_tokens": 0},
            }))

    frozen = _policy(JevPolicy(identity_mode="active", acceptance_policy_version="identity-positive-v1", max_calls_per_window=1))
    resolver = EntityResolver(EntityReader(real_postgres_client), "project-1", ["project-1"], jev_client=BudgetedJev())
    mention = ContextBlockMention(block_ids=(block.block_id,), name="Bob", entity_type="Person", topic="Work", origin="vp01")
    budget = JevWorkBudget(1, max_observations=1)
    async def allocate():
        pytest.fail("Unchanged accepted identity must survive both passes")
    first = await resolver.resolve_context_block_mentions([mention],
        block_text_by_id={block.block_id: block.markdown}, policy=frozen,
        allocate_entity_id=allocate, window_id=window.window_id, work_budget=budget)
    second = await resolver.resolve_context_block_mentions([mention],
        block_text_by_id={block.block_id: block.markdown}, policy=frozen,
        allocate_entity_id=allocate, window_id=window.window_id, pass_number=2,
        work_budget=budget, previous_identity_decisions=first.identity_decisions)
    assert second.entity_ids == first.entity_ids
    assert budget.calls == budget.observations == 1
    build = _build(_WindowContext(window.window_id, context), impact=(block.block_id,),
        entity_ids=second.entity_ids, entities={}, existing_classifications={entity_id: "Person" for entity_id in second.entity_ids},
        associations=second.block_entity_associations, ingestion_policy=frozen)
    assemble_context_entity_result(build, second)
    build.trace.identity_decisions.extend(second.identity_decisions)
    writer = SemanticCommitWriter(real_postgres_client)
    assert not (await writer.commit(build)).resumed
    assert (await writer.commit(build)).resumed
    records = await SemanticWindowReader(real_postgres_client).list_jev_identity_decisions(
        window.window_id, user_name="ada", project_id="project-1")
    assert len(records) == 1
    assert records[0]["accepted_entity_id"] == first.entity_ids[0]
    assert records[0]["reused_for_pass"] == 2


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
@pytest.mark.parametrize("forged", [None, "unavailable", "weak", "missing_mention"])
@pytest.mark.parametrize("known_type", [True, False])
async def test_jev_extraction_decision_commits_once_and_is_owner_scoped(
    real_postgres_client, forged, known_type,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Delta joined the project.")
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
        entity_ids=(10,),
        entities={10: _entity(10, "Delta", "Company")},
        associations=(ContextBlockEntityAssociation(
            block_id=block.block_id, entity_id=10,
            mention_text="Other" if forged == "missing_mention" else "Delta"
        ),),
        ingestion_policy=_policy(JevPolicy(extraction_mode="active")),
    )
    _add_jev_extraction_observation(build, block.block_id)
    if not known_type:
        observation = build.trace.extraction_decisions[0]
        observation["proposed_entity_type"] = None
        observation["record"]["option_mapping"] = {"type_1": "Person", "type_2": "Company"}
        observation["record"]["result"]["response"]["answers"]["type_choice"] = {
            "type": "choice", "choice": "type_2", "confidence": 0.9,
            "probabilities": {"type_1": 0.05, "type_2": 0.9, "not_an_entity": 0.025, "insufficient_evidence": 0.025},
        }
    writer = SemanticCommitWriter(real_postgres_client)
    if forged is not None:
        observation = build.trace.extraction_decisions[0]
        if forged == "unavailable":
            observation["record"]["result"] = JevResult(outcome="unavailable").model_dump(mode="json")
        elif forged == "weak":
            observation["record"]["result"]["response"]["answers"]["entity_evidence"]["noul"] = 0.1
        with pytest.raises(ValueError, match="JEV extraction"):
            await writer.commit(build)
        assert await real_postgres_client.fetch_one(
            "SELECT count(*) AS count FROM public.project_jev_extraction_decisions WHERE window_id = %s",
            (window.window_id,),
        ) == {"count": 0}
        assert (await SemanticWindowReader(real_postgres_client).get_window(
            window.window_id, user_name="ada", project_id="project-1"
        )).stage == SemanticWindowStage.CONTEXT_COMMITTED
        return
    assert (await writer.commit(build)).resumed is False
    assert (await writer.commit(build)).resumed is True

    reader = SemanticWindowReader(real_postgres_client)
    records = await reader.list_jev_extraction_decisions(
        window.window_id, user_name="ada", project_id="project-1"
    )
    assert len(records) == 1
    assert records[0]["accepted_entity_type"] == "Company"
    assert (
        await reader.list_jev_extraction_decisions(
            window.window_id, user_name="other", project_id="project-1"
        )
        == []
    )
    await real_postgres_client.execute(
        "DELETE FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_jev_extraction_decisions WHERE window_id = %s",
        (window.window_id,),
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
@pytest.mark.parametrize("forged", [None, "unavailable", "weak", "wrong_reuse", "malformed_distribution"])
async def test_active_jev_identity_commit_revalidates_acceptance(real_postgres_client, forged):
    await _seed_message(real_postgres_client)
    await _seed_merge_entities(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Ada joined the meeting.")
    context = await _commit_context(real_postgres_client, window, (block,))
    assert await window_writer.advance_stage(
        window_id=window.window_id, user_name="ada", project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    build = _build(
        _WindowContext(window.window_id, context), impact=(block.block_id,),
        entity_ids=(3,), entities={}, existing_classifications={3: "Person"},
        associations=(ContextBlockEntityAssociation(
            block_id=block.block_id, entity_id=3, mention_text="Ada"
        ),), ingestion_policy=_policy(JevPolicy(
            identity_mode="active", acceptance_policy_version="identity-positive-v1"
        )),
    )
    response = JevResponse.model_validate({
        "model": "jev-1.13.0",
        "answers": {
            "identity_choice": {
                "type": "choice", "choice": "candidate_1", "confidence": 0.95,
                "probabilities": {"candidate_1": 0.9, "none_of_these": 0.05, "insufficient_evidence": 0.05},
            },
            "identity_evidence": {"type": "noul", "noul": 0.95},
        }, "usage": {"input_tokens": 20, "output_tokens": 0},
    })
    record = JevDecisionRecord(
        capability="identity", mode="active", project_id=build.project_id,
        window_id=build.window_id, pass_number=1, occurrence_key=f"0:{block.block_id}",
        evidence_block_ids=(block.block_id,), domain_version=build.policy.domain.version,
        question_version="identity-v1", acceptance_policy_version="identity-positive-v1",
        pinned_model=build.policy.jev.model, option_mapping={"candidate_1": "3"},
        result=JevResult(outcome="available", response=response),
        baseline_outcome="deterministic_abstention", acceptance_status="accepted",
    )
    decision = {"outcome": "reused", "candidate_id": 3, "jev": {
        "record": record.model_dump(mode="json"), "suggested_entity_id": 3,
        "accepted_entity_id": 3, "eligible_candidate_count": 1,
        "eligible_candidate_ids": [3], "candidate_catalog_truncated": False,
        "offered_candidate_count": 1, "candidate_set_truncated": False,
    }}
    build.trace.identity_decisions.append(decision)
    if forged == "unavailable":
        decision["jev"]["record"]["result"] = JevResult(outcome="unavailable").model_dump(mode="json")
    elif forged == "weak":
        decision["jev"]["record"]["result"]["response"]["answers"]["identity_evidence"]["noul"] = 0.1
    elif forged == "wrong_reuse":
        decision["candidate_id"] = 4
    elif forged == "malformed_distribution":
        decision["jev"]["record"]["result"]["response"]["answers"]["identity_choice"]["probabilities"] = {"candidate_1": 0.9}
    writer = SemanticCommitWriter(real_postgres_client)
    if forged is not None:
        with pytest.raises(ValueError):
            await writer.commit(build)
        assert await real_postgres_client.fetch_one(
            "SELECT count(*) AS count FROM public.project_jev_identity_decisions WHERE window_id = %s",
            (window.window_id,),
        ) == {"count": 0}
        assert (await SemanticWindowReader(real_postgres_client).get_window(
            window.window_id, user_name="ada", project_id="project-1"
        )).stage == SemanticWindowStage.CONTEXT_COMMITTED
    else:
        assert not (await writer.commit(build)).resumed
        assert (await writer.commit(build)).resumed
        records = await SemanticWindowReader(real_postgres_client).list_jev_identity_decisions(
            window.window_id, user_name="ada", project_id="project-1"
        )
        assert records[0]["accepted_entity_id"] == 3


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
@pytest.mark.parametrize("active", [False, True, "forged", "wrong_model", "bad_distribution", "missing_response"])
async def test_jev_classification_decision_commits_once_and_is_owner_scoped(
    real_postgres_client,
    active,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Delta joined the finance project.")
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
        entity_ids=(10,),
        entities={10: _entity(10, "Delta", "Company")},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=10,
                mention_text="Delta",
            ),
        ),
        ingestion_policy=_classification_policy(),
    )
    _add_jev_classification_observation(build, block.block_id)
    if active:
        build.policy = replace(build.policy, jev=build.policy.jev.model_copy(update={
            "classification_mode": "active",
            "classification_acceptance_policy_version": "override-positive-v1",
        }))
        classification = build.entity_result.project_classifications[10]
        build.entity_result.project_classifications[10] = replace(classification, topic="Finance")
        build.entity_result.pending_entity_writes[10] = replace(
            build.entity_result.pending_entity_writes[10], topic="Finance"
        )
        observation = build.trace.classification_decisions[0]
        observation["accepted_topic"] = "Finance"
        observation["record"].update(mode="active", acceptance_status="accepted",
            acceptance_policy_version="override-positive-v1")
        observation["aggregation"]["operational_topic"] = "Finance"
        if active == "forged":
            observation["record"]["result"]["response"]["answers"]["topic_evidence"]["noul"] = 0.1
        elif active == "wrong_model":
            observation["record"]["result"]["response"]["model"] = "wrong-model"
        elif active == "bad_distribution":
            observation["record"]["result"]["response"]["answers"]["topic_choice"]["probabilities"]["topic_1"] = 0.4
        elif active == "missing_response":
            observation["record"]["result"]["response"] = None
    writer = SemanticCommitWriter(real_postgres_client)
    if isinstance(active, str):
        with pytest.raises(ValueError):
            await writer.commit(build)
        assert await real_postgres_client.fetch_one(
            "SELECT count(*) AS count FROM public.project_jev_classification_decisions WHERE window_id = %s",
            (window.window_id,),
        ) == {"count": 0}
        return
    assert (await writer.commit(build)).resumed is False
    assert (await writer.commit(build)).resumed is True

    reader = SemanticWindowReader(real_postgres_client)
    records = await reader.list_jev_classification_decisions(
        window.window_id, user_name="ada", project_id="project-1"
    )
    assert len(records) == 1
    assert records[0]["suggested_topic"] == "Finance"
    assert records[0]["accepted_topic"] == ("Finance" if active else None)
    assert records[0]["aggregation"]["operational_topic"] == ("Finance" if active else "Work")
    assert (
        await reader.list_jev_classification_decisions(
            window.window_id, user_name="other", project_id="project-1"
        )
        == []
    )
    await real_postgres_client.execute(
        "DELETE FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    )
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count "
        "FROM public.project_jev_classification_decisions WHERE window_id = %s",
        (window.window_id,),
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_jev_classification_decision_rejects_unowned_evidence_atomically(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Delta joined the finance project.")
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
        entity_ids=(10,),
        entities={10: _entity(10, "Delta", "Company")},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=10,
                mention_text="Delta",
            ),
        ),
        ingestion_policy=_classification_policy(),
    )
    _add_jev_classification_observation(
        build, block.block_id, evidence_block_id=uuid4()
    )
    with pytest.raises(ValueError, match="outside the committed window scope"):
        await SemanticCommitWriter(real_postgres_client).commit(build)
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "context_committed"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count "
        "FROM public.project_jev_classification_decisions WHERE window_id = %s",
        (window.window_id,),
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id = 10"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_jev_identity_decision_rejects_unowned_evidence_atomically(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Bob joined the meeting.")
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
        entity_ids=(),
        entities={},
        associations=(),
        ingestion_policy=_policy(JevPolicy(identity_mode="observe")),
    )
    _add_jev_observation(build, block.block_id, evidence_block_id=uuid4())
    with pytest.raises(ValueError, match="outside the committed window scope"):
        await SemanticCommitWriter(real_postgres_client).commit(build)
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    ) == {"stage": "context_committed"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_jev_identity_decisions WHERE window_id = %s",
        (window.window_id,),
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_records_project_name_support(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Sarah is the project owner.")
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
        entity_ids=(10,),
        entities={10: _entity(10, "Sarah", "Person")},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=10,
                mention_text="Sarah",
            ),
        ),
    )

    await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id, name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 10
        """
    ) == [
        {
            "entity_id": 10,
            "name": "Sarah",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        }
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_records_project_support_for_new_alias(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (10, 'ada', 'Ada Lovelace');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-1', 10, 'ada', 'Person', 'Work');
        """
    )
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Ada is the project owner.")
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
        entity_ids=(10,),
        entities={},
        existing_classifications={10: "Person"},
        alias_updates={10: ("Ada",)},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=10,
                mention_text="Ada",
            ),
        ),
    )

    await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_all(
        """
        SELECT name, project_id, source_kind, source_key
        FROM public.entity_name_supports
        WHERE entity_id = 10
        """
    ) == [
        {
            "name": "Ada",
            "project_id": "project-1",
            "source_kind": "project",
            "source_key": "project-1",
        }
    ]


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
    assert await ProjectContextReader(
        real_postgres_client
    ).get_committed_window_affected_entity_ids(
        first_window.window_id,
        user_name="ada",
        project_id="project-1",
    ) == (10, 11)
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
        existing_classifications={11: "Company"},
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

    no_op_build = _empty_build(no_op_window.window_id, context)
    committed = await writer.commit(no_op_build)
    resumed = await writer.commit(no_op_build)

    assert committed.resumed is False
    assert committed.relationships_written == 0
    assert resumed.resumed is True
    assert await ProjectContextReader(
        real_postgres_client
    ).get_committed_window_affected_entity_ids(
        no_op_window.window_id,
        user_name="ada",
        project_id="project-1",
    ) == ()
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
    failed = await window_writer.record_failure(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        failure_stage="knowledge_reconciliation",
        failure_code="ConnectionError",
        error_summary="graph write unavailable",
        failed_at_ms=1_000,
        next_retry_at_ms=31_000,
    )
    assert failed is not None
    assert failed.attempt_count == 1

    summary = await SemanticCommitWriter(real_postgres_client).commit(
        _empty_build(window.window_id, context)
    )

    assert summary.resumed is False
    assert summary.relationships_written == 0
    assert await real_postgres_client.fetch_one(
        """
        SELECT stage, attempt_count, last_failure_stage, last_failure_code,
               last_failure_at_ms, last_error_summary, next_retry_at_ms
        FROM public.project_semantic_windows
        WHERE window_id = %s
        """,
        (window.window_id,),
    ) == {
        "stage": "knowledge_committed",
        "attempt_count": 0,
        "last_failure_stage": None,
        "last_failure_code": None,
        "last_failure_at_ms": None,
        "last_error_summary": None,
        "next_retry_at_ms": None,
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_rolls_back_every_write_when_late_relationship_validation_fails(
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
        entities={
            10: _entity(10, "Sarah", "Person"),
            11: _entity(11, "Delta", "Company"),
        },
        associations=(
            ContextBlockEntityAssociation(block_id=block.block_id, entity_id=10, mention_text="Sarah"),
            ContextBlockEntityAssociation(block_id=block.block_id, entity_id=11, mention_text="Delta"),
        ),
        relationship=ContextRelationshipWrite(
            support_block_ids=(block.block_id,),
            entity_a_id=10,
            entity_b_id=11,
            relationship_type="owns",
            canonical_type="OWNS",
            source_type="Company",
            target_type="Company",
            domain_version=1,
        ),
    )

    with pytest.raises(ValueError, match="does not match"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.entities WHERE entity_id = 10"
    ) == {"count": 0}
    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.entity_name_supports
        WHERE entity_id IN (10, 11)
        """
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
        existing_classifications={10: "Person"},
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
        existing_classifications={10: "Person"},
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
async def test_semantic_commit_admits_a_currently_readable_foreign_identity(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (10, 'ada', 'Acme Labs');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-2', 10, 'ada', 'Vendor', 'Archive');
        INSERT INTO public.project_read_scopes (
            user_name, project_id, readable_project_id
        ) VALUES ('ada', 'project-1', 'project-2');
        """
    )
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Acme Labs sponsors the project.")
    context = await _commit_context(real_postgres_client, window, (block,))
    assert await window_writer.advance_stage(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )

    summary = await SemanticCommitWriter(real_postgres_client).commit(
        _build(
            _WindowContext(window.window_id, context),
            impact=(block.block_id,),
            entity_ids=(10,),
            entities={},
            missing_classifications={10: "Company"},
            associations=(
                ContextBlockEntityAssociation(
                    block_id=block.block_id,
                    entity_id=10,
                    mention_text="Acme Labs",
                ),
            ),
        )
    )

    assert summary.entities_written == 0
    assert await real_postgres_client.fetch_all(
        """
        SELECT project_id, entity_type, topic
        FROM public.project_entity_contexts
        WHERE entity_id = 10
        ORDER BY project_id
        """
    ) == [
        {"project_id": "project-1", "entity_type": "Company", "topic": "Work"},
        {"project_id": "project-2", "entity_type": "Vendor", "topic": "Archive"},
    ]
    assert await real_postgres_client.fetch_one(
        "SELECT canonical_name FROM public.entities WHERE entity_id = 10"
    ) == {"canonical_name": "Acme Labs"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_rejects_conflicting_existing_local_classification(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (10, 'ada', 'Acme Labs');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-1', 10, 'ada', 'Person', 'Work');
        """
    )
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Acme Labs sponsors the project.")
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
        entity_ids=(10,),
        entities={},
        existing_classifications={10: "Company"},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=10,
                mention_text="Acme Labs",
            ),
        ),
    )

    with pytest.raises(ValueError, match="classification conflicts"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_one(
        """
        SELECT entity_type, topic
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1' AND entity_id = 10
        """
    ) == {"entity_type": "Person", "topic": "Work"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.context_block_entities"
    ) == {"count": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_rejects_a_foreign_identity_after_scope_revocation(
    real_postgres_client,
):
    await _seed_message(real_postgres_client)
    await real_postgres_client.execute(
        """
        INSERT INTO public.entities (entity_id, user_name, canonical_name)
        VALUES (10, 'ada', 'Acme Labs');
        INSERT INTO public.project_entity_contexts (
            project_id, entity_id, user_name, entity_type, topic
        ) VALUES ('project-2', 10, 'ada', 'Vendor', 'Archive');
        """
    )
    window = _window()
    window_writer = SemanticWindowWriter(real_postgres_client)
    assert (await window_writer.claim_window(window, _membership())).claimed
    block = _block("Acme Labs sponsors the project.")
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
        entity_ids=(10,),
        entities={},
        missing_classifications={10: "Company"},
        associations=(
            ContextBlockEntityAssociation(
                block_id=block.block_id,
                entity_id=10,
                mention_text="Acme Labs",
            ),
        ),
    )

    with pytest.raises(ValueError, match="inactive or unreadable"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_one(
        """
        SELECT count(*) AS count
        FROM public.project_entity_contexts
        WHERE project_id = 'project-1' AND entity_id = 10
        """
    ) == {"count": 0}


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


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_merge_first_rejects_a_stale_semantic_commit_without_partial_state(
    real_postgres_client,
):
    build, _ = await _prepare_merge_commit_interleaving(real_postgres_client)
    merge_locked = asyncio.Event()
    release_merge = asyncio.Event()
    merge_task = asyncio.create_task(
        _PausedMergeWriter(
            real_postgres_client,
            merge_locked,
            release_merge,
        ).merge(
            user_name="ada",
            survivor_id=2,
            retired_id=3,
            merge_id=str(uuid4()),
        )
    )
    commit_task = None
    try:
        await asyncio.wait_for(merge_locked.wait(), timeout=2)
        commit_task = asyncio.create_task(
            SemanticCommitWriter(real_postgres_client).commit(build)
        )
        await _assert_waiting(commit_task)

        release_merge.set()
        await asyncio.wait_for(merge_task, timeout=5)
        with pytest.raises(ValueError, match="inactive or unreadable"):
            await asyncio.wait_for(commit_task, timeout=5)
    finally:
        release_merge.set()
        await asyncio.gather(
            merge_task,
            *(task for task in (commit_task,) if task is not None),
            return_exceptions=True,
        )

    assert await real_postgres_client.fetch_one(
        """
        SELECT status, redirect_entity_id
        FROM public.entities
        WHERE entity_id = 3
        """
    ) == {"status": "redirected", "redirect_entity_id": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (build.window_id,),
    ) == {"stage": "context_committed"}
    await _assert_no_retired_identity_references(real_postgres_client)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_semantic_commit_first_is_migrated_by_the_waiting_global_merge(
    real_postgres_client,
):
    build, block_id = await _prepare_merge_commit_interleaving(real_postgres_client)
    semantic_locked = asyncio.Event()
    release_semantic_commit = asyncio.Event()
    semantic_task = asyncio.create_task(
        _PausedSemanticCommitWriter(
            real_postgres_client,
            semantic_locked,
            release_semantic_commit,
        ).commit(build)
    )
    merge_task = None
    try:
        await asyncio.wait_for(semantic_locked.wait(), timeout=2)
        merge_task = asyncio.create_task(
            GlobalEntityMergeWriter(real_postgres_client).merge(
                user_name="ada",
                survivor_id=2,
                retired_id=3,
                merge_id=str(uuid4()),
            )
        )
        await _assert_waiting(merge_task)

        release_semantic_commit.set()
        summary = await asyncio.wait_for(semantic_task, timeout=5)
        merged = await asyncio.wait_for(merge_task, timeout=5)
    finally:
        release_semantic_commit.set()
        await asyncio.gather(
            semantic_task,
            *(task for task in (merge_task,) if task is not None),
            return_exceptions=True,
        )

    assert summary.resumed is False
    assert summary.block_entity_associations_written == 2
    assert summary.relationships_written == 1
    assert merged["mutation_count"] > 0
    assert await real_postgres_client.fetch_all(
        """
        SELECT entity_id
        FROM public.context_block_entities
        WHERE block_id = %s
        ORDER BY entity_id
        """,
        (block_id,),
    ) == [{"entity_id": 2}, {"entity_id": 4}]
    assert await real_postgres_client.fetch_one(
        """
        SELECT entity_a_id, entity_b_id
        FROM public.relationships
        WHERE project_id = 'project-1'
        """
    ) == {"entity_a_id": 2, "entity_b_id": 4}
    assert await real_postgres_client.fetch_one(
        """
        SELECT source_entity_id, target_entity_id
        FROM public.relationship_observations
        WHERE project_id = 'project-1'
        """
    ) == {"source_entity_id": 2, "target_entity_id": 4}
    assert await real_postgres_client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (build.window_id,),
    ) == {"stage": "knowledge_committed"}
    await _assert_no_retired_identity_references(real_postgres_client)
