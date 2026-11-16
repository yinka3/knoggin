"""Fresh-schema contracts for message-anchored semantic Knowledge commits."""

import asyncio
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextRevisionOrigin,
    ContextSupportKind,
)
from common.schema.ingestion.contracts import (
    EntityMention,
    EntityResult,
    EntityWrite,
    ProjectEntityClassification,
    RelationshipWrite,
    ResolvedEntityMention,
)
from common.schema.jev import JevDecisionRecord, JevPolicy, JevResponse, JevResult
from common.schema.semantic_window import (
    SemanticWindowMessage,
    SemanticWindowOrigin,
    SemanticWindowRecord,
    SemanticWindowStage,
)
from common.schema.settings import EntityResolutionSettings, ExtractionSettings
from core.ingestion.batch import EpisodeEvidence, EvidenceMessage, SemanticWindowBuild
from core.ingestion.policy import IngestionPolicy
from core.knowledge.context.models import ContextBlockSupport, ContextMaterialization
from core.knowledge.context.render import context_block_hash, context_document_hash
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.db.writers.global_entity_merge_writer import GlobalEntityMergeWriter
from core.knowledge.db.writers.project_context_writer import ProjectContextWriter
from core.knowledge.db.writers.semantic_commit_writer import SemanticCommitWriter
from core.knowledge.db.writers.semantic_window_writer import SemanticWindowWriter

MESSAGES = {
    101: ("Sarah owns Delta.", 1_000),
    102: ("John owns Delta.", 2_000),
    103: ("Delta hired Sarah.", 3_000),
}


def _domain():
    return DomainConfig.from_mapping({
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
                "labels": ["owns", "has ownership of"],
            }
        },
    }).compile()


def _policy(jev=None):
    return IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=_domain(),
        jev=jev,
    )


def _classification_policy():
    domain = DomainConfig.from_mapping({
        "version": 1,
        "topics": {"Work": {"active": True}, "Finance": {"active": True}},
        "entity_types": {
            "Company": {
                "topic": "Work", "allowed_topics": ["Work", "Finance"],
                "labels": ["company"],
            }
        },
    }).compile()
    return IngestionPolicy.capture(
        extraction=ExtractionSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=JevPolicy(classification_mode="observe"),
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


async def _seed_messages(client, message_ids=tuple(MESSAGES)):
    await client.execute(
        "INSERT INTO public.sessions (session_id, user_name, project_id) "
        "VALUES ('session-1', 'ada', 'project-1')"
    )
    for message_id in message_ids:
        content, timestamp = MESSAGES[message_id]
        await client.execute(
            """
            INSERT INTO public.messages (
                user_name, session_id, message_id, project_id, role, content,
                timestamp_ms, lifecycle_state, exchange_state, exchange_outcome,
                exchange_closed_at_ms
            ) VALUES ('ada', 'session-1', %s, 'project-1', 'user', %s, %s,
                      'sealed', 'closed', 'user_only', %s)
            """,
            (message_id, content, timestamp, message_id),
        )


async def _ready_window(client, *, episodes=(("ep-1", (101, 102)),)):
    """A context_committed window whose Episodes cover the given messages."""

    message_ids = tuple(m for _, ids in episodes for m in ids) or (101,)
    await _seed_messages(client)
    window = _window()
    writer = SemanticWindowWriter(client)
    membership = [
        SemanticWindowMessage(
            message_id=message_id, session_id="session-1",
            exchange_user_message_id=message_id, role="user", ordinal=ordinal,
        )
        for ordinal, message_id in enumerate(message_ids)
    ]
    assert (await writer.claim_window(window, membership)).claimed
    for ordinal, (episode_id, ids) in enumerate(episodes):
        await client.execute(
            "INSERT INTO public.episodes (episode_id, project_id, summary) "
            "VALUES (%s, 'project-1', 'A summary.')",
            (episode_id,),
        )
        for position, message_id in enumerate(ids):
            await client.execute(
                """INSERT INTO public.episode_messages
                   (episode_id, project_id, session_id, message_id, message_position)
                   VALUES (%s, 'project-1', 'session-1', %s, %s)""",
                (episode_id, message_id, position),
            )
        await client.execute(
            """INSERT INTO public.project_semantic_window_episodes
               (window_id, project_id, episode_id, ordinal) VALUES (%s, 'project-1', %s, %s)""",
            (window.window_id, episode_id, ordinal),
        )
    block = ContextBlockRecord(
        block_id=uuid4(), project_id="project-1", section_key="current_state",
        markdown="Sarah owns Delta.", content_hash=context_block_hash("Sarah owns Delta."),
        assertion_kind=AssertionKind.USER_ASSERTED, source_time_ms=1_000,
    )
    context = await ProjectContextWriter(client).commit_revision(
        user_name="ada", project_id="project-1", expected_parent_revision_id=None,
        window_id=window.window_id, origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1, edit_summary="Context changed",
        materialization=ContextMaterialization(
            blocks=(block,),
            content_hash=context_document_hash((block,), _domain()),
            new_block_ids=frozenset({block.block_id}),
            impacted_block_ids=frozenset({block.block_id}),
            supports=(ContextBlockSupport(
                block_id=block.block_id, message_id=message_ids[0], session_id="session-1",
                support_kind=ContextSupportKind.USER_MESSAGE,
            ),),
        ),
    )
    assert await writer.advance_stage(
        window_id=window.window_id, user_name="ada", project_id="project-1",
        expected_stage=SemanticWindowStage.CLAIMED,
        next_stage=SemanticWindowStage.CONTEXT_COMMITTED,
        context_revision_id=context.revision_id,
    )
    return window, episodes


def _entity(entity_id, name, entity_type):
    return EntityWrite(
        entity_id=entity_id, is_new=True, canonical_name=name,
        entity_type=entity_type, topic="Work", aliases=(name,),
    )


def _build(
    window,
    episodes,
    *,
    mentions=(),
    entities=None,
    existing=None,
    missing=None,
    relationships=(),
    alias_updates=None,
    policy=None,
):
    """``mentions``: (entity_id, name, message ids, entity type)."""

    entities = entities or {}
    types = {
        **{i: e.entity_type for i, e in entities.items()},
        **(existing or {}), **(missing or {}),
    }
    entity_ids = tuple(dict.fromkeys(entity_id for entity_id, *_ in mentions))
    classifications = {
        entity_id: ProjectEntityClassification(
            entity_id=entity_id, entity_type=types[entity_id], topic="Work",
            membership="existing" if entity_id in (existing or {}) else "missing",
        )
        for entity_id in entity_ids
        if entity_id != 1
    }
    policy = policy or _policy()
    build = SemanticWindowBuild(
        window_id=window.window_id,
        user_name="ada",
        project_id="project-1",
        policy=policy,
        policy_snapshot={"ingestion_policy": policy.semantic_window_snapshot()},
        episodes=tuple(
            EpisodeEvidence(
                episode_id=episode_id, summary="A summary.",
                messages=tuple(
                    EvidenceMessage(message_id=m, role="user", content=MESSAGES[m][0],
                                    timestamp_ms=MESSAGES[m][1])
                    for m in ids
                ),
            )
            for episode_id, ids in episodes
        ),
    )
    build.set_entity_result(EntityResult(
        entity_ids=entity_ids,
        new_entity_ids=frozenset(entities),
        alias_updated_ids=frozenset(alias_updates or {}),
        alias_updates=alias_updates or {},
        pending_entity_writes=dict(entities),
        project_classifications=classifications,
        resolved_mentions=tuple(
            ResolvedEntityMention(
                mention=EntityMention(
                    message_ids=tuple(message_ids), name=name, entity_type=entity_type,
                    topic="Work", origin="vp01",
                ),
                entity_id=entity_id,
                occurrence_index=index,
            )
            for index, (entity_id, name, message_ids, entity_type) in enumerate(mentions)
        ),
    ))
    build.set_relationship_writes(relationships)
    return build


def _owns(a, b, *message_ids):
    return RelationshipWrite(
        support_message_ids=message_ids, entity_a_id=a, entity_b_id=b,
        relationship_type="owns", canonical_type="OWNS", source_type="Person",
        target_type="Company", domain_version=1,
    )


SARAH_DELTA = {10: _entity(10, "Sarah", "Person"), 11: _entity(11, "Delta", "Company")}
SARAH_DELTA_MENTIONS = (
    (10, "Sarah", (101,), "Person"), (11, "Delta", (101, 102), "Company"),
)


def _add_jev_observation(build, message_id, *, evidence_message_id=None):
    record = JevDecisionRecord(
        capability="identity", mode="observe", project_id=build.project_id,
        window_id=build.window_id, pass_number=1, occurrence_key=f"0:{message_id}",
        evidence_message_ids=(evidence_message_id or message_id,),
        domain_version=build.policy.domain.version, question_version="identity-v1",
        acceptance_policy_version="observe-v1", pinned_model="jev-1.13.0",
        option_mapping={"candidate_1": "42"},
        result=JevResult(outcome="unavailable", reason="missing_api_key"),
        baseline_outcome="deterministic_abstention", acceptance_status="unavailable",
    )
    build.trace.identity_decisions.append({
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
    })


def _add_jev_classification_observation(build, message_id, *, evidence_message_id=None):
    result = JevResult(
        outcome="available",
        response=JevResponse.model_validate({
            "model": "jev-1.13.0",
            "answers": {
                "topic_choice": {
                    "type": "choice", "choice": "topic_2",
                    "probabilities": {
                        "topic_1": 0.05, "topic_2": 0.9, "insufficient_evidence": 0.05,
                    },
                    "confidence": 0.9,
                },
                "topic_evidence": {"type": "noul", "noul": 0.9},
            },
            "usage": {"input_tokens": 20, "output_tokens": 0},
        }),
    )
    aggregate = {
        "entity_id": 10, "entity_type": "Company", "membership": "missing",
        "status": "consistent_proposal", "proposed_topics": ["Finance"],
        "operational_topic": "Work", "observation_count": 1,
    }
    record = JevDecisionRecord(
        capability="classification", mode="observe", project_id=build.project_id,
        window_id=build.window_id, pass_number=1, occurrence_key=f"0:{message_id}:topic",
        evidence_message_ids=(evidence_message_id or message_id,),
        domain_version=build.policy.domain.version,
        question_version=build.policy.jev.classification_question_version,
        acceptance_policy_version=build.policy.jev.classification_acceptance_policy_version,
        pinned_model=build.policy.jev.model,
        option_mapping={"topic_1": "Work", "topic_2": "Finance"},
        result=result, baseline_outcome="default_topic", acceptance_status="proposed",
    )
    build.trace.classification_decisions.append({
        "record": record.model_dump(mode="json"),
        "entity_id": 10, "entity_type": "Company", "baseline_topic": "Work",
        "suggested_topic": "Finance", "accepted_topic": None,
        "allowed_topics": ["Work", "Finance"], "option_set_truncated": False,
        "aggregation": aggregate,
    })
    build.trace.classification_aggregates.append(aggregate)


async def _count(client, table, where="true", args=()):
    row = await client.fetch_one(f"SELECT count(*) AS count FROM public.{table} WHERE {where}", args)
    return row["count"]


async def _stage(client, window):
    row = await client.fetch_one(
        "SELECT stage FROM public.project_semantic_windows WHERE window_id = %s",
        (window.window_id,),
    )
    return row["stage"]


# Entities and relationships


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_commit_anchors_entities_and_relationships_to_messages_once(
    real_postgres_client,
):
    window, episodes = await _ready_window(real_postgres_client)
    build = _build(
        window, episodes, mentions=SARAH_DELTA_MENTIONS, entities=SARAH_DELTA,
        relationships=(_owns(10, 11, 101),),
    )
    writer = SemanticCommitWriter(real_postgres_client)

    summary = await writer.commit(build)

    assert (summary.resumed, summary.entities_written, summary.relationships_written) == (
        False, 2, 1,
    )
    assert (await writer.commit(build)).resumed is True
    assert await real_postgres_client.fetch_all(
        "SELECT message_id, entity_id, occurrence_provenance_complete "
        "FROM public.message_entity_refs ORDER BY message_id, entity_id"
    ) == [
        {"message_id": 101, "entity_id": 10, "occurrence_provenance_complete": True},
        {"message_id": 101, "entity_id": 11, "occurrence_provenance_complete": True},
        {"message_id": 102, "entity_id": 11, "occurrence_provenance_complete": True},
    ]
    assert await real_postgres_client.fetch_all(
        "SELECT occurrence_index, message_id, resolved_entity_id, mention_text "
        "FROM public.message_entity_occurrences ORDER BY occurrence_index, message_id"
    ) == [
        {"occurrence_index": 0, "message_id": 101, "resolved_entity_id": 10,
         "mention_text": "Sarah"},
        {"occurrence_index": 1, "message_id": 101, "resolved_entity_id": 11,
         "mention_text": "Delta"},
        {"occurrence_index": 1, "message_id": 102, "resolved_entity_id": 11,
         "mention_text": "Delta"},
    ]
    observation = await real_postgres_client.fetch_one(
        "SELECT observation_id, observed_at_ms FROM public.relationship_observations"
    )
    assert observation["observed_at_ms"] == 1_000
    assert await real_postgres_client.fetch_all(
        "SELECT message_id FROM public.relationship_observation_messages "
        "WHERE observation_id = %s",
        (observation["observation_id"],),
    ) == [{"message_id": 101}]
    assert await _stage(real_postgres_client, window) == "knowledge_committed"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_separate_support_sets_are_separate_observations(real_postgres_client):
    window, episodes = await _ready_window(real_postgres_client)
    build = _build(
        window, episodes, mentions=SARAH_DELTA_MENTIONS, entities=SARAH_DELTA,
        relationships=(_owns(10, 11, 101), _owns(10, 11, 102), _owns(10, 11, 101, 102)),
    )

    await SemanticCommitWriter(real_postgres_client).commit(build)

    rows = await real_postgres_client.fetch_all(
        """SELECT observation.support_key, array_agg(support.message_id ORDER BY support.message_id) AS ids
           FROM public.relationship_observations AS observation
           JOIN public.relationship_observation_messages AS support
             ON support.observation_id = observation.observation_id
           GROUP BY observation.observation_id, observation.support_key"""
    )
    assert sorted(row["ids"] for row in rows) == [[101], [101, 102], [102]]
    assert len({row["support_key"] for row in rows}) == 3
    assert await _count(real_postgres_client, "relationships") == 1


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_entity_recency_and_observation_time_come_from_message_times(
    real_postgres_client,
):
    window, episodes = await _ready_window(
        real_postgres_client, episodes=(("ep-1", (101, 103)),),
    )
    await real_postgres_client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (11, 'ada', 'Delta');
           INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic, last_mentioned_ms)
           VALUES ('project-1', 11, 'ada', 'Company', 'Work', 9000)"""
    )
    build = _build(
        window, episodes,
        mentions=((10, "Sarah", (101, 103), "Person"), (11, "Delta", (101,), "Company")),
        entities={10: SARAH_DELTA[10]}, existing={11: "Company"},
        relationships=(_owns(10, 11, 101, 103),),
    )

    await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await real_postgres_client.fetch_all(
        "SELECT entity_id, last_mentioned_ms FROM public.project_entity_contexts "
        "ORDER BY entity_id"
    ) == [
        {"entity_id": 10, "last_mentioned_ms": 3_000},
        # An older mention never moves recency backwards.
        {"entity_id": 11, "last_mentioned_ms": 9_000},
    ]
    assert (await real_postgres_client.fetch_one(
        "SELECT observed_at_ms FROM public.relationship_observations"
    ))["observed_at_ms"] == 3_000


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_build_that_no_longer_matches_its_episodes_writes_nothing(
    real_postgres_client,
):
    window, _ = await _ready_window(real_postgres_client)
    build = _build(
        window, (("ep-1", (101, 102, 103)),),
        mentions=SARAH_DELTA_MENTIONS, entities=SARAH_DELTA,
    )

    with pytest.raises(ValueError, match="no longer matches its Episodes"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await _count(real_postgres_client, "entities", "entity_id IN (10, 11)") == 0
    assert await _stage(real_postgres_client, window) == "context_committed"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_late_relationship_failure_rolls_back_every_write(real_postgres_client):
    window, episodes = await _ready_window(real_postgres_client)
    forged = _owns(10, 11, 101)
    object.__setattr__(forged, "canonical_type", None)
    build = _build(
        window, episodes, mentions=SARAH_DELTA_MENTIONS, entities=SARAH_DELTA,
        relationships=(forged,),
    )

    with pytest.raises(ValueError, match="does not match current endpoint"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    for table in ("entities", "message_entity_refs", "message_entity_occurrences",
                  "relationship_observations"):
        assert await _count(
            real_postgres_client, table,
            "entity_id <> 1" if table == "entities" else "true",
        ) == 0, table
    assert await _stage(real_postgres_client, window) == "context_committed"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_window_without_episodes_commits_an_empty_result(real_postgres_client):
    window, _ = await _ready_window(real_postgres_client, episodes=())
    build = _build(window, ())

    summary = await SemanticCommitWriter(real_postgres_client).commit(build)

    assert (summary.resumed, summary.entities_written) == (False, 0)
    assert await _stage(real_postgres_client, window) == "knowledge_committed"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_name_supports_record_mention_text_and_new_aliases(real_postgres_client):
    window, episodes = await _ready_window(real_postgres_client)
    await real_postgres_client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (11, 'ada', 'Delta Corp');
           INSERT INTO public.entity_aliases (entity_id, alias) VALUES (11, 'Delta Corp');
           INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 11, 'ada', 'Company', 'Work')"""
    )

    await SemanticCommitWriter(real_postgres_client).commit(_build(
        window, episodes, mentions=((11, "Delta", (101,), "Company"),),
        existing={11: "Company"}, alias_updates={11: ("Delta",)},
    ))

    assert await real_postgres_client.fetch_all(
        "SELECT name, source_kind, source_key FROM public.entity_name_supports "
        "WHERE entity_id = 11 ORDER BY name"
    ) == [{"name": "Delta", "source_kind": "project", "source_key": "project-1"}]


# Project classifications


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_readable_foreign_identity_gets_this_projects_classification(
    real_postgres_client,
):
    window, episodes = await _ready_window(real_postgres_client)
    await real_postgres_client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (10, 'ada', 'Acme Labs');
           INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-2', 10, 'ada', 'Vendor', 'Archive');
           INSERT INTO public.project_read_scopes
               (user_name, project_id, readable_project_id)
           VALUES ('ada', 'project-1', 'project-2')"""
    )

    summary = await SemanticCommitWriter(real_postgres_client).commit(_build(
        window, episodes, mentions=((10, "Acme Labs", (101,), "Company"),),
        missing={10: "Company"},
    ))

    assert summary.entities_written == 0
    assert await real_postgres_client.fetch_all(
        "SELECT project_id, entity_type FROM public.project_entity_contexts "
        "WHERE entity_id = 10 ORDER BY project_id"
    ) == [
        {"project_id": "project-1", "entity_type": "Company"},
        {"project_id": "project-2", "entity_type": "Vendor"},
    ]


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_conflicting_local_classification_is_rejected(real_postgres_client):
    window, episodes = await _ready_window(real_postgres_client)
    await real_postgres_client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (10, 'ada', 'Acme Labs');
           INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 10, 'ada', 'Person', 'Work')"""
    )

    with pytest.raises(ValueError, match="classification conflicts"):
        await SemanticCommitWriter(real_postgres_client).commit(_build(
            window, episodes, mentions=((10, "Acme Labs", (101,), "Company"),),
            existing={10: "Company"},
        ))

    assert await _count(real_postgres_client, "message_entity_refs") == 0


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_foreign_identity_is_rejected_after_its_scope_is_revoked(
    real_postgres_client,
):
    window, episodes = await _ready_window(real_postgres_client)
    await real_postgres_client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (10, 'ada', 'Acme Labs');
           INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-2', 10, 'ada', 'Vendor', 'Archive')"""
    )

    with pytest.raises(ValueError, match="inactive or unreadable"):
        await SemanticCommitWriter(real_postgres_client).commit(_build(
            window, episodes, mentions=((10, "Acme Labs", (101,), "Company"),),
            missing={10: "Company"},
        ))


# JEV decisions


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_jev_identity_decision_commits_once_and_is_owner_scoped(
    real_postgres_client,
):
    window, episodes = await _ready_window(real_postgres_client)
    build = _build(window, episodes, policy=_policy(JevPolicy(identity_mode="observe")))
    _add_jev_observation(build, 101)
    writer = SemanticCommitWriter(real_postgres_client)

    assert (await writer.commit(build)).resumed is False
    assert (await writer.commit(build)).resumed is True

    reader = SemanticWindowReader(real_postgres_client)
    records = await reader.list_jev_identity_decisions(
        window.window_id, user_name="ada", project_id="project-1"
    )
    assert [r["record"]["occurrence_key"] for r in records] == ["0:101"]
    by_message = await reader.list_jev_decisions_for_message(
        101, user_name="ada", project_id="project-1",
    )
    assert by_message["excerpt"] == "Sarah owns Delta."
    assert [d["capability"] for d in by_message["decisions"]] == ["identity"]
    assert await reader.list_jev_decisions_for_message(
        102, user_name="ada", project_id="project-1",
    ) == {"message_id": 102, "excerpt": "John owns Delta.", "decisions": []}
    assert await reader.list_jev_decisions_for_message(
        101, user_name="other", project_id="project-1",
    ) is None


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_jev_identity_decision_citing_another_message_writes_nothing(
    real_postgres_client,
):
    window, episodes = await _ready_window(real_postgres_client)
    build = _build(window, episodes, policy=_policy(JevPolicy(identity_mode="observe")))
    _add_jev_observation(build, 101, evidence_message_id=103)

    with pytest.raises(ValueError, match="outside the committed window scope"):
        await SemanticCommitWriter(real_postgres_client).commit(build)

    assert await _count(real_postgres_client, "project_jev_identity_decisions") == 0
    assert await _stage(real_postgres_client, window) == "context_committed"


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_jev_classification_decision_commits_and_rejects_unowned_evidence(
    real_postgres_client,
):
    window, episodes = await _ready_window(real_postgres_client)
    delta = {10: EntityWrite(
        entity_id=10, is_new=True, canonical_name="Delta", entity_type="Company",
        topic="Work", aliases=("Delta",),
    )}

    def build(evidence_message_id=None):
        current = _build(
            window, episodes, mentions=((10, "Delta", (101,), "Company"),),
            entities=delta, policy=_classification_policy(),
        )
        _add_jev_classification_observation(
            current, 101, evidence_message_id=evidence_message_id,
        )
        return current

    with pytest.raises(ValueError, match="outside the committed window scope"):
        await SemanticCommitWriter(real_postgres_client).commit(build(103))
    assert await _count(real_postgres_client, "entities", "entity_id = 10") == 0

    await SemanticCommitWriter(real_postgres_client).commit(build())
    assert await real_postgres_client.fetch_all(
        "SELECT occurrence_key, entity_id, suggested_topic "
        "FROM public.project_jev_classification_decisions"
    ) == [{"occurrence_key": "0:101:topic", "entity_id": 10, "suggested_topic": "Finance"}]


# Merges racing a commit


async def _merge_race_build(client):
    window, episodes = await _ready_window(client)
    await client.execute(
        """INSERT INTO public.entities (entity_id, user_name, canonical_name)
           VALUES (2, 'ada', 'Ada Lovelace'), (3, 'ada', 'Augusta Ada King'),
                  (4, 'ada', 'Analytical Engine');
           INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 2, 'ada', 'Person', 'Work'),
                  ('project-1', 3, 'ada', 'Person', 'Work'),
                  ('project-1', 4, 'ada', 'Company', 'Work')"""
    )
    return _build(
        window, episodes,
        mentions=((3, "Augusta Ada King", (101,), "Person"),
                  (4, "Analytical Engine", (101,), "Company")),
        existing={3: "Person", 4: "Company"},
        relationships=(_owns(3, 4, 101),),
    )


class _PausedMergeWriter(GlobalEntityMergeWriter):
    """Pause after the merge has locked its identity rows."""

    def __init__(self, client, locked, release):
        super().__init__(client)
        self._locked, self._release = locked, release

    async def snapshot(self, user_name, survivor_id, retired_id, *, cur=None):
        snapshot = await super().snapshot(user_name, survivor_id, retired_id, cur=cur)
        if cur is not None and not self._locked.is_set():
            self._locked.set()
            await self._release.wait()
        return snapshot


class _PausedSemanticCommitWriter(SemanticCommitWriter):
    """Pause after reused identities are locked for publication."""

    def __init__(self, client, locked, release):
        super().__init__(client)
        self._locked, self._release = locked, release

    async def _write_aliases(self, cur, result, *, user_name, project_id):
        if not self._locked.is_set():
            self._locked.set()
            await self._release.wait()
        return await super()._write_aliases(
            cur, result, user_name=user_name, project_id=project_id,
        )


async def _assert_waiting(task):
    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(task), timeout=0.1)


async def _assert_no_retired_identity_references(client):
    assert await client.fetch_one(
        """SELECT
            (SELECT count(*) FROM public.message_entity_refs WHERE entity_id = 3) AS refs,
            (SELECT count(*) FROM public.project_entity_contexts WHERE entity_id = 3) AS contexts,
            (SELECT count(*) FROM public.relationships
             WHERE entity_a_id = 3 OR entity_b_id = 3) AS relationships,
            (SELECT count(*) FROM public.relationship_observations
             WHERE source_entity_id = 3 OR target_entity_id = 3) AS observations"""
    ) == {"refs": 0, "contexts": 0, "relationships": 0, "observations": 0}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_merge_first_rejects_a_stale_commit_without_partial_state(
    real_postgres_client,
):
    build = await _merge_race_build(real_postgres_client)
    locked, release = asyncio.Event(), asyncio.Event()
    merge = asyncio.create_task(_PausedMergeWriter(real_postgres_client, locked, release).merge(
        user_name="ada", survivor_id=2, retired_id=3, merge_id=str(uuid4()),
    ))
    commit = None
    try:
        await asyncio.wait_for(locked.wait(), timeout=2)
        commit = asyncio.create_task(SemanticCommitWriter(real_postgres_client).commit(build))
        await _assert_waiting(commit)
        release.set()
        await asyncio.wait_for(merge, timeout=5)
        with pytest.raises(ValueError, match="inactive or unreadable"):
            await asyncio.wait_for(commit, timeout=5)
    finally:
        release.set()
        await asyncio.gather(merge, *(t for t in (commit,) if t), return_exceptions=True)

    assert await _stage(real_postgres_client, build) == "context_committed"
    await _assert_no_retired_identity_references(real_postgres_client)


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_commit_first_is_migrated_by_the_waiting_merge(real_postgres_client):
    build = await _merge_race_build(real_postgres_client)
    locked, release = asyncio.Event(), asyncio.Event()
    commit = asyncio.create_task(
        _PausedSemanticCommitWriter(real_postgres_client, locked, release).commit(build)
    )
    merge = None
    try:
        await asyncio.wait_for(locked.wait(), timeout=2)
        merge = asyncio.create_task(GlobalEntityMergeWriter(real_postgres_client).merge(
            user_name="ada", survivor_id=2, retired_id=3, merge_id=str(uuid4()),
        ))
        await _assert_waiting(merge)
        release.set()
        summary = await asyncio.wait_for(commit, timeout=5)
        merged = await asyncio.wait_for(merge, timeout=5)
    finally:
        release.set()
        await asyncio.gather(commit, *(t for t in (merge,) if t), return_exceptions=True)

    assert (summary.resumed, summary.relationships_written) == (False, 1)
    assert merged["mutation_count"] > 0
    assert await real_postgres_client.fetch_all(
        "SELECT entity_id FROM public.message_entity_refs WHERE message_id = 101 "
        "ORDER BY entity_id"
    ) == [{"entity_id": 2}, {"entity_id": 4}]
    assert await real_postgres_client.fetch_one(
        "SELECT source_entity_id, target_entity_id FROM public.relationship_observations"
    ) == {"source_entity_id": 2, "target_entity_id": 4}
    await _assert_no_retired_identity_references(real_postgres_client)
