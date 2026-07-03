from datetime import datetime, timezone

import pytest

from common.schema.contracts import EntityProfilesResult, FactResolutionSummary
from common.schema.primitives import Fact, FactRecord, ProfileUpdate
from common.scoping import IDENTITY_SCOPE
from common.utils.time_utils import frozen_time
from infrastructure.job.base import JobContext
from infrastructure.redis_client import RedisKeys
from knoggin_server.ingestion.jobs.profile_job import ProfileRefinementJob
from knoggin_server.knowledge.entity.profile import EntityProfile
from tests.fixtures.fakes import FakeRedis


def job_context(*, idle_seconds=0):
    return JobContext(
        user_name="ada",
        project_id="project-1",
        idle_seconds=idle_seconds,
    )


def fact_record(
    content="Existing profile fact",
    *,
    fact_id="fact-1",
    source_entity_id=2,
    source_msg_id=1,
):
    return FactRecord(
        id=fact_id,
        source_entity_id=source_entity_id,
        content=content,
        source_msg_id=source_msg_id,
        valid_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        embedding=[0.1, 0.2],
    )


class FakeEntities:
    def __init__(self, profiles=None, aliases=None, user_id=1):
        self.readable_project_ids = ["project-1"]
        self.profiles = profiles or {
            1: EntityProfile(canonical_name="ada", entity_type="person"),
            2: EntityProfile(canonical_name="Widget", entity_type="concept"),
            3: EntityProfile(canonical_name="Backend", entity_type="project"),
            4: EntityProfile(canonical_name="Tests", entity_type="concept"),
            5: EntityProfile(canonical_name="Profiles", entity_type="concept"),
        }
        self.aliases = aliases or {1: ["ada"]}
        self.user_id = user_id
        self.embedding_calls = []
        self.embedding_service = FakeEmbedding()

    async def get_id(self, name):
        return self.user_id if name == "ada" else None

    def get_mentions_for_id(self, entity_id):
        profile = self.profiles[entity_id]
        return list(
            dict.fromkeys([profile.canonical_name, *self.aliases.get(entity_id, [])])
        )

    def get_cached_profile(self, entity_id):
        return self.profiles.get(entity_id)

    def has_cached_entity(self, entity_id):
        return entity_id in self.profiles

    async def compute_embedding(self, entity_id, resolution_text, embedding=None):
        self.embedding_calls.append((entity_id, resolution_text))
        return embedding or [float(entity_id), 0.5]


class FakeEmbedding:
    async def encode(self, texts):
        return [[float(index + 2), 0.5] for index, _ in enumerate(texts)]

    async def encode_single(self, text):
        return [1.0, 0.5]


class FakeKnowledgeStore:
    def __init__(self):
        self.recent_project_messages = []
        self.facts_for_entities = {}
        self.facts_for_entity = []
        self.entities_by_ids = {}
        self.update_checkpoint_calls = []
        self.update_profile_calls = []
        self.recent_project_message_calls = []
        self.facts_for_entities_calls = []
        self.facts_for_entity_calls = []
        self.entities_by_ids_calls = []

    async def get_recent_project_messages(
        self,
        user_name,
        project_id,
        limit,
        before_message_id=None,
    ):
        self.recent_project_message_calls.append(
            (user_name, project_id, limit, before_message_id)
        )
        return self.recent_project_messages

    async def get_facts_for_entities(
        self, entity_ids, *, visible_project_ids, active_only
    ):
        self.facts_for_entities_calls.append((list(entity_ids), active_only))
        return self.facts_for_entities

    async def get_facts_for_entity(
        self, entity_id, *, visible_project_ids, active_only
    ):
        self.facts_for_entity_calls.append((entity_id, active_only))
        return self.facts_for_entity

    async def get_entities_by_ids(
        self, entity_ids, *, visible_project_ids
    ):
        self.entities_by_ids_calls.append(list(entity_ids))
        return [
            {
                "id": entity_id,
                "last_profiled_msg_id": self.entities_by_ids.get(entity_id, 0),
            }
            for entity_id in entity_ids
        ]

    async def update_entity_checkpoint(
        self, entity_id, current_msg_id, project_id=None
    ):
        self.update_checkpoint_calls.append((entity_id, current_msg_id, project_id))

    async def update_entity_profile(
        self,
        entity_id,
        canonical_name,
        embedding,
        last_msg_id,
        project_id=None,
    ):
        self.update_profile_calls.append(
            {
                "entity_id": entity_id,
                "canonical_name": canonical_name,
                "embedding": embedding,
                "last_msg_id": last_msg_id,
                "project_id": project_id,
            }
        )


class FakeLLM:
    def __init__(self, result=None):
        self.result = result or EntityProfilesResult()
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        return self.result


def make_job(
    *,
    redis=None,
    entities=None,
    knowledge_store=None,
    llm=None,
    msg_window=6,
    volume_threshold=3,
    idle_threshold=30,
    profile_batch_size=2,
):
    return ProfileRefinementJob(
        llm=llm or FakeLLM(),
        entities=entities or FakeEntities(),
        knowledge_store=knowledge_store or FakeKnowledgeStore(),
        executor=None,
        embedding_service=FakeEmbedding(),
        redis_client=redis or FakeRedis(),
        msg_window=msg_window,
        volume_threshold=volume_threshold,
        idle_threshold=idle_threshold,
        profile_batch_size=profile_batch_size,
        contradiction_sim_low=0.25,
        contradiction_sim_high=0.9,
        contradiction_batch_size=2,
        contradiction_prompt="judge contradictions",
    )


def patch_profile_events(monkeypatch):
    events = []

    async def fake_emit(*args, **kwargs):
        events.append((args, kwargs))

    monkeypatch.setattr("knoggin_server.ingestion.jobs.profile_job.emit", fake_emit)
    return events


@pytest.mark.no_network
async def test_profile_refinement_should_run_uses_dirty_count_volume_and_idle(
    monkeypatch,
):
    events = patch_profile_events(monkeypatch)
    redis = FakeRedis()
    job = make_job(redis=redis, volume_threshold=3, idle_threshold=30)
    dirty_key = RedisKeys.dirty_entities("ada", "project-1")

    assert await job.should_run(job_context(idle_seconds=100)) is False

    await redis.sadd(dirty_key, "2", "3")
    assert await job.should_run(job_context(idle_seconds=10)) is False

    assert await job.should_run(job_context(idle_seconds=30)) is True

    await redis.sadd(dirty_key, "4")
    assert await job.should_run(job_context(idle_seconds=0)) is True

    event_names = [args[2] for args, _ in events]
    assert event_names == [
        "profile_skipped",
        "profile_trigger_idle",
        "profile_trigger_volume",
    ]


@pytest.mark.no_network
async def test_get_conversation_context_applies_ratio_formatting_and_sorting():
    knowledge_store = FakeKnowledgeStore()
    knowledge_store.recent_project_messages = [
        {
            "id": 1,
            "role": "user",
            "content": "older user",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "session_id": "session-1",
        },
        {
            "id": 2,
            "role": "assistant",
            "content": "older assistant",
            "timestamp": "2026-01-01T10:01:00+00:00",
            "session_id": "session-1",
        },
        {
            "id": 3,
            "role": "user",
            "content": "middle user",
            "timestamp": "2026-01-01T10:02:00+00:00",
            "session_id": "session-2",
        },
        {
            "id": 4,
            "role": "assistant",
            "content": "new assistant",
            "timestamp": "2026-01-01T10:03:00+00:00",
            "session_id": "session-2",
        },
        {
            "id": 5,
            "role": "user",
            "content": "new user",
            "timestamp": "2026-01-01T10:04:00+00:00",
            "session_id": "session-3",
        },
    ]
    job = make_job(knowledge_store=knowledge_store)

    conversation = await job._get_conversation_context(
        job_context(),
        num_turns=4,
        user_ratio=0.75,
        up_to_msg_id=99,
    )

    assert knowledge_store.recent_project_message_calls == [("ada", "project-1", 8, 99)]
    assert [turn["id"] for turn in conversation] == [1, 3, 4, 5]
    assert conversation[0]["formatted"].endswith("[USER]: older user")
    assert conversation[0]["formatted"].startswith("[MSG_1] [2026-01-01 10:00]")
    assert conversation[2]["formatted"] == (
        "[2026-01-01 10:03] [AGENT]: new assistant"
    )
    assert conversation[2]["user_msg_id"] is None


@pytest.mark.no_network
def test_source_session_by_msg_id_maps_only_user_messages_with_session_id():
    conversation = [
        {"user_msg_id": 1, "session_id": "session-1"},
        {"user_msg_id": None, "session_id": "session-2"},
        {"user_msg_id": 3},
        {"user_msg_id": 4, "session_id": "session-4"},
    ]

    assert ProfileRefinementJob._source_session_by_msg_id(conversation) == {
        1: "session-1",
        4: "session-4",
    }


@pytest.mark.no_network
async def test_execute_filters_dirty_ids_force_limit_clears_processed_and_merges(
    monkeypatch,
):
    events = patch_profile_events(monkeypatch)
    redis = FakeRedis()
    dirty_key = RedisKeys.dirty_entities("ada", "project-1")
    await redis.sadd(dirty_key, "bad", "1", "2", "3", "4", "5", "6")
    await redis.set(RedisKeys.project_last_processed("ada", "project-1"), "12")
    job = make_job(redis=redis, volume_threshold=2)
    seen_entity_ids = []
    written_updates = []

    async def get_conversation_context(ctx, num_turns, **kwargs):
        assert kwargs == {"up_to_msg_id": 12}
        return [{"formatted": "[MSG_12] update", "user_msg_id": 12}]

    async def run_updates(ctx, entity_ids, conversation):
        seen_entity_ids.extend(entity_ids)
        return (
            [
                {
                    "id": 2,
                    "canonical_name": "Widget",
                    "embedding": [0.2],
                    "last_msg_id": 12,
                    "project_id": "project-1",
                }
            ],
            [2, 3, 4, 5],
        )

    async def write_updates(updates, project_id):
        written_updates.extend((updates, project_id))

    async def maybe_refine_user(ctx, current_msg_id):
        assert current_msg_id == 12
        return False

    job._get_conversation_context = get_conversation_context
    job._run_updates = run_updates
    job._write_updates = write_updates
    job._maybe_refine_user = maybe_refine_user

    with frozen_time("2026-02-03T04:05:06+00:00"):
        result = await job.execute(job_context(), force=True)

    assert result.success is True
    assert result.summary == "Refined 1 profiles"
    assert seen_entity_ids == [2, 3, 4, 5, 6]
    assert written_updates[1] == "project-1"
    assert await redis.smembers(dirty_key) == {"1", "6", "bad"}
    assert await redis.smembers(RedisKeys.merge_queue("ada", "project-1")) == {"2"}
    assert [event[0][2] for event in events] == [
        "profiles_refined",
        "merge_queue_marked",
        "dirty_entities_cleared",
    ]
    merge_event = events[1][0]
    assert merge_event == (
        "project-1",
        "job",
        "merge_queue_marked",
        {
            "user_name": "ada",
            "project_id": "project-1",
            "merge_key": RedisKeys.merge_queue("ada", "project-1"),
            "entity_ids": ["2"],
            "marked_count": 1,
            "reason": "profile_refined",
        },
    )
    clear_event = events[2][0]
    assert clear_event == (
        "project-1",
        "job",
        "dirty_entities_cleared",
        {
            "user_name": "ada",
            "project_id": "project-1",
            "dirty_key": dirty_key,
            "entity_ids": ["2", "3", "4", "5"],
            "cleared_count": 4,
            "reason": "profile_processed",
        },
    )
    assert await redis.get(
        RedisKeys.project_profile_complete("ada", "project-1")
    ) == "1770091506.0"


@pytest.mark.no_network
async def test_execute_empty_conversation_returns_failure_without_profile_complete(
    monkeypatch,
):
    redis = FakeRedis()
    dirty_key = RedisKeys.dirty_entities("ada", "project-1")
    await redis.sadd(dirty_key, "2")
    job = make_job(redis=redis)

    async def get_conversation_context(*_args, **_kwargs):
        return []

    job._get_conversation_context = get_conversation_context

    result = await job.execute(job_context())

    assert result.success is False
    assert result.summary == "No context found"
    assert await redis.get(
        RedisKeys.project_profile_complete("ada", "project-1")
    ) is None
    assert await redis.smembers(dirty_key) == {"2"}


@pytest.mark.no_network
async def test_run_updates_clears_missing_profiles_without_fetching_facts():
    knowledge_store = FakeKnowledgeStore()
    entities = FakeEntities(
        profiles={1: EntityProfile(canonical_name="ada", entity_type="person")}
    )
    job = make_job(entities=entities, knowledge_store=knowledge_store)

    updates, clear_ids = await job._run_updates(
        job_context(),
        [2, 3],
        [{"formatted": "[MSG_1] update", "user_msg_id": 1}],
    )

    assert updates == []
    assert clear_ids == [2, 3]
    assert knowledge_store.facts_for_entities_calls == []


@pytest.mark.no_network
async def test_run_updates_keeps_dirty_ids_when_fact_fetch_fails():
    knowledge_store = FakeKnowledgeStore()
    knowledge_store.facts_for_entities = None
    job = make_job(knowledge_store=knowledge_store)

    updates, clear_ids = await job._run_updates(
        job_context(),
        [2, 3],
        [{"formatted": "[MSG_1] update", "user_msg_id": 1}],
    )

    assert updates == []
    assert clear_ids == []
    assert knowledge_store.facts_for_entities_calls == [([2, 3], True)]


@pytest.mark.no_network
async def test_run_updates_filters_old_conversation_by_entity_checkpoint():
    knowledge_store = FakeKnowledgeStore()
    knowledge_store.facts_for_entities = {2: [], 3: []}
    knowledge_store.entities_by_ids = {2: 10, 3: 0}
    job = make_job(knowledge_store=knowledge_store)
    seen_batches = []

    async def process_single_batch(
        ctx,
        batch,
        ents_to_facts,
        current_msg_id,
        valid_msg_ids,
        source_session_by_msg_id,
    ):
        seen_batches.append(
            {
                "batch": batch,
                "current_msg_id": current_msg_id,
                "valid_msg_ids": valid_msg_ids,
                "source_session_by_msg_id": source_session_by_msg_id,
            }
        )
        return []

    job._process_single_batch = process_single_batch

    updates, clear_ids = await job._run_updates(
        job_context(),
        [2, 3],
        [
            {
                "formatted": "[MSG_5] old update",
                "user_msg_id": 5,
                "session_id": "session-5",
            }
        ],
    )

    assert updates == []
    assert clear_ids == [2, 3]
    assert [item["ent_id"] for item in seen_batches[0]["batch"]] == [3]
    assert seen_batches[0]["valid_msg_ids"] == {5}
    assert seen_batches[0]["source_session_by_msg_id"] == {5: "session-5"}


@pytest.mark.no_network
async def test_process_single_batch_checkpoints_wrong_or_empty_llm_profiles():
    knowledge_store = FakeKnowledgeStore()
    llm = FakeLLM(
        EntityProfilesResult(
            profiles=[
                ProfileUpdate(
                    canonical_name="Wrong Name",
                    facts=[Fact(content="Ignored fact", source_msg_id=1)],
                ),
                ProfileUpdate(canonical_name="Backend", facts=[]),
            ]
        )
    )
    job = make_job(knowledge_store=knowledge_store, llm=llm)

    updates = await job._process_single_batch(
        job_context(),
        [
            {
                "ent_id": 2,
                "entity_name": "Widget",
                "entity_type": "concept",
                "existing_facts": [],
                "known_aliases": ["Widget"],
                "conversation_text": "[MSG_1] Widget update",
            },
            {
                "ent_id": 3,
                "entity_name": "Backend",
                "entity_type": "project",
                "existing_facts": [],
                "known_aliases": ["Backend"],
                "conversation_text": "[MSG_1] Backend update",
            },
        ],
        {2: [], 3: []},
        9,
        {1},
        {1: "session-1"},
    )

    assert updates == []
    assert knowledge_store.update_checkpoint_calls == [
        (2, 9, "project-1"),
        (3, 9, "project-1"),
    ]


@pytest.mark.no_network
async def test_process_single_batch_applies_facts_redirties_and_returns_updates(
    monkeypatch,
):
    redis = FakeRedis()
    knowledge_store = FakeKnowledgeStore()
    entities = FakeEntities()
    existing_fact = fact_record("Widget used to rely on broad profile summaries")
    active_fact = fact_record(
        "Widget now requires direct source evidence",
        fact_id="active-1",
    )
    llm = FakeLLM(
        EntityProfilesResult(
            profiles=[
                ProfileUpdate(
                    canonical_name="Widget",
                    facts=[
                        Fact(
                            content="Widget now requires direct source evidence",
                            source_msg_id=7,
                        )
                    ],
                )
            ]
        )
    )
    job = make_job(
        redis=redis, knowledge_store=knowledge_store, entities=entities, llm=llm
    )
    apply_calls = []

    async def fake_apply_fact_changes(*args, **kwargs):
        apply_calls.append((args, kwargs))
        return FactResolutionSummary(
            active_facts=[active_fact],
            failed_invalidations=["fact-old"],
        )

    monkeypatch.setattr(
        "knoggin_server.ingestion.jobs.profile_job."
        "FactResolver.apply_fact_changes",
        fake_apply_fact_changes,
    )

    updates = await job._process_single_batch(
        job_context(),
        [
            {
                "ent_id": 2,
                "entity_name": "Widget",
                "entity_type": "concept",
                "existing_facts": [existing_fact],
                "known_aliases": ["Widget"],
                "conversation_text": "[MSG_7] Widget update",
            }
        ],
        {2: [existing_fact]},
        12,
        {7},
        {7: "session-7"},
    )

    assert updates == [
        {
            "id": 2,
            "canonical_name": "Widget",
            "embedding": [2.0, 0.5],
            "last_msg_id": 12,
            "project_id": "project-1",
        }
    ]
    assert await redis.smembers(RedisKeys.dirty_entities("ada", "project-1")) == {"2"}
    assert entities.embedding_calls == [
        (2, "Widget (concept). Widget now requires direct source evidence")
    ]
    assert knowledge_store.update_checkpoint_calls == []

    args, kwargs = apply_calls[0]
    assert args[:5] == (
        2,
        args[1],
        [existing_fact],
        {7},
        "project-1",
    )
    merge_result = args[1]
    assert [fact.content for fact in merge_result.new_contents] == [
        "Widget now requires direct source evidence"
    ]
    assert args[5] is knowledge_store
    assert args[6] is job.embedding_service
    assert args[7] is llm
    assert kwargs == {
        "user_name": "ada",
        "project_id": "project-1",
        "contradiction_sim_low": 0.25,
        "contradiction_sim_high": 0.9,
        "contradiction_batch_size": 2,
        "contradiction_prompt": "judge contradictions",
        "source_session_by_msg_id": {7: "session-7"},
        "audit_change_type": "profile_extraction",
        "actor": "profile_refinement",
        "reason": "profile_extraction",
    }


@pytest.mark.no_network
async def test_maybe_refine_user_gates_and_sets_short_ttl():
    ran_key = RedisKeys.project_user_profile_ran("ada", "project-1")
    redis = FakeRedis()
    await redis.set(ran_key, "true")
    job = make_job(redis=redis)

    async def fail_refine(*_args):
        raise AssertionError("user refinement should be skipped while TTL exists")

    job._refine_user_profile = fail_refine
    assert await job._maybe_refine_user(job_context(), curr_msg_id=9) is False

    missing_user_job = make_job(entities=FakeEntities(user_id=None))
    assert (
        await missing_user_job._maybe_refine_user(job_context(), curr_msg_id=9)
        is False
    )

    missing_profile_job = make_job(entities=FakeEntities(user_id=99))
    assert (
        await missing_profile_job._maybe_refine_user(job_context(), curr_msg_id=9)
        is False
    )

    redis = FakeRedis()
    job = make_job(redis=redis)

    async def no_update_refine(ctx, user_id, profile, curr_msg_id):
        assert (ctx.user_name, user_id, profile.canonical_name, curr_msg_id) == (
            "ada",
            1,
            "ada",
            9,
        )
        return False

    job._refine_user_profile = no_update_refine

    assert await job._maybe_refine_user(job_context(), curr_msg_id=9) is False
    assert await redis.get(ran_key) == "true"
    assert (ran_key, 300) in redis.expirations


@pytest.mark.no_network
@pytest.mark.parametrize(
    "case_name",
    [
        "no_conversation",
        "empty_conversation_text",
        "missing_existing_facts",
        "no_llm_profiles",
        "wrong_canonical_name",
        "no_new_facts",
    ],
)
async def test_refine_user_profile_skips_incomplete_or_weak_inputs(case_name):
    knowledge_store = FakeKnowledgeStore()
    llm_result = EntityProfilesResult(
        profiles=[
            ProfileUpdate(
                canonical_name="ada",
                facts=[Fact(content="Ada prefers scoped profile updates")],
            )
        ]
    )

    if case_name == "missing_existing_facts":
        knowledge_store.facts_for_entity = None
    else:
        knowledge_store.facts_for_entity = []

    if case_name == "no_llm_profiles":
        llm_result = EntityProfilesResult()
    elif case_name == "wrong_canonical_name":
        llm_result = EntityProfilesResult(
            profiles=[
                ProfileUpdate(
                    canonical_name="Someone Else",
                    facts=[Fact(content="ignored")],
                )
            ]
        )
    elif case_name == "no_new_facts":
        llm_result = EntityProfilesResult(
            profiles=[ProfileUpdate(canonical_name="ada", facts=[])]
        )

    job = make_job(knowledge_store=knowledge_store, llm=FakeLLM(llm_result))

    async def get_conversation_context(*_args, **_kwargs):
        if case_name == "no_conversation":
            return []
        if case_name == "empty_conversation_text":
            return [{"formatted": "", "user_msg_id": 7}]
        return [
            {
                "formatted": "[MSG_7] Ada prefers scoped profile updates",
                "user_msg_id": 7,
                "session_id": "session-7",
            }
        ]

    job._get_conversation_context = get_conversation_context

    assert (
        await job._refine_user_profile(
            job_context(),
            user_id=1,
            profile=EntityProfile(canonical_name="ada", entity_type="person"),
            curr_msg_id=9,
        )
        is False
    )
    assert knowledge_store.update_profile_calls == []


@pytest.mark.no_network
async def test_refine_user_profile_applies_global_scope_and_redirties_user(
    monkeypatch,
):
    events = patch_profile_events(monkeypatch)
    redis = FakeRedis()
    knowledge_store = FakeKnowledgeStore()
    entities = FakeEntities(aliases={1: ["ada", "Ada Lovelace"]})
    old_fact = fact_record(
        "Ada previously allowed broad profile updates",
        fact_id="old-1",
        source_entity_id=1,
    )
    newer_fact = fact_record(
        "Ada prefers scoped profile updates",
        fact_id="old-2",
        source_entity_id=1,
    )
    active_fact = fact_record(
        "Ada requires direct evidence for profile updates",
        fact_id="active-1",
        source_entity_id=1,
    )
    knowledge_store.facts_for_entity = [old_fact, newer_fact]
    llm = FakeLLM(
        EntityProfilesResult(
            profiles=[
                ProfileUpdate(
                    canonical_name="ada",
                    facts=[
                        Fact(
                            content="Ada requires direct evidence for profile updates",
                            source_msg_id=7,
                        )
                    ],
                )
            ]
        )
    )
    job = make_job(
        redis=redis, knowledge_store=knowledge_store, entities=entities, llm=llm
    )
    job.max_facts_context = 1
    apply_calls = []
    enriched_calls = []

    async def get_conversation_context(ctx, num_turns, **kwargs):
        assert num_turns == int(job.msg_window * 1.5)
        assert kwargs == {"up_to_msg_id": 12}
        return [
            {
                "formatted": "[MSG_7] Ada wants evidence-backed profiles",
                "user_msg_id": 7,
                "session_id": "session-7",
            },
            {
                "formatted": "[2026-01-01 10:01] [AGENT]: acknowledged",
                "user_msg_id": None,
                "session_id": "session-7",
            },
        ]

    async def fake_enrich_facts_with_sources(
        facts,
        knowledge_store,
        visible_project_ids,
        user_name=None,
    ):
        enriched_calls.append(
            (list(facts), knowledge_store, visible_project_ids, user_name)
        )
        return [{"content": fact.content} for fact in facts]

    async def fake_apply_fact_changes(*args, **kwargs):
        apply_calls.append((args, kwargs))
        return FactResolutionSummary(
            active_facts=[active_fact],
            created_facts=[active_fact],
            invalidated_fact_ids=["old-1"],
            failed_invalidations=["old-2"],
        )

    monkeypatch.setattr(
        "knoggin_server.ingestion.jobs.profile_job.enrich_facts_with_sources",
        fake_enrich_facts_with_sources,
    )
    monkeypatch.setattr(
        "knoggin_server.ingestion.jobs.profile_job."
        "FactResolver.apply_fact_changes",
        fake_apply_fact_changes,
    )
    job._get_conversation_context = get_conversation_context

    result = await job._refine_user_profile(
        job_context(),
        user_id=1,
        profile=EntityProfile(canonical_name="ada", entity_type="person"),
        curr_msg_id=12,
    )

    assert result is True
    assert knowledge_store.facts_for_entity_calls == [(1, True)]
    assert enriched_calls == [
        (
            [old_fact, newer_fact],
            knowledge_store,
            entities.readable_project_ids,
            "ada",
        )
    ]
    assert len(llm.calls) == 1
    assert "Ada Lovelace" in llm.calls[0]["user"]
    assert "Ada prefers scoped profile updates" in llm.calls[0]["user"]
    assert "Ada previously allowed broad profile updates" not in llm.calls[0]["user"]

    args, kwargs = apply_calls[0]
    assert args[:5] == (
        1,
        args[1],
        [old_fact, newer_fact],
        {7},
        "project-1",
    )
    merge_result = args[1]
    assert [fact.content for fact in merge_result.new_contents] == [
        "Ada requires direct evidence for profile updates"
    ]
    assert args[5] is knowledge_store
    assert args[6] is job.embedding_service
    assert args[7] is llm
    assert kwargs == {
        "user_name": "ada",
        "project_id": IDENTITY_SCOPE,
        "contradiction_sim_low": 0.25,
        "contradiction_sim_high": 0.9,
        "contradiction_batch_size": 2,
        "contradiction_prompt": "judge contradictions",
        "source_session_by_msg_id": {7: "session-7"},
        "audit_change_type": "profile_extraction",
        "actor": "profile_refinement",
        "reason": "user_profile_extraction",
    }

    assert await redis.smembers(RedisKeys.dirty_entities("ada", "project-1")) == {"1"}
    assert entities.embedding_calls == [
        (1, "ada (person). Ada requires direct evidence for profile updates")
    ]
    assert knowledge_store.update_profile_calls == [
        {
            "entity_id": 1,
            "canonical_name": "ada",
            "embedding": [1.0, 0.5],
            "last_msg_id": 12,
            "project_id": IDENTITY_SCOPE,
        }
    ]
    assert [args[2] for args, _ in events] == [
        "llm_call",
        "dirty_entities_marked",
        "user_profile_refined",
    ]
    assert events[1][0][3] == {
        "user_name": "ada",
        "project_id": "project-1",
        "dirty_key": RedisKeys.dirty_entities("ada", "project-1"),
        "entity_ids": [1],
        "marked_count": 1,
        "reason": "fact_invalidation_failed",
    }
    assert events[-1][0][3] == {
        "user_name": "ada",
        "facts_invalidated": 1,
        "facts_created": 1,
    }
