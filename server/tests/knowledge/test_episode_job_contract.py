import pytest

import core.ingestion.jobs.episode_job as episode_job_module
from common.schema.contracts import (
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
)
from common.schema.primitives import Episode, MessageEpisode
from common.schema.settings import EpisodeSettings, IngestionSettings
from core.ingestion.jobs.episode_job import EpisodeJob
from infrastructure.job.base import JobContext


class FakeEpisodeStore:
    def __init__(self):
        self.checkpoint_calls = []
        self.window_calls = []

    async def get_last_evaluated_episode_message_id(self, **scope):
        self.checkpoint_calls.append(scope)
        return 12

    async def get_next_episode_window(self, **kwargs):
        self.window_calls.append(kwargs)
        return [{"message_id": 13}]


def make_episode(episode_id):
    return Episode(
        episode_id=episode_id,
        project_id="project-1",
        session_id="session-1",
        summary="Prior conversation summary.",
        messages=[MessageEpisode(message_id=11, message_position=0)],
    )


class CandidateEpisodeStore(FakeEpisodeStore):
    async def get_next_episode_window(self, **kwargs):
        self.window_calls.append(kwargs)
        return [{"message_id": 13}, {"message_id": 14}]

    async def get_entity_ids_for_messages(self, message_ids, **scope):
        assert message_ids == [13, 14]
        assert scope["session_id"] == "session-1"
        return {13: [2, 3], 14: [3]}

    async def get_relationship_ids_for_messages(self, message_ids, **scope):
        assert message_ids == [13, 14]
        assert scope["project_id"] == "project-1"
        return {13: ["project-1:2:3"], 14: []}

    async def get_episode_generation_catalog(self, message_ids, **scope):
        assert message_ids == [13, 14]
        assert scope["user_name"] == "ada"
        return (
            [
                {
                    "entity_id": 2,
                    "canonical_name": "Ada",
                    "type": "person",
                    "aliases": ["Ada Lovelace"],
                },
                {
                    "entity_id": 3,
                    "canonical_name": "episodic memory",
                    "type": "concept",
                    "aliases": [],
                },
            ],
            [
                {
                    "relationship_id": "project-1:2:3",
                    "entity_a": {
                        "entity_id": 2,
                        "canonical_name": "Ada",
                        "type": "person",
                    },
                    "entity_b": {
                        "entity_id": 3,
                        "canonical_name": "episodic memory",
                        "type": "concept",
                    },
                    "relationship_type": "adopted",
                    "confidence": 0.9,
                    "context": "Ada selected episodic memory.",
                    "evidence_message_ids": [13],
                }
            ],
        )

    async def get_recent_episodes(self, **scope):
        assert scope["limit"] == 1
        return [make_episode("episode-previous")]

    async def get_episodes_for_entities(self, entity_ids, **scope):
        assert entity_ids == [2, 3]
        assert scope["limit"] == 3
        return [
            make_episode("episode-previous"),
            make_episode("episode-overlap"),
        ]


class FakeEpisodeLLM:
    def __init__(self, decision):
        self.decision = decision
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        return self.decision


class FakeEmbeddingService:
    def __init__(self):
        self.calls = []

    async def encode(self, texts):
        self.calls.append(texts)
        return [[0.25] * 1024]


async def _one_session():
    return ["session-1"]


class SequenceEpisodeLLM:
    def __init__(self, *responses):
        self.responses = list(responses)
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        return self.responses.pop(0)


class WritingEpisodeStore(CandidateEpisodeStore):
    def __init__(self):
        super().__init__()
        self.writes = []

    async def write_episode_window(self, episode, window_message_ids, **scope):
        self.writes.append((episode, window_message_ids, scope))
        return True


class ConsolidatingEpisodeStore(WritingEpisodeStore):
    async def get_entity_ids_for_messages(self, message_ids, **scope):
        if message_ids == [13, 14]:
            return await super().get_entity_ids_for_messages(message_ids, **scope)
        assert message_ids == [11, 13, 14]
        return {11: [2], 13: [2, 3], 14: [3]}

    async def get_relationship_ids_for_messages(self, message_ids, **scope):
        if message_ids == [13, 14]:
            return await super().get_relationship_ids_for_messages(message_ids, **scope)
        assert message_ids == [11, 13, 14]
        return {11: ["project-1:2:3"], 13: ["project-1:2:3"], 14: []}

    async def get_episode_generation_catalog(self, message_ids, **scope):
        if message_ids == [13, 14]:
            return await super().get_episode_generation_catalog(message_ids, **scope)
        assert message_ids == [11, 13, 14]
        entities, relationships = await super().get_episode_generation_catalog(
            [13, 14], **scope
        )
        return entities, [
            {**relationships[0], "evidence_message_ids": [11, 13]}
        ]

    async def get_episode_source_messages(self, episode_id, **scope):
        assert episode_id == "episode-previous"
        assert scope["session_id"] == "session-1"
        return [
            {
                "message_id": 11,
                "role": "user",
                "content": "Store traceable conversation summaries.",
                "timestamp_ms": 1700000000000,
                "message_position": 0,
            }
        ]


@pytest.mark.no_network
async def test_episode_job_derives_target_window_from_ingestion_batches():
    job = EpisodeJob(
        knowledge_store=object(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
    )

    assert job.name == "episode"
    assert job.target_message_count == 24
    assert job.max_message_count == 24
    assert job.prior_episode_candidate_count == 3
    assert await job.should_run(
        JobContext(user_name="ada", project_id="project-1")
    ) is False


def test_episode_job_rejects_a_maximum_smaller_than_its_target_window():
    with pytest.raises(ValueError, match="at least the target window size"):
        EpisodeJob(
            knowledge_store=object(),
            settings=EpisodeSettings(batch_multiple=4, max_message_count=24),
            ingestion_settings=IngestionSettings(batch_size=8),
        )


@pytest.mark.no_network
async def test_episode_job_loads_the_next_window_after_its_checkpoint():
    store = FakeEpisodeStore()
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
    )

    messages = await job.load_next_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert messages == [{"message_id": 13}]
    assert store.checkpoint_calls == [
        {"user_name": "ada", "project_id": "project-1", "session_id": "session-1"}
    ]
    assert store.window_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "after_message_id": 12,
            "message_count": 24,
        }
    ]


@pytest.mark.no_network
async def test_episode_job_builds_full_candidate_context_and_bounds_prior_episodes():
    store = CandidateEpisodeStore()
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
    )

    context = await job.load_candidate_context(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert context is not None
    assert [message["message_id"] for message in context.messages] == [13, 14]
    assert context.entity_ids_by_message == {13: [2, 3], 14: [3]}
    assert context.relationship_ids_by_message == {13: ["project-1:2:3"], 14: []}
    assert context.entity_catalog[0]["canonical_name"] == "Ada"
    assert context.relationship_catalog[0]["relationship_type"] == "adopted"
    assert context.entity_ids == [2, 3]
    assert context.relationship_ids == ["project-1:2:3"]
    assert [episode.episode_id for episode in context.prior_episodes] == [
        "episode-previous",
        "episode-overlap",
    ]


@pytest.mark.no_network
async def test_episode_job_generates_a_grounded_episode_decision():
    decision = LLMEpisodeDecision(
        action="consolidate",
        target_episode_id="ep2",
        summary="The team continued its storage implementation discussion.",
        importance=0.8,
        message_influences=[
            {"message_id": "m1", "influence_weight": 0.8},
            {"message_id": "m2", "influence_weight": 0.5},
        ],
        focus_entities=[{"entity_id": "e1", "prominence_weight": 0.9}],
        central_relationships=[
            {"relationship_id": "r1", "prominence_weight": 0.7}
        ],
    )
    llm = FakeEpisodeLLM(decision)
    job = EpisodeJob(
        knowledge_store=CandidateEpisodeStore(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=llm,
    )

    generated = await job.generate_decision(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert generated.action == "consolidate"
    assert generated.target_episode_id == "episode-previous"
    assert [item.message_id for item in generated.message_influences] == [13, 14]
    assert [item.entity_id for item in generated.focus_entities] == [2]
    assert [item.relationship_id for item in generated.central_relationships] == [
        "project-1:2:3"
    ]
    assert llm.calls[0]["response_model"] is LLMEpisodeDecision
    assert llm.calls[0]["temperature"] == 0.0
    assert '"message_id": "m1"' in llm.calls[0]["user"]
    assert '"entity_id": "e1"' in llm.calls[0]["user"]
    assert '"relationship_id": "r1"' in llm.calls[0]["user"]
    assert '"episode_id": "ep2"' in llm.calls[0]["user"]
    assert '"message_id": 13' not in llm.calls[0]["user"]
    assert '"entity_id": 2' not in llm.calls[0]["user"]
    assert '"relationship_id": "project-1:2:3"' not in llm.calls[0]["user"]
    assert '"episode_id": "episode-previous"' not in llm.calls[0]["user"]
    assert '"canonical_name": "Ada"' in llm.calls[0]["user"]
    assert '"relationship_type": "adopted"' in llm.calls[0]["user"]


@pytest.mark.no_network
async def test_episode_job_rejects_unknown_local_decision_reference():
    llm = FakeEpisodeLLM(
        LLMEpisodeDecision(
            action="create",
            summary="An unsupported focus entity was selected.",
            message_influences=[
                {"message_id": "m1", "influence_weight": 0.8},
                {"message_id": "m2", "influence_weight": 0.5},
            ],
            focus_entities=[{"entity_id": "e99", "prominence_weight": 0.9}],
        )
    )
    job = EpisodeJob(
        knowledge_store=CandidateEpisodeStore(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=llm,
    )

    with pytest.raises(ValueError, match="Unknown local ID 'e99'"):
        await job.generate_decision(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
        )


@pytest.mark.no_network
async def test_episode_job_rejects_duplicate_local_message_references():
    llm = FakeEpisodeLLM(
        LLMEpisodeDecision(
            action="create",
            summary="A repeated message reference should be rejected.",
            message_influences=[
                {"message_id": "m1", "influence_weight": 0.8},
                {"message_id": "m1", "influence_weight": 0.5},
            ],
        )
    )
    job = EpisodeJob(
        knowledge_store=CandidateEpisodeStore(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=llm,
    )

    with pytest.raises(
        ValueError,
        match="must cover each source message exactly once",
    ):
        await job.generate_decision(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
        )


@pytest.mark.no_network
async def test_episode_job_embeds_the_current_narrative_before_writing():
    decision = LLMEpisodeDecision(
        action="create",
        summary="The team adopted semantic search for episodes.",
        new_developments=["Episode vectors will be stored with summaries."],
        message_influences=[
            {"message_id": "m1", "influence_weight": 0.8},
            {"message_id": "m2", "influence_weight": 0.5},
        ],
    )
    store = WritingEpisodeStore()
    embeddings = FakeEmbeddingService()
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=FakeEpisodeLLM(decision),
        embedding_service=embeddings,
    )

    result = await job.process_next_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert result is not None
    persisted_episode, message_ids, scope = store.writes[0]
    assert persisted_episode.embedding == [0.25] * 1024
    assert message_ids == [13, 14]
    assert scope["project_id"] == "project-1"
    assert embeddings.calls == [
        [
            "Summary:\nThe team adopted semantic search for episodes.\n\n"
            "New developments:\n- Episode vectors will be stored with summaries."
        ]
    ]
    assert result.episode_source_message_count == 2
    assert result.entity_link_count == 0
    assert result.relationship_link_count == 0


@pytest.mark.no_network
async def test_episode_job_emits_operational_episode_metrics(monkeypatch):
    events = []

    async def capture_emit(scope_id, component, event, data):
        events.append((scope_id, component, event, data))

    monkeypatch.setattr(episode_job_module, "emit", capture_emit)
    job = EpisodeJob(
        knowledge_store=WritingEpisodeStore(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=FakeEpisodeLLM(
            LLMEpisodeDecision(
                action="create",
                summary="The team selected episodic retrieval.",
                message_influences=[
                    {"message_id": "m1", "influence_weight": 0.8},
                    {"message_id": "m2", "influence_weight": 0.5},
                ],
                focus_entities=[{"entity_id": "e1", "prominence_weight": 0.9}],
                central_relationships=[
                    {
                        "relationship_id": "r1",
                        "prominence_weight": 0.7,
                    }
                ],
            )
        ),
        embedding_service=FakeEmbeddingService(),
        session_ids_provider=lambda: _one_session(),
    )

    result = await job.execute(JobContext(user_name="ada", project_id="project-1"))

    assert result.success is True
    processed = events[0]
    assert processed[:3] == ("project-1", "job", "episode_processed")
    assert processed[3] == {
        "user_name": "ada",
        "project_id": "project-1",
        "session_id": "session-1",
        "action": "create",
        "episode_id": processed[3]["episode_id"],
        "source_message_count": 2,
        "episode_source_message_count": 2,
        "entity_link_count": 1,
        "relationship_link_count": 1,
        "consolidation_limit_hit": False,
        "episode_at_max_size": False,
        "processing_latency_ms": processed[3]["processing_latency_ms"],
    }
    assert processed[3]["processing_latency_ms"] >= 0


@pytest.mark.no_network
async def test_episode_job_emits_validation_metrics_for_invalid_ids(monkeypatch):
    events = []

    async def capture_emit(scope_id, component, event, data):
        events.append((scope_id, component, event, data))

    monkeypatch.setattr(episode_job_module, "emit", capture_emit)
    job = EpisodeJob(
        knowledge_store=CandidateEpisodeStore(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=FakeEpisodeLLM(
            LLMEpisodeDecision(
                action="create",
                summary="The wrong entity was selected.",
                message_influences=[
                    {"message_id": "m1", "influence_weight": 0.8},
                    {"message_id": "m2", "influence_weight": 0.5},
                ],
                focus_entities=[{"entity_id": "e99", "prominence_weight": 0.9}],
            )
        ),
    )

    with pytest.raises(ValueError, match="Unknown local ID 'e99'"):
        await job.process_next_window(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
        )

    assert events == [
        (
            "project-1",
            "job",
            "episode_validation_failed",
            {
                "user_name": "ada",
                "project_id": "project-1",
                "session_id": "session-1",
                "stage": "decision",
                "source_message_count": 2,
                "invalid_identifier": True,
                "reason": "invalid_identifier",
                "error": "invalid identifier",
            },
        ),
        (
            "project-1",
            "job",
            "local_reference_resolution_failed",
            {
                "pipeline": "episode",
                "reference_type": "episode_decision",
                "reason": "validation_rejected",
                "stage": "decision",
            },
        ),
    ]


@pytest.mark.no_network
async def test_episode_job_regenerates_all_consolidated_message_influences():
    initial = LLMEpisodeDecision(
        action="consolidate",
        target_episode_id="ep2",
        summary="The storage discussion continued.",
        message_influences=[
            {"message_id": "m1", "influence_weight": 0.8},
            {"message_id": "m2", "influence_weight": 0.5},
        ],
        focus_entities=[{"entity_id": "e1", "prominence_weight": 0.9}],
        central_relationships=[
            {"relationship_id": "r1", "prominence_weight": 0.7}
        ],
    )
    regenerated = LLMEpisodeConsolidation(
        summary="The team adopted traceable episodic summaries and vector search.",
        new_developments=["Semantic retrieval is now part of the plan."],
        message_influences=[
            {"message_id": "m1", "influence_weight": 0.2},
            {"message_id": "m2", "influence_weight": 0.95},
            {"message_id": "m3", "influence_weight": 0.55},
        ],
        focus_entities=[{"entity_id": "e1", "prominence_weight": 0.9}],
        central_relationships=[
            {"relationship_id": "r1", "prominence_weight": 0.7}
        ],
    )
    store = ConsolidatingEpisodeStore()
    llm = SequenceEpisodeLLM(initial, regenerated)
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=llm,
        embedding_service=FakeEmbeddingService(),
    )

    result = await job.process_next_window(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )

    assert result is not None
    assert result.action == "consolidate"
    persisted_episode, window_message_ids, _ = store.writes[0]
    assert window_message_ids == [13, 14]
    assert {
        message.message_id: message.influence_weight
        for message in persisted_episode.messages
    } == {11: 0.2, 13: 0.95, 14: 0.55}
    assert llm.calls[1]["response_model"] is LLMEpisodeConsolidation
    assert '"message_id": "m1"' in llm.calls[1]["user"]
    assert '"message_id": 11' not in llm.calls[1]["user"]
