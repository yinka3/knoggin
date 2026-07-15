import pytest

from common.schema.contracts import EpisodeDecision
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
        summary=f"Summary for {episode_id}",
        messages=[MessageEpisode(message_id=1, message_position=0)],
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
    assert context.entity_ids == [2, 3]
    assert context.relationship_ids == ["project-1:2:3"]
    assert [episode.episode_id for episode in context.prior_episodes] == [
        "episode-previous",
        "episode-overlap",
    ]


@pytest.mark.no_network
async def test_episode_job_generates_a_grounded_episode_decision():
    decision = EpisodeDecision(
        action="consolidate",
        target_episode_id="episode-previous",
        summary="The team continued its storage implementation discussion.",
        importance=0.8,
        message_influences=[
            {"message_id": 13, "influence_weight": 0.8},
            {"message_id": 14, "influence_weight": 0.5},
        ],
        focus_entities=[{"entity_id": 2, "prominence_weight": 0.9}],
        central_relationships=[
            {"relationship_id": "project-1:2:3", "prominence_weight": 0.7}
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

    assert generated == decision
    assert llm.calls[0]["response_model"] is EpisodeDecision
    assert llm.calls[0]["temperature"] == 0.0
    assert '"message_id": 13' in llm.calls[0]["user"]
    assert '"episode_id": "episode-previous"' in llm.calls[0]["user"]


@pytest.mark.no_network
async def test_episode_job_rejects_decision_ids_outside_candidate_context():
    llm = FakeEpisodeLLM(
        EpisodeDecision(
            action="create",
            summary="An unsupported focus entity was selected.",
            message_influences=[
                {"message_id": 13, "influence_weight": 0.8},
                {"message_id": 14, "influence_weight": 0.5},
            ],
            focus_entities=[{"entity_id": 99, "prominence_weight": 0.9}],
        )
    )
    job = EpisodeJob(
        knowledge_store=CandidateEpisodeStore(),
        settings=EpisodeSettings(batch_multiple=3, max_message_count=24),
        ingestion_settings=IngestionSettings(batch_size=8),
        llm=llm,
    )

    with pytest.raises(ValueError, match="must belong to the source window"):
        await job.generate_decision(
            user_name="ada",
            project_id="project-1",
            session_id="session-1",
        )
