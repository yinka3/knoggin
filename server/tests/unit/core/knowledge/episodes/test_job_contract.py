import pytest
from pydantic import ValidationError

from common.schema.episode.generation import (
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import Episode, MessageEpisode
from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.job import EpisodeJob
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from infrastructure.job.base import JobContext


class _ReadinessStore:
    def __init__(self, ready: bool) -> None:
        self.ready = ready
        self.readiness_calls = []
        self.window_reads = 0

    async def has_ready_project_episode_window(self, **kwargs) -> bool:
        self.readiness_calls.append(kwargs)
        return self.ready

    async def get_next_project_episode_window(self, **_kwargs):
        self.window_reads += 1
        raise AssertionError("should_run must not load an episode window")


class _ConsolidationStore:
    def __init__(self, prior):
        self.prior = prior
        self.written = None

    async def get_next_project_episode_window(self, **_kwargs):
        return [
            {"message_id": 10, "session_id": "session-a", "role": "user", "content": "Continue launch", "timestamp_ms": 3},
            {"message_id": 11, "session_id": "session-a", "role": "assistant", "content": "Launch continues", "timestamp_ms": 4, "user_msg_id": 10},
        ]

    async def get_nearby_project_episodes(self, **_kwargs):
        return [self.prior]

    async def get_project_episode_source_messages(self, *_args, **_kwargs):
        return [
            {"message_id": 1, "session_id": "session-a", "role": "user", "content": "Start launch", "timestamp_ms": 1},
            {"message_id": 2, "session_id": "session-a", "role": "assistant", "content": "Launch started", "timestamp_ms": 2},
        ]

    async def write_project_episode_window(self, episodes, messages, **_kwargs):
        self.written = (episodes, messages)
        return True


class _ConsolidationLLM:
    def __init__(self):
        self.response_models = []

    async def generate_structured(self, *, response_model, **_kwargs):
        self.response_models.append(response_model)
        if response_model is LLMEpisodeWindowDecision:
            return LLMEpisodeWindowDecision(proposals=[
                LLMEpisodeDecision(
                    action="consolidate",
                    target_episode_id="episode:1",
                    summary="The launch continued.",
                    message_influences=["message:1", "message:2"],
                )
            ])
        return LLMEpisodeConsolidation(
            action="consolidate",
            summary="The complete launch episode.",
            message_influences=[
                "message:1", "message:2", "message:3", "message:4"
            ],
        )


class _ConsolidationEmbedding:
    async def encode(self, texts):
        return [[0.1] * 1024 for _ in texts]


@pytest.mark.no_network
async def test_episode_should_run_uses_project_readiness_without_loading_messages():
    async def project_window_size() -> int:
        return 12

    store = _ReadinessStore(ready=True)
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(),
        episode_window_size=8,
        episode_window_size_provider=project_window_size,
        llm=object(),
        embedding_service=object(),
    )

    assert await job.should_run(JobContext(user_name="ada", project_id="project-1"))
    assert store.readiness_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "message_count": 12,
        }
    ]
    assert store.window_reads == 0


@pytest.mark.no_network
def test_episode_policy_only_snapshots_current_generation_controls():
    policy = EpisodeGenerationPolicy.capture(
        settings=EpisodeSettings(max_narrative_chars=5000),
        episode_window_size=12,
    )

    assert policy.metadata() == {
        "version": policy.version,
        "target_message_count": 12,
        "max_episode_source_messages": 72,
        "max_episode_source_tokens": 12000,
        "max_narrative_chars": 5000,
        "prompt_narrative_chars": 4500,
        "prior_episode_candidate_count": 3,
    }
    with pytest.raises(ValidationError, match="Extra inputs are not permitted"):
        EpisodeSettings(max_message_count=12)


@pytest.mark.no_network
async def test_consolidation_regenerates_from_complete_source_evidence():
    prior = Episode(
        episode_id="prior",
        project_id="project-1",
        session_id="session-a",
        summary="Prior launch",
        messages=[
            MessageEpisode(message_id=1, session_id="session-a", message_position=0),
            MessageEpisode(message_id=2, session_id="session-a", message_position=1),
        ],
    )
    store = _ConsolidationStore(prior)
    llm = _ConsolidationLLM()
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(),
        episode_window_size=8,
        llm=llm,
        embedding_service=_ConsolidationEmbedding(),
    )

    build = await job.process_next_window(user_name="ada", project_id="project-1")

    assert build is not None
    assert llm.response_models == [LLMEpisodeWindowDecision, LLMEpisodeConsolidation]
    assert store.written is not None
    episodes, window = store.written
    assert window[0]["message_id"] == 10
    assert episodes[0].episode_id == "prior"
    assert [message.message_id for message in episodes[0].messages] == [1, 2, 10, 11]
