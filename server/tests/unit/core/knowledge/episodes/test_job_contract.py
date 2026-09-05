import pytest
from pydantic import ValidationError

from common.schema.episode.generation import (
    LLMEpisodeConsolidation,
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.episode.models import Episode, MessageEpisode
from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.generator import EpisodeGenerator
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


class _WindowStore:
    def __init__(self):
        self.written = None

    async def get_next_project_episode_window(self, **_kwargs):
        return [
            {
                "message_id": 10,
                "session_id": "session-a",
                "role": "user",
                "content": "Continue launch",
                "timestamp_ms": 3,
            },
            {
                "message_id": 11,
                "session_id": "session-a",
                "role": "assistant",
                "content": "Launch continues",
                "timestamp_ms": 4,
                "user_msg_id": 10,
            },
        ]

    async def get_nearby_project_episodes(self, **_kwargs):
        return []

    async def write_project_episode_window(self, episodes, messages, **_kwargs):
        self.written = (episodes, messages)
        return True


class _EmptyWindowLLM:
    async def generate_structured(self, *, response_model, **_kwargs):
        assert response_model is LLMEpisodeWindowDecision
        return LLMEpisodeWindowDecision()


class _ConcurrentPolicyLLM:
    def __init__(self):
        self.job = None
        self.system_prompts = []

    async def generate_structured(self, *, response_model, system, **_kwargs):
        assert response_model is LLMEpisodeWindowDecision
        self.system_prompts.append(system)
        assert self.job is not None
        # Simulate a settings reload while the provider await is in progress.
        self.job.update_settings(
            EpisodeSettings(
                max_narrative_chars=500,
                max_episode_source_messages=1,
            ),
            episode_window_size=8,
        )
        return LLMEpisodeWindowDecision(
            proposals=[
                LLMEpisodeDecision(
                    action="create",
                    summary="The launch continued.",
                    message_influences=["message:1"],
                )
            ]
        )


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
async def test_episode_generator_uses_supplied_frozen_messages_without_selecting_or_persisting():
    store = _WindowStore()
    generator = EpisodeGenerator(
        store,
        llm=_EmptyWindowLLM(),
        embedding_service=_ConsolidationEmbedding(),
    )
    policy = EpisodeGenerationPolicy.capture(
        settings=EpisodeSettings(),
        episode_window_size=8,
    )
    messages = await store.get_next_project_episode_window()

    build = await generator.generate(
        user_name="ada",
        project_id="project-1",
        messages=messages,
        policy=policy,
    )

    assert [message["message_id"] for message in build.messages] == [10, 11]
    assert build.final_episodes == []
    assert store.written is None
    assert EpisodeGenerationPolicy.from_semantic_window_snapshot(
        policy.semantic_window_snapshot()
    ) == policy


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


@pytest.mark.no_network
async def test_empty_window_proposal_still_persists_source_checkpoint_input():
    store = _WindowStore()
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(),
        episode_window_size=8,
        llm=_EmptyWindowLLM(),
        embedding_service=_ConsolidationEmbedding(),
    )

    build = await job.process_next_window(user_name="ada", project_id="project-1")

    assert build is not None
    assert build.final_episodes == []
    assert store.written is not None
    assert store.written[0] == []
    assert [message["message_id"] for message in store.written[1]] == [10, 11]


@pytest.mark.no_network
async def test_episode_job_uses_one_policy_snapshot_during_provider_await():
    store = _WindowStore()
    llm = _ConcurrentPolicyLLM()
    job = EpisodeJob(
        knowledge_store=store,
        settings=EpisodeSettings(),
        episode_window_size=8,
        llm=llm,
        embedding_service=_ConsolidationEmbedding(),
    )
    llm.job = job

    build = await job.process_next_window(user_name="ada", project_id="project-1")

    assert build is not None
    assert "3600" in llm.system_prompts[0]
    assert build.policy.max_narrative_chars == 4000
    assert build.policy.max_episode_source_messages == 72
    assert build.final_episodes[0].generator_metadata["episode_policy"] == build.policy.metadata()
    assert job.policy.max_narrative_chars == 500
    assert job.policy.max_episode_source_messages == 1
