import pytest
from pydantic import ValidationError

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
