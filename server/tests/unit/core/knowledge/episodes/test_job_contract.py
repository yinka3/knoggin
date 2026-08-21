import pytest

from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.job import EpisodeJob
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
        settings=EpisodeSettings(max_message_count=12),
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
