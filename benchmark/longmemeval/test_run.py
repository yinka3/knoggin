import asyncio

from core.agent import prompt_context
from run import _record_episode_results, _remembering_render


class Retrieval:
    async def episode_check(self, query, **_):
        return {
            "resolution": "semantic",
            "results": [{"query": query, "episodes": [{"episode_id": "e1"}, {"episode_id": "e2"}]}],
        }

    async def read_recent_episodes(self, **_):
        return {"resolution": "recent", "results": [{"episodes": [{"episode_id": "e3"}]}]}


def test_records_which_episodes_each_search_returned():
    retrieval, seen = Retrieval(), []
    _record_episode_results(retrieval, seen)

    asyncio.run(retrieval.episode_check("q", session_id="s"))
    asyncio.run(retrieval.read_recent_episodes(session_id="s"))

    assert seen == [
        {"call": "episode_check", "resolution": "semantic", "episode_ids": ["e1", "e2"]},
        {"call": "read_recent_episodes", "resolution": "recent", "episode_ids": ["e3"]},
    ]


def test_remembers_the_last_notebook_the_agent_read(monkeypatch):
    monkeypatch.setattr(prompt_context, "render_notebook", lambda notebook: f"page {notebook}")
    pages = []

    render = _remembering_render(pages)
    render(1)
    render(2)

    assert pages[-1] == "page 2"
