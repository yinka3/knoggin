from types import SimpleNamespace

import pytest

from common.exceptions import ToolExecutionError
from common.schema.source.references import SourceConsulted
from core.agent.tools.sources import AnswerSourceTools


class Harness(AnswerSourceTools):
    def __init__(self, history, sources_by_message):
        self.user_name, self.project_id, self.session_id = "ada", "project-1", "session-1"
        self.agent_run = SimpleNamespace(history=history)
        self.calls = []

        async def get_message_source_refs(message_id, **scope):
            self.calls.append((message_id, scope))
            return sources_by_message.get(message_id, [])

        async def get_document_info(*, document_id):
            return {"relative_path": "notes/plan.md"}

        self.knowledge_store = SimpleNamespace(get_message_source_refs=get_message_source_refs)
        self.document_service = SimpleNamespace(get_document_info=get_document_info)


def _source(**fields):
    return SourceConsulted(
        locator={"kind": "text_lines", "start_line": 1, "end_line": 2},
        excerpt="the cited passage", source_status="available",
        contributing_message_id=12, **fields,
    )


@pytest.mark.no_network
async def test_the_agent_can_list_the_sources_of_an_earlier_answer():
    history = [
        {"message_id": 11, "role": "user", "content": "What is the plan?"},
        {"message_id": 12, "role": "assistant", "content": "The plan is local-only."},
        {"message_id": 13, "role": "user", "content": "And the price?"},
        {"message_id": 14, "role": "assistant", "content": "About $20."},
    ]
    tools = Harness(history, {
        12: [_source(source_kind="text_document", document_id="doc-1", source_project_id="project-1")],
        14: [_source(source_kind="web_page", canonical_url="https://example.com/pricing")],
    })

    latest = await tools.list_answer_sources()
    earlier = await tools.list_answer_sources(answers_back=2)

    assert latest["answer"] == "About $20."
    assert latest["sources"][0]["url"] == "https://example.com/pricing"
    assert earlier["sources"][0]["document"] == "notes/plan.md"
    assert tools.calls[0] == (14, {"user_name": "ada", "project_id": "project-1", "session_id": "session-1"})
    with pytest.raises(ToolExecutionError):
        await tools.list_answer_sources(answers_back=3)
