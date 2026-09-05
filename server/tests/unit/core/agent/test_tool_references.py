from types import SimpleNamespace

import pytest

from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits
from core.agent.tool_references import localize_agent_tool_result


def _run() -> AgentRun:
    return AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query="Find the document",
        run_id="run-1",
        agent=AgentIdentity(
            config=SimpleNamespace(id="agent-1"),
            name="Stella",
            persona="",
        ),
        limits=AgentRunLimits(),
    )


@pytest.mark.no_network
def test_document_result_localization_keeps_absent_folder_reference_null():
    result = {
        "data": [
            {
                "document_id": "00000000-0000-4000-8000-000000000001",
                "project_id": "project-1",
            }
        ]
    }

    localized = localize_agent_tool_result(_run(), "search_documents", result)

    item = localized["data"][0]
    assert item["document_id"].startswith("doc_")
    assert "project_id" not in item
