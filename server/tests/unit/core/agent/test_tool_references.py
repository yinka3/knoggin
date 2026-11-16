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

    localized = localize_agent_tool_result(_run(), "search_project_documents", result)

    item = localized["data"][0]
    assert item["document_id"].startswith("doc_")
    assert "project_id" not in item


@pytest.mark.no_network
def test_a_run_knows_when_it_has_read_web_content():
    run = _run()
    run.record_tool_call("search_project_documents", {"query": "plan"})
    assert run.read_web is False
    run.record_tool_call("read_web_page", {"url": "https://example.com"})
    assert run.read_web is True


@pytest.mark.no_network
def test_linked_episodes_are_openable_handles_in_reads_and_notebook():
    from core.agent.notebook import RunNotebook
    from core.agent.notebook_renderer import render_notebook

    current = '00000000-0000-4000-8000-000000000001'
    older = '00000000-0000-4000-8000-000000000002'
    result = {'data': [{'episode_id': current, 'context_updates': [{'episode_id': older}]}]}
    run = _run()
    localized = localize_agent_tool_result(run, 'read_episode_messages', result)
    handle = localized['data'][0]['context_updates'][0]['episode_id']
    assert run.short_uuid_references[handle] == older
    notebook = RunNotebook()
    notebook.apply('read_episode_messages', result)
    rendered = render_notebook(notebook, local_uuid_references=run.short_uuid_references)
    assert f'contributed to Context updates from: {handle}' in rendered
    assert older not in rendered and current not in rendered
