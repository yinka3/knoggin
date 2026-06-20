import pytest

from knoggin_server.agent.tools.search import SearchTools


class EmptyFileRAG:
    def __init__(self):
        self.session_ids = []

    async def list_files(self, *, session_id=None, visibility_scope=None):
        self.session_ids.append(session_id)
        return []


@pytest.mark.no_network
async def test_search_files_reports_project_empty_state():
    tools = SearchTools()
    tools.file_rag = EmptyFileRAG()
    tools.session_id = "session-1"

    assert await tools.search_files("alpha") == [
        {"error": "No indexed files available in this project"}
    ]
    assert tools.file_rag.session_ids == ["session-1"]
