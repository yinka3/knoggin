import pytest

from common.schema.contracts import FactMergeResult
from common.schema.primitives import Fact
from knoggin_server.knowledge.services.fact_resolution import FactResolutionUtils


class RecordingGraph:
    def __init__(self):
        self.create_calls = []

    async def create_facts_batch(
        self, entity_id, facts, user_name=None, session_id=None, project_id=None
    ):
        self.create_calls.append(
            {
                "entity_id": entity_id,
                "facts": list(facts),
                "user_name": user_name,
                "session_id": session_id,
                "project_id": project_id,
            }
        )
        return len(facts)


class FakeEmbedding:
    async def encode_single(self, content):
        return [float(len(content)), 0.1]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_fact_resolution_uses_source_session_map_for_project_context():
    graph = RecordingGraph()
    merge_result = FactMergeResult(
        new_contents=[
            Fact(content="Alice uses Linear.", source_msg_id=1),
            Fact(content="Bob uses Notion.", source_msg_id=2),
        ]
    )

    summary = await FactResolutionUtils.apply_fact_changes(
        101,
        merge_result,
        existing_facts=[],
        valid_msg_ids={1, 2},
        session_id="project-1",
        graph_client=graph,
        embedding_service=FakeEmbedding(),
        llm=object(),
        user_name="ada",
        project_id="project-1",
        source_session_by_msg_id={1: "session-a", 2: "session-b"},
    )

    call = graph.create_calls[0]
    facts = call["facts"]

    assert call["session_id"] is None
    assert call["project_id"] == "project-1"
    assert [fact.source_session_id for fact in facts] == ["session-a", "session-b"]
    assert [fact.source_user_name for fact in facts] == ["ada", "ada"]
    assert [fact.source_msg_id for fact in summary.created_facts] == [1, 2]
