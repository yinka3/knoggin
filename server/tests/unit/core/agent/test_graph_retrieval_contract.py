import pytest

from core.agent.formatters import format_graph_results
from core.agent.tools.graph import GraphTools


class GraphRetrievalTool(GraphTools):
    def __init__(self):
        self.active_topics = ["Identity"]
        self.readable_project_ids = ["project-1"]
        self.search_cfg = {"default_activity_hours": 72}
        self.user_name = "ada"
        self.session_id = "session-1"
        self.resolved = {}
        self.hydrated_refs = []

    async def _resolve_entity_name(self, entity):
        return self.resolved.get(entity)

    async def _hydrate_evidence(self, refs):
        self.hydrated_refs.append(refs)
        return [
            {"id": ref.get("message_id", ref), "message": "evidence"}
            for ref in refs
        ]


@pytest.mark.no_network
async def test_recent_activity_uses_default_window_scope_and_hydrates_evidence():
    class FakeKnowledgeStore:
        def __init__(self):
            self.calls = []

        async def get_recent_activity(
            self, canonical, active_topics, hours, visible_project_ids
        ):
            self.calls.append((canonical, active_topics, hours, visible_project_ids))
            return [
                {
                    "entity": "Grace",
                    "evidence_refs": [{"message_id": 7}],
                    "time": 123,
                }
            ]

    knowledge_store = FakeKnowledgeStore()
    tool = GraphRetrievalTool()
    tool.knowledge_store = knowledge_store
    tool.resolved = {"Ada": "Ada Lovelace"}

    result = await tool.get_recent_activity("Ada", hours=0)

    assert knowledge_store.calls == [("Ada Lovelace", ["Identity"], 72, ["project-1"])]
    assert tool.hydrated_refs == [[{"message_id": 7}]]
    assert result == [
        {
            "entity": "Grace",
            "time": 123,
            "evidence": [{"id": 7, "message": "evidence"}],
        }
    ]


@pytest.mark.no_network
async def test_recent_activity_unknown_entity_returns_clean_error_without_graph_call():
    class FakeKnowledgeStore:
        async def get_recent_activity(self, *args, **kwargs):
            raise AssertionError("graph should not be queried for unknown entity")

    tool = GraphRetrievalTool()
    tool.knowledge_store = FakeKnowledgeStore()

    assert await tool.get_recent_activity("Missing") == [
        {"error": "Entity not found: 'Missing'"}
    ]


@pytest.mark.no_network
async def test_get_connections_reports_hidden_inactive_topics_without_evidence_leak():
    class FakeKnowledgeStore:
        def __init__(self):
            self.calls = []

        async def get_related_entities(
            self, names, active_topics=None, limit=25, visible_project_ids=None
        ):
            self.calls.append((names, active_topics, limit, visible_project_ids))
            if active_topics == ["Identity"]:
                return []
            return [
                {
                    "target": "Dormant",
                    "evidence_refs": [{"message_id": 7}],
                    "context": "inactive detail",
                },
                {
                    "target": "Archived",
                    "evidence_refs": [{"message_id": 8}],
                    "context": "another inactive detail",
                },
            ]

    knowledge_store = FakeKnowledgeStore()
    tool = GraphRetrievalTool()
    tool.knowledge_store = knowledge_store
    tool.resolved = {"Ada": "Ada Lovelace"}

    result = await tool.get_connections("Ada")

    assert knowledge_store.calls == [
        (["Ada Lovelace"], ["Identity"], 50, ["project-1"]),
        (["Ada Lovelace"], None, 25, ["project-1"]),
    ]
    assert result == [
        {
            "hidden": True,
            "count": 2,
            "message": "2 connection(s) exist through inactive topics",
        }
    ]
    assert tool.hydrated_refs == []


@pytest.mark.no_network
async def test_find_path_locks_inactive_shortcut_steps_and_strips_evidence():
    class FakeKnowledgeStore:
        def __init__(self):
            self.calls = []

        async def find_path_filtered(
            self,
            source,
            target,
            active_topics=None,
            max_depth=4,
            visible_project_ids=None,
        ):
            self.calls.append(
                (source, target, active_topics, max_depth, visible_project_ids)
            )
            if active_topics == ["Identity"]:
                return [], True
            return (
                [
                    {
                        "step": 0,
                        "entity_a": "Ada",
                        "entity_b": "Dormant",
                        "topic_a": "Identity",
                        "topic_b": "Archive",
                        "evidence_refs": [{"message_id": 7}],
                    },
                    {
                        "step": 1,
                        "entity_a": "Dormant",
                        "entity_b": "Grace",
                        "topic_a": "Archive",
                        "topic_b": "Identity",
                        "evidence_refs": [{"message_id": 8}],
                    },
                ],
                False,
            )

    knowledge_store = FakeKnowledgeStore()
    tool = GraphRetrievalTool()
    tool.knowledge_store = knowledge_store
    tool.resolved = {"Ada": "Ada", "Grace": "Grace"}

    result = await tool.find_path("Ada", "Grace")

    assert knowledge_store.calls == [
        ("Ada", "Grace", ["Identity"], 4, ["project-1"]),
        ("Ada", "Grace", None, 4, ["project-1"]),
    ]
    assert result == [
        {
            "step": 0,
            "entity_a": "Ada",
            "entity_b": "Dormant",
            "topic_a": "Identity",
            "topic_b": "Archive",
            "status": "LOCKED",
            "locked_reason": "Inactive topic(s): Archive",
            "evidence": [],
        },
        {
            "step": 1,
            "entity_a": "Dormant",
            "entity_b": "Grace",
            "topic_a": "Archive",
            "topic_b": "Identity",
            "status": "LOCKED",
            "locked_reason": "Inactive topic(s): Archive",
            "evidence": [],
        },
    ]
    assert tool.hydrated_refs == []


@pytest.mark.no_network
def test_graph_result_format_states_that_relationships_are_observed_evidence():
    result = format_graph_results(
        [
            {
                "source": "Ade",
                "target": "Acme",
                "observed_relationship_label": "works at",
                "evidence_message_count": 2,
                "observation_count": 3,
                "first_observed": 100,
                "last_observed": 200,
            }
        ]
    )

    assert "Observed: Ade --works at--> Acme" in result
    assert "not a current-state claim" in result
    assert "2 messages, 3 observations" in result
