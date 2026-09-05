from types import SimpleNamespace

import pytest

from core.agent.formatters import format_graph_results
from core.knowledge.retrieval import KnowledgeRetrieval


class _Postgres:
    async def fetch_all(self, _query, _params):
        return [{"session_id": "session-1"}]


def _retrieval(store):
    class Entities:
        async def get_profile(self, entity_id):
            if entity_id in {2, 3}:
                return SimpleNamespace(canonical_name={2: "Ade", 3: "Acme"}[entity_id])
            return None

    return KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=Entities(),
        embedding_service=SimpleNamespace(),
        knowledge_store=store,
        postgres=_Postgres(),
        search_config={"default_activity_hours": 72},
    )


@pytest.mark.no_network
async def test_recent_activity_uses_stable_id_and_message_evidence():
    class Store:
        def __init__(self):
            self.calls = []

        async def get_recent_activity(self, entity_id, **kwargs):
            self.calls.append((entity_id, kwargs))
            return [
                {
                    "entity_id": 2,
                    "entity": "Ade",
                    "project_id": "project-1",
                    "evidence_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 7,
                        }
                    ],
                    "time": 123,
                }
            ]

        async def get_messages_by_ids(self, message_ids, **kwargs):
            assert message_ids == [7]
            assert kwargs["visible_project_ids"] == ["project-1"]
            return [
                {
                    "id": 7,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "content": "Ade discussed Acme",
                    "timestamp": 1_700_000_000_000,
                }
            ]

    store = Store()
    result = await _retrieval(store).get_recent_activity(2, session_id="session-1", hours=0)

    assert store.calls == [
        (
            2,
            {
                "hours": 72,
                "visible_project_ids": ["project-1"],
            },
        )
    ]
    assert result[0]["evidence"][0]["id"] == "msg_7"


@pytest.mark.no_network
async def test_connections_keep_stored_direction_when_selected_from_target():
    class Store:
        async def get_related_entities(self, entity_ids, **kwargs):
            assert entity_ids == [3]
            assert kwargs == {"limit": 50, "visible_project_ids": ["project-1"]}
            return [
                {
                    "project_id": "project-1",
                    "relationship_id": "project-1:2:3:works_at",
                    "source_entity_id": 2,
                    "target_entity_id": 3,
                    "source": "Ade",
                    "target": "Acme",
                    "relationship_type": "works_at",
                    "symmetric": False,
                    "evidence_refs": [],
                }
            ]

    result = await _retrieval(Store()).get_connections(3, session_id="session-1")

    assert result == [
        {
            "project_id": "project-1",
            "relationship_id": "project-1:2:3:works_at",
            "source_entity_id": 2,
            "target_entity_id": 3,
            "source": "Ade",
            "target": "Acme",
            "relationship_type": "works_at",
            "symmetric": False,
            "evidence": [],
        }
    ]


@pytest.mark.no_network
async def test_path_returns_canonical_direction_and_project_attribution():
    class Store:
        async def find_path(self, entity_a_id, entity_b_id, **kwargs):
            assert (entity_a_id, entity_b_id) == (3, 2)
            assert kwargs == {"max_depth": 4, "visible_project_ids": ["project-1"]}
            return [
                {
                    "step": 0,
                    "entity_a_id": 3,
                    "entity_b_id": 2,
                    "relationship_id": "project-1:2:3:works_at",
                    "project_id": "project-1",
                    "source_entity_id": 2,
                    "target_entity_id": 3,
                    "source": "Ade",
                    "target": "Acme",
                    "relationship_type": "works_at",
                    "symmetric": False,
                    "relationship_semantics": "observed_evidence",
                    "evidence_refs": [],
                }
            ]

    result = await _retrieval(Store()).find_path(3, 2, session_id="session-1")

    assert result[0]["source"] == "Ade"
    assert result[0]["target"] == "Acme"
    assert result[0]["project_id"] == "project-1"
    assert result[0]["evidence"] == []


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
