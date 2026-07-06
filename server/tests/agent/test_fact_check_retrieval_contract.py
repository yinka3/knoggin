from datetime import datetime, timezone

import pytest

from common.schema.primitives import FactRecord
from core.agent.tools.graph import GraphTools
from core.knowledge.entity.profile import EntityProfile


def fact(
    fact_id,
    content,
    entity_id=2,
    embedding=None,
    invalid_at=None,
):
    return FactRecord(
        id=fact_id,
        source_entity_id=entity_id,
        content=content,
        valid_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        invalid_at=invalid_at,
        confidence=0.9,
        embedding=embedding or [],
        source_msg_id=7,
        source_user_name="ada",
        source_session_id="session-1",
    )


class FactCheckTool(GraphTools):
    def __init__(self):
        self.active_topics = ["General"]
        self.readable_project_ids = ["project-1"]
        self.search_cfg = {}
        self.user_name = "ada"
        self.session_id = "session-1"
        self.fallback_calls = []

    async def search_messages(self, query):
        self.fallback_calls.append(query)
        return [{"id": "msg_1", "message": "fallback hit"}]


@pytest.mark.no_network
async def test_fact_check_exact_entity_returns_all_facts_with_canonical_name():
    class FakeEntities:
        async def get_id(self, name):
            assert name == "Ada"
            return 2

        async def get_profile(self, entity_id):
            assert entity_id == 2
            return EntityProfile(canonical_name="Ada Lovelace")

    class FakeKnowledgeStore:
        def __init__(self):
            self.calls = []

        async def get_facts_for_entity(
            self, entity_id, *, visible_project_ids, active_only=True
        ):
            self.calls.append((entity_id, active_only))
            return [
                fact("fact-active", "Ada writes algorithms", entity_id=entity_id),
                fact(
                    "fact-inactive",
                    "Ada used an old toolkit",
                    entity_id=entity_id,
                    invalid_at=datetime(2026, 1, 2, tzinfo=timezone.utc),
                ),
            ]

    knowledge_store = FakeKnowledgeStore()
    tool = FactCheckTool()
    tool.entities = FakeEntities()
    tool.knowledge_store = knowledge_store

    result = await tool.fact_check("Ada", "what does Ada know?")

    assert knowledge_store.calls == [(2, False)]
    assert result["resolution"] == "exact"
    assert result["results"][0]["entity_name"] == "Ada Lovelace"
    assert [f["id"] for f in result["results"][0]["facts"]] == [
        "fact-active",
        "fact-inactive",
    ]
    assert "embedding" not in result["results"][0]["facts"][0]


@pytest.mark.no_network
async def test_fact_check_vector_candidates_use_visible_projects_and_profiles():
    class FakeEntities:
        async def get_id(self, name):
            return None

        async def get_profile(self, entity_id):
            return EntityProfile(canonical_name=f"Entity {entity_id}")

    class FakeEmbeddingService:
        def __init__(self):
            self.calls = []

        async def encode_single(self, text):
            self.calls.append(text)
            return [0.1, 0.2]

    class FakeKnowledgeStore:
        def __init__(self):
            self.search_calls = []

        async def search_entities_by_embedding(
            self, embedding, limit, score_threshold, visible_project_ids
        ):
            self.search_calls.append(
                (embedding, limit, score_threshold, visible_project_ids)
            )
            return [(3, 0.87)]

        async def get_facts_for_entities(
            self, entity_ids, *, visible_project_ids, active_only=True
        ):
            assert entity_ids == [3]
            assert active_only is False
            return {3: [fact("fact-3", "Vector fact", entity_id=3)]}

    embedding = FakeEmbeddingService()
    knowledge_store = FakeKnowledgeStore()
    tool = FactCheckTool()
    tool.entities = FakeEntities()
    tool.embedding_service = embedding
    tool.knowledge_store = knowledge_store

    result = await tool.fact_check("Unknown entity", "what matters?")

    assert embedding.calls == ["Unknown entity"]
    assert knowledge_store.search_calls == [([0.1, 0.2], 5, 0.69, ["project-1"])]
    assert result == {
        "resolution": "vector",
        "results": [
            {
                "entity_name": "Entity 3",
                "similarity": 0.87,
                "facts": [fact("fact-3", "Vector fact", entity_id=3).to_dict()],
            }
        ],
    }


@pytest.mark.no_network
async def test_fact_check_falls_back_to_message_search_when_no_entity_candidates():
    class FakeEntities:
        async def get_id(self, name):
            return None

    class FakeEmbeddingService:
        async def encode_single(self, text):
            return [0.1, 0.2]

    class FakeKnowledgeStore:
        async def search_entities_by_embedding(
            self, embedding, limit, score_threshold, visible_project_ids
        ):
            return []

    tool = FactCheckTool()
    tool.entities = FakeEntities()
    tool.embedding_service = FakeEmbeddingService()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.fact_check("Unknown", "fallback query")

    assert tool.fallback_calls == ["fallback query"]
    assert result == {
        "resolution": "fallback",
        "results": [{"id": "msg_1", "message": "fallback hit"}],
    }


@pytest.mark.no_network
async def test_fact_check_trims_large_vector_fact_sets_by_query_similarity():
    class FakeEntities:
        async def get_id(self, name):
            return None

        async def get_profile(self, entity_id):
            return EntityProfile(canonical_name="Large Entity")

    class FakeEmbeddingService:
        def __init__(self):
            self.calls = []

        async def encode_single(self, text):
            self.calls.append(text)
            if text == "Large Entity":
                return [0.0, 1.0]
            return [1.0, 0.0]

    class FakeKnowledgeStore:
        async def search_entities_by_embedding(
            self, embedding, limit, score_threshold, visible_project_ids
        ):
            return [(4, 0.91)]

        async def get_facts_for_entities(
            self, entity_ids, *, visible_project_ids, active_only=True
        ):
            facts = []
            for index in range(1001):
                facts.append(
                    fact(
                        f"fact-{index}",
                        f"Fact {index}",
                        entity_id=4,
                        embedding=[1.0 - (index / 2000.0), index / 2000.0],
                    )
                )
            facts.append(fact("fact-no-embedding", "No embedding", entity_id=4))
            return {4: facts}

    embedding = FakeEmbeddingService()
    tool = FactCheckTool()
    tool.entities = FakeEntities()
    tool.embedding_service = embedding
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.fact_check("Large Entity", "query for strongest facts")

    returned_facts = result["results"][0]["facts"]
    assert embedding.calls == ["Large Entity", "query for strongest facts"]
    assert len(returned_facts) == 500
    assert returned_facts[0]["id"] == "fact-0"
    assert returned_facts[-1]["id"] == "fact-499"
    assert "fact-500" not in {item["id"] for item in returned_facts}
    assert "fact-no-embedding" not in {item["id"] for item in returned_facts}
