from datetime import datetime, timezone

import pytest

import core.agent.tools.graph as graph_module
from common.schema.primitives import EntityEpisode, Episode, MessageEpisode
from core.agent.tools.graph import GraphTools
from core.knowledge.entity.profile import EntityProfile


def episode(episode_id: str, entity_id: int = 2) -> Episode:
    return Episode(
        episode_id=episode_id,
        project_id="project-1",
        session_id="session-1",
        summary="Ada chose the episodic memory approach.",
        new_developments=["Episode generation is scheduled."],
        importance=0.8,
        source_message_count=1,
        first_message_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        last_message_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        messages=[
            MessageEpisode(
                message_id=7,
                influence_weight=0.9,
                message_position=0,
            )
        ],
        entities=[
            EntityEpisode(
                entity_id=entity_id,
                is_focus_entity=True,
                prominence_weight=0.9,
                first_seen_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
                last_seen_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        ],
    )


def source_message(message_id: int = 7) -> dict:
    return {
        "message_id": message_id,
        "role": "user",
        "content": "Let's use episodic memory.",
        "timestamp_ms": 1760000000000,
        "influence_weight": 0.9,
        "influence_reason": "Decision stated explicitly.",
        "message_position": 0,
        "attached_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
    }


class EpisodeTool(GraphTools):
    def __init__(self):
        self.active_topics = ["General"]
        self.readable_project_ids = ["project-1"]
        self.search_cfg = {}
        self.user_name = "ada"
        self.project_id = "project-1"
        self.session_id = "session-1"
        self.fallback_calls = []

    async def search_messages(self, query):
        self.fallback_calls.append(query)
        return [{"id": "msg_1", "message": "fallback hit"}]


@pytest.mark.no_network
async def test_episode_check_exact_entity_returns_scoped_episode_evidence():
    class FakeEntities:
        async def get_id(self, name):
            assert name == "Ada"
            return 2

        async def get_profile(self, entity_id):
            assert entity_id == 2
            return EntityProfile(canonical_name="Ada Lovelace")

    class FakeKnowledgeStore:
        def __init__(self):
            self.entity_calls = []

        async def get_episodes_for_entity(self, entity_id, **scope):
            self.entity_calls.append((entity_id, scope))
            return [episode("episode-1", entity_id)]

        async def get_episode_source_messages(self, episode_id, **scope):
            assert episode_id == "episode-1"
            weaker = source_message(8)
            weaker["influence_weight"] = 0.1
            return [source_message(), weaker]

    knowledge_store = FakeKnowledgeStore()
    tool = EpisodeTool()
    tool.entities = FakeEntities()
    tool.knowledge_store = knowledge_store
    tool.episode_retrieval_limit = 2

    result = await tool.episode_check(
        "What did Ada decide?", entity_name="Ada"
    )

    assert knowledge_store.entity_calls == [
        (
            2,
            {
                "user_name": "ada",
                "project_id": "project-1",
                "session_id": "session-1",
                "limit": 2,
            },
        )
    ]
    assert result["resolution"] == "exact"
    match = result["results"][0]
    assert match["entity_name"] == "Ada Lovelace"
    assert match["episodes"][0]["episode_id"] == "episode-1"
    assert match["episodes"][0]["source_message_count"] == 1
    assert match["episodes"][0]["evidence"][0]["attached_at"] == (
        "2025-01-01T00:00:00+00:00"
    )
    assert match["episodes"][0]["evidence"][0]["message"] == (
        "Let's use episodic memory."
    )
    assert len(match["episodes"][0]["evidence"]) == 2


@pytest.mark.no_network
async def test_episode_check_emits_retrieval_and_expansion_metrics(monkeypatch):
    events = []

    async def capture_emit(scope_id, component, event, data):
        events.append((scope_id, component, event, data))

    monkeypatch.setattr(graph_module, "emit", capture_emit)

    class FakeEntities:
        async def get_id(self, name):
            return 2

        async def get_profile(self, entity_id):
            return EntityProfile(canonical_name="Ada")

    class FakeKnowledgeStore:
        async def get_episodes_for_entity(self, entity_id, **scope):
            return [episode("episode-1", entity_id)]

        async def get_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.entities = FakeEntities()
    tool.knowledge_store = FakeKnowledgeStore()

    await tool.episode_check("What did Ada decide?", entity_name="Ada")

    scope_id, component, event, data = events[0]
    assert (scope_id, component, event) == (
        "session-1",
        "agent",
        "episode_retrieval_completed",
    )
    assert data["strategy"] == "exact_entity"
    assert data["episode_count"] == 1
    assert data["focus_episode_count"] == 1
    assert data["expanded_source_message_count"] == 1
    assert data["returned_evidence_count"] == 1
    assert data["retrieval_latency_ms"] >= 0
    assert data["source_message_expansion_latency_ms"] >= 0


@pytest.mark.no_network
async def test_read_recent_episodes_returns_latest_summaries_without_search():
    class FakeKnowledgeStore:
        def __init__(self):
            self.recent_calls = []

        async def get_recent_episodes(self, **scope):
            self.recent_calls.append(scope)
            return [episode("episode-latest"), episode("episode-prior")]

        async def get_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    knowledge_store = FakeKnowledgeStore()
    tool = EpisodeTool()
    tool.knowledge_store = knowledge_store
    tool.episode_retrieval_limit = 2

    result = await tool.read_recent_episodes()

    assert knowledge_store.recent_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
            "limit": 2,
        }
    ]
    assert result["resolution"] == "recent"
    assert [item["episode_id"] for item in result["results"][0]["episodes"]] == [
        "episode-latest",
        "episode-prior",
    ]


@pytest.mark.no_network
async def test_episode_check_vector_candidates_return_episode_context():
    class FakeEntities:
        async def get_id(self, name):
            return None

        async def get_profile(self, entity_id):
            return EntityProfile(canonical_name=f"Entity {entity_id}")

    class FakeEmbeddingService:
        async def encode_single(self, text):
            assert text == "Unknown entity"
            return [0.1, 0.2]

    class FakeKnowledgeStore:
        async def search_entities_by_embedding(self, *args, **kwargs):
            assert kwargs["visible_project_ids"] == ["project-1"]
            return [(3, 0.87)]

        async def get_episodes_for_entity(self, entity_id, **scope):
            return [episode("episode-3", entity_id)]

        async def get_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.entities = FakeEntities()
    tool.embedding_service = FakeEmbeddingService()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check("what matters?", entity_name="Unknown entity")

    assert result["resolution"] == "vector"
    assert result["results"][0]["entity_name"] == "Entity 3"
    assert result["results"][0]["episodes"][0]["episode_id"] == "episode-3"


@pytest.mark.no_network
async def test_episode_check_searches_episodes_before_raw_message_fallback():
    class FakeKnowledgeStore:
        async def search_episodes(self, query, **scope):
            assert query == "What changed in the memory design?"
            return [episode("episode-question")]

        async def get_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check("What changed in the memory design?")

    assert tool.fallback_calls == []
    assert result["resolution"] == "question"
    assert result["results"][0]["episodes"][0]["episode_id"] == "episode-question"


@pytest.mark.no_network
async def test_episode_check_uses_semantic_episode_matches_before_lexical_search():
    class FakeEmbeddingService:
        async def encode_single(self, query):
            assert query == "How will we find related memories?"
            return [0.1] * 1024

    class FakeKnowledgeStore:
        async def search_episodes_by_embedding(self, embedding, **scope):
            assert embedding == [0.1] * 1024
            assert scope["session_id"] == "session-1"
            return [(episode("episode-semantic"), 0.91)]

        async def search_episodes(self, query, **scope):
            raise AssertionError("lexical search should not run after a semantic hit")

        async def get_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.embedding_service = FakeEmbeddingService()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check("How will we find related memories?")

    assert result["resolution"] == "semantic"
    semantic_episode = result["results"][0]["episodes"][0]
    assert semantic_episode["episode_id"] == "episode-semantic"
    assert semantic_episode["similarity"] == 0.91


@pytest.mark.no_network
async def test_read_episode_returns_all_scoped_source_messages():
    class FakeKnowledgeStore:
        async def get_episode(self, episode_id, **scope):
            assert episode_id == "episode-1"
            return episode(episode_id)

        async def get_episode_source_messages(self, episode_id, **scope):
            return [source_message(7), source_message(8)]

    tool = EpisodeTool()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.read_episode("episode-1")

    assert [message["id"] for message in result] == [7, 8]
    assert all(message["context"][0]["is_hit"] for message in result)
