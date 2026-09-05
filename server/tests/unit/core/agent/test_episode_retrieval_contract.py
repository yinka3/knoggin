from datetime import datetime, timezone

import pytest

import core.knowledge.retrieval as retrieval_module
from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    EpisodeCard,
    MessageEpisode,
)
from core.knowledge.entity.profile import EntityProfile
from core.knowledge.retrieval import KnowledgeRetrieval


def episode(episode_id: str, entity_id: int = 2) -> Episode:
    return Episode(
        episode_id=episode_id,
        project_id="project-1",
        session_id="session-1",
        summary="Ada chose the episodic memory approach.",
        new_developments=["Episode generation is scheduled."],
        source_message_count=1,
        first_message_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        last_message_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
        messages=[
            MessageEpisode(
                message_id=7,
                session_id="session-1",
                message_position=0,
            )
        ],
        entities=[
            EntityEpisode(
                entity_id=entity_id,
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
        "message_position": 0,
        "attached_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
    }


def episode_card(episode_id: str, entity_id: int = 2) -> EpisodeCard:
    full = episode(episode_id, entity_id)
    return EpisodeCard(
        episode_id=full.episode_id,
        project_id=full.project_id,
        summary=full.summary,
        new_developments=full.new_developments,
        source_message_count=full.source_message_count,
        first_message_at=full.first_message_at,
        last_message_at=full.last_message_at,
        entities=full.entities,
    )


class EpisodeTool(KnowledgeRetrieval):
    def __init__(self):
        super().__init__(
            project_id="project-1",
            readable_project_ids=["project-1"],
            user_name="ada",
            entities=object(),
            embedding_service=None,
            knowledge_store=object(),
            postgres=object(),
        )
        self.fallback_calls = []

    async def search_messages(self, query, *, session_id, limit=None):
        self.fallback_calls.append(query)
        return [{"id": "msg_1", "message": "fallback hit"}]


class EpisodeStore:
    async def get_project_episode_source_refs(self, _episode_id, **_scope):
        return []


@pytest.mark.no_network
async def test_episode_check_exact_entity_returns_scoped_episode_card():
    class FakeEntities:
        async def get_id(self, name):
            assert name == "Ada"
            return 2

        async def get_profile(self, entity_id):
            assert entity_id == 2
            return EntityProfile(canonical_name="Ada Lovelace")

    class FakeKnowledgeStore(EpisodeStore):
        def __init__(self):
            self.entity_calls = []
            self.source_message_calls = 0

        async def get_project_episodes_for_entities(self, entity_ids, **scope):
            self.entity_calls.append((entity_ids, scope))
            return [episode_card("episode-1", entity_ids[0])]

        async def get_project_episode_source_messages(self, episode_id, **scope):
            self.source_message_calls += 1
            return [source_message(), source_message(8)]

    knowledge_store = FakeKnowledgeStore()
    tool = EpisodeTool()
    tool.entities = FakeEntities()
    tool.knowledge_store = knowledge_store

    result = await tool.episode_check(
        "What did Ada decide?", session_id="session-1", entity_id=2
    )

    assert knowledge_store.entity_calls == [
        (
            [2],
            {
                "user_name": "ada",
                "project_id": "project-1",
                "limit": 5,
                "visible_project_ids": ["project-1"],
            },
        )
    ]
    assert result["resolution"] == "exact"
    match = result["results"][0]
    assert match["entity_name"] == "Ada Lovelace"
    assert match["episodes"][0]["episode_id"] == "episode-1"
    assert match["episodes"][0]["source_message_count"] == 1
    assert match["episodes"][0]["evidence"] == []
    assert knowledge_store.source_message_calls == 0


@pytest.mark.no_network
async def test_episode_serialization_includes_separate_sources_consulted():
    class FakeKnowledgeStore(EpisodeStore):
        async def get_project_episodes_for_entities(self, entity_ids, **scope):
            return [episode_card("episode-1", entity_ids[0])]

        async def get_project_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

        async def get_project_episode_source_refs(self, episode_id, **scope):
            assert episode_id == "episode-1"
            return [
                {
                    "source_kind": "web_search_result",
                    "locator": {
                        "kind": "search_result",
                        "provider": "serper",
                        "query": "release",
                        "rank": 1,
                    },
                    "excerpt": "Provider snippet.",
                    "canonical_url": "https://example.test/release",
                    "source_status": "search_result_snippet",
                    "contributing_message_id": 7,
                }
            ]

    class FakeEntities:
        async def get_id(self, name):
            return 2

        async def get_profile(self, entity_id):
            return EntityProfile(canonical_name="Ada")

    tool = EpisodeTool()
    tool.entities = FakeEntities()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check(
        "What did Ada decide?", session_id="session-1", entity_id=2
    )

    consulted = result["results"][0]["episodes"][0]["sources_consulted"]
    assert consulted == [
        {
            "source_kind": "web_search_result",
            "locator": {
                "kind": "search_result",
                "provider": "serper",
                "query": "release",
                "rank": 1,
            },
            "excerpt": "Provider snippet.",
            "canonical_url": "https://example.test/release",
            "source_status": "search_result_snippet",
            "contributing_message_id": 7,
        }
    ]


@pytest.mark.no_network
async def test_episode_check_emits_retrieval_without_source_expansion(monkeypatch):
    events = []

    async def capture_emit(scope_id, component, event, data):
        events.append((scope_id, component, event, data))

    monkeypatch.setattr(retrieval_module, "emit", capture_emit)

    class FakeEntities:
        async def get_id(self, name):
            return 2

        async def get_profile(self, entity_id):
            return EntityProfile(canonical_name="Ada")

    class FakeKnowledgeStore(EpisodeStore):
        async def get_project_episodes_for_entities(self, entity_ids, **scope):
            return [episode_card("episode-1", entity_ids[0])]

        async def get_project_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.entities = FakeEntities()
    tool.knowledge_store = FakeKnowledgeStore()

    await tool.episode_check(
        "What did Ada decide?", session_id="session-1", entity_id=2
    )

    scope_id, component, event, data = events[0]
    assert (scope_id, component, event) == (
        "session-1",
        "agent",
        "episode_retrieval_completed",
    )
    assert data["strategy"] == "exact_entity"
    assert data["episode_count"] == 1
    assert data["matched_entity_episode_count"] == 1
    assert data["source_message_expansion_skipped_count"] == 1
    assert "expanded_source_message_count" not in data
    assert "returned_evidence_count" not in data
    assert data["retrieval_latency_ms"] >= 0


@pytest.mark.no_network
async def test_read_recent_episodes_returns_latest_summaries_without_search():
    class FakeKnowledgeStore(EpisodeStore):
        def __init__(self):
            self.recent_calls = []
            self.source_message_calls = 0

        async def get_recent_project_episodes(self, **scope):
            self.recent_calls.append(scope)
            return [episode_card("episode-latest"), episode_card("episode-prior")]

        async def get_project_episode_source_messages(self, episode_id, **scope):
            self.source_message_calls += 1
            return [source_message()]

    knowledge_store = FakeKnowledgeStore()
    tool = EpisodeTool()
    tool.knowledge_store = knowledge_store

    result = await tool.read_recent_episodes(session_id="session-1")

    assert knowledge_store.recent_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "limit": 2,
            "visible_project_ids": ["project-1"],
        }
    ]
    assert result["resolution"] == "recent"
    assert [item["episode_id"] for item in result["results"][0]["episodes"]] == [
        "episode-latest",
        "episode-prior",
    ]
    assert knowledge_store.source_message_calls == 0


@pytest.mark.no_network
async def test_episode_check_searches_episodes_before_raw_message_fallback():
    class FakeKnowledgeStore(EpisodeStore):
        async def search_project_episodes(self, query, **scope):
            assert query == "What changed in the memory design?"
            return [episode_card("episode-question")]

        async def get_project_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check(
        "What changed in the memory design?", session_id="session-1"
    )

    assert tool.fallback_calls == []
    assert result["resolution"] == "question"
    assert result["results"][0]["episodes"][0]["episode_id"] == "episode-question"


@pytest.mark.no_network
async def test_episode_check_uses_semantic_episode_matches_before_lexical_search():
    class FakeEmbeddingService:
        async def encode_single(self, query):
            assert query == "How will we find related memories?"
            return [0.1] * 1024

    class FakeKnowledgeStore(EpisodeStore):
        async def search_project_episodes_by_embedding(self, embedding, **scope):
            assert embedding == [0.1] * 1024
            return [(episode_card("episode-semantic"), 0.91)]

        async def search_project_episodes(self, query, **scope):
            raise AssertionError("lexical search should not run after a semantic hit")

        async def get_project_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.embedding_service = FakeEmbeddingService()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check(
        "How will we find related memories?", session_id="session-1"
    )

    assert result["resolution"] == "semantic"
    semantic_episode = result["results"][0]["episodes"][0]
    assert semantic_episode["episode_id"] == "episode-semantic"
    assert semantic_episode["similarity"] == 0.91


@pytest.mark.no_network
async def test_read_episode_returns_all_scoped_source_messages():
    class FakeKnowledgeStore(EpisodeStore):
        async def get_project_episode(self, episode_id, **scope):
            assert episode_id == "episode-1"
            return episode(episode_id)

        async def get_project_episode_source_messages(self, episode_id, **scope):
            return [source_message(7), source_message(8)]

    tool = EpisodeTool()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.read_episode("episode-1", session_id="session-1")

    assert [message["id"] for message in result] == [7, 8]
    assert all(message["context"][0]["is_hit"] for message in result)
