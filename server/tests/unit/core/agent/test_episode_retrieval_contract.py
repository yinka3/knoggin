from datetime import date, datetime, timezone
from types import SimpleNamespace

import pytest

import core.knowledge.retrieval as retrieval_module
from common.schema.episode.models import (
    EntityEpisode,
    Episode,
    EpisodeCard,
    EpisodeEvent,
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
                name="Ada",
                first_seen_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
                last_seen_at=datetime(2025, 1, 1, tzinfo=timezone.utc),
            )
        ],
    )


def source_message(message_id: int = 7) -> dict:
    return {
        "message_id": message_id,
        "session_id": "session-1",
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
        )
        self.fallback_calls = []

    async def search_messages(self, query, *, session_id, limit=None):
        self.fallback_calls.append(query)
        return [{"id": "msg_1", "message": "fallback hit"}]


class EpisodeStore:
    async def get_episode_context_links(self, episode_ids, **scope):
        return {eid: {'context_updates': [], 'context_updated_by': []} for eid in episode_ids}

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

    assert result["results"][0]["episodes"][0]["entities"][0]["name"] == "Ada"
    assert knowledge_store.entity_calls == [
        (
            [2],
            {
                "user_name": "ada",
                "project_id": "project-1",
                "limit": 10,
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
            "limit": 3,
            "offset": 0,
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
        async def search_project_episodes_by_events(self, _query, **_scope):
            return []

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
    assert result["resolution"] == "lexical"
    assert result["results"][0]["episodes"][0]["episode_id"] == "episode-question"


@pytest.mark.no_network
async def test_episode_check_combines_semantic_and_lexical_matches():
    class FakeEmbeddingService:
        async def encode_query(self, query):
            assert query == "How will we find related memories?"
            return [0.1] * 768

    class FakeKnowledgeStore(EpisodeStore):
        async def search_project_episodes_by_embedding(self, embedding, **scope):
            assert embedding == [0.1] * 768
            return [(episode_card("episode-semantic"), 0.91)]

        async def search_project_episodes_by_events(self, _query, **_scope):
            return []

        async def search_project_episodes(self, query, **scope):
            return [episode_card("episode-lexical")]

        async def get_project_episode_source_messages(self, episode_id, **scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.embedding_service = FakeEmbeddingService()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check(
        "How will we find related memories?", session_id="session-1"
    )

    assert result["resolution"] == "hybrid"
    episodes = result["results"][0]["episodes"]
    assert {episode["episode_id"] for episode in episodes} == {
        "episode-semantic",
        "episode-lexical",
    }
    semantic_episode = next(
        episode for episode in episodes if episode["episode_id"] == "episode-semantic"
    )
    assert semantic_episode["similarity"] == 0.91


@pytest.mark.no_network
async def test_episode_check_finds_an_episode_through_its_events():
    """A passing mention lives in an event even when the summary buries it."""

    buried = episode_card("episode-europe").model_copy(
        update={
            "events": [
                EpisodeEvent(
                    what="Ada has tried three of Emma's recipes",
                    start_date=date(2023, 5, 29),
                    end_date=date(2023, 5, 29),
                    precision="day",
                    source_message_ids=[7],
                )
            ]
        }
    )

    class FakeKnowledgeStore(EpisodeStore):
        async def search_project_episodes(self, _query, **_scope):
            return [episode_card("episode-older")]

        async def search_project_episodes_by_events(self, query, **_scope):
            assert query == "Emma recipes tried"
            return [buried]

    tool = EpisodeTool()
    tool.embedding_service = None
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check("Emma recipes tried", session_id="session-1")

    episodes = {e["episode_id"]: e for e in result["results"][0]["episodes"]}
    assert set(episodes) == {"episode-older", "episode-europe"}
    assert episodes["episode-europe"]["events"] == [
        {
            "what": "Ada has tried three of Emma's recipes",
            "kind": "other",
            "when": "2023-05-29",
            "date_text": None,
        }
    ]


@pytest.mark.no_network
async def test_episode_check_keeps_lexical_results_when_semantic_search_fails():
    class FailingEmbeddingService:
        async def encode_query(self, _query):
            raise RuntimeError("model unavailable")

    class FakeKnowledgeStore(EpisodeStore):
        async def search_project_episodes_by_events(self, _query, **_scope):
            return []

        async def search_project_episodes(self, _query, **_scope):
            return [episode_card("episode-lexical")]

        async def get_project_episode_source_messages(self, _episode_id, **_scope):
            return [source_message()]

    tool = EpisodeTool()
    tool.embedding_service = FailingEmbeddingService()
    tool.knowledge_store = FakeKnowledgeStore()

    result = await tool.episode_check("memory design", session_id="session-1")

    assert result["resolution"] == "lexical"
    assert result["results"][0]["episodes"][0]["episode_id"] == "episode-lexical"
    assert tool.fallback_calls == []


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

    assert [message["id"] for message in result] == ["msg_7", "msg_8"]
    assert all(message["session_id"] == "session-1" for message in result)
    assert all(message["context"][0]["is_hit"] for message in result)


@pytest.mark.no_network
async def test_episode_and_message_hydration_share_durable_message_identity():
    class FakeKnowledgeStore(EpisodeStore):
        async def get_project_episode(self, episode_id, **_scope):
            return episode(episode_id)

        async def get_project_episode_source_messages(self, _episode_id, **_scope):
            return [source_message(7)]

        async def get_messages_by_ids(self, _message_ids, **_scope):
            return [
                {
                    "id": 7,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "role": "user",
                    "content": "Let's use episodic memory.",
                    "timestamp": 1760000000000,
                }
            ]

    tool = EpisodeTool()
    tool.knowledge_store = FakeKnowledgeStore()

    episode_messages = await tool.read_episode(
        "episode-1", session_id="session-1"
    )
    hydrated_messages = await tool._hydrate_evidence(
        [
            {
                "user_name": "ada",
                "session_id": "session-1",
                "message_id": 7,
            }
        ],
        session_id="session-1",
    )

    identity_fields = ("id", "user_name", "session_id", "timestamp")
    assert {
        key: episode_messages[0][key] for key in identity_fields
    } == {key: hydrated_messages[0][key] for key in identity_fields}


async def _dated_search(*, after=None, before=None, episodes=()):
    calls = []

    class Store(EpisodeStore):
        async def search_project_episodes_by_events(self, _query, **_scope):
            return []

        async def search_project_episodes(self, query, **scope):
            calls.append(("lexical", scope))
            return list(episodes)

        async def search_project_episodes_by_embedding(self, embedding, **scope):
            calls.append(("semantic", scope))
            return []

        async def get_project_episode_source_refs(self, episode_id, **scope):
            return []

    class Embedding:
        async def encode_query(self, query):
            return [0.1]

    tool = EpisodeTool()
    tool.knowledge_store = Store()
    tool.embedding_service = Embedding()
    result = await tool.episode_check(
        "what happened", session_id="session-1", after=after, before=before
    )
    return result, dict(calls)


async def test_a_bare_before_date_covers_that_whole_day():
    _, calls = await _dated_search(after="2023-05-20", before="2023-05-20")

    assert calls["lexical"]["after"] == datetime(2023, 5, 20, tzinfo=timezone.utc)
    assert calls["lexical"]["before"] == datetime(
        2023, 5, 20, 23, 59, 59, 999999, tzinfo=timezone.utc
    )
    assert calls["semantic"]["before"] == calls["lexical"]["before"]


async def test_similarity_threshold_applies_only_without_a_date_range():
    _, undated = await _dated_search()
    _, dated = await _dated_search(after="2023-05-20")

    assert undated["semantic"]["score_threshold"] == 0.5
    assert dated["semantic"]["score_threshold"] == 0.0


async def test_an_empty_date_range_does_not_fall_back_to_undated_messages():
    result, _ = await _dated_search(after="2023-05-20")

    assert result == {"resolution": "no_episodes_in_range", "results": []}


@pytest.mark.parametrize(
    "after,before",
    [("last week", None), (None, "soon"), ("2023-05-21", "2023-05-20")],
)
async def test_bad_date_ranges_are_rejected(after, before):
    with pytest.raises(ValueError):
        await _dated_search(after=after, before=before)


async def test_episode_search_can_refuse_the_raw_message_fallback():
    class Store(EpisodeStore):
        async def search_project_episodes_by_events(self, _query, **_scope):
            return []

        async def search_project_episodes(self, query, **scope):
            return []

    class RawSearchMustNotRun(EpisodeTool):
        async def search_messages(self, *args, **kwargs):
            raise AssertionError("raw message search ran")

    tool = RawSearchMustNotRun()
    tool.knowledge_store = Store()
    tool.embedding_service = None

    result = await tool.episode_check("anything", session_id="session-1", raw_fallback=False)

    assert result == {"resolution": "no_episodes", "results": []}


class _ExpansionStore(EpisodeStore):
    def __init__(self, hits):
        self.hits = hits
        self.searched = []

    async def search_project_episodes_by_events(self, _query, **_scope):
        return []

    async def search_project_episodes(self, query, **scope):
        self.searched.append(query)
        return [episode_card(episode_id) for episode_id in self.hits.get(query, [])]

    async def get_project_episode_source_refs(self, episode_id, **scope):
        return []


async def test_alternate_queries_merge_into_one_ranked_result():
    tool = EpisodeTool()
    tool.embedding_service = None
    tool.knowledge_store = _ExpansionStore(
        {"kitchen": ["a"], "air fryer": ["b", "a"], "blender": ["c"]}
    )

    result = await tool.episode_check(
        "kitchen", session_id="session-1", alternate_queries=["air fryer", "blender"]
    )

    found = [item["episode_id"] for item in result["results"][0]["episodes"]]
    # Found by two wordings, so it outranks those found by one.
    assert found[0] == "a"
    assert set(found) == {"a", "b", "c"}
    assert len(result["results"]) == 1


async def test_alternate_queries_skip_repeats_and_stop_at_the_configured_count():
    tool = EpisodeTool()
    tool.embedding_service = None
    tool.knowledge_store = _ExpansionStore({"q": ["a"]})

    await tool.episode_check(
        "q",
        session_id="session-1",
        alternate_queries=["Q ", "two", "", "three", "four", "five"],
    )

    assert tool.knowledge_store.searched == ["q", "two", "three", "four"]


async def test_browsing_episodes_by_date_reports_the_next_page():
    calls = []

    class Store(EpisodeStore):
        async def get_recent_project_episodes(self, **scope):
            calls.append(scope)
            return [episode_card(episode_id) for episode_id in ("a", "b", "c")]

        async def get_project_episode_source_refs(self, episode_id, **scope):
            return []

    tool = EpisodeTool()
    tool.knowledge_store = Store()

    result = await tool.read_recent_episodes(
        session_id="session-1", limit=2, after="2023-05-20", before="2023-05-20", offset=4
    )

    assert calls[0]["limit"] == 3 and calls[0]["offset"] == 4
    assert calls[0]["after"] == datetime(2023, 5, 20, tzinfo=timezone.utc)
    assert [item["episode_id"] for item in result["results"][0]["episodes"]] == ["a", "b"]
    assert result["has_more"] is True and result["next_offset"] == 6


async def test_episode_search_limit_shrinks_the_page_but_never_grows_it():
    ranked = list("abcdefghijkl")
    tool = EpisodeTool()
    tool.embedding_service = None
    tool.knowledge_store = _ExpansionStore({"art": ranked})

    small = await tool.episode_check("art", session_id="session-1", limit=3)
    large = await tool.episode_check("art", session_id="session-1", limit=50)

    assert [e["episode_id"] for e in small["results"][0]["episodes"]] == ranked[:3]
    assert small["next_offset"] == 3
    assert [e["episode_id"] for e in large["results"][0]["episodes"]] == ranked[:10]


async def test_episode_search_pages_through_ranked_matches():
    tool = EpisodeTool()
    tool.embedding_service = None
    ranked = list("abcdefghijkl")
    tool.knowledge_store = _ExpansionStore({"art": ranked})

    first = await tool.episode_check("art", session_id="session-1")
    second = await tool.episode_check("art", session_id="session-1", offset=10)

    assert [e["episode_id"] for e in first["results"][0]["episodes"]] == ranked[:10]
    assert first["has_more"] is True and first["next_offset"] == 10
    assert [e["episode_id"] for e in second["results"][0]["episodes"]] == ["k", "l"]
    assert second["has_more"] is False and second["next_offset"] is None


async def test_a_page_past_the_last_match_does_not_fall_back_to_raw_messages():
    tool = EpisodeTool()
    tool.embedding_service = None
    tool.knowledge_store = _ExpansionStore({"art": ["a"]})

    result = await tool.episode_check("art", session_id="session-1", offset=5)

    assert result == {"resolution": "no_more_episodes", "results": [], "has_more": False, "next_offset": None}
    assert tool.fallback_calls == []


async def test_listing_events_pages_by_date_and_hides_internal_ids():
    calls = []

    class Store(EpisodeStore):
        async def list_project_events(self, **scope):
            calls.append(scope)
            return [
                {
                    "episode_id": f"ep-{i}",
                    "what": f"Event {i}",
                    "kind": "trip",
                    "start_date": date(2023, 3, i),
                    "end_date": date(2023, 3, i),
                    "precision": "day",
                    "date_text": "last weekend",
                    "source_message_ids": [i],
                    "in_range": "inside",
                }
                for i in (1, 2, 3)
            ]

    tool = EpisodeTool()
    tool.knowledge_store = Store()
    tool._search_settings = SimpleNamespace(event_page_size=2)

    result = await tool.list_events(
        session_id="session-1", after="2023-03-01", before="2023-03-31", kind="Trip", offset=4
    )

    assert calls[0]["limit"] == 3 and calls[0]["offset"] == 4 and calls[0]["kind"] == "Trip"
    assert calls[0]["before"] == datetime(2023, 3, 31, 23, 59, 59, 999999, tzinfo=timezone.utc)
    events = result["results"][0]["events"]
    assert [e["what"] for e in events] == ["Event 1", "Event 2"]
    assert events[0] == {
        "episode_id": "ep-1",
        "what": "Event 1",
        "kind": "trip",
        "when": "2023-03-01",
        "precision": "day",
        "date_text": "last weekend",
        "in_range": "inside",
    }
    assert result["has_more"] is True and result["next_offset"] == 6
