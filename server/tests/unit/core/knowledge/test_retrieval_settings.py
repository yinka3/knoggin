"""Internal retrieval captures validated policy, never web-provider settings."""

from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from pydantic import ValidationError

from common.schema.settings import SearchSettings
from core.knowledge.retrieval import KnowledgeRetrieval


def _retrieval(settings=None):
    return KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=["project-1"],
        user_name="ada",
        entities=SimpleNamespace(get_profile=AsyncMock(return_value=object())),
        embedding_service=SimpleNamespace(
            encode_query=AsyncMock(return_value=[0.1]),
            rerank=AsyncMock(return_value=[0.9, 0.8]),
        ),
        knowledge_store=SimpleNamespace(
            get_visible_session_ids=AsyncMock(return_value=["session-1"]),
            search_messages_fts=AsyncMock(return_value=[]),
            search_messages_semantic=AsyncMock(return_value=[]),
            search_entity=AsyncMock(return_value=[]),
            get_recent_activity=AsyncMock(return_value=[]),
        ),
        search_settings=settings,
    )


@pytest.mark.no_network
@pytest.mark.parametrize("configured", [False, True])
async def test_retrieval_defaults_and_captured_limits(configured):
    settings = SearchSettings(
        fts_limit=13, semantic_message_limit=17, semantic_message_threshold=0.6,
        rerank_candidates=2, default_message_limit=3, default_entity_limit=4,
        default_activity_hours=72,
    ) if configured else SearchSettings()
    expected = settings.model_copy()
    retrieval = _retrieval(settings if configured else None)
    # The caller owns its model; mutation must not change a loaded runtime.
    for field in SearchSettings.model_fields:
        setattr(settings, field, 1)

    await retrieval._search_messages("query", session_id="session-1", k=3)
    store = retrieval.knowledge_store
    assert store.search_messages_fts.await_args.kwargs["limit"] == expected.fts_limit
    assert store.search_messages_semantic.await_args.kwargs["limit"] == expected.semantic_message_limit
    assert store.search_messages_semantic.await_args.kwargs["threshold"] == expected.semantic_message_threshold

    retrieval._search_messages = AsyncMock(return_value=[])
    await retrieval.search_messages("query", session_id="session-1")
    assert retrieval._search_messages.await_args.kwargs["k"] == expected.default_message_limit
    await retrieval.search_entities("query")
    assert store.search_entity.await_args.kwargs["limit"] == expected.default_entity_limit
    await retrieval.get_recent_activity(2, session_id="session-1")
    assert store.get_recent_activity.await_args.kwargs["hours"] == expected.default_activity_hours

    await retrieval.search_messages("query", session_id="session-1", limit=6)
    assert retrieval._search_messages.await_args.kwargs["k"] == 6
    await retrieval.search_entities("query", limit=7)
    assert store.search_entity.await_args.kwargs["limit"] == 7
    await retrieval.get_recent_activity(2, session_id="session-1", hours=8)
    assert store.get_recent_activity.await_args.kwargs["hours"] == 8


@pytest.mark.no_network
async def test_rerank_bound_is_captured_and_channels_have_independent_limits():
    settings = SearchSettings(fts_limit=3, semantic_message_limit=9, rerank_candidates=2)
    retrieval = _retrieval(settings)
    settings.rerank_candidates = 1
    retrieval.knowledge_store.search_messages_fts.return_value = [
        (1, 1.0, "session-1"), (2, 0.9, "session-1"), (3, 0.8, "session-1"),
    ]
    retrieval._hydrate_evidence = AsyncMock(return_value=[
        {"id": "msg_1", "session_id": "session-1", "message": "first"},
        {"id": "msg_2", "session_id": "session-1", "message": "second"},
    ])
    results = await retrieval._search_messages("query", session_id="session-1", k=3)
    assert len(results) == 2
    retrieval.embedding_service.rerank.assert_awaited_once_with("query", ["first", "second"])
    assert retrieval.knowledge_store.search_messages_semantic.await_args.kwargs["limit"] == 9


@pytest.mark.parametrize("field", list(SearchSettings.model_fields))
def test_retrieval_revalidates_mutated_models(field):
    settings = SearchSettings()
    setattr(settings, field, -1)
    with pytest.raises(ValidationError):
        _retrieval(settings)


def test_retrieval_rejects_loose_provider_dictionaries():
    with pytest.raises(TypeError, match="must be SearchSettings"):
        _retrieval({"fts_limit": 10, "provider": "web", "api_key": "secret"})
    with pytest.raises(ValidationError):
        SearchSettings.model_validate({"provider": "web", "api_key": "secret"})
