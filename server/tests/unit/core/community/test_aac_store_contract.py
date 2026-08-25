from __future__ import annotations

from typing import Any

import pytest

from core.community.aac_store import AACStore


class RecordingPostgres:
    def __init__(self, *, rows: list[list[dict[str, Any]]] | None = None) -> None:
        self.write_calls: list[tuple[str, dict[str, Any]]] = []
        self.read_calls: list[tuple[str, dict[str, Any]]] = []
        self.rows = list(rows or [])

    async def execute(self, query: str, params: dict[str, Any]) -> int:
        self.write_calls.append((query, params))
        return 1

    async def fetch_all(
        self,
        query: str,
        params: dict[str, Any],
    ) -> list[dict[str, Any]]:
        self.read_calls.append((query, params))
        return self.rows.pop(0) if self.rows else []


@pytest.mark.storage
@pytest.mark.no_network
async def test_aac_store_persists_user_level_discussion_and_timeline():
    postgres = RecordingPostgres()
    store = AACStore(postgres)

    await store.create_discussion(
        discussion_id="discussion-1",
        user_name="ada",
        topic="Review contradictory evidence",
        token_budget=50_000,
    )
    timeline_id = await store.append_timeline(
        discussion_id="discussion-1",
        user_name="ada",
        kind="agent_message",
        agent_id="agent-1",
        content="The source dates conflict.",
    )
    await store.finish_discussion(
        discussion_id="discussion-1",
        user_name="ada",
        status="completed",
        tokens_used=51_200,
    )

    assert timeline_id
    create_query, create_params = postgres.write_calls[0]
    assert "INSERT INTO public.aac_discussions" in create_query
    assert create_params == {
        "discussion_id": "discussion-1",
        "user_name": "ada",
        "topic": "Review contradictory evidence",
        "token_budget": 50_000,
    }
    timeline_query, timeline_params = postgres.write_calls[1]
    assert "INSERT INTO public.aac_timeline" in timeline_query
    assert "FROM public.aac_discussions" in timeline_query
    assert timeline_params["agent_id"] == "agent-1"
    finish_query, finish_params = postgres.write_calls[2]
    assert "status = 'active'" in finish_query
    assert finish_params["tokens_used"] == 51_200


@pytest.mark.storage
@pytest.mark.no_network
async def test_aac_store_keeps_insights_independent_and_scopes_agent_reads():
    postgres = RecordingPostgres(
        rows=[
            [
                {
                    "insight_id": "insight-1",
                    "author_agent_id": "agent-1",
                    "visibility": "shared",
                    "content": "Prefer contemporaneous evidence.",
                }
            ]
        ]
    )
    store = AACStore(postgres)

    insight_id = await store.create_insight(
        user_name="ada",
        discussion_id="discussion-1",
        author_agent_id="agent-1",
        visibility="shared",
        content="Prefer contemporaneous evidence.",
    )
    rows = await store.search_insights(
        user_name="ada",
        viewer_agent_id="agent-2",
        query="evidence",
    )

    insight_query, insight_params = postgres.write_calls[0]
    assert "INSERT INTO public.aac_insights" in insight_query
    assert "WHERE %(discussion_id)s IS NULL OR EXISTS" in insight_query
    assert insight_params["insight_id"] == insight_id
    read_query, read_params = postgres.read_calls[0]
    assert "visibility = 'shared' OR author_agent_id = %(viewer_agent_id)s" in read_query
    assert read_params == {
        "user_name": "ada",
        "viewer_agent_id": "agent-2",
        "query": "evidence",
        "limit": 20,
    }
    assert rows[0]["insight_id"] == "insight-1"


@pytest.mark.storage
@pytest.mark.no_network
async def test_aac_store_allows_only_other_agents_to_vote_on_shared_insights():
    postgres = RecordingPostgres()
    store = AACStore(postgres)

    await store.cast_insight_vote(
        insight_id="insight-1",
        user_name="ada",
        voter_agent_id="agent-2",
        vote="up",
        reason="It reflects the cited material.",
    )
    removed = await store.remove_insight_vote(
        insight_id="insight-1",
        user_name="ada",
        voter_agent_id="agent-2",
    )

    vote_query, vote_params = postgres.write_calls[0]
    assert "author_agent_id <> %(voter_agent_id)s" in vote_query
    assert "ON CONFLICT (insight_id, voter_agent_id) DO UPDATE" in vote_query
    assert vote_params["reason"] == "It reflects the cited material."
    assert "DELETE FROM public.aac_insight_votes" in postgres.write_calls[1][0]
    assert removed is True


@pytest.mark.storage
@pytest.mark.no_network
@pytest.mark.parametrize(
    ("operation", "kwargs", "message"),
    [
        (
            "append_timeline",
            {
                "discussion_id": "discussion-1",
                "user_name": "ada",
                "kind": "unknown",
                "content": "Nope",
            },
            "timeline kind",
        ),
        (
            "create_insight",
            {
                "user_name": "ada",
                "author_agent_id": "agent-1",
                "visibility": "hidden",
                "content": "Nope",
            },
            "visibility",
        ),
        (
            "cast_insight_vote",
            {
                "insight_id": "insight-1",
                "user_name": "ada",
                "voter_agent_id": "agent-2",
                "vote": "maybe",
                "reason": "Nope",
            },
            "vote",
        ),
    ],
)
async def test_aac_store_rejects_invalid_domain_state_before_database_access(
    operation,
    kwargs,
    message,
):
    postgres = RecordingPostgres()
    store = AACStore(postgres)

    with pytest.raises(ValueError, match=message):
        await getattr(store, operation)(**kwargs)

    assert postgres.write_calls == []
