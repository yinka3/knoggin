import asyncio
from uuid import uuid4

import pytest

from core.community.aac_store import AACStore

pytestmark = [pytest.mark.requires_postgres, pytest.mark.storage, pytest.mark.no_network]


async def test_timeline_pages_preserve_insertion_order_and_scope(real_postgres_client):
    store = AACStore(real_postgres_client)
    user = f"aac-{uuid4()}"
    discussion = str(uuid4())
    await store.create_discussion(discussion_id=discussion, user_name=user, topic="pages", token_budget=100)
    args = dict(discussion_id=discussion, user_name=user, kind="system_event")
    # Deliberately reverse lexical ID order and tie timestamps: neither orders pages.
    ids = [f"{prefix}-{uuid4()}" for prefix in ("z", "y", "x")]
    for index, event_id in enumerate(ids):
        await store.append_timeline(**args, content=str(index), timeline_id=event_id)
    await real_postgres_client.execute(
        "UPDATE public.aac_timeline SET created_at = '2026-01-01' WHERE discussion_id = %s",
        (discussion,),
    )
    first = await store.list_timeline(discussion_id=discussion, user_name=user, limit=2)
    assert [event["timeline_id"] for event in first] == ids[:2]
    await store.append_timeline(**args, content="1", timeline_id=ids[1])
    last_id = await store.append_timeline(**args, content="3")
    second = await store.list_timeline(discussion_id=discussion, user_name=user,
                                       limit=2, after_sequence=first[-1]["event_sequence"])
    assert [event["timeline_id"] for event in second] == [ids[2], last_id]
    assert await store.list_timeline(discussion_id=discussion, user_name=user,
                                     after_sequence=second[-1]["event_sequence"]) == []
    assert await store.list_timeline(discussion_id=discussion, user_name="other") == []


async def test_insight_visibility_and_concurrent_votes_are_scoped(real_postgres_client):
    store = AACStore(real_postgres_client)
    user = f"aac-{uuid4()}"
    args = dict(user_name=user, author_agent_id="author", content="evidence")
    shared = await store.create_insight(**args)
    private = await store.create_insight(**args, visibility="private")
    assert {r["insight_id"] for r in await store.search_insights(user_name=user, viewer_agent_id="reader")} == {shared}
    assert {r["insight_id"] for r in await store.search_insights(user_name=user, viewer_agent_id="author")} == {shared, private}
    assert {r["insight_id"] for r in await store.list_user_insights(user_name=user)} == {shared, private}
    assert await store.search_insights(user_name="other", viewer_agent_id="author") == []
    for insight_id, scoped_user, voter in ((shared, user, "author"), (private, user, "reader"), (shared, "other", "reader")):
        with pytest.raises(ValueError, match="another agent"):
            await store.cast_insight_vote(insight_id=insight_id, user_name=scoped_user, voter_agent_id=voter, vote="up")
    await asyncio.gather(*(
        store.cast_insight_vote(insight_id=shared, user_name=user,
                                voter_agent_id="reader", vote=vote, reason=vote)
        for vote in ("up", "down", "up", "down")
    ))
    votes = await store.list_insight_votes(insight_id=shared, user_name=user)
    assert len(votes) == 1
    assert votes[0]["voter_agent_id"] == "reader"
    assert votes[0]["vote"] == votes[0]["reason"]
    assert await store.list_insight_votes(insight_id=shared, user_name="other") == []
