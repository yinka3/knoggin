from uuid import uuid4

import pytest

from core.community.aac_store import AACStore


@pytest.mark.requires_postgres
@pytest.mark.storage
@pytest.mark.no_network
async def test_aac_terminal_retry_preserves_durable_outcome(real_postgres_client):
    store = AACStore(real_postgres_client)
    discussion_id = str(uuid4())
    await store.create_discussion(discussion_id=discussion_id, user_name="ada", topic="AAC retry", token_budget=100)
    outcome = dict(discussion_id=discussion_id, user_name="ada", status="completed", end_reason="token_budget", tokens_used=10)
    await store.finish_discussion(**outcome)
    first = await real_postgres_client.fetch_one(
        "SELECT status, end_reason, tokens_used, ended_at FROM public.aac_discussions WHERE discussion_id = %s",
        (discussion_id,),
    )
    await store.finish_discussion(**outcome)
    with pytest.raises(ValueError, match="conflicts"):
        await store.finish_discussion(**{**outcome, "status": "failed", "end_reason": "failed"})
    second = await real_postgres_client.fetch_one(
        "SELECT status, end_reason, tokens_used, ended_at FROM public.aac_discussions WHERE discussion_id = %s",
        (discussion_id,),
    )
    assert first == second
