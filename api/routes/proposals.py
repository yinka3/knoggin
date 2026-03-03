import json
import asyncio
from fastapi import APIRouter, Depends, HTTPException

from api.state import AppState
from api.deps import get_app_state
from shared.redisclient import RedisKeys
from api.commands.handlers.merge import _build_merge_snapshot, _execute_undo, UNDO_TTL_SECONDS
from loguru import logger
from datetime import datetime, timezone
from jobs.jobs_utils import cosine_similarity

router = APIRouter()

@router.get("/{session_id}/merges")
async def list_merge_proposals(session_id: str, state: AppState = Depends(get_app_state)):
    redis = state.resources.redis
    proposals_key = RedisKeys.merge_proposals(state.user_name, session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        return {"proposals": [], "message": "No pending merge proposals"}

    proposals = []
    for idx, raw in enumerate(raw_proposals, start=1):
        p = json.loads(raw)
        proposals.append({
            "index": idx,
            "primary_id": p.get("primary_id"),
            "secondary_id": p.get("secondary_id"),
            "primary_name": p["primary_name"],
            "secondary_name": p["secondary_name"],
            "score": p.get("llm_score", 0),
            "created_at": p.get("created_at")
        })

    return {"proposals": proposals}


@router.post("/{session_id}/merges/{index}/approve")
async def approve_merge_proposal(session_id: str, index: int, state: AppState = Depends(get_app_state)):
    redis = state.resources.redis
    store = state.resources.store
    proposals_key = RedisKeys.merge_proposals(state.user_name, session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        raise HTTPException(status_code=404, detail="No pending merge proposals.")

    if index < 1 or index > len(raw_proposals):
        raise HTTPException(status_code=400, detail=f"Invalid index. There are only {len(raw_proposals)} proposals.")

    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)

    primary_id = proposal["primary_id"]
    secondary_id = proposal["secondary_id"]

    resolver = None
    if session_id in state.active_sessions:
        resolver = state.active_sessions[session_id].ent_resolver

    # Build full snapshot before merge
    snapshot = await _build_merge_snapshot(
        store, resolver, primary_id, secondary_id,
        proposal["primary_name"], proposal["secondary_name"]
    )

    undo_key = RedisKeys.merge_undo(session_id, primary_id, secondary_id)
    await redis.setex(undo_key, UNDO_TTL_SECONDS, json.dumps(snapshot))

    loop = asyncio.get_running_loop()
    success = await loop.run_in_executor(None, store.merge_entities, primary_id, secondary_id)

    if not success:
        await redis.delete(undo_key)
        raise HTTPException(status_code=500, detail="Merge failed in database.")

    if resolver:
        resolver.merge_into(primary_id, secondary_id)

    # Duplicate fact cleanup
    try:
        all_facts = await loop.run_in_executor(
            None, store.get_facts_for_entity, primary_id, False
        )
        if all_facts and len(all_facts) > 1:
            active = [f for f in all_facts if f.invalid_at is None and f.embedding]
            to_invalidate = []
            seen = set()

            for i, fact_a in enumerate(active):
                if fact_a.id in seen:
                    continue
                for j, fact_b in enumerate(active):
                    if j <= i or fact_b.id in seen:
                        continue
                    sim = cosine_similarity(fact_a.embedding, fact_b.embedding)
                    if sim >= 0.96:
                        to_invalidate.append(fact_b.id)
                        seen.add(fact_b.id)

            if to_invalidate:
                now = datetime.now(timezone.utc)
                for fact_id in to_invalidate:
                    try:
                        await loop.run_in_executor(None, store.invalidate_fact, fact_id, now)
                    except Exception:
                        pass
                logger.info(f"Invalidated {len(to_invalidate)} duplicate facts after manual merge")
    except Exception as e:
        logger.warning(f"Duplicate fact cleanup failed: {e}")

    # Remove proposal
    await redis.lrem(proposals_key, 1, raw)

    return {
        "merged": True,
        "primary": proposal["primary_name"],
        "secondary": proposal["secondary_name"],
        "undo_available_for": f"{UNDO_TTL_SECONDS // 3600} hours"
    }


@router.post("/{session_id}/merges/{index}/reject")
async def reject_merge_proposal(session_id: str, index: int, state: AppState = Depends(get_app_state)):
    redis = state.resources.redis
    proposals_key = RedisKeys.merge_proposals(state.user_name, session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        raise HTTPException(status_code=404, detail="No pending merge proposals.")

    if index < 1 or index > len(raw_proposals):
        raise HTTPException(status_code=400, detail=f"Invalid index. There are only {len(raw_proposals)} proposals.")

    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)

    await redis.lrem(proposals_key, 1, raw)

    return {
        "rejected": True,
        "primary": proposal["primary_name"],
        "secondary": proposal["secondary_name"]
    }
