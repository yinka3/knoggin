import json
import asyncio
import numpy as np
from functools import partial
from fastapi import APIRouter, Depends, HTTPException
from pydantic import BaseModel
from api.state import AppState
from api.deps import get_app_state
from common.utils.events import emit
from common.infra.redis import RedisKeys
from loguru import logger
from datetime import datetime, timezone
from common.schema.dtypes import Fact

router = APIRouter()

UNDO_TTL_SECONDS = 7200
class MergeApprovalRequest(BaseModel):
    primary_id: int
    secondary_id: int

class MergeUndoRequest(BaseModel):
    primary_id: int
    secondary_id: int

@router.get("/{session_id}/merges")
async def list_merge_proposals(session_id: str, state: AppState = Depends(get_app_state)):
    redis = state.resources.redis
    proposals_key = RedisKeys.merge_proposals(state.user_name, session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        return {"proposals": [], "message": "No pending merge proposals"}

    proposals = []
    for idx, raw in enumerate(raw_proposals, start=1):
        try:
            p = json.loads(raw)
        except json.JSONDecodeError:
            continue
        proposals.append({
            "index": idx,
            "primary_id": p.get("primary_id"),
            "secondary_id": p.get("secondary_id"),
            "primary_name": p.get("primary_name", "Unknown"),
            "secondary_name": p.get("secondary_name", "Unknown"),
            "score": p.get("llm_score", 0),
            "created_at": p.get("created_at")
        })

    return {"proposals": proposals}


@router.post("/{session_id}/merges/{index}/approve")
async def approve_merge_proposal(
    session_id: str, 
    index: int, 
    body: MergeApprovalRequest,
    state: AppState = Depends(get_app_state)
):
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

    if proposal["primary_id"] != body.primary_id or proposal["secondary_id"] != body.secondary_id:
        raise HTTPException(
            status_code=409, 
            detail="Proposal at this index no longer matches. The list may have changed. Please refresh and retry."
        )

    primary_id = proposal["primary_id"]
    secondary_id = proposal["secondary_id"]

    resolver = None
    if session_id in state.active_sessions:
        resolver = state.active_sessions[session_id].ent_resolver

    snapshot = await _build_merge_snapshot(
        store, resolver, primary_id, secondary_id,
        proposal["primary_name"], proposal["secondary_name"]
    )

    undo_key = RedisKeys.merge_undo(session_id, primary_id, secondary_id)
    await redis.setex(undo_key, UNDO_TTL_SECONDS, json.dumps(snapshot))

    success = await store.merge_entities(primary_id, secondary_id)

    if not success:
        await redis.delete(undo_key)
        raise HTTPException(status_code=500, detail="Merge failed in database.")

    if resolver:
        resolver.merge_into(primary_id, secondary_id)

    try:
        all_facts = await store.get_facts_for_entity(primary_id, False)
        if all_facts and len(all_facts) > 1:
            active = [f for f in all_facts if f.invalid_at is None and f.embedding]
            
            if len(active) > 1:
                embs = np.array([f.embedding for f in active])
                norms = np.linalg.norm(embs, axis=1, keepdims=True)
                norms[norms == 0] = 1e-10
                norm_embs = embs / norms
                sim_matrix = np.dot(norm_embs, norm_embs.T)

                to_invalidate = []
                seen = set()

                for i in range(len(active)):
                    if active[i].id in seen:
                        continue
                    matches = np.where(sim_matrix[i, i+1:] >= 0.96)[0] + (i + 1)
                    for j in matches:
                        if active[j].id not in seen:
                            to_invalidate.append(active[j].id)
                            seen.add(active[j].id)

                if to_invalidate:
                    now = datetime.now(timezone.utc)
                    for fact_id in to_invalidate:
                        try:
                            await store.invalidate_fact(fact_id, now)
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
async def reject_merge_proposal(
    session_id: str, 
    index: int, 
    body: MergeApprovalRequest,
    state: AppState = Depends(get_app_state)
):
    redis = state.resources.redis
    proposals_key = RedisKeys.merge_proposals(state.user_name, session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        raise HTTPException(status_code=404, detail="No pending merge proposals.")

    if index < 1 or index > len(raw_proposals):
        raise HTTPException(status_code=400, detail=f"Invalid index. There are only {len(raw_proposals)} proposals.")

    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)

    if proposal["primary_id"] != body.primary_id or proposal["secondary_id"] != body.secondary_id:
        raise HTTPException(
            status_code=409,
            detail="Proposal at this index no longer matches. The list may have changed. Please refresh and retry."
        )

    await redis.lrem(proposals_key, 1, raw)

    return {
        "rejected": True,
        "primary": proposal["primary_name"],
        "secondary": proposal["secondary_name"]
    }


@router.post("/{session_id}/merges/undo")
async def undo_merge(
    session_id: str,
    body: MergeUndoRequest,
    state: AppState = Depends(get_app_state)
):
    redis = state.resources.redis
    store = state.resources.store

    undo_key = RedisKeys.merge_undo(session_id, body.primary_id, body.secondary_id)
    raw = await redis.get(undo_key)

    if not raw:
        raise HTTPException(
            status_code=404,
            detail="No undo available for this merge. The window may have expired."
        )

    snapshot = json.loads(raw)

    resolver = None
    if session_id in state.active_sessions:
        resolver = state.active_sessions[session_id].ent_resolver

    if not resolver:
        raise HTTPException(
            status_code=400,
            detail="Session must be active to undo a merge."
        )

    result = await _execute_undo(store, resolver, redis, snapshot, session_id)

    await redis.delete(undo_key)

    return {
        "undone": True,
        "restored_entity": snapshot["entity"]["canonical_name"],
        "new_entity_id": result["new_entity_id"],
        "facts_restored": result["facts_restored"],
        "relationships_restored": result["relationships_restored"],
        "transferred_edges_removed": result["transferred_edges_removed"]
    }


async def _build_merge_snapshot(
    store, resolver, primary_id: int, secondary_id: int,
    primary_name: str, secondary_name: str
) -> dict:
    """Capture full pre-merge state for undo capability."""
    entity_data = await store.get_entity_by_id(secondary_id)
    embedding = resolver.get_embedding_for_id(secondary_id)

    facts_raw = await store.get_facts_for_entity(secondary_id, False)
    facts = []
    if facts_raw:
        for f in facts_raw:
            facts.append({
                "id": f.id,
                "content": f.content,
                "valid_at": f.valid_at.isoformat(),
                "invalid_at": f.invalid_at.isoformat() if f.invalid_at else None,
                "embedding": f.embedding,
                "source_msg_id": f.source_msg_id,
                "confidence": f.confidence,
                "source": getattr(f, "source", "user")
            })

    sec_rels = await store.get_entity_relationships(secondary_id)
    pri_rels = await store.get_entity_relationships(primary_id)
    parents = await store.get_parent_entities(secondary_id)
    children = await store.get_child_entities(secondary_id)

    return {
        "primary_id": primary_id,
        "primary_name": primary_name,
        "secondary_id": secondary_id,
        "secondary_name": secondary_name,
        "merged_at": datetime.now(timezone.utc).isoformat(),

        "entity": {
            "canonical_name": entity_data["canonical_name"] if entity_data else secondary_name,
            "aliases": (entity_data.get("aliases") or []) if entity_data else [],
            "type": (entity_data.get("type") or "unknown") if entity_data else "unknown",
            "topic": (entity_data.get("topic") or "General") if entity_data else "General",
            "embedding": embedding or [],
            "session_id": (entity_data.get("session_id")) if entity_data else None
        },

        "facts": facts,

        "relationships": [
            {
                "neighbor_id": r["neighbor_id"],
                "neighbor_name": r["neighbor_name"],
                "weight": r.get("weight", 1),
                "message_ids": r.get("message_ids") or [],
                "context": r.get("context"),
                "confidence": r.get("confidence", 1.0)
            }
            for r in (sec_rels or [])
        ],

        "primary_relationships": [
            {"neighbor_id": r["neighbor_id"], "weight": r.get("weight", 1)}
            for r in (pri_rels or [])
        ],

        "parents": [{"id": p["id"], "name": p["canonical_name"]} for p in (parents or [])],
        "children": [{"id": c["id"], "name": c["canonical_name"]} for c in (children or [])]
    }

async def _execute_undo(store, resolver, redis, snapshot: dict, session_id: str) -> dict:
    """Restore secondary entity from snapshot."""
    primary_id = snapshot["primary_id"]
    ent_data = snapshot["entity"]

    new_id = await redis.incr(RedisKeys.global_next_ent_id())

    entity = {
        "id": new_id,
        "canonical_name": ent_data["canonical_name"],
        "type": ent_data["type"],
        "confidence": 0.9,
        "topic": ent_data["topic"],
        "embedding": ent_data["embedding"],
        "aliases": ent_data["aliases"],
        "session_id": ent_data.get("session_id", "undo")
    }
    await store.write_batch([entity], [])

    facts_restored = 0
    if snapshot["facts"]:
        fact_objects = []
        for fd in snapshot["facts"]:
            fact_objects.append(Fact(
                id=fd["id"],
                content=fd["content"],
                valid_at=datetime.fromisoformat(fd["valid_at"]),
                invalid_at=datetime.fromisoformat(fd["invalid_at"]) if fd.get("invalid_at") else None,
                embedding=fd.get("embedding", []),
                source_msg_id=fd.get("source_msg_id"),
                confidence=fd.get("confidence", 1.0),
                source_entity_id=new_id,
                source=fd.get("source", "user")
            ))
        try:
            facts_restored = await store.create_facts_batch(new_id, fact_objects)
        except Exception as e:
            logger.error(f"Failed to restore facts during undo: {e}")

    rels_restored = 0
    for rel in snapshot["relationships"]:
        neighbor_id = rel["neighbor_id"]
        if neighbor_id == primary_id:
            continue
        try:
            rel_data = [{
                "entity_a": ent_data["canonical_name"],
                "entity_b": rel["neighbor_name"],
                "entity_a_id": new_id,
                "entity_b_id": neighbor_id,
                "message_id": rel["message_ids"][0] if rel["message_ids"] else "undo",
                "confidence": rel.get("confidence", 1.0),
                "context": rel.get("context")
            }]
            await store.write_batch([], rel_data)
            rels_restored += 1
        except Exception as e:
            logger.warning(f"Failed to restore relationship to {rel['neighbor_name']}: {e}")

    primary_pre_merge_neighbors = {r["neighbor_id"] for r in snapshot["primary_relationships"]}
    edges_removed = 0
    for rel in snapshot["relationships"]:
        neighbor_id = rel["neighbor_id"]
        if neighbor_id == primary_id:
            continue
        if neighbor_id not in primary_pre_merge_neighbors:
            try:
                await store.delete_relationship(primary_id, neighbor_id)
                edges_removed += 1
            except Exception as e:
                logger.warning(f"Failed to remove transferred edge ({primary_id}, {neighbor_id}): {e}")

    for parent in snapshot.get("parents", []):
        try:
            await store.create_hierarchy_edge(parent["id"], new_id)
        except Exception as e:
            logger.warning(f"Failed to restore parent edge: {e}")

    for child in snapshot.get("children", []):
        try:
            await store.create_hierarchy_edge(new_id, child["id"])
        except Exception as e:
            logger.warning(f"Failed to restore child edge: {e}")

    resolver.register_entity(
        new_id,
        ent_data["canonical_name"],
        ent_data["aliases"],
        ent_data["type"],
        ent_data["topic"]
    )

    await emit(session_id, "system", "merge_undone", {
        "restored_entity": ent_data["canonical_name"],
        "new_id": new_id,
        "facts_restored": facts_restored,
        "relationships_restored": rels_restored,
        "edges_removed": edges_removed
    })

    return {
        "new_entity_id": new_id,
        "facts_restored": facts_restored,
        "relationships_restored": rels_restored,
        "transferred_edges_removed": edges_removed
    }