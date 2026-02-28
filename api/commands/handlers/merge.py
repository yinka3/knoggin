import asyncio
import json
from datetime import datetime, timezone
from functools import partial

from loguru import logger

from api.commands.registry import command, CommandContext
from jobs.jobs_utils import cosine_similarity
from shared.events import emit
from shared.redisclient import RedisKeys
from shared.schema.dtypes import Fact

UNDO_TTL_SECONDS = 7200


async def _build_merge_snapshot(
    store, resolver, primary_id: int, secondary_id: int,
    primary_name: str, secondary_name: str
) -> dict:
    """Capture full pre-merge state for undo capability."""
    loop = asyncio.get_running_loop()

    entity_data = await loop.run_in_executor(None, store.get_entity_by_id, secondary_id)
    embedding = resolver.get_embedding_for_id(secondary_id)

    facts_raw = await loop.run_in_executor(None, store.get_facts_for_entity, secondary_id, False)
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

    sec_rels = await loop.run_in_executor(None, store.get_entity_relationships, secondary_id)
    pri_rels = await loop.run_in_executor(None, store.get_entity_relationships, primary_id)
    parents = await loop.run_in_executor(None, store.get_parent_entities, secondary_id)
    children = await loop.run_in_executor(None, store.get_child_entities, secondary_id)

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
    loop = asyncio.get_running_loop()

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
    await loop.run_in_executor(None, partial(store.write_batch, [entity], []))

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
            facts_restored = await loop.run_in_executor(
                None, partial(store.create_facts_batch, new_id, fact_objects)
            )
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
                "message_id": rel["message_ids"][0] if rel["message_ids"] else "undo",
                "confidence": rel.get("confidence", 1.0),
                "context": rel.get("context")
            }]
            await loop.run_in_executor(None, partial(store.write_batch, [], rel_data))
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
                await loop.run_in_executor(
                    None, partial(store.delete_relationship, primary_id, neighbor_id)
                )
                edges_removed += 1
            except Exception as e:
                logger.warning(f"Failed to remove transferred edge ({primary_id}, {neighbor_id}): {e}")

    for parent in snapshot.get("parents", []):
        try:
            await loop.run_in_executor(
                None, partial(store.create_hierarchy_edge, parent["id"], new_id)
            )
        except Exception as e:
            logger.warning(f"Failed to restore parent edge: {e}")

    for child in snapshot.get("children", []):
        try:
            await loop.run_in_executor(
                None, partial(store.create_hierarchy_edge, new_id, child["id"])
            )
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


@command("/merge list", description="List pending merge proposals")
async def handle_merge_list(ctx: CommandContext) -> dict:
    redis = ctx.state.resources.redis
    proposals_key = RedisKeys.merge_proposals(ctx.state.user_name, ctx.session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        return {"proposals": [], "message": "No pending merge proposals"}

    proposals = []
    for idx, raw in enumerate(raw_proposals, start=1):
        p = json.loads(raw)
        proposals.append({
            "index": idx,
            "primary_name": p["primary_name"],
            "secondary_name": p["secondary_name"],
            "score": p.get("llm_score", 0)
        })

    return {"proposals": proposals}


@command("/merge approve", description="Approve a merge proposal")
async def handle_merge_approve(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Index required. Use /merge list to see proposals.")

    try:
        index = int(ctx.args.strip())
    except ValueError:
        raise ValueError("Index must be a number. Use /merge list to see proposals.")

    redis = ctx.state.resources.redis
    store = ctx.state.resources.store
    proposals_key = RedisKeys.merge_proposals(ctx.state.user_name, ctx.session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        raise ValueError("No pending merge proposals.")

    if index < 1 or index > len(raw_proposals):
        raise ValueError(f"Invalid index. Use /merge list to see {len(raw_proposals)} proposals.")

    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)

    primary_id = proposal["primary_id"]
    secondary_id = proposal["secondary_id"]

    resolver = None
    if ctx.session_id in ctx.state.active_sessions:
        resolver = ctx.state.active_sessions[ctx.session_id].ent_resolver

    # Build full snapshot before merge
    snapshot = await _build_merge_snapshot(
        store, resolver, primary_id, secondary_id,
        proposal["primary_name"], proposal["secondary_name"]
    )

    undo_key = RedisKeys.merge_undo(ctx.session_id, primary_id, secondary_id)
    await redis.setex(undo_key, UNDO_TTL_SECONDS, json.dumps(snapshot))

    loop = asyncio.get_running_loop()
    success = await loop.run_in_executor(None, store.merge_entities, primary_id, secondary_id)

    if not success:
        await redis.delete(undo_key)
        raise RuntimeError("Merge failed")

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
        "undo_available_for": f"{UNDO_TTL_SECONDS // 3600} hours",
        "undo_hint": f"Use '/merge undo {proposal['primary_name']}' to reverse"
    }


@command("/merge reject", description="Reject a merge proposal by index")
async def handle_merge_reject(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Index required. Use /merge list to see proposals.")

    try:
        index = int(ctx.args.strip())
    except ValueError:
        raise ValueError("Index must be a number. Use /merge list to see proposals.")

    redis = ctx.state.resources.redis
    proposals_key = RedisKeys.merge_proposals(ctx.state.user_name, ctx.session_id)

    raw_proposals = await redis.lrange(proposals_key, 0, -1)

    if not raw_proposals:
        raise ValueError("No pending merge proposals.")

    if index < 1 or index > len(raw_proposals):
        raise ValueError(f"Invalid index. Use /merge list to see {len(raw_proposals)} proposals.")

    raw = raw_proposals[index - 1]
    proposal = json.loads(raw)

    await redis.lrem(proposals_key, 1, raw)

    return {
        "rejected": True,
        "primary": proposal["primary_name"],
        "secondary": proposal["secondary_name"]
    }


@command("/merge undo", description="Undo a recent merge by entity name")
async def handle_merge_undo(ctx: CommandContext) -> dict:
    if not ctx.args.strip():
        raise ValueError("Entity name required")

    entity_name = ctx.args.strip().lower()
    redis = ctx.state.resources.redis
    store = ctx.state.resources.store

    matches = []
    async for key in redis.scan_iter(match=f"merge_undo:{ctx.session_id}:*"):
        raw = await redis.get(key)
        if raw:
            snapshot = json.loads(raw)
            if (snapshot["primary_name"].lower() == entity_name or
                snapshot["secondary_name"].lower() == entity_name):
                matches.append((key, snapshot))

    if not matches:
        raise ValueError(f"No undo available for '{ctx.args.strip()}'. Window may have expired.")

    if len(matches) > 1:
        options = [f"{s['primary_name']} <- {s['secondary_name']}" for _, s in matches]
        raise ValueError(f"Multiple matches found: {', '.join(options)}. Be more specific.")

    undo_key, snapshot = matches[0]

    resolver = None
    if ctx.session_id in ctx.state.active_sessions:
        resolver = ctx.state.active_sessions[ctx.session_id].ent_resolver

    if not resolver:
        raise RuntimeError("Session must be active to undo a merge")

    result = await _execute_undo(store, resolver, redis, snapshot, ctx.session_id)

    await redis.delete(undo_key)

    return {
        "undone": True,
        "restored_entity": snapshot["entity"]["canonical_name"],
        "new_entity_id": result["new_entity_id"],
        "facts_restored": result["facts_restored"],
        "relationships_restored": result["relationships_restored"],
        "transferred_edges_removed": result["transferred_edges_removed"],
        "note": "Entity restored with a new ID. Original ID was consumed by the merge."
    }