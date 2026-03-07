"""Shared graph write logic — used by both Context and SDK extractor.

Extracted from Context._write_to_graph to avoid reimplementation.
"""

import asyncio
from concurrent.futures import ThreadPoolExecutor
from functools import partial
from typing import Dict, List, Optional, Set

from loguru import logger

from db.store import MemGraphStore
from main.entity_resolve import EntityResolver
from shared.models.schema.dtypes import BatchResult, MessageConnections
from shared.infra.redis import RedisKeys

import redis.asyncio as aioredis


async def write_batch_to_graph(
    batch: BatchResult,
    store: MemGraphStore,
    executor: ThreadPoolExecutor,
    resolver: EntityResolver,
    session_id: str,
    user_name: str = None,
    redis_client: aioredis.Redis = None,
) -> None:
    """Write entities, aliases, and connections from a BatchResult to the graph.

    Handles:
    - Zombie detection (validate_existing_ids)
    - New entity writes with embeddings/aliases
    - Alias-updated entity writes
    - Alias persistence (update_entity_aliases)
    - Relationship writes from extraction_result
    - Dirty entity tracking (if redis_client provided)

    Args:
        batch: The BatchResult from processor.run()
        store: Memgraph store instance
        executor: Thread pool for blocking graph ops
        resolver: Entity resolver (for profiles, embeddings, aliases)
        session_id: Current session ID
        user_name: For dirty entity tracking (optional)
        redis_client: For dirty entity tracking (optional)
    """
    loop = asyncio.get_running_loop()

    entity_ids = batch.entity_ids
    new_entity_ids = batch.new_entity_ids
    alias_updated_ids = batch.alias_updated_ids
    extraction_result = batch.extraction_result or []
    alias_updates = batch.alias_updates

    # ── Zombie validation ───────────────────────────────────
    valid_existing_ids = set()
    existing_candidates = list(set(entity_ids) - new_entity_ids)

    if existing_candidates:
        validation_result = await loop.run_in_executor(
            executor, store.validate_existing_ids, existing_candidates,
        )

        if validation_result is None:
            logger.warning(
                f"Could not validate {len(existing_candidates)} entities, assuming valid"
            )
            valid_existing_ids = set(existing_candidates)
        else:
            valid_existing_ids = validation_result
            missing = set(existing_candidates) - valid_existing_ids
            if missing:
                logger.critical(
                    f"SPLIT BRAIN DETECTED: Resolver thinks IDs {missing} exist, "
                    f"but Graph does not. Dropping writes to prevent Zombie Resurrection."
                )
                resolver.remove_entities(list(missing))

    # ── Alias persistence ───────────────────────────────────
    if alias_updates:
        await loop.run_in_executor(
            executor, store.update_entity_aliases, alias_updates,
        )
        logger.info(f"Persisted alias updates for {len(alias_updates)} entities")

    safe_ids = valid_existing_ids.union(new_entity_ids)

    # ── Build entity writes ─────────────────────────────────
    entities_to_write = []

    for eid in new_entity_ids:
        profile = resolver.entity_profiles.get(eid)
        if profile:
            entities_to_write.append({
                "id": eid,
                "canonical_name": profile["canonical_name"],
                "type": profile.get("type", ""),
                "confidence": 1.0,
                "topic": profile.get("topic", "General"),
                "embedding": resolver.get_embedding_for_id(eid),
                "aliases": resolver.get_mentions_for_id(eid),
                "session_id": profile.get("session_id") or session_id,
            })

    for eid in alias_updated_ids:
        if eid in new_entity_ids:
            continue
        if eid not in safe_ids:
            logger.warning(f"Skipping alias update for Zombie ID {eid}")
            continue
        profile = resolver.entity_profiles.get(eid)
        if profile:
            entities_to_write.append({
                "id": eid,
                "canonical_name": profile["canonical_name"],
                "type": profile.get("type", ""),
                "confidence": 1.0,
                "topic": profile.get("topic", "General"),
                "embedding": resolver.get_embedding_for_id(eid),
                "aliases": resolver.get_mentions_for_id(eid),
                "session_id": profile.get("session_id") or session_id,
            })

    # ── Build entity lookup for relationship resolution ─────
    entity_lookup = {}
    for eid in safe_ids:
        profile = resolver.entity_profiles.get(eid)
        if profile:
            canonical = profile.get("canonical_name")
            if not canonical:
                continue
            entry = {
                "id": eid,
                "canonical_name": canonical,
                "type": profile.get("type"),
                "topic": profile.get("topic", "General"),
            }
            entity_lookup[canonical.lower()] = entry
            for mention in resolver.get_mentions_for_id(eid):
                if mention:
                    entity_lookup[mention.lower()] = entry

    # ── Build relationships ─────────────────────────────────
    relationships = []
    for msg_result in extraction_result:
        for pair in msg_result.entity_pairs:
            
            ent_a_name = pair.entity_a.lower() if pair.entity_a else None
            ent_b_name = pair.entity_b.lower() if pair.entity_b else None
            
            ent_a = entity_lookup.get(ent_a_name) if ent_a_name else None
            ent_b = entity_lookup.get(ent_b_name) if ent_b_name else None

            if ent_a and ent_b:
                relationships.append({
                    "entity_a": ent_a["canonical_name"],
                    "entity_b": ent_b["canonical_name"],
                    "entity_a_id": ent_a["id"],
                    "entity_b_id": ent_b["id"],
                    "message_id": f"msg_{msg_result.message_id}",
                    "confidence": pair.confidence,
                    "context": pair.context,
                })
            else:
                logger.warning(
                    f"Skipping pair: {pair.entity_a} - {pair.entity_b} "
                    f"(Entity missing or Zombie)"
                )

    # ── Write to graph ──────────────────────────────────────
    if entities_to_write or relationships:
        await loop.run_in_executor(
            executor,
            partial(store.write_batch, entities_to_write, relationships),
        )

    # ── Dirty entity tracking (for profile refinement) ──────
    if redis_client and user_name and safe_ids:
        dirty_key = RedisKeys.dirty_entities(user_name, session_id)
        await redis_client.sadd(dirty_key, *[str(eid) for eid in safe_ids])
        await redis_client.delete(
            RedisKeys.profile_complete(user_name, session_id)
        )

    zombies_filtered = len(existing_candidates) - len(valid_existing_ids)

    logger.info(
        f"Graph write: {len(entities_to_write)} entities, "
        f"{len(relationships)} relationships "
        f"(Filtered {zombies_filtered} zombies)"
    )


async def write_batch_callback(
    batch: BatchResult,
    store: MemGraphStore,
    executor: ThreadPoolExecutor,
    resolver: EntityResolver,
    session_id: str,
    user_name: str = None,
    redis_client: aioredis.Redis = None,
) -> tuple[bool, Optional[str]]:
    """Callback wrapper matching the DLQ's expected signature.

    Returns (success, error_message).
    """
    if not batch.extraction_result:
        return True, None

    try:
        await write_batch_to_graph(
            batch, store, executor, resolver,
            session_id, user_name, redis_client,
        )
        return True, None
    except Exception as e:
        logger.error(f"Graph write callback failed: {e}")
        if batch.new_entity_ids:
            resolver.remove_entities(list(batch.new_entity_ids))
            logger.info(
                f"Cleaned {len(batch.new_entity_ids)} phantom entities from resolver"
            )
        return False, str(e)