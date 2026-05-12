from typing import Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.dtypes import BatchResult
from infrastructure.memgraph_client import MemgraphClient
from infrastructure.redis_client import RedisKeys
from knoggin.knowledge.services.entity_service import EntityManager


async def write_batch_to_graph(
    batch: BatchResult,
    memgraph: MemgraphClient,
    entities: EntityManager,
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
        memgraph: Memgraph memgraph instance
        entities: Entity entities (for profiles, embeddings, aliases)
        session_id: Current session ID
        user_name: For dirty entity tracking (optional)
        redis_client: For dirty entity tracking (optional)
    """
    entity_ids = batch.entity_ids
    new_entity_ids = batch.new_entity_ids
    alias_updated_ids = batch.alias_updated_ids
    extraction_result = batch.extraction_result or []
    alias_updates = batch.alias_updates

    # ── Zombie validation ───────────────────────────────────
    valid_existing_ids = set()
    existing_candidates = list(set(entity_ids) - new_entity_ids)

    if existing_candidates:
        validation_result = await memgraph.validate_existing_ids(existing_candidates)

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
                entities.remove_entities(list(missing))

    # ── Alias persistence ───────────────────────────────────
    if alias_updates:
        await memgraph.update_entity_aliases(alias_updates)
        logger.info(f"Persisted alias updates for {len(alias_updates)} entities")

    safe_ids = valid_existing_ids.union(new_entity_ids)

    # ── Build entity writes ─────────────────────────────────
    entities_to_write = []

    for eid in new_entity_ids:
        profile = entities.entity_profiles.get(eid)
        if profile:
            entities_to_write.append(
                {
                    "id": eid,
                    "canonical_name": profile["canonical_name"],
                    "type": profile.get("type", ""),
                    "confidence": 1.0,
                    "topic": profile.get("topic", "General"),
                    "embedding": await entities.get_embedding_for_id(eid),
                    "aliases": entities.get_mentions_for_id(eid),
                    "session_id": profile.get("session_id") or session_id,
                }
            )

    for eid in alias_updated_ids:
        if eid in new_entity_ids:
            continue
        if eid not in safe_ids:
            logger.warning(f"Skipping alias update for Zombie ID {eid}")
            continue
        profile = entities.entity_profiles.get(eid)
        if profile:
            entities_to_write.append(
                {
                    "id": eid,
                    "canonical_name": profile["canonical_name"],
                    "type": profile.get("type", ""),
                    "confidence": 1.0,
                    "topic": profile.get("topic", "General"),
                    "embedding": await entities.get_embedding_for_id(eid),
                    "aliases": entities.get_mentions_for_id(eid),
                    "session_id": profile.get("session_id") or session_id,
                }
            )

    # ── Build entity lookup for relationship resolution ─────
    entity_lookup = {}
    for eid in safe_ids:
        profile = entities.entity_profiles.get(eid)
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
            for mention in entities.get_mentions_for_id(eid):
                if mention:
                    entity_lookup[mention.lower()] = entry

    # ── Build relationships ─────────────────────────────────
    relationships = []
    for msg_result in extraction_result:
        msg_id = (
            msg_result["message_id"]
            if isinstance(msg_result, dict)
            else msg_result.message_id
        )
        pairs = (
            msg_result["entity_pairs"]
            if isinstance(msg_result, dict)
            else msg_result.entity_pairs
        )

        for pair in pairs:
            if isinstance(pair, dict):
                pair_entity_a = pair.get("entity_a")
                pair_entity_b = pair.get("entity_b")
                pair_confidence = pair.get("confidence", 1.0)
                pair_context = pair.get("context")
            else:
                pair_entity_a = pair.entity_a
                pair_entity_b = pair.entity_b
                pair_confidence = pair.confidence
                pair_context = pair.context

            ent_a_name = pair_entity_a.lower() if pair_entity_a else None
            ent_b_name = pair_entity_b.lower() if pair_entity_b else None

            ent_a = entity_lookup.get(ent_a_name) if ent_a_name else None
            ent_b = entity_lookup.get(ent_b_name) if ent_b_name else None

            if ent_a and ent_b:
                relationships.append(
                    {
                        "entity_a": ent_a["canonical_name"],
                        "entity_b": ent_b["canonical_name"],
                        "entity_a_id": ent_a["id"],
                        "entity_b_id": ent_b["id"],
                        "message_id": f"msg_{msg_id}",
                        "confidence": pair_confidence,
                        "context": pair_context,
                    }
                )
            else:
                logger.warning(
                    f"Skipping pair: {pair_entity_a} - {pair_entity_b} "
                    f"(Entity missing or Zombie)"
                )

    # ── Write to graph ──────────────────────────────────────
    if entities_to_write or relationships:
        await memgraph.write_batch(entities_to_write, relationships)

    # ── Dirty entity tracking (for profile refinement) ──────
    if redis_client and user_name and safe_ids:
        dirty_key = RedisKeys.dirty_entities(user_name, session_id)
        await redis_client.sadd(dirty_key, *[str(eid) for eid in safe_ids])
        await redis_client.delete(RedisKeys.profile_complete(user_name, session_id))

    zombies_filtered = len(existing_candidates) - len(valid_existing_ids)

    logger.info(
        f"Graph write: {len(entities_to_write)} entities, "
        f"{len(relationships)} relationships "
        f"(Filtered {zombies_filtered} zombies)"
    )


async def write_batch_callback(
    batch: BatchResult,
    memgraph: MemgraphClient,
    entities: EntityManager,
    session_id: str,
    user_name: str = None,
    redis_client: aioredis.Redis = None,
) -> tuple[bool, Optional[str]]:
    """Callback wrapper matching the DLQ's expected signature.

    Returns (success, error_message).
    """
    has_writes = bool(
        batch.extraction_result
        or batch.new_entity_ids
        or batch.alias_updated_ids
        or batch.alias_updates
    )
    if not has_writes:
        return True, None

    try:
        await write_batch_to_graph(
            batch,
            memgraph,
            entities,
            session_id,
            user_name,
            redis_client,
        )
        return True, None
    except Exception as e:
        logger.error(f"Graph write callback failed: {e}")
        if batch.new_entity_ids:
            entities.remove_entities(list(batch.new_entity_ids))
            logger.info(
                f"Cleaned {len(batch.new_entity_ids)} phantom entities from entities"
            )
        return False, str(e)
