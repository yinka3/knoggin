from typing import Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.contracts import (
    AliasUpdate,
    BatchResult,
    EngineScope,
    EngineWorkUnit,
    EntityWrite,
    GraphMutationPlan,
    GraphWriteSummary,
    RelationshipWrite,
    SkippedRelationship,
    UserRelationshipWrite,
)
from common.scoping import IDENTITY_ENTITY_ID
from infrastructure.graph_interface import GraphInterface
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.services.entity_service import EntityManager


def _resolve_scope(
    batch: BatchResult,
    session_id: str,
    project_id: str,
    user_name: Optional[str],
) -> EngineScope:
    batch_scope = batch.scope
    scope_values = {
        "user_name": (batch_scope.user_name if batch_scope else None) or user_name,
        "session_id": (batch_scope.session_id if batch_scope else None) or session_id,
        "project_id": (batch_scope.project_id if batch_scope else None) or project_id,
    }
    missing_scope = [key for key, value in scope_values.items() if not value]
    if missing_scope:
        raise ValueError(f"Graph batch write missing required scope: {missing_scope}")

    return EngineScope(**scope_values)


def _normalize_embedding(value) -> Optional[list[float]]:
    if value is None:
        return None
    if hasattr(value, "tolist"):
        return value.tolist()
    return list(value)


async def build_graph_mutation_plan(
    batch: BatchResult,
    graph_client: GraphInterface,
    entities: EntityManager,
    session_id: str,
    project_id: str,
    user_name: str = None,
) -> GraphMutationPlan:
    """Build typed graph-write intent from a processed batch."""

    scope = _resolve_scope(batch, session_id, project_id, user_name)
    entity_ids = batch.entity_ids
    new_entity_ids = set(batch.new_entity_ids)
    alias_updated_ids = set(batch.alias_updated_ids)
    alias_update_ids = set(batch.alias_updates)

    valid_existing_ids = set()
    zombie_entity_ids = set()
    existing_candidates = list(
        (set(entity_ids) | alias_updated_ids | alias_update_ids) - new_entity_ids
    )

    if existing_candidates:
        validation_result = await graph_client.validate_existing_ids(
            existing_candidates
        )

        if validation_result is None:
            logger.warning(
                f"Could not validate {len(existing_candidates)} entities, assuming valid"
            )
            valid_existing_ids = set(existing_candidates)
        else:
            valid_existing_ids = validation_result
            zombie_entity_ids = set(existing_candidates) - valid_existing_ids
            if zombie_entity_ids:
                logger.critical(
                    f"SPLIT BRAIN DETECTED: Resolver thinks IDs {zombie_entity_ids} "
                    "exist, but Graph does not. Dropping writes to prevent Zombie "
                    "Resurrection."
                )
                entities.remove_entities(list(zombie_entity_ids))

    safe_entity_ids = valid_existing_ids.union(new_entity_ids)
    alias_updates = [
        AliasUpdate(entity_id=entity_id, aliases=list(aliases))
        for entity_id, aliases in batch.alias_updates.items()
        if aliases and entity_id in safe_entity_ids
    ]
    entity_writes = []

    async def build_entity_write(entity_id: int) -> Optional[EntityWrite]:
        if entity_id == IDENTITY_ENTITY_ID:
            return None

        profile = entities.entity_profiles.get(entity_id)
        if not profile:
            return None

        embedding = await entities.get_embedding_for_id(entity_id)
        return EntityWrite(
            id=entity_id,
            is_new=entity_id in new_entity_ids,
            canonical_name=profile["canonical_name"],
            type=profile.get("type", ""),
            confidence=1.0,
            topic=profile.get("topic", "General"),
            embedding=_normalize_embedding(embedding),
            aliases=list(entities.get_mentions_for_id(entity_id)),
            user_name=scope.user_name,
            session_id=profile.get("session_id") or scope.session_id,
            project_id=(
                profile.get("project_id") or entities.project_id or scope.project_id
            ),
        )

    for entity_id in new_entity_ids:
        write = await build_entity_write(entity_id)
        if write:
            entity_writes.append(write)

    for entity_id in alias_updated_ids:
        if entity_id in new_entity_ids:
            continue
        if entity_id not in safe_entity_ids:
            logger.warning(f"Skipping alias update for Zombie ID {entity_id}")
            continue
        write = await build_entity_write(entity_id)
        if write:
            entity_writes.append(write)

    entity_lookup = {}
    for entity_id in safe_entity_ids:
        profile = entities.entity_profiles.get(entity_id)
        if not profile:
            continue
        canonical = profile.get("canonical_name")
        if not canonical:
            continue
        entry = {
            "id": entity_id,
            "canonical_name": canonical,
            "type": profile.get("type"),
            "topic": profile.get("topic", "General"),
        }
        entity_lookup[canonical.lower()] = entry
        for mention in entities.get_mentions_for_id(entity_id):
            if mention:
                entity_lookup[mention.lower()] = entry

    relationship_writes = []
    user_relationship_writes = []
    skipped_relationships = []
    for msg_result in batch.relationship_observations:
        msg_id = msg_result.message_id

        for pair in msg_result.entity_pairs:
            pair_entity_a = pair.entity_a
            pair_entity_b = pair.entity_b
            ent_a_name = pair_entity_a.lower() if pair_entity_a else None
            ent_b_name = pair_entity_b.lower() if pair_entity_b else None

            ent_a = entity_lookup.get(ent_a_name) if ent_a_name else None
            ent_b = entity_lookup.get(ent_b_name) if ent_b_name else None

            if not (ent_a and ent_b):
                logger.warning(
                    f"Skipping pair: {pair_entity_a} - {pair_entity_b} "
                    "(Entity missing or Zombie)"
                )
                skipped_relationships.append(
                    SkippedRelationship(
                        entity_a=pair_entity_a,
                        entity_b=pair_entity_b,
                        message_id=msg_id,
                        reason="entity_missing_or_zombie",
                        metadata={
                            "entity_a_found": ent_a is not None,
                            "entity_b_found": ent_b is not None,
                        },
                    )
                )
                continue

            evidence_ref = {
                "user_name": scope.user_name,
                "session_id": scope.session_id,
                "message_id": int(str(msg_id).removeprefix("msg_")),
            }
            relationship_writes.append(
                RelationshipWrite(
                    entity_a=ent_a["canonical_name"],
                    entity_b=ent_b["canonical_name"],
                    entity_a_id=ent_a["id"],
                    entity_b_id=ent_b["id"],
                    message_id=f"msg_{msg_id}",
                    evidence_ref=evidence_ref,
                    user_name=scope.user_name,
                    session_id=scope.session_id,
                    project_id=scope.project_id,
                    confidence=pair.confidence,
                    context=pair.context,
                )
            )

    for msg_result in batch.user_relationship_observations:
        msg_id = msg_result.message_id

        for pair in msg_result.user_connections:
            target_name = pair.entity_name
            target_lookup_name = target_name.lower() if target_name else None
            target = entity_lookup.get(target_lookup_name) if target_lookup_name else None

            if not target:
                logger.warning(
                    f"Skipping user relationship: {scope.user_name} - {target_name} "
                    "(target entity missing or Zombie)"
                )
                skipped_relationships.append(
                    SkippedRelationship(
                        entity_a=scope.user_name,
                        entity_b=target_name,
                        message_id=msg_id,
                        reason="user_target_missing_or_zombie",
                        metadata={"entity_found": target is not None},
                    )
                )
                continue

            evidence_ref = {
                "user_name": scope.user_name,
                "session_id": scope.session_id,
                "message_id": int(str(msg_id).removeprefix("msg_")),
            }
            user_relationship_writes.append(
                UserRelationshipWrite(
                    user_entity_id=IDENTITY_ENTITY_ID,
                    entity_name=target["canonical_name"],
                    entity_id=target["id"],
                    message_id=f"msg_{msg_id}",
                    evidence_ref=evidence_ref,
                    user_name=scope.user_name,
                    session_id=scope.session_id,
                    project_id=scope.project_id,
                    confidence=pair.confidence,
                    context=pair.context,
                )
            )

    batch_work_unit_id = batch.work_unit.id if batch.work_unit else None
    graph_work_unit = EngineWorkUnit.for_graph_write(
        scope, batch_id=batch_work_unit_id
    )

    return GraphMutationPlan(
        work_unit=graph_work_unit,
        scope=scope,
        entity_ids=entity_ids,
        safe_entity_ids=safe_entity_ids,
        new_entity_ids=new_entity_ids,
        alias_updates=alias_updates,
        entity_writes=entity_writes,
        relationship_writes=relationship_writes,
        user_relationship_writes=user_relationship_writes,
        skipped_relationships=skipped_relationships,
        dirty_entity_ids=safe_entity_ids,
        zombie_entity_ids=zombie_entity_ids,
    )


def _summary_for_skipped_plan(plan: GraphMutationPlan) -> GraphWriteSummary:
    return GraphWriteSummary(
        zombies_filtered=len(plan.zombie_entity_ids),
        relationships_skipped=len(plan.skipped_relationships),
    )


def _attach_graph_work_summary(
    batch: BatchResult, plan: GraphMutationPlan, summary: GraphWriteSummary
) -> None:
    if not batch.work_unit:
        return
    batch.work_unit.metadata["graph_write"] = summary.model_dump(mode="json")
    batch.work_unit.metadata["graph_write_work_unit_id"] = plan.work_unit.id
    batch.work_unit.metadata["graph_write_work_unit"] = plan.work_unit.model_dump(
        mode="json"
    )


async def execute_graph_mutation_plan(
    plan: GraphMutationPlan,
    graph_client: GraphInterface,
    redis_client: aioredis.Redis = None,
) -> GraphWriteSummary:
    """Execute a typed graph mutation plan using existing graph-client APIs."""

    alias_update_map = {
        update.entity_id: update.aliases
        for update in plan.alias_updates
        if update.aliases
    }
    if alias_update_map:
        await graph_client.update_entity_aliases(
            alias_update_map, project_id=plan.scope.project_id
        )

    entity_payloads, relationship_payloads = plan.to_graph_payloads()
    if entity_payloads or relationship_payloads:
        await graph_client.write_batch(entity_payloads, relationship_payloads)

    dirty_count = 0
    if redis_client and plan.scope.user_name and plan.dirty_entity_ids:
        dirty_key = RedisKeys.dirty_entities(
            plan.scope.user_name, plan.scope.project_id
        )
        await redis_client.sadd(
            dirty_key, *[str(entity_id) for entity_id in plan.dirty_entity_ids]
        )
        await redis_client.delete(
            RedisKeys.project_profile_complete(
                plan.scope.user_name, plan.scope.project_id
            )
        )
        dirty_count = len(plan.dirty_entity_ids)

    return GraphWriteSummary(
        entities_written=len(entity_payloads),
        relationships_written=len(relationship_payloads),
        user_relationships_written=len(plan.user_relationship_writes),
        aliases_updated=len(alias_update_map),
        dirty_entities_marked=dirty_count,
        zombies_filtered=len(plan.zombie_entity_ids),
        relationships_skipped=len(plan.skipped_relationships),
    )


async def write_batch_to_graph(
    batch: BatchResult,
    graph_client: GraphInterface,
    entities: EntityManager,
    session_id: str,
    project_id: str,
    user_name: str = None,
    redis_client: aioredis.Redis = None,
) -> GraphWriteSummary:
    """Build and execute graph mutations from a processed batch."""

    plan = await build_graph_mutation_plan(
        batch,
        graph_client,
        entities,
        session_id=session_id,
        project_id=project_id,
        user_name=user_name,
    )

    if not plan.has_writes():
        summary = _summary_for_skipped_plan(plan)
        plan.work_unit.mark_skipped("No graph writes")
        _attach_graph_work_summary(batch, plan, summary)
        return summary

    plan.work_unit.mark_running()
    try:
        summary = await execute_graph_mutation_plan(plan, graph_client, redis_client)
    except Exception as e:
        plan.work_unit.mark_failed(str(e))
        raise

    plan.work_unit.mark_succeeded(
        f"{summary.entities_written} entities, {summary.relationships_written} relationships"
    )
    _attach_graph_work_summary(batch, plan, summary)

    logger.info(
        f"Graph write: {summary.entities_written} entities, "
        f"{summary.relationships_written} relationships "
        f"({summary.user_relationships_written} user-root), "
        f"(Filtered {summary.zombies_filtered} zombies, "
        f"skipped {summary.relationships_skipped} relationships)"
    )
    return summary


async def write_batch_callback(
    batch: BatchResult,
    graph_client: GraphInterface,
    entities: EntityManager,
    session_id: str,
    project_id: str,
    user_name: str = None,
    redis_client: aioredis.Redis = None,
) -> tuple[bool, Optional[str]]:
    """Callback wrapper matching the DLQ's expected signature.

    Returns (success, error_message).
    """
    has_writes = batch.has_graph_writes()
    if not has_writes:
        return True, None

    try:
        await write_batch_to_graph(
            batch,
            graph_client,
            entities,
            session_id=session_id,
            project_id=project_id,
            user_name=user_name,
            redis_client=redis_client,
        )
        return True, None
    except Exception as e:
        logger.error(f"Graph write callback failed: {e}")
        if batch.new_entity_ids:
            # We must aggressively purge these newly created entities from the cache.
            # If we leave them, the live pipeline might resolve future mentions to these
            # "phantom" IDs before the DLQ can retry. This would cause the live pipeline
            # to trip the zombie check (trying to write facts to an ID that doesn't exist in the DB).
            entities.remove_entities(list(batch.new_entity_ids))
            logger.info(
                f"Cleaned {len(batch.new_entity_ids)} phantom entities from entities"
            )
        return False, str(e)
