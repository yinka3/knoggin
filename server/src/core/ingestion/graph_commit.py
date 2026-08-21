import asyncio
from dataclasses import asdict
from typing import Optional

import redis.asyncio as aioredis
from loguru import logger

from common.schema.ingestion.contracts import (
    AliasUpdate,
    EntityWrite,
    EpisodeEligibility,
    ExecutionScope,
    GraphWriteSummary,
    MessageEntityRef,
    RelationshipWrite,
    SkippedRelationship,
)
from common.scoping import IDENTITY_ENTITY_ID
from common.utils.diagnostic_context import diagnostic_scope
from common.utils.events import emit
from core.ingestion.batch import IngestionBatch
from core.ingestion.ports import IngestionGraphPersistence
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.redis_client import RedisKeys
from infrastructure.work_record import WorkRecord


class GraphWritePostgresCommittedError(RuntimeError):
    """Redis-side failure after the PostgreSQL graph transaction committed."""


def _normalize_embedding(value) -> Optional[list[float]]:
    if value is None:
        return None
    if hasattr(value, "tolist"):
        return value.tolist()
    return list(value)


async def prepare_ingestion_batch_graph_writes(
    batch: IngestionBatch,
    knowledge_store: IngestionGraphPersistence,
    entities: EntityResolver,
) -> None:
    """Fill one completed ingestion batch with its owned graph-write commands."""

    if not isinstance(batch, IngestionBatch):
        raise TypeError(
            "prepare_ingestion_batch_graph_writes requires an IngestionBatch"
        )
    scope = batch.scope
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
        validation_result = await knowledge_store.validate_existing_ids(
            existing_candidates,
            visible_project_ids=[scope.project_id],
        )

        if validation_result is None:
            logger.warning(
                "Could not validate "
                f"{len(existing_candidates)} entities, assuming valid"
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
        AliasUpdate(entity_id=entity_id, aliases=tuple(aliases))
        for entity_id, aliases in batch.alias_updates.items()
        if aliases and entity_id in safe_entity_ids
    ]
    entity_writes = []

    async def build_entity_write(entity_id: int) -> Optional[EntityWrite]:
        if entity_id == IDENTITY_ENTITY_ID:
            return None

        profile = entities.get_cached_profile(entity_id)
        if not profile:
            return None

        embedding = await entities.get_embedding_for_id(entity_id)
        return EntityWrite(
            entity_id=entity_id,
            is_new=entity_id in new_entity_ids,
            canonical_name=profile.canonical_name,
            entity_type=profile.entity_type,
            topic=profile.topic,
            embedding=_normalize_embedding(embedding),
            aliases=tuple(entities.get_mentions_for_id(entity_id)),
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
        profile = entities.get_cached_profile(entity_id)
        if not profile:
            continue
        canonical = profile.canonical_name
        if not canonical:
            continue
        entry = {
            "id": entity_id,
            "canonical_name": canonical,
            "type": profile.entity_type,
            "topic": profile.topic,
        }
        entity_lookup[canonical.lower()] = entry
        for mention in entities.get_mentions_for_id(entity_id):
            if mention:
                entity_lookup[mention.lower()] = entry

    relationship_writes = []
    skipped_relationships = []
    for observation in batch.relationship_observations:
        msg_id = observation.message_id
        pair_entity_a = observation.entity_a_name
        pair_entity_b = observation.entity_b_name
        ent_a_name = pair_entity_a.lower() if pair_entity_a else None
        ent_b_name = pair_entity_b.lower() if pair_entity_b else None

        ent_a = entity_lookup.get(ent_a_name) if ent_a_name else None
        ent_b = entity_lookup.get(ent_b_name) if ent_b_name else None

        if observation.identity_rooted:
            ent_a = {"id": IDENTITY_ENTITY_ID}

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

        relationship_writes.append(
            RelationshipWrite(
                entity_a_id=ent_a["id"],
                entity_b_id=ent_b["id"],
                relationship_type=observation.relationship_type,
                message_id=msg_id,
                confidence=observation.confidence,
                context=observation.context,
                observed_label=observation.observed_label,
                canonical_type=observation.canonical_type,
                domain_status=observation.domain_status,
                source_type=observation.source_type,
                target_type=observation.target_type,
                symmetric=observation.symmetric,
            )
        )

    batch_work_unit_id = batch.work_unit.id if batch.work_unit else None
    graph_work_unit = WorkRecord.for_graph_write(scope, batch_id=batch_work_unit_id)
    message_entity_refs = [
        MessageEntityRef(message_id=message_id, entity_id=entity_id)
        for entity_id in sorted(safe_entity_ids)
        for message_id in sorted(set(batch.entity_message_map.get(entity_id, [])))
    ]
    batch.set_graph_write_buffers(
        graph_work_unit=graph_work_unit,
        safe_entity_ids=safe_entity_ids,
        graph_alias_updates=alias_updates,
        entity_writes=entity_writes,
        relationship_writes=relationship_writes,
        message_entity_refs=message_entity_refs,
        # Eligibility is a canonical message lifecycle transition, not a
        # side-effect of whether entity extraction happened to run.
        eligible_messages=(),
        skipped_relationships=skipped_relationships,
        zombie_entity_ids=zombie_entity_ids,
        dirty_entity_ids=set(safe_entity_ids),
    )


def _attach_graph_work_summary(
    batch: IngestionBatch,
    graph_work,
    summary: GraphWriteSummary,
) -> None:
    if batch.work_unit is None:
        return
    batch.work_unit.metadata["graph_write"] = asdict(summary)
    batch.work_unit.metadata["graph_write_work_unit_id"] = graph_work.id
    batch.work_unit.metadata["graph_write_work_record"] = graph_work.snapshot()


async def _execute_graph_write_buffers(
    *,
    scope: ExecutionScope,
    alias_updates: list[AliasUpdate],
    entity_writes: list[EntityWrite],
    relationship_writes: list[RelationshipWrite],
    message_entity_refs: list[MessageEntityRef],
    eligible_messages: list[EpisodeEligibility],
    dirty_entity_ids: set[int],
    zombie_entity_ids: set[int],
    skipped_relationships: list[SkippedRelationship],
    knowledge_store: IngestionGraphPersistence,
    redis_client: aioredis.Redis = None,
) -> GraphWriteSummary:
    """Persist one already-prepared set of graph write buffers."""

    alias_update_map = {
        update.entity_id: list(update.aliases)
        for update in alias_updates
        if update.aliases
    }
    if alias_update_map:
        await knowledge_store.update_entity_aliases(
            alias_update_map, project_id=scope.project_id
        )

    postgres_committed = False
    if entity_writes or relationship_writes or message_entity_refs or eligible_messages:
        await knowledge_store.write_batch(
            entity_writes,
            relationship_writes,
            message_entity_refs=message_entity_refs,
            eligible_messages=eligible_messages,
            scope=scope,
        )
        postgres_committed = True

    dirty_entities_marked = 0
    if redis_client is not None and dirty_entity_ids:
        try:
            dirty_key = RedisKeys.dirty_entities(scope.user_name, scope.project_id)
            dirty_entities_marked = await redis_client.sadd(
                dirty_key,
                *[str(entity_id) for entity_id in sorted(dirty_entity_ids)],
            )
            await redis_client.delete(
                RedisKeys.project_profile_complete(scope.user_name, scope.project_id)
            )
            await emit(
                scope.project_id,
                "job",
                "dirty_entities_marked",
                {
                    "user_name": scope.user_name,
                    "project_id": scope.project_id,
                    "dirty_key": dirty_key,
                    "entity_ids": sorted(dirty_entity_ids),
                    "marked_count": dirty_entities_marked,
                    "reason": "graph_write",
                },
            )
        except Exception as exc:
            if postgres_committed:
                raise GraphWritePostgresCommittedError(str(exc)) from exc
            raise

    return GraphWriteSummary(
        entities_written=len(entity_writes),
        relationships_written=len(relationship_writes),
        aliases_updated=len(alias_update_map),
        dirty_entities_marked=dirty_entities_marked,
        zombies_filtered=len(zombie_entity_ids),
        relationships_skipped=len(skipped_relationships),
    )


async def write_ingestion_batch_to_graph(
    batch: IngestionBatch,
    knowledge_store: IngestionGraphPersistence,
    entities: EntityResolver,
    redis_client: aioredis.Redis = None,
) -> GraphWriteSummary:
    """Persist one sealed batch under its diagnostic correlation scope."""

    with diagnostic_scope(
        user_name=batch.scope.user_name,
        project_id=batch.scope.project_id,
        session_id=batch.scope.session_id,
        ingestion_batch_id=batch.batch_id,
        work_id=batch.work_unit.id,
    ):
        return await _write_ingestion_batch_to_graph(
            batch,
            knowledge_store,
            entities,
            redis_client=redis_client,
        )


async def _write_ingestion_batch_to_graph(
    batch: IngestionBatch,
    knowledge_store: IngestionGraphPersistence,
    entities: EntityResolver,
    redis_client: aioredis.Redis = None,
) -> GraphWriteSummary:
    """Prepare and persist graph buffers directly from one ingestion aggregate."""

    # DLQ replays may carry an already-prepared, sealed graph command set.  In
    # that case the commands are the durable recovery boundary and must not be
    # rebuilt from an in-memory resolver that may belong to a prior process.
    if not (
        batch.sealed
        and batch.stage.value in {"sealed", "graph_committed"}
        and batch.graph_work_unit is not None
    ):
        try:
            await prepare_ingestion_batch_graph_writes(batch, knowledge_store, entities)
        except asyncio.CancelledError:
            batch.cancel_work("Ingestion cancelled while preparing graph writes")
            raise
        batch.seal_for_commit()
    batch.require_sealed_for_commit()
    graph_work = batch.graph_work_unit
    if graph_work is None:
        raise RuntimeError("Prepared IngestionBatch requires graph work telemetry")
    graph_work.mark_running()
    if not (
        batch.entity_writes
        or batch.relationship_writes
        or batch.message_entity_refs
        or batch.eligible_messages
        or batch.graph_alias_updates
    ):
        summary = GraphWriteSummary(
            zombies_filtered=len(batch.zombie_entity_ids),
            relationships_skipped=len(batch.skipped_relationships),
        )
        graph_work.mark_skipped("No graph writes")
        _attach_graph_work_summary(batch, graph_work, summary)
        batch.mark_graph_committed()
        return summary

    try:
        summary = await _execute_graph_write_buffers(
            scope=batch.scope,
            alias_updates=batch.graph_alias_updates,
            entity_writes=batch.entity_writes,
            relationship_writes=batch.relationship_writes,
            message_entity_refs=batch.message_entity_refs,
            eligible_messages=batch.eligible_messages,
            dirty_entity_ids=batch.dirty_entity_ids,
            zombie_entity_ids=batch.zombie_entity_ids,
            skipped_relationships=batch.skipped_relationships,
            knowledge_store=knowledge_store,
            redis_client=redis_client,
        )
    except asyncio.CancelledError:
        batch.cancel_work("Ingestion cancelled while writing graph data")
        raise
    except GraphWritePostgresCommittedError as exc:
        graph_work.mark_failed(str(exc))
        batch.work_unit.metadata["postgres_graph_committed"] = True
        raise
    except Exception as exc:
        graph_work.mark_failed(str(exc))
        raise

    graph_work.mark_succeeded(
        f"{summary.entities_written} entities, {summary.relationships_written} relationships"
    )
    for alias_update in batch.graph_alias_updates:
        entities.commit_new_aliases(alias_update.entity_id, list(alias_update.aliases))
    _attach_graph_work_summary(batch, graph_work, summary)
    batch.mark_graph_committed()
    return summary


async def write_batch_callback(
    batch: IngestionBatch,
    knowledge_store: IngestionGraphPersistence,
    entities: EntityResolver,
    session_id: str,
    project_id: str,
    user_name: str = None,
    redis_client: aioredis.Redis = None,
) -> tuple[bool, Optional[str]]:
    """Callback wrapper matching the DLQ's expected signature.

    Returns (success, error_message).
    """
    try:
        await write_ingestion_batch_to_graph(
            batch,
            knowledge_store,
            entities,
            redis_client=redis_client,
        )
        return True, None
    except Exception as e:
        logger.error(f"Graph write callback failed: {e}")
        postgres_graph_committed = bool(
            batch.work_unit.metadata.get("postgres_graph_committed")
        )
        if batch.new_entity_ids and not postgres_graph_committed:
            # We must aggressively purge these newly created entities from the cache.
            # If we leave them, the live pipeline might resolve future mentions to these
            # "phantom" IDs before the DLQ can retry. This would cause the live pipeline
            # to trip the zombie check against an ID missing from the DB.
            entities.remove_entities(list(batch.new_entity_ids))
            logger.info(
                f"Cleaned {len(batch.new_entity_ids)} phantom entities from entities"
            )
        return False, str(e)
