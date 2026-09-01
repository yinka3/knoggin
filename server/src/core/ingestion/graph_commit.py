"""Build and commit one in-memory ingestion result atomically."""

from __future__ import annotations

from dataclasses import asdict
from typing import TYPE_CHECKING, Optional

from loguru import logger

from common.schema.ingestion.contracts import (
    AliasUpdate,
    EntityWrite,
    GraphWriteSummary,
    IngestionCommit,
    MessageEntityRef,
    MessageSourceTime,
    RelationshipWrite,
    SkippedRelationship,
)
from common.scoping import IDENTITY_ENTITY_ID
from common.utils.diagnostic_context import diagnostic_scope
from core.ingestion.batch import IngestionBatch
from core.knowledge.entity.resolver import EntityResolver

if TYPE_CHECKING:
    from core.knowledge.store import KnowledgeStore


def _normalize_embedding(value) -> Optional[list[float]]:
    if value is None:
        return None
    if hasattr(value, "tolist"):
        return value.tolist()
    return list(value)


async def build_ingestion_commit(
    batch: IngestionBatch,
    knowledge_store: KnowledgeStore,
    entities: EntityResolver,
) -> tuple[IngestionCommit, tuple[EntityWrite, ...], tuple[AliasUpdate, ...], int]:
    """Translate one active in-memory learning result into a durable change set."""

    if not isinstance(batch, IngestionBatch):
        raise TypeError("build_ingestion_commit requires an IngestionBatch")
    if batch.released:
        raise ValueError("Ingestion commit requires an active IngestionBatch")

    scope = batch.scope
    new_entity_ids = set(batch.new_entity_ids)
    existing_candidates = list(
        (
            set(batch.entity_ids)
            | set(batch.alias_updated_ids)
            | set(batch.alias_updates)
        )
        - new_entity_ids
    )
    valid_existing_ids: set[int] = set()
    if existing_candidates:
        validation_result = await knowledge_store.validate_existing_ids(
            existing_candidates,
            visible_project_ids=entities.readable_project_ids,
        )
        valid_existing_ids = validation_result
        missing_existing_ids = set(existing_candidates) - valid_existing_ids
        if missing_existing_ids:
            logger.warning(
                "Skipping {} entity references absent from durable graph state",
                len(missing_existing_ids),
            )

    writable_entity_ids = valid_existing_ids | new_entity_ids
    alias_updates = tuple(
        AliasUpdate(entity_id=entity_id, aliases=tuple(aliases))
        for entity_id, aliases in batch.alias_updates.items()
        if aliases and entity_id in writable_entity_ids
    )

    async def entity_write_for(entity_id: int) -> Optional[EntityWrite]:
        if entity_id == IDENTITY_ENTITY_ID:
            return None
        pending = batch.pending_entity_writes.get(entity_id)
        if pending is not None:
            return pending
        profile = entities.get_cached_profile(entity_id)
        if profile is None:
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

    entity_writes: list[EntityWrite] = []
    # Every accepted identity needs a context in this project before its
    # message provenance can be committed. Existing identities may have been
    # discovered through another readable project, so alias changes alone are
    # not the admission signal here.
    for entity_id in sorted(writable_entity_ids):
        write = await entity_write_for(entity_id)
        if write is not None:
            entity_writes.append(write)

    entity_lookup: dict[str, dict[str, object]] = {}
    for entity_id in writable_entity_ids:
        pending = batch.pending_entity_writes.get(entity_id)
        if pending is not None:
            canonical_name = pending.canonical_name
            entity_type = pending.entity_type
            aliases = pending.aliases
        else:
            profile = entities.get_cached_profile(entity_id)
            if profile is None:
                continue
            canonical_name = profile.canonical_name
            entity_type = profile.entity_type
            aliases = entities.get_mentions_for_id(entity_id)
        entry = {
            "id": entity_id,
            "canonical_name": canonical_name,
            "type": entity_type,
        }
        entity_lookup[canonical_name.casefold()] = entry
        for alias in aliases:
            if alias:
                entity_lookup[alias.casefold()] = entry

    relationship_writes: list[RelationshipWrite] = []
    skipped_relationships: list[SkippedRelationship] = []
    for observation in batch.relationship_observations:
        source_name = observation.entity_a_name
        target_name = observation.entity_b_name
        source = entity_lookup.get(source_name.casefold()) if source_name else None
        target = entity_lookup.get(target_name.casefold()) if target_name else None
        if observation.identity_rooted:
            source = {"id": IDENTITY_ENTITY_ID}
        if source is None or target is None:
            skipped_relationships.append(
                SkippedRelationship(
                    entity_a=source_name,
                    entity_b=target_name,
                    message_id=observation.message_id,
                    reason="entity_missing_from_durable_commit",
                    metadata={
                        "entity_a_found": source is not None,
                        "entity_b_found": target is not None,
                    },
                )
            )
            continue
        relationship_writes.append(
            RelationshipWrite(
                entity_a_id=int(source["id"]),
                entity_b_id=int(target["id"]),
                relationship_type=observation.relationship_type,
                message_id=observation.message_id,
                context=observation.context,
                observed_label=observation.observed_label,
                canonical_type=observation.canonical_type,
                domain_status=observation.domain_status,
                source_type=observation.source_type,
                target_type=observation.target_type,
                symmetric=observation.symmetric,
                domain_version=observation.domain_version,
            )
        )

    message_entity_refs = tuple(
        MessageEntityRef(message_id=message_id, entity_id=entity_id)
        for entity_id in sorted(writable_entity_ids)
        for message_id in sorted(set(batch.entity_message_map.get(entity_id, [])))
    )
    commit = IngestionCommit(
        scope=scope,
        batch_id=batch.batch_id,
        message_ids=tuple(int(message["id"]) for message in batch.messages),
        source_message_times=tuple(
            MessageSourceTime(
                message_id=int(message["id"]),
                timestamp_ms=message.get("timestamp"),
            )
            for message in batch.messages
        ),
        entity_writes=tuple(entity_writes),
        alias_updates=alias_updates,
        message_entity_refs=message_entity_refs,
        relationship_writes=tuple(relationship_writes),
    )
    return commit, tuple(entity_writes), alias_updates, len(skipped_relationships)


async def write_ingestion_batch_to_graph(
    batch: IngestionBatch,
    knowledge_store: KnowledgeStore,
    entities: EntityResolver,
) -> GraphWriteSummary:
    """Commit an active batch and refresh resolver state after success only."""

    with diagnostic_scope(
        user_name=batch.scope.user_name,
        project_id=batch.scope.project_id,
        session_id=batch.scope.session_id,
        ingestion_batch_id=batch.batch_id,
        work_id=batch.work_unit.id,
    ):
        commit, entity_writes, alias_updates, skipped_relationships = (
            await build_ingestion_commit(batch, knowledge_store, entities)
        )
        summary = await knowledge_store.commit_ingestion(commit)

        summary.relationships_skipped = skipped_relationships
        batch.work_unit.metadata["graph_write"] = asdict(summary)
        try:
            entities.apply_committed_entity_writes(entity_writes)
            for alias_update in alias_updates:
                entities.commit_new_aliases(
                    alias_update.entity_id, list(alias_update.aliases)
                )
        except Exception as exc:
            logger.warning(
                "Ingestion commit {} succeeded but resolver cache refresh failed: {}",
                batch.batch_id,
                exc,
            )
        return summary
