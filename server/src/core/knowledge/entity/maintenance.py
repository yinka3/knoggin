"""Deterministic duplicate discovery for explicit maintenance work."""

from __future__ import annotations

from typing import TYPE_CHECKING

from loguru import logger

if TYPE_CHECKING:
    from core.knowledge.entity.resolver import EntityResolver


class EntityMaintenance:
    """Own candidate discovery outside the live ingestion resolver flow."""

    def __init__(self, resolver: "EntityResolver") -> None:
        self._resolver = resolver

    async def discover_duplicate_candidates(self, dirty_ids: set[int] | None = None) -> list:
        """Return deterministic duplicate candidates for an explicit review."""

        resolver = self._resolver
        with resolver._lock:
            scan_targets = dirty_ids or resolver._index.iter_profile_ids()
        if not scan_targets:
            return []

        candidate_pairs = await resolver._collect_candidate_pairs(
            list(scan_targets),
            resolver._build_generic_tokens(resolver.get_alias_version()),
        )
        if not candidate_pairs:
            return []

        entity_ids = {
            entity_id for pair in candidate_pairs for entity_id in pair
        }
        evidence_by_entity = await resolver.knowledge_store.get_merge_evidence_for_entities(
            sorted(entity_ids),
            project_id=resolver.project_id,
        )
        candidates = []
        for pair, metadata in candidate_pairs.items():
            candidate = await resolver._classify_pair(
                *pair,
                metadata,
                evidence_by_entity,
            )
            if candidate:
                candidate["reasons"] = list(metadata["reasons"])
                candidates.append(candidate)
        logger.info("Duplicate maintenance discovery found {} candidates", len(candidates))
        return candidates
