"""Assemble one window's entity result from its Episode mentions."""

from __future__ import annotations

import json

from loguru import logger

from common.schema.ingestion.contracts import EntityResult
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.text_processor import TextProcessor
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.jev_client import JevWorkBudget


def _log_jev_observations(kind: str, observations) -> None:
    """Emit bounded, evidence-free pilot records to the configured log sinks."""

    for observation in observations:
        if observation is None:
            continue
        logger.bind(**{f"jev_{kind}_observation": True}).info(
            f"jev_{kind}_observation {{}}",
            json.dumps(observation, sort_keys=True, separators=(",", ":")),
        )


class EntityBuildService:
    """Produce a complete pending entity result without mutating Knowledge."""

    def __init__(
        self,
        *,
        processor: TextProcessor,
        resolver: EntityResolver,
        allocate_entity_id,
    ) -> None:
        if not isinstance(processor, TextProcessor):
            raise TypeError("Entity build requires a TextProcessor")
        if not isinstance(resolver, EntityResolver):
            raise TypeError("Entity build requires an EntityResolver")
        if not callable(allocate_entity_id):
            raise TypeError("Entity build requires an entity ID allocator")
        self.processor = processor
        self.resolver = resolver
        self._allocate_entity_id = allocate_entity_id

    async def build(self, semantic_build: SemanticWindowBuild) -> EntityResult:
        """Extract and resolve one window's mentions in memory only."""

        semantic_build.jev_work_budget = JevWorkBudget(
            semantic_build.policy.jev.max_calls_per_window,
            semantic_build.policy.jev.max_elapsed_seconds_per_window,
        )
        mentions = await self.processor.extract_mentions(semantic_build)
        timing: dict = {}
        resolution = await self.resolver.resolve_mentions(
            mentions,
            message_text_by_id=semantic_build.message_text_by_id,
            policy=semantic_build.policy,
            allocate_entity_id=self._allocate_entity_id,
            window_id=semantic_build.window_id,
            pass_number=semantic_build.identity_pass_number,
            work_budget=semantic_build.jev_work_budget,
            timing=timing,
        )
        trace = semantic_build.trace
        trace.resolver_timings.append({
            "pass_number": semantic_build.identity_pass_number,
            **timing,
            "jev_calls_consumed": semantic_build.jev_work_budget.calls,
            "jev_remaining_seconds": semantic_build.jev_work_budget.remaining_seconds(),
        })
        trace.identity_decisions[:] = resolution.identity_decisions
        trace.classification_decisions[:] = resolution.classification_decisions
        trace.classification_aggregates[:] = resolution.classification_aggregates
        _log_jev_observations(
            "identity", (d.get("jev") for d in resolution.identity_decisions)
        )
        _log_jev_observations("classification", resolution.classification_decisions)
        result = EntityResult(
            entity_ids=resolution.entity_ids,
            new_entity_ids=resolution.new_entity_ids,
            alias_updated_ids=resolution.alias_updated_ids,
            alias_updates=resolution.alias_updates,
            pending_entity_writes=resolution.pending_entity_writes,
            project_classifications=resolution.project_classifications,
            resolved_mentions=resolution.resolved_mentions,
        )
        semantic_build.set_entity_result(result)
        return result
