"""Assembly for Context-first entity extraction."""

from __future__ import annotations

import json
import re

from loguru import logger

from common.schema.ingestion.contracts import (
    ContextEntityResult,
    MessageEntityRef,
    ResolvedContextBlockMention,
)
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.text_processor import TextProcessor
from core.knowledge.entity.resolver import ContextEntityResolution, EntityResolver
from infrastructure.jev_client import JevWorkBudget


def _log_jev_identity_observations(identity_decisions) -> None:
    """Emit bounded, evidence-free pilot records to the configured log sinks."""

    for decision in identity_decisions:
        observation = decision.get("jev")
        if observation is not None:
            # This excludes raw evidence and remains bounded by the admitted
            # candidate and call limits. JSON remains usable in plain log sinks.
            logger.bind(jev_identity_observation=True).info(
                "jev_identity_observation {}",
                json.dumps(observation, sort_keys=True, separators=(",", ":")),
            )


def _log_jev_classification_observations(classification_decisions) -> None:
    """Emit bounded, evidence-free topic proposals for pilot review."""

    for observation in classification_decisions:
        logger.bind(jev_classification_observation=True).info(
            "jev_classification_observation {}",
            json.dumps(observation, sort_keys=True, separators=(",", ":")),
        )


def _literal_mention_is_present(mention: str, text: str) -> bool:
    """Require a literal, token-bounded canonical-message occurrence."""

    normalized = " ".join(mention.split())
    if not normalized or not text:
        return False
    expression = r"\s+".join(re.escape(token) for token in normalized.split())
    if normalized[0].isalnum():
        expression = r"(?<!\w)" + expression
    if normalized[-1].isalnum():
        expression = expression + r"(?!\w)"
    return re.search(expression, text, flags=re.IGNORECASE) is not None


def assemble_context_entity_result(
    build: SemanticWindowBuild,
    resolution: ContextEntityResolution,
) -> ContextEntityResult:
    """Turn resolver output into pending writes and literal message references.

    Context supports prove that a block is grounded. They do not prove that every
    supporting message contains every extracted entity, so message refs are
    derived only after a literal check against the frozen canonical messages.
    """

    if not isinstance(build, SemanticWindowBuild):
        raise TypeError("Context entity assembly requires a SemanticWindowBuild")
    resolved_mentions = resolution.resolved_mentions
    if any(
        not isinstance(item, ResolvedContextBlockMention) for item in resolved_mentions
    ):
        raise TypeError("Context resolution must contain typed resolved mentions")
    message_refs: dict[tuple[int, int], MessageEntityRef] = {}
    for resolved in resolved_mentions:
        for block_id in resolved.mention.block_ids:
            for support in build.block_supports.get(block_id, ()):
                message_text = build.message_text_by_id.get(support.message_id)
                if message_text is None:
                    continue
                if _literal_mention_is_present(resolved.mention.name, message_text):
                    key = (support.message_id, resolved.entity_id)
                    message_refs[key] = MessageEntityRef(
                        message_id=support.message_id,
                        entity_id=resolved.entity_id,
                    )

    result = ContextEntityResult(
        entity_ids=resolution.entity_ids,
        new_entity_ids=resolution.new_entity_ids,
        alias_updated_ids=resolution.alias_updated_ids,
        alias_updates=resolution.alias_updates,
        pending_entity_writes=resolution.pending_entity_writes,
        project_classifications=resolution.project_classifications,
        block_entity_associations=resolution.block_entity_associations,
        message_entity_refs=tuple(message_refs.values()),
    )
    build.set_entity_result(result)
    return result


class ContextEntityBuildService:
    """Produce a complete pending entity result without mutating Knowledge."""

    def __init__(
        self,
        *,
        processor: TextProcessor,
        resolver: EntityResolver,
        allocate_entity_id,
    ) -> None:
        if not isinstance(processor, TextProcessor):
            raise TypeError("Context entity build requires a TextProcessor")
        if not isinstance(resolver, EntityResolver):
            raise TypeError("Context entity build requires an EntityResolver")
        if not callable(allocate_entity_id):
            raise TypeError("Context entity build requires an entity ID allocator")
        self.processor = processor
        self.resolver = resolver
        self._allocate_entity_id = allocate_entity_id

    async def build(self, semantic_build: SemanticWindowBuild) -> ContextEntityResult:
        """Extract and resolve one Context impact closure in memory only."""

        if semantic_build.jev_work_budget is None:
            semantic_build.jev_work_budget = JevWorkBudget(
                semantic_build.policy.jev.max_calls_per_window,
                semantic_build.policy.jev.max_elapsed_seconds_per_window,
            )
        semantic_build.identity_pass_number += 1
        mentions = await self.processor.extract_context_mentions(semantic_build)
        timing = {}
        resolution = await self.resolver.resolve_context_block_mentions(
            mentions,
            block_text_by_id={
                block.block_id: block.markdown
                for block in semantic_build.knowledge_input_blocks
            },
            policy=semantic_build.policy,
            allocate_entity_id=self._allocate_entity_id,
            window_id=semantic_build.window_id,
            pass_number=semantic_build.identity_pass_number,
            work_budget=semantic_build.jev_work_budget,
            timing=timing,
        )
        semantic_build.trace.resolver_timings.append(
            {
                "pass_number": semantic_build.identity_pass_number,
                **timing,
                "jev_calls_consumed": semantic_build.jev_work_budget.calls,
                "jev_remaining_seconds": semantic_build.jev_work_budget.remaining_seconds(),
            }
        )
        semantic_build.trace.identity_decisions.extend(resolution.identity_decisions)
        semantic_build.trace.classification_decisions.extend(
            resolution.classification_decisions
        )
        semantic_build.trace.classification_aggregates.extend(
            resolution.classification_aggregates
        )
        _log_jev_identity_observations(resolution.identity_decisions)
        _log_jev_classification_observations(resolution.classification_decisions)
        return assemble_context_entity_result(semantic_build, resolution)
