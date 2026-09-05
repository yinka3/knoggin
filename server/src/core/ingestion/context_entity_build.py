"""Assembly for Context-first entity extraction."""

from __future__ import annotations

import re
from collections.abc import Mapping

from common.schema.ingestion.contracts import (
    ContextEntityResult,
    MessageEntityRef,
    ResolvedContextBlockMention,
)
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.text_processor import TextProcessor
from core.knowledge.entity.resolver import EntityResolver


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
    resolution: Mapping[str, object],
) -> ContextEntityResult:
    """Turn resolver output into pending writes and literal message references.

    Context supports prove that a block is grounded. They do not prove that every
    supporting message contains every extracted entity, so message refs are
    derived only after a literal check against the frozen canonical messages.
    """

    if not isinstance(build, SemanticWindowBuild):
        raise TypeError("Context entity assembly requires a SemanticWindowBuild")
    resolved_mentions = resolution.get("resolved_mentions")
    if not isinstance(resolved_mentions, tuple) or any(
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
        entity_ids=resolution["entity_ids"],
        new_entity_ids=resolution["new_entity_ids"],
        alias_updated_ids=resolution["alias_updated_ids"],
        alias_updates=resolution["alias_updates"],
        pending_entity_writes=resolution["pending_entity_writes"],
        block_entity_associations=resolution["block_entity_associations"],
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

        mentions = await self.processor.extract_context_mentions(semantic_build)
        resolution = await self.resolver.resolve_context_block_mentions(
            mentions,
            block_text_by_id={
                block.block_id: block.markdown
                for block in semantic_build.knowledge_input_blocks
            },
            policy=semantic_build.policy,
            allocate_entity_id=self._allocate_entity_id,
            issues=semantic_build.issues,
        )
        return assemble_context_entity_result(semantic_build, resolution)
