"""Context-grounded VP-02 relationship extraction for semantic windows."""

from __future__ import annotations

from typing import Dict
from uuid import UUID

from common.conf.relationship_config import normalize_relationship
from common.exceptions import LLMBudgetExceededError, LLMResponseError
from common.schema.ingestion.contracts import (
    ContextRelationshipWrite,
    ValidationIssue,
)
from common.schema.ingestion.extraction import ContextRelationshipExtraction
from common.scoping import IDENTITY_ENTITY_ID
from common.utils.core_utils import format_context_vp02_input
from common.utils.events import emit
from core.ingestion.batch import SemanticWindowBuild
from core.ingestion.prompts import get_context_connection_reasoning_prompt
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.llm_client import LLMService


class ContextRelationshipExtractor:
    """Run VP-02 against immutable Context blocks after entity resolution.

    This class has no message-event heuristics and never calls a GLiNER
    relationship API.
    """

    def __init__(
        self,
        *,
        user_name: str,
        llm: LLMService,
        entities: EntityResolver,
    ) -> None:
        self.user_name = user_name
        self.llm = llm
        self.entities = entities

    @staticmethod
    def _name_key(value: str) -> str:
        return value.strip().casefold()

    @staticmethod
    def _record_issue(
        build: SemanticWindowBuild,
        *,
        code: str,
        message: str,
        item_ref: str | None = None,
        metadata: Dict | None = None,
    ) -> None:
        build.issues.append(
            ValidationIssue(
                stage="context_connections",
                code=code,
                message=message,
                item_ref=item_ref,
                metadata=metadata or {},
            )
        )

    async def _candidates(
        self, build: SemanticWindowBuild, block_local_ids: Dict[UUID, str]
    ) -> tuple[list[Dict], Dict[str, tuple[int, str]]]:
        if build.entity_result is None:
            raise ValueError("Context VP-02 requires a resolved entity result")

        source_blocks_by_entity: Dict[int, set[UUID]] = {}
        for association in build.entity_result.block_entity_associations:
            source_blocks_by_entity.setdefault(association.entity_id, set()).add(
                association.block_id
            )

        candidates: list[Dict] = []
        names: Dict[str, tuple[int, str]] = {}
        for entity_id in build.entity_result.entity_ids:
            pending = build.entity_result.pending_entity_writes.get(entity_id)
            profile = None if pending is not None else await self.entities.get_profile(entity_id)
            if pending is None and profile is None:
                self._record_issue(
                    build,
                    code="context_connection_candidate_profile_missing",
                    message="Context VP-02 candidate has no visible entity profile",
                    item_ref=str(entity_id),
                )
                continue
            canonical_name = pending.canonical_name if pending is not None else profile.canonical_name
            entity_type = pending.entity_type if pending is not None else profile.entity_type
            aliases = list(pending.aliases) if pending is not None else self.entities.get_mentions_for_id(entity_id)
            key = self._name_key(canonical_name)
            if key in names and names[key][0] != entity_id:
                raise ValueError("Context VP-02 candidates cannot share canonical names")
            names[key] = (entity_id, entity_type)
            candidates.append(
                {
                    "canonical_name": canonical_name,
                    "type": entity_type,
                    "mentions": aliases,
                    "source_blocks": sorted(
                        block_local_ids[block_id]
                        for block_id in source_blocks_by_entity.get(entity_id, set())
                        if block_id in block_local_ids
                    ),
                }
            )

        identity_key = self._name_key(self.user_name)
        if identity_key not in names:
            identity_type = build.policy.domain.canonical_entity_type("Identity") or "Identity"
            names[identity_key] = (IDENTITY_ENTITY_ID, identity_type)
            candidates.append(
                {
                    "canonical_name": self.user_name,
                    "type": identity_type,
                    "mentions": (),
                    "source_blocks": (),
                }
            )
        return candidates, names

    async def extract(
        self, build: SemanticWindowBuild
    ) -> tuple[ContextRelationshipWrite, ...]:
        """Return validated multi-block relationship commands for one window."""

        if not isinstance(build, SemanticWindowBuild):
            raise TypeError("Context VP-02 requires a SemanticWindowBuild")
        if build.entity_result is None or not build.entity_result.entity_ids:
            build.set_relationship_writes(())
            return ()

        blocks = list(build.knowledge_input_blocks)
        if not blocks:
            build.set_relationship_writes(())
            return ()
        block_local_ids = {block.block_id: f"b{index}" for index, block in enumerate(blocks, 1)}
        local_blocks = {local_id: block_id for block_id, local_id in block_local_ids.items()}
        candidates, valid_names = await self._candidates(build, block_local_ids)
        if len(candidates) < 2:
            build.set_relationship_writes(())
            return ()

        build.trace.relationship_model = getattr(self.llm, "extraction_model", None)
        build.trace.relationship_prompt = "VEGAPUNK-02-CONTEXT"
        prompt = format_context_vp02_input(
            candidates,
            [
                {
                    "local_id": block_local_ids[block.block_id],
                    "section_key": block.section_key,
                    "markdown": block.markdown,
                }
                for block in blocks
            ],
            relationship_block=build.policy.domain.relationship_block,
        )
        await emit(
            str(build.window_id),
            "semantic",
            "llm_call",
            {"stage": "context_connections", "prompt": prompt},
            verbose_only=True,
        )
        try:
            result: ContextRelationshipExtraction = await self.llm.generate_structured(
                response_model=ContextRelationshipExtraction,
                system=get_context_connection_reasoning_prompt(self.user_name),
                user=prompt,
                temperature=0.0,
            )
        except LLMBudgetExceededError:
            raise
        if result is None:
            raise LLMResponseError("Context VP-02 relationship extraction returned no result")

        build.trace.relationships_seen = len(result.connections)
        writes_by_identity: Dict[tuple[int, int, str], ContextRelationshipWrite] = {}
        for mention in result.connections:
            endpoint_a = valid_names.get(self._name_key(mention.entity_a))
            endpoint_b = valid_names.get(self._name_key(mention.entity_b))
            if endpoint_a is None or endpoint_b is None:
                build.trace.relationships_rejected += 1
                self._record_issue(
                    build,
                    code="invalid_context_connection_entity",
                    message="Context VP-02 returned an unknown canonical entity",
                    item_ref=f"{mention.entity_a}->{mention.entity_b}",
                )
                continue
            if endpoint_a[0] == endpoint_b[0]:
                build.trace.relationships_rejected += 1
                self._record_issue(
                    build,
                    code="self_context_connection",
                    message="Context VP-02 returned a self relationship",
                    item_ref=mention.entity_a,
                )
                continue
            try:
                support_block_ids = tuple(local_blocks[local_id] for local_id in mention.block_ids)
            except KeyError:
                build.trace.relationships_rejected += 1
                self._record_issue(
                    build,
                    code="invalid_context_connection_block",
                    message="Context VP-02 cited a block outside the current impact closure",
                    item_ref=f"{mention.entity_a}->{mention.entity_b}",
                    metadata={"block_ids": mention.block_ids},
                )
                continue
            normalization = normalize_relationship(
                build.policy.domain,
                mention.relationship,
                source_type=endpoint_a[1],
                target_type=endpoint_b[1],
            )
            try:
                write = ContextRelationshipWrite(
                    support_block_ids=support_block_ids,
                    entity_a_id=endpoint_a[0],
                    entity_b_id=endpoint_b[0],
                    relationship_type=normalization.persistence_type,
                    observed_label=normalization.observed_label,
                    canonical_type=normalization.canonical_type,
                    domain_status=normalization.domain_status,
                    source_type=normalization.source_type,
                    target_type=normalization.target_type,
                    symmetric=normalization.symmetric,
                    domain_version=build.policy.domain.version,
                    context=mention.context or mention.relationship,
                )
            except ValueError as exc:
                build.trace.relationships_rejected += 1
                self._record_issue(
                    build,
                    code="invalid_context_connection",
                    message=str(exc),
                    item_ref=f"{mention.entity_a}->{mention.entity_b}",
                )
                continue
            identity = (
                write.entity_a_id,
                write.entity_b_id,
                write.relationship_type,
            )
            existing = writes_by_identity.get(identity)
            if existing is None or (
                len(write.support_block_ids), tuple(map(str, write.support_block_ids))
            ) < (
                len(existing.support_block_ids), tuple(map(str, existing.support_block_ids))
            ):
                writes_by_identity[identity] = write

        writes = tuple(writes_by_identity.values())
        build.trace.relationships_accepted = len(writes)
        build.trace.relationships_recognized = sum(
            write.domain_status == "recognized" for write in writes
        )
        build.trace.relationships_unrecognized = len(writes) - build.trace.relationships_recognized
        build.set_relationship_writes(writes)
        return writes
