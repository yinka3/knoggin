from __future__ import annotations

from typing import Dict, List, Optional, Set, Tuple

from common.conf.relationship_config import normalize_relationship
from common.exceptions import LLMBudgetExceededError, LLMResponseError
from common.schema.ingestion.contracts import RelationshipObservation, ValidationIssue
from common.schema.ingestion.extraction import RelationshipExtraction
from common.utils.core_utils import format_vp02_input
from common.utils.events import emit
from common.utils.local_references import build_local_id_maps, resolve_local_id
from core.ingestion.batch import IngestionBatch
from core.ingestion.prompts import get_connection_reasoning_prompt
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.llm_client import LLMService


class RelationshipExtractor:
    """Build validated in-memory relationship observations for one batch."""

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
    def _record_issue(
        issues: Optional[List[ValidationIssue]],
        *,
        stage: str,
        code: str,
        message: str,
        severity: str = "warning",
        item_ref: Optional[str] = None,
        metadata: Optional[Dict] = None,
    ) -> None:
        if issues is None:
            return

        issues.append(
            ValidationIssue(
                stage=stage,
                code=code,
                message=message,
                severity=severity,
                item_ref=item_ref,
                metadata=metadata or {},
            )
        )

    @staticmethod
    def _normalize_name(name: str) -> str:
        return (name or "").strip().casefold()

    async def _build_connection_candidates(
        self,
        batch: IngestionBatch,
    ) -> Tuple[List[Dict], Set[str], Dict[str, set], Dict[str, str], Dict[str, str]]:
        candidates = []
        valid_entity_names = set()
        entity_source_msgs_by_name: Dict[str, set] = {}
        canonical_name_by_name: Dict[str, str] = {}
        entity_type_by_name: Dict[str, str] = {}

        for ent_id in batch.entity_ids:
            source_msgs = set(batch.entity_message_map.get(ent_id, []))
            pending = batch.pending_entity_writes.get(ent_id)
            profile = (
                None if pending is not None else await self.entities.get_profile(ent_id)
            )
            if pending is None and not profile:
                self._record_issue(
                    batch.issues,
                    stage="connections",
                    code="connection_candidate_profile_missing",
                    message="Connection extraction candidate entity has no profile",
                    item_ref=str(ent_id),
                    metadata={
                        "entity_id": ent_id,
                        "source_msgs": sorted(source_msgs),
                    },
                )
                continue

            canonical_name = (
                pending.canonical_name
                if pending is not None
                else profile.canonical_name
            )
            entity_type = (
                pending.entity_type if pending is not None else profile.entity_type
            )
            mentions = (
                list(pending.aliases)
                if pending is not None
                else self.entities.get_mentions_for_id(ent_id)
            )
            for name in [canonical_name, *mentions]:
                normalized = self._normalize_name(name)
                if not normalized:
                    continue
                valid_entity_names.add(normalized)
                entity_source_msgs_by_name[normalized] = source_msgs
                canonical_name_by_name[normalized] = canonical_name
                entity_type_by_name[normalized] = entity_type

            candidates.append(
                {
                    "canonical_name": canonical_name,
                    "type": entity_type,
                    "mentions": mentions,
                    "source_msgs": sorted(source_msgs),
                }
            )

        return (
            candidates,
            valid_entity_names,
            entity_source_msgs_by_name,
            canonical_name_by_name,
            entity_type_by_name,
        )

    async def extract(
        self,
        batch: IngestionBatch,
    ) -> List[RelationshipObservation]:
        """Extract connections using state owned by one ingestion batch."""

        if not isinstance(batch, IngestionBatch):
            raise TypeError("RelationshipExtractor.extract requires an IngestionBatch")
        if not batch.entity_ids:
            return []
        messages = batch.messages
        session_text = batch.session_text
        session_id = batch.scope.session_id
        trace = batch.trace
        issues = batch.issues

        trace.relationship_model = getattr(self.llm, "extraction_model", None)
        trace.relationship_prompt = "VEGAPUNK-02"

        (
            candidates,
            valid_entity_names,
            entity_source_msgs_by_name,
            canonical_name_by_name,
            entity_type_by_name,
        ) = await self._build_connection_candidates(batch)

        if not candidates:
            return []

        system_03 = get_connection_reasoning_prompt(self.user_name)
        message_local_ids, message_ids_by_local = build_local_id_maps(
            (message["id"] for message in messages),
            "m",
        )
        user_03 = format_vp02_input(
            candidates,
            [
                {
                    "id": m["id"],
                    "text": m["message"],
                    "role": m.get("role"),
                }
                for m in messages
            ],
            session_text,
            user_name=self.user_name,
            message_local_ids=message_local_ids,
            relationship_block=batch.policy.domain.relationship_block,
        )

        await emit(
            session_id,
            "pipeline",
            "llm_call",
            {"stage": "connections", "prompt": user_03},
            verbose_only=True,
        )

        try:
            conn_result: RelationshipExtraction = await self.llm.generate_structured(
                response_model=RelationshipExtraction,
                system=system_03,
                user=user_03,
                temperature=0.0,
            )
        except LLMBudgetExceededError:
            # Budget exhaustion is a recoverable admission pause, not an empty
            # extraction result.  Let the durable worker leave this batch ready
            # for retry after the user resets or increases the budget.
            raise
        if conn_result is None:
            raise LLMResponseError("VP-02 connection extraction returned no result")

        if not conn_result.connections and not conn_result.user_connections:
            if trace is not None:
                trace.relationships_seen = 0
                trace.user_relationships_seen = 0
            return []

        valid_msg_ids = {m["id"] for m in messages}
        if trace is not None:
            trace.relationships_seen = len(conn_result.connections)
            trace.user_relationships_seen = len(conn_result.user_connections)

        observations: List[RelationshipObservation] = []
        seen_relationships = set()
        for conn in conn_result.connections:
            entity_a_key = self._normalize_name(conn.entity_a)
            entity_b_key = self._normalize_name(conn.entity_b)
            canonical_a = canonical_name_by_name.get(entity_a_key)
            canonical_b = canonical_name_by_name.get(entity_b_key)
            try:
                actual_msg_id = int(resolve_local_id(conn.msg_id, message_ids_by_local))
            except ValueError:
                if trace is not None:
                    trace.relationships_rejected += 1
                await emit(
                    session_id,
                    "pipeline",
                    "local_reference_resolution_failed",
                    {
                        "pipeline": "relationships",
                        "reference_type": "message",
                        "reason": "unknown_id",
                    },
                )
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_msg_id",
                    message=f"VP-02 returned invalid local msg_id {conn.msg_id}",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "valid_msg_ids": sorted(message_ids_by_local),
                    },
                )
                continue
            if actual_msg_id not in valid_msg_ids:
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_msg_id",
                    message="VP-02 local msg_id resolved outside the current message set",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={"msg_id": conn.msg_id},
                )
                continue
            if (
                entity_a_key not in valid_entity_names
                or entity_b_key not in valid_entity_names
            ):
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_entity_name",
                    message="VP-02 returned a relationship with an unknown entity name",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "entity_a": conn.entity_a,
                        "entity_b": conn.entity_b,
                    },
                )
                continue
            if canonical_a == canonical_b:
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="self_relationship",
                    message="VP-02 returned a self relationship",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "entity_a": conn.entity_a,
                        "entity_b": conn.entity_b,
                        "canonical_name": canonical_a,
                    },
                )
                continue
            entity_a_source_msgs = entity_source_msgs_by_name.get(entity_a_key, set())
            entity_b_source_msgs = entity_source_msgs_by_name.get(entity_b_key, set())
            if (
                actual_msg_id not in entity_a_source_msgs
                or actual_msg_id not in entity_b_source_msgs
            ):
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_relationship_evidence_msg_id",
                    message=(
                        "VP-02 returned a relationship msg_id that was not a "
                        "source message for both entities"
                    ),
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_a_source_msgs": sorted(entity_a_source_msgs),
                        "entity_b_source_msgs": sorted(entity_b_source_msgs),
                    },
                )
                continue
            source_type = entity_type_by_name.get(entity_a_key)
            target_type = entity_type_by_name.get(entity_b_key)
            normalization = normalize_relationship(
                batch.policy.domain,
                conn.relationship,
                source_type=source_type,
                target_type=target_type,
            )
            relationship_key = (
                actual_msg_id,
                (
                    tuple(
                        sorted(
                            (
                                self._normalize_name(canonical_a),
                                self._normalize_name(canonical_b),
                            )
                        )
                    )
                    if normalization.symmetric
                    else (
                        self._normalize_name(canonical_a),
                        self._normalize_name(canonical_b),
                    )
                ),
                normalization.persistence_type,
            )
            if relationship_key in seen_relationships:
                if trace is not None:
                    trace.relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="duplicate_relationship",
                    message="VP-02 returned a duplicate relationship",
                    item_ref=f"{conn.entity_a}->{conn.entity_b}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_a": canonical_a,
                        "entity_b": canonical_b,
                        "relationship": conn.relationship,
                    },
                )
                continue
            seen_relationships.add(relationship_key)
            observations.append(
                RelationshipObservation(
                    message_id=actual_msg_id,
                    entity_a_name=canonical_a,
                    entity_b_name=canonical_b,
                    relationship_type=normalization.persistence_type,
                    observed_label=normalization.observed_label,
                    canonical_type=normalization.canonical_type,
                    domain_status=normalization.domain_status,
                    source_type=normalization.source_type,
                    target_type=normalization.target_type,
                    symmetric=normalization.symmetric,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                )
            )
            if trace is not None:
                if normalization.domain_status == "recognized":
                    trace.relationships_recognized += 1
                else:
                    trace.relationships_unrecognized += 1
                trace.relationships_accepted += 1

        seen_user_connections = set()
        user_name_key = self._normalize_name(self.user_name)
        for conn in conn_result.user_connections:
            entity_name_key = self._normalize_name(conn.entity_name)
            canonical_entity_name = canonical_name_by_name.get(entity_name_key)
            try:
                actual_msg_id = int(resolve_local_id(conn.msg_id, message_ids_by_local))
            except ValueError:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                await emit(
                    session_id,
                    "pipeline",
                    "local_reference_resolution_failed",
                    {
                        "pipeline": "relationships",
                        "reference_type": "message",
                        "reason": "unknown_id",
                    },
                )
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_msg_id",
                    message=(
                        "VP-02 returned invalid local user connection msg_id "
                        f"{conn.msg_id}"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "valid_msg_ids": sorted(message_ids_by_local),
                    },
                )
                continue
            if actual_msg_id not in valid_msg_ids:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_msg_id",
                    message=(
                        "VP-02 local user connection msg_id resolved outside the "
                        "current message set"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={"msg_id": conn.msg_id},
                )
                continue
            if entity_name_key not in valid_entity_names:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_entity",
                    message=(
                        "VP-02 returned a user connection with an unknown entity name"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={"entity_name": conn.entity_name},
                )
                continue
            entity_source_msgs = entity_source_msgs_by_name.get(entity_name_key, set())
            if actual_msg_id not in entity_source_msgs:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="invalid_user_connection_evidence_msg_id",
                    message=(
                        "VP-02 returned a user connection msg_id that was not "
                        "a source message for the target entity"
                    ),
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_source_msgs": sorted(entity_source_msgs),
                    },
                )
                continue
            if (
                entity_name_key == user_name_key
                or self._normalize_name(canonical_entity_name) == user_name_key
            ):
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="self_user_connection",
                    message="VP-02 returned a user connection to the user root itself",
                    item_ref=f"user->{conn.entity_name}",
                )
                continue
            source_type = batch.policy.domain.canonical_entity_type("Identity")
            target_type = entity_type_by_name.get(entity_name_key)
            normalization = normalize_relationship(
                batch.policy.domain,
                conn.relationship,
                source_type=source_type,
                target_type=target_type,
            )
            user_relationship_key = (
                actual_msg_id,
                self._normalize_name(canonical_entity_name),
                normalization.persistence_type,
            )
            if user_relationship_key in seen_user_connections:
                if trace is not None:
                    trace.user_relationships_rejected += 1
                self._record_issue(
                    issues,
                    stage="connections",
                    code="duplicate_user_connection",
                    message="VP-02 returned a duplicate user relationship",
                    item_ref=f"user->{conn.entity_name}",
                    metadata={
                        "msg_id": conn.msg_id,
                        "entity_name": canonical_entity_name,
                        "relationship": conn.relationship,
                    },
                )
                continue
            seen_user_connections.add(user_relationship_key)
            observations.append(
                RelationshipObservation(
                    message_id=actual_msg_id,
                    entity_a_name=self.user_name,
                    entity_b_name=canonical_entity_name,
                    relationship_type=normalization.persistence_type,
                    observed_label=normalization.observed_label,
                    canonical_type=normalization.canonical_type,
                    domain_status=normalization.domain_status,
                    source_type=normalization.source_type,
                    target_type=normalization.target_type,
                    symmetric=normalization.symmetric,
                    confidence=conn.confidence,
                    context=conn.context or conn.relationship,
                    identity_rooted=True,
                )
            )
            if trace is not None:
                if normalization.domain_status == "recognized":
                    trace.relationships_recognized += 1
                else:
                    trace.relationships_unrecognized += 1
                trace.user_relationships_accepted += 1

        return observations
