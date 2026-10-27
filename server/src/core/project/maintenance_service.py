"""Project-scoped knowledge maintenance and repair workflows."""

from __future__ import annotations

import asyncio
import hashlib
import json
from collections.abc import Awaitable, Callable, Iterable, Mapping
from uuid import UUID

from loguru import logger

from common.conf.relationship_config import normalize_relationship
from common.exceptions import MaintenanceConflictError
from common.schema.evidence import (
    EvidenceBundle,
    EvidenceNode,
    EvidencePointer,
    EvidenceSubject,
)
from common.schema.maintenance import (
    MaintenanceImpactItem,
    MaintenanceImpactPreview,
    OrphanedExchangeBlockage,
    SemanticWindowBlockage,
    SessionBlockageInspection,
)
from core.knowledge.conflict.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflict.conflict_service import (
    ConflictService,
    load_conflict_evidence,
    snapshot_conflict_evidence,
)
from core.knowledge.conflict.conflicts import (
    ConflictDiscoveryPackage,
    ConflictOrigin,
    ConflictResolutionKind,
    ConflictWriteResult,
    LLMConflictCandidate,
)
from core.knowledge.db.readers.conflict_discovery_reader import (
    ConflictDiscoveryReader,
)
from core.knowledge.db.readers.conflict_reader import ConflictReader
from core.knowledge.db.readers.relationship_observation_reader import (
    RelationshipObservationReader,
)
from core.knowledge.db.writers.conflict_writer import ConflictWriter
from core.knowledge.db.writers.maintenance_review_writer import MaintenanceReviewWriter
from core.knowledge.db.writers.relationship_advisory_writer import (
    RelationshipAdvisoryWriter,
)
from core.knowledge.db.writers.relationship_interpretation_writer import (
    RelationshipInterpretationWriter,
)
from core.knowledge.entity.identity_reassignment_writer import (
    IdentityReassignmentWriter,
)
from core.knowledge.evidence_service import EvidenceService
from core.knowledge.maintenance.maintenance_impact import MaintenanceImpactPlanner
from core.knowledge.maintenance.maintenance_reviews import (
    EntityContextChangePlan,
    IdentityReassignmentPlan,
    MaintenanceReviewDetail,
    RelationshipAdvisoryPlan,
    RelationshipInterpretationPlan,
)
from core.knowledge.relationship_advisories import (
    AdvisoryThresholds,
    RelationshipAdvisory,
    RelationshipAdvisoryDecision,
)
from core.project.domain_config_store import (
    DomainConfigConflict,
    DomainConfigStore,
)
from core.project.entity_cleanup import EntityCleanupWorkflow
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources

ProjectLookup = Callable[[str], Awaitable[dict | None]]


class ProjectMaintenanceService:
    """Own explicit project knowledge repair, review, and rebuild operations.

    Project lifecycle code supplies the project lookup and live-runtime state so
    these workflows share the same exclusion lock and cache invalidation rules
    without making the maintenance service responsible for runtime ownership.
    """

    def __init__(
        self,
        *,
        resources: RuntimeResources,
        user_name: str,
        project_lookup: ProjectLookup,
        active_projects: Mapping[str, ProjectRuntime],
        project_leases: Mapping[str, set[str]],
    ) -> None:
        self.resources = resources
        self.user_name = user_name
        self.pg = resources.postgres
        self._project_lookup = project_lookup
        self._active_projects = active_projects
        self._project_leases = project_leases
        self._lock = asyncio.Lock()
        self._domain_store = DomainConfigStore(self.pg)
        # Semantic review workflows live above KnowledgeStore. The persistence
        # facade remains focused on durable reads and writes.
        self._maintenance_reviews = MaintenanceReviewWriter(self.pg)
        self._conflict_writer = ConflictWriter(
            self.pg,
            reviews=self._maintenance_reviews,
        )
        self._conflict_service = ConflictService(self._conflict_writer)
        self._conflict_discovery_reader = ConflictDiscoveryReader(self.pg)
        self._conflict_reader = ConflictReader(self.pg)
        self._relationship_observation_reader = RelationshipObservationReader(self.pg)
        self._relationship_advisory_writer = RelationshipAdvisoryWriter(
            self.pg,
            reviews=self._maintenance_reviews,
        )
        self._relationship_interpretation_writer = RelationshipInterpretationWriter(
            self.pg,
            reviews=self._maintenance_reviews,
        )

    @property
    def lock(self) -> asyncio.Lock:
        """Lock shared with project lifecycle transitions."""

        return self._lock

    async def _require_domain_project(
        self,
        project_id: str,
        *,
        allow_archived: bool,
    ) -> dict:
        project = await self._project_lookup(project_id)
        if project is None:
            raise ValueError(f"Project '{project_id}' does not exist")
        allowed_statuses = {"active"}
        if allow_archived:
            allowed_statuses.add("archived")
        if project["status"] not in allowed_statuses:
            raise ValueError(
                f"Project '{project_id}' is {project['status']} and cannot use "
                "knowledge maintenance operations"
            )
        return project

    def _require_knowledge_store(self):
        knowledge_store = self.resources.knowledge_store
        if knowledge_store is None:
            raise RuntimeError("Knowledge storage is unavailable")
        return knowledge_store

    async def retry_semantic_window(self, project_id: str, window_id: str):
        """Explicitly resume one failed frozen semantic window.

        This is an operator workflow, not a new ingestion path: the durable
        window keeps its membership, policy snapshot, Context checkpoint, and
        stage.  If the project runtime is loaded, wake its sole semantic owner
        after the transaction makes the checkpoint eligible again.
        """

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            window = (
                await self._require_knowledge_store().retry_project_semantic_window(
                    window_id=window_id,
                    user_name=self.user_name,
                    project_id=project_id,
                )
            )
            if window is None:
                raise ValueError("Failed active semantic window is unavailable")
            runtime = self._active_projects.get(project_id)
            if runtime is not None:
                runtime.signal_semantic_work()
            return window

    async def flush_semantic_memory(self, project_id: str) -> bool:
        """Save finished conversation to memory now. Returns False when the
        project is not loaded; its pending conversation is then saved after
        the idle wait once it runs again."""

        await self._require_domain_project(project_id, allow_archived=False)
        runtime = self._active_projects.get(project_id)
        return runtime is not None and runtime.flush_semantic_work()

    async def inspect_semantic_window_blockages(
        self, project_id: str, *, limit: int = 100
    ) -> tuple[SemanticWindowBlockage, ...]:
        """Return bounded failed-window diagnostics without exposing storage rows."""

        await self._require_domain_project(project_id, allow_archived=True)
        windows = (
            await self._require_knowledge_store().list_failed_project_semantic_windows(
                user_name=self.user_name,
                project_id=project_id,
                limit=limit,
            )
        )
        return tuple(
            SemanticWindowBlockage(
                window_id=window.window_id,
                stage=window.stage,
                kind=(
                    "retry_exhausted"
                    if window.next_retry_at_ms is None
                    else "retry_scheduled"
                ),
                attempt_count=window.attempt_count,
                failure_stage=window.last_failure_stage,
                failure_code=window.last_failure_code,
                failed_at_ms=window.last_failure_at_ms,
                next_retry_at_ms=window.next_retry_at_ms,
            )
            for window in windows
        )

    async def inspect_session_blockages(
        self,
        project_id: str,
        *,
        stale_before_ms: int,
        limit: int = 100,
    ) -> SessionBlockageInspection:
        """Find old open exchanges only in sessions without live owners."""

        if (
            not isinstance(stale_before_ms, int)
            or isinstance(stale_before_ms, bool)
            or stale_before_ms < 0
        ):
            raise ValueError("stale_before_ms must be a non-negative integer")
        if (
            not isinstance(limit, int)
            or isinstance(limit, bool)
            or not 1 <= limit <= 100
        ):
            raise ValueError("limit must be between 1 and 100")
        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            live_sessions = self._project_leases.get(project_id, set())
            rows = await self.pg.fetch_all(
                """
                SELECT message.session_id, message.message_id, message.timestamp_ms,
                       count(*) OVER () AS total_count
                FROM public.messages AS message
                JOIN public.sessions AS session
                  ON session.user_name = message.user_name
                 AND session.project_id = message.project_id
                 AND session.session_id = message.session_id
                WHERE message.user_name = %s
                  AND message.project_id = %s
                  AND message.role = 'user'
                  AND message.exchange_state = 'open'
                  AND message.timestamp_ms <= %s
                  AND session.status = 'open'
                  AND NOT (message.session_id = ANY(%s::text[]))
                ORDER BY message.timestamp_ms ASC, message.message_id ASC
                LIMIT %s
                """,
                (
                    self.user_name,
                    project_id,
                    stale_before_ms,
                    list(live_sessions),
                    limit,
                ),
            )
        total = int(rows[0]["total_count"]) if rows else 0
        return SessionBlockageInspection(
            exchanges=tuple(
                OrphanedExchangeBlockage(
                    session_id=row["session_id"],
                    user_message_id=int(row["message_id"]),
                    opened_at_ms=int(row["timestamp_ms"]),
                )
                for row in rows
            ),
            total_count=total,
            truncated=total > len(rows),
        )

    async def repair_orphaned_exchange(
        self,
        project_id: str,
        *,
        session_id: str,
        user_message_id: int,
        expected_opened_at_ms: int,
        stale_before_ms: int,
    ):
        """Close one inspected orphan as failed after rechecking every guard."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            if session_id in self._project_leases.get(project_id, set()):
                raise ValueError("Cannot repair an exchange owned by a live session")
            closure = (
                await self._require_knowledge_store().close_orphaned_user_exchange(
                    user_name=self.user_name,
                    project_id=project_id,
                    session_id=session_id,
                    user_message_id=user_message_id,
                    expected_opened_at_ms=expected_opened_at_ms,
                    stale_before_ms=stale_before_ms,
                )
            )
            runtime = self._active_projects.get(project_id)
            if runtime is not None:
                runtime.signal_semantic_work()
            return closure

    @staticmethod
    def _validate_expected_domain_version(value: object) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError("expected_domain_version must be a non-negative integer")
        return value

    async def refresh_relationship_advisories(
        self,
        project_id: str,
        *,
        thresholds: AdvisoryThresholds | None = None,
    ) -> list[RelationshipAdvisory]:
        """Discover advisories and materialize their pending review state."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            domain = await self._domain_store.load(self.user_name, project_id)
            advisories = await self._relationship_observation_reader.get_advisories(
                user_name=self.user_name,
                project_id=project_id,
                thresholds=thresholds,
            )
            compiled_domain = domain.compile()
            advisories = [
                advisory
                for advisory in advisories
                if normalize_relationship(
                    compiled_domain,
                    advisory.observed_label,
                    source_type=advisory.source_type,
                    target_type=advisory.target_type,
                ).domain_status == "unrecognized"
            ]
            for advisory in advisories:
                if advisory.disposition != "pending":
                    continue
                evidence_ids = self._relationship_advisory_writer.evidence_ids(advisory)
                bundles = await self._require_knowledge_store().get_relationship_observations_evidence(
                    list(evidence_ids),
                    user_name=self.user_name,
                    project_id=project_id,
                )
                await self._relationship_advisory_writer.materialize_pending(
                    user_name=self.user_name,
                    project_id=project_id,
                    advisory=advisory,
                    domain_version=domain.version,
                    evidence_snapshot=EvidenceService.snapshot(bundles),
                )
            return advisories

    async def list_maintenance_reviews(self, project_id: str):
        """Return durable typed review history for this project."""

        await self._require_domain_project(project_id, allow_archived=True)
        return await self._maintenance_reviews.list(
            user_name=self.user_name,
            project_id=project_id,
        )

    async def flag_jev_entity_context_decision(
        self, project_id: str, *, capability: str, window_id: str,
        pass_number: int, occurrence_key: str, entity_id: int,
        entity_type: str | None, topic: str | None, reasoning: str,
    ):
        """Open a review for a typed correction to a decision-linked entity context."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            if capability not in {"extraction", "classification"}:
                raise ValueError("This decision cannot change entity type or topic")
            row = await self._require_knowledge_store().get_jev_decision(
                capability, window_id, pass_number, occurrence_key,
                user_name=self.user_name, project_id=project_id,
            )
            if row is None:
                raise FileNotFoundError("JEV decision not found")
            observation = row["decision"]
            linked_id = (
                observation.get("entity_id") if capability == "classification"
                else observation.get("operational_entity_id")
            )
            if linked_id != entity_id:
                raise ValueError("JEV decision does not identify this entity")
            current = await self.pg.fetch_one(
                """SELECT entity_type, topic, human_correction_review_id
                   FROM public.project_entity_contexts
                   WHERE user_name = %s AND project_id = %s AND entity_id = %s""",
                (self.user_name, project_id, entity_id),
            )
            if current is None:
                raise FileNotFoundError("Entity context not found")
            if current["human_correction_review_id"] is not None:
                raise MaintenanceConflictError()
            stored_domain = await self._domain_store.load(self.user_name, project_id)
            domain = stored_domain.compile()
            target_type = entity_type or current["entity_type"]
            if target_type not in domain.active_entity_types:
                raise ValueError("Entity type is not active in this project domain")
            target_topic = topic or (
                domain.topic_for_entity_type(target_type)
                if target_type != current["entity_type"] else current["topic"]
            )
            if target_topic not in domain.allowed_topics_for_entity_type(target_type):
                raise ValueError("Topic is not allowed for this entity type")
            if (target_type, target_topic) == (current["entity_type"], current["topic"]):
                raise ValueError("Correction leaves the entity unchanged")
            block_ids = observation["record"]["evidence_block_ids"]
            if not block_ids or len(block_ids) > 8:
                raise ValueError("JEV decision evidence is unavailable or too large")
            bundles = tuple([
                await self._require_knowledge_store().get_context_block_evidence(
                    block_id, user_name=self.user_name, project_id=project_id,
                )
                for block_id in block_ids
            ])
            if any(node.status == "missing" for bundle in bundles for node in bundle.nodes):
                raise ValueError("JEV decision evidence is missing")
            snapshot = EvidenceService.snapshot(bundles)
            decision_hash = hashlib.sha256(json.dumps(
                observation, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()
            return await self._maintenance_reviews.open(
                user_name=self.user_name, scope="project", project_id=project_id,
                kind="jev_entity_context_correction", reasoning=reasoning,
                proposed_plan=EntityContextChangePlan(
                    entity_id=entity_id, project_id=project_id,
                    entity_type=target_type, topic=target_topic,
                ),
                evidence_refs=[EvidencePointer(kind="context_block", identifier=block_id)
                               for block_id in block_ids],
                evidence_snapshot=snapshot,
                expected_state={
                    "decision_hash": decision_hash,
                    "capability": capability, "window_id": window_id,
                    "pass_number": pass_number, "occurrence_key": occurrence_key,
                    "entity_type": current["entity_type"], "topic": current["topic"],
                    "domain_version": stored_domain.version,
                },
                dedupe_key=f"jev-context:{window_id}:{pass_number}:{occurrence_key}:{entity_id}",
            )

    async def flag_jev_identity_decision(
        self, project_id: str, *, window_id: str, pass_number: int,
        occurrence_key: str, source_entity_id: int, target_entity_id: int | None,
        new_entity_name: str | None, entity_type: str | None, topic: str | None,
        reasoning: str,
    ):
        """Open a review for one proved, decision-linked identity occurrence."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            decision = await self._require_knowledge_store().get_jev_decision(
                "identity", window_id, pass_number, occurrence_key,
                user_name=self.user_name, project_id=project_id,
            )
            if decision is None:
                raise FileNotFoundError("JEV identity decision not found")
            observation = decision["decision"]
            if observation.get("operational_entity_id") != source_entity_id or (
                observation.get("superseded_by_pass") is not None
            ):
                raise MaintenanceConflictError()
            block_ids = tuple(UUID(str(value)) for value in observation["record"]["evidence_block_ids"])
            plan = IdentityReassignmentPlan(
                window_id=UUID(window_id), occurrence_key=occurrence_key,
                source_entity_id=source_entity_id, target_entity_id=target_entity_id,
                new_entity_name=new_entity_name, entity_type=entity_type,
                topic=topic, block_ids=block_ids,
            )
            state = await self._identity_occurrence_state(project_id, plan, observation)
            domain = await self._domain_store.load(self.user_name, project_id)
            if target_entity_id is None:
                compiled = domain.compile()
                if entity_type not in compiled.active_entity_types or (
                    topic not in compiled.allowed_topics_for_entity_type(entity_type)
                ):
                    raise ValueError("New identity classification is not in the project domain")
            bundles = tuple([
                await self._require_knowledge_store().get_context_block_evidence(
                    str(block_id), user_name=self.user_name, project_id=project_id,
                ) for block_id in block_ids
            ])
            if any(node.status == "missing" for bundle in bundles for node in bundle.nodes):
                raise MaintenanceConflictError()
            decision_hash = hashlib.sha256(json.dumps(
                observation, sort_keys=True, separators=(",", ":")
            ).encode()).hexdigest()
            return await self._maintenance_reviews.open(
                user_name=self.user_name, scope="project", project_id=project_id,
                kind="jev_identity_reassignment", reasoning=reasoning,
                proposed_plan=plan,
                evidence_refs=[EvidencePointer(kind="context_block", identifier=str(block_id))
                               for block_id in block_ids],
                evidence_snapshot=EvidenceService.snapshot(bundles),
                expected_state={
                    "decision_hash": decision_hash,
                    "occurrence_hash": state["fingerprint"],
                    "domain_version": domain.version,
                    "pass_number": pass_number,
                },
                dedupe_key=f"jev-identity:{window_id}:{pass_number}:{occurrence_key}",
            )

    async def _identity_occurrence_state(
        self, project_id: str, plan: IdentityReassignmentPlan, observation: dict,
        *, cur=None,
    ) -> dict:
        """Prove that stored occurrence rows cover the current associations."""

        query = """SELECT occurrence.block_id::text AS block_id,
                      occurrence.pass_number, occurrence.occurrence_index,
                      occurrence.resolved_entity_id, occurrence.mention_text,
                      occurrence.source_start, occurrence.source_end,
                      occurrence.corrected_entity_id,
                      occurrence.human_correction_review_id,
                      block.content_hash,
                      association.entity_id IS NOT NULL AS association_exists,
                      coalesce(association.occurrence_provenance_complete, false)
                        AS provenance_complete,
                      EXISTS (
                          SELECT 1 FROM public.project_contexts AS context
                          JOIN public.project_context_revision_blocks AS membership
                            ON membership.revision_id = context.current_revision_id
                           AND membership.project_id = context.project_id
                          WHERE context.project_id = occurrence.project_id
                            AND membership.block_id = occurrence.block_id
                      ) AS current_block,
                      semantic_window.stage AS window_stage
               FROM public.context_block_entity_occurrences AS occurrence
               JOIN public.project_semantic_windows AS semantic_window
                 ON semantic_window.window_id = occurrence.window_id
                AND semantic_window.project_id = occurrence.project_id
               JOIN public.project_context_blocks AS block
                 ON block.block_id = occurrence.block_id
                AND block.project_id = occurrence.project_id
               LEFT JOIN public.context_block_entities AS association
                 ON association.block_id = occurrence.block_id
                AND association.project_id = occurrence.project_id
                AND association.entity_id = occurrence.resolved_entity_id
               WHERE occurrence.window_id = %s AND occurrence.project_id = %s
                 AND occurrence.identity_decision_key = %s
                 AND semantic_window.user_name = %s
               ORDER BY occurrence.block_id"""
        args = (plan.window_id, project_id, plan.occurrence_key, self.user_name)
        if cur is None:
            rows = await self.pg.fetch_all(query, args)
        else:
            await cur.execute(query, args)
            rows = await cur.fetchall()
        expected_blocks = {str(block_id) for block_id in plan.block_ids}
        if (not rows or len(rows) != len(expected_blocks)
                or {row["block_id"] for row in rows} != expected_blocks) or any(
            row["resolved_entity_id"] != plan.source_entity_id
            or row["corrected_entity_id"] is not None
            or row["human_correction_review_id"] is not None
            or not row["association_exists"]
            or not row["provenance_complete"]
            or not row["current_block"]
            or row["window_stage"] != "completed"
            or row["mention_text"].casefold() != observation.get("mention_text", "").casefold()
            for row in rows
        ):
            raise MaintenanceConflictError()
        target = None
        if plan.target_entity_id is not None:
            target_query = """SELECT entity.canonical_name, context.entity_type, context.topic
                   FROM public.entities AS entity
                   JOIN public.project_entity_contexts AS context
                     ON context.entity_id = entity.entity_id
                    AND context.user_name = entity.user_name
                   WHERE entity.entity_id = %s AND entity.user_name = %s
                     AND entity.status = 'active' AND context.project_id = %s"""
            target_args = (plan.target_entity_id, self.user_name, project_id)
            if cur is None:
                target = await self.pg.fetch_one(target_query, target_args)
            else:
                await cur.execute(target_query, target_args)
                target = await cur.fetchone()
            if target is None:
                raise ValueError("Target entity needs an active context in this project")
        fingerprint = hashlib.sha256(json.dumps(
            {"rows": rows, "target": target}, sort_keys=True, default=str,
        ).encode()).hexdigest()
        return {"rows": rows, "target": target, "fingerprint": fingerprint}

    async def reopen_jev_entity_context_correction(
        self, project_id: str, entity_id: int, *, expected_review_id: str,
    ) -> dict:
        """Release one confirmed context choice for future user or domain changes."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            async with self.pg.transaction() as cur:
                await cur.execute(
                    """SELECT status FROM public.projects
                       WHERE user_name = %s AND project_id = %s FOR UPDATE""",
                    (self.user_name, project_id),
                )
                project = await cur.fetchone()
                if project is None or project["status"] != "active":
                    raise ValueError("Project is not active")
                await cur.execute(
                    """UPDATE public.project_entity_contexts
                       SET human_correction_review_id = NULL,
                           updated_at_ms = floor(extract(epoch from clock_timestamp()) * 1000)::bigint
                       WHERE user_name = %s AND project_id = %s AND entity_id = %s
                         AND human_correction_review_id = %s
                       RETURNING entity_type, topic""",
                    (self.user_name, project_id, entity_id, expected_review_id),
                )
                row = await cur.fetchone()
                if row is None:
                    raise MaintenanceConflictError()
            runtime = self._active_projects.get(project_id)
            if runtime is not None:
                runtime.entities.remove_entities([entity_id])
            return {"entity_id": entity_id, "entity_type": row["entity_type"],
                    "topic": row["topic"], "human_correction_active": False}

    async def get_maintenance_review_detail(
        self,
        project_id: str,
        review_id: str,
    ) -> MaintenanceReviewDetail:
        """Return an immutable review snapshot beside current bounded evidence."""

        await self._require_domain_project(project_id, allow_archived=True)
        review = await self._maintenance_reviews.get(
            review_id,
            user_name=self.user_name,
            project_id=project_id,
        )
        if review is None:
            raise FileNotFoundError("Maintenance review not found")
        if review.kind == "hypothesis_evaluation":
            return await self._hypothesis_review_detail(project_id, review)
        observation_ids = [
            int(pointer.identifier)
            for pointer in review.evidence_refs
            if pointer.kind == "relationship_observation"
        ]
        context_ids = [
            pointer.identifier
            for pointer in review.evidence_refs
            if pointer.kind == "context_block"
        ]
        unsupported = tuple(
            pointer
            for pointer in review.evidence_refs
            if pointer.kind not in {"relationship_observation", "context_block"}
        )
        store = self._require_knowledge_store()
        bundles = list(
            await load_conflict_evidence(
                store,
                observation_ids=observation_ids,
                user_name=self.user_name,
                project_id=project_id,
            ) if observation_ids else ()
        )
        for block_id in context_ids:
            bundles.append(
                await store.get_context_block_evidence(
                    block_id,
                    user_name=self.user_name,
                    project_id=project_id,
                )
            )
        current_snapshot = EvidenceService.snapshot(tuple(bundles))
        has_missing = unsupported or any(
            any(node.status == "missing" for node in bundle.nodes) for bundle in bundles
        )
        if has_missing:
            state = "partially_unavailable"
        elif current_snapshot.state_token == review.evidence_snapshot.state_token:
            state = "current"
        else:
            state = "changed"
        return MaintenanceReviewDetail(
            review=review,
            stored_snapshot=review.evidence_snapshot,
            current_evidence=tuple(bundles),
            unavailable_pointers=unsupported,
            evidence_state=state,
        )

    async def _hypothesis_review_detail(self, project_id, review) -> MaintenanceReviewDetail:
        """Resolve the captured citations used by a confirmed board conclusion."""

        refs = tuple(ref for ref in review.evidence_refs if ref.kind == "source_reference")
        unsupported = tuple(ref for ref in review.evidence_refs if ref.kind != "source_reference")
        rows = await self.pg.fetch_all(
            """SELECT ref.source_ref_id::text AS source_ref_id, ref.content_hash,
                      coalesce(ref.metadata ->> 'document_name', ref.metadata ->> 'title',
                               ref.canonical_url, 'Captured source passage') AS title,
                      CASE WHEN ref.document_id IS NOT NULL AND
                          (document.document_id IS NULL OR document.status = 'deleted' OR
                           document.current_snapshot_id IS DISTINCT FROM ref.parse_snapshot_id)
                      THEN true ELSE false END AS source_stale
               FROM public.message_source_refs AS ref
               JOIN public.messages AS message
                 ON message.message_id = ref.message_id
                AND message.project_id = ref.project_id
                AND message.session_id = ref.session_id
               LEFT JOIN public.project_documents AS document
                 ON document.document_id = ref.document_id
                AND document.project_id = ref.source_project_id
               WHERE ref.source_ref_id = ANY(%s::uuid[]) AND ref.project_id = %s
                 AND message.user_name = %s AND message.role = 'assistant'""",
            ([ref.identifier for ref in refs], project_id, self.user_name),
        ) if refs else []
        found = {row["source_ref_id"]: row for row in rows}
        recorded = review.expected_state.get("citation_versions", {})
        nodes = []
        changed = False
        missing = bool(unsupported)
        token_parts = []
        for ref in refs:
            row = found.get(ref.identifier)
            if row is None:
                missing = True
                nodes.append(EvidenceNode(pointer=ref, status="missing"))
                token_parts.append((ref.identifier, None, None))
                continue
            stale = bool(row["source_stale"])
            version = row["content_hash"]
            changed |= stale or version != recorded.get(ref.identifier)
            nodes.append(EvidenceNode(
                pointer=ref, label=str(row["title"])[:200],
                content_hash=version, status="retired" if stale else "available",
            ))
            token_parts.append((ref.identifier, version, stale))
        token = hashlib.sha256(json.dumps(token_parts, sort_keys=True).encode()).hexdigest()
        bundle = EvidenceBundle(
            subject=EvidenceSubject(kind="maintenance_review", identifier=review.review_id),
            nodes=tuple(nodes), total_nodes=len(nodes), total_edges=0,
            state_token=token,
        )
        return MaintenanceReviewDetail(
            review=review, stored_snapshot=review.evidence_snapshot,
            current_evidence=(bundle,), unavailable_pointers=unsupported,
            evidence_state="partially_unavailable" if missing else "changed" if changed else "current",
        )

    async def preview_maintenance_review(
        self,
        project_id: str,
        review_id: str,
    ) -> tuple[MaintenanceReviewDetail, MaintenanceImpactPreview]:
        """Return evidence and typed consequences without performing writes."""

        detail = await self.get_maintenance_review_detail(project_id, review_id)
        if detail.review.status != "open":
            raise ValueError("Only an open maintenance review can be previewed")
        if detail.evidence_state != "current":
            raise MaintenanceConflictError()
        if isinstance(detail.review.proposed_plan, IdentityReassignmentPlan):
            return detail, await self._preview_identity_reassignment(project_id, detail.review)
        return detail, MaintenanceImpactPlanner.preview(detail.review)

    async def _require_current_identity_review(self, project_id, review) -> dict:
        plan = review.proposed_plan
        decision = await self._require_knowledge_store().get_jev_decision(
            "identity", plan.window_id, review.expected_state["pass_number"],
            plan.occurrence_key, user_name=self.user_name, project_id=project_id,
        )
        if decision is None:
            raise MaintenanceConflictError()
        observation = decision["decision"]
        decision_hash = hashlib.sha256(json.dumps(
            observation, sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest()
        if decision_hash != review.expected_state.get("decision_hash"):
            raise MaintenanceConflictError()
        state = await self._identity_occurrence_state(project_id, plan, observation)
        domain = await self._domain_store.load(self.user_name, project_id)
        if (state["fingerprint"] != review.expected_state.get("occurrence_hash")
                or domain.version != review.expected_state.get("domain_version")):
            raise MaintenanceConflictError()
        return state

    async def _preview_identity_reassignment(self, project_id, review) -> MaintenanceImpactPreview:
        await self._require_current_identity_review(project_id, review)
        plan = review.proposed_plan
        blocks = [str(block_id) for block_id in plan.block_ids]

        async def affected(query, args):
            rows = await self.pg.fetch_all(
                f"""WITH affected AS ({query})
                    SELECT id, count(*) OVER () AS total FROM affected
                    ORDER BY id LIMIT 129""", args,
            )
            total = int(rows[0]["total"]) if rows else 0
            return tuple(str(row["id"]) for row in rows[:128]), total

        observations, observation_count = await affected(
            """SELECT DISTINCT observation.observation_id::text AS id
               FROM public.relationship_observations AS observation
               JOIN public.relationship_observation_blocks AS support
                 ON support.observation_id = observation.observation_id
                AND support.project_id = observation.project_id
               WHERE observation.user_name = %s AND observation.project_id = %s
                 AND support.block_id = ANY(%s::uuid[])
                 AND (observation.source_entity_id = %s OR observation.target_entity_id = %s)
                 AND observation.retired_at IS NULL""",
            (self.user_name, project_id, blocks, plan.source_entity_id, plan.source_entity_id),
        )
        messages, message_count = await affected(
            """SELECT DISTINCT link.message_id::text AS id
               FROM public.message_entity_occurrences AS link
               JOIN public.context_block_entity_occurrences AS occurrence
                 ON occurrence.window_id = link.window_id
                AND occurrence.pass_number = link.pass_number
                AND occurrence.occurrence_index = link.occurrence_index
                AND occurrence.block_id = link.block_id
               WHERE occurrence.window_id = %s AND occurrence.project_id = %s
                 AND occurrence.identity_decision_key = %s""",
            (plan.window_id, project_id, plan.occurrence_key),
        )
        episodes, episode_count = await affected(
            """SELECT DISTINCT link.episode_id AS id
               FROM public.episode_entity_occurrences AS link
               JOIN public.context_block_entity_occurrences AS occurrence
                 ON occurrence.window_id = link.window_id
                AND occurrence.pass_number = link.pass_number
                AND occurrence.occurrence_index = link.occurrence_index
                AND occurrence.block_id = link.block_id
               WHERE occurrence.window_id = %s AND occurrence.project_id = %s
                 AND occurrence.identity_decision_key = %s""",
            (plan.window_id, project_id, plan.occurrence_key),
        )
        if observation_count:
            other = await self.pg.fetch_one(
                """SELECT 1 FROM public.context_block_entity_occurrences
                   WHERE project_id = %s AND block_id = ANY(%s::uuid[])
                     AND coalesce(corrected_entity_id, resolved_entity_id) = %s
                     AND NOT (window_id = %s AND identity_decision_key = %s)
                   LIMIT 1""",
                (project_id, blocks, plan.source_entity_id,
                 plan.window_id, plan.occurrence_key),
            )
            if other is not None:
                raise MaintenanceConflictError()
        for message_id in messages:
            rows = await self.pg.fetch_all(
                """SELECT entity_id, occurrence_provenance_complete
                   FROM public.message_entity_refs
                   WHERE message_id = %s AND entity_id = ANY(%s)""",
                (int(message_id), [plan.source_entity_id, plan.target_entity_id or -1]),
            )
            by_entity = {row["entity_id"]: row["occurrence_provenance_complete"] for row in rows}
            if not by_entity.get(plan.source_entity_id) or (
                plan.target_entity_id in by_entity and not by_entity[plan.target_entity_id]
            ):
                raise MaintenanceConflictError()
        for episode_id in episodes:
            rows = await self.pg.fetch_all(
                """SELECT entity_id, occurrence_provenance_complete
                   FROM public.episode_entities
                   WHERE episode_id = %s AND entity_id = ANY(%s)""",
                (episode_id, [plan.source_entity_id, plan.target_entity_id or -1]),
            )
            by_entity = {row["entity_id"]: row["occurrence_provenance_complete"] for row in rows}
            if not by_entity.get(plan.source_entity_id) or (
                plan.target_entity_id in by_entity and not by_entity[plan.target_entity_id]
            ):
                raise MaintenanceConflictError()
        def impact(kind, mode, ids, total):
            return MaintenanceImpactItem(
                kind=kind, mode=mode, identifiers=ids,
                total_count=total, truncated=total > len(ids),
            )
        return MaintenanceImpactPreview(
            review_id=review.review_id,
            evidence_state_token=review.evidence_snapshot.state_token,
            impacts=(
                impact("context_block_entity", "direct_mutation", blocks, len(blocks)),
                impact("relationship_observation", "direct_mutation", observations, observation_count),
                impact("message_entity_ref", "direct_mutation", messages, message_count),
                impact("episode_entity_link", "derived_rebuild", episodes, episode_count),
                impact("live_entity_cache", "cache_invalidation", (project_id,), 1),
            ),
        )

    async def apply_jev_identity_review(
        self, project_id: str, review_id: str, *, expected_state: dict,
    ) -> dict:
        """Apply a reviewed occurrence correction with all derived links in one transaction."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            detail, _impact = await self.preview_maintenance_review(project_id, review_id)
            review = detail.review
            if not isinstance(review.proposed_plan, IdentityReassignmentPlan):
                raise ValueError("Review is not an identity reassignment")
            if review.expected_state != expected_state:
                raise MaintenanceConflictError()
            plan = review.proposed_plan
            decision = await self._require_knowledge_store().get_jev_decision(
                "identity", plan.window_id, review.expected_state["pass_number"],
                plan.occurrence_key, user_name=self.user_name, project_id=project_id,
            )
            if decision is None:
                raise MaintenanceConflictError()
            observation = decision["decision"]
            async with self.pg.transaction() as cur:
                await cur.execute(
                    """SELECT status, domain_config FROM public.projects
                       WHERE user_name = %s AND project_id = %s FOR UPDATE""",
                    (self.user_name, project_id),
                )
                project = await cur.fetchone()
                if project is None or project["status"] != "active" or (
                    project["domain_config"].get("version")
                    != review.expected_state["domain_version"]
                ):
                    raise MaintenanceConflictError()
                await cur.execute(
                    """SELECT current_revision_id FROM public.project_contexts
                       WHERE user_name = %s AND project_id = %s FOR UPDATE""",
                    (self.user_name, project_id),
                )
                if await cur.fetchone() is None:
                    raise MaintenanceConflictError()
                await cur.execute(
                    """SELECT status FROM public.maintenance_reviews
                       WHERE review_id = %s AND user_name = %s AND project_id = %s
                       FOR UPDATE""",
                    (review_id, self.user_name, project_id),
                )
                locked_review = await cur.fetchone()
                if locked_review is None or locked_review["status"] != "open":
                    raise MaintenanceConflictError()
                await cur.execute(
                    """SELECT block_id FROM public.context_block_entity_occurrences
                       WHERE window_id = %s AND project_id = %s
                         AND identity_decision_key = %s FOR UPDATE""",
                    (plan.window_id, project_id, plan.occurrence_key),
                )
                if len(await cur.fetchall()) != len(plan.block_ids):
                    raise MaintenanceConflictError()
                await cur.execute(
                    """SELECT block_id FROM public.context_block_entities
                       WHERE project_id = %s AND block_id = ANY(%s::uuid[])
                         AND entity_id = %s FOR UPDATE""",
                    (project_id, [str(block_id) for block_id in plan.block_ids],
                     plan.source_entity_id),
                )
                if len(await cur.fetchall()) != len(plan.block_ids):
                    raise MaintenanceConflictError()
                state = await self._identity_occurrence_state(
                    project_id, plan, observation, cur=cur,
                )
                if state["fingerprint"] != review.expected_state["occurrence_hash"]:
                    raise MaintenanceConflictError()
                target_id = await IdentityReassignmentWriter(self.pg).apply(
                    cur, user_name=self.user_name, project_id=project_id,
                    review_id=review_id, plan=plan,
                )
                applied = await self._maintenance_reviews.transition(
                    review_id, user_name=self.user_name, project_id=project_id,
                    status="applied", expected_state=expected_state,
                    actor=self.user_name, reason="User confirmed identity reassignment",
                    cur=cur,
                )
            runtime = self._active_projects.get(project_id)
            if runtime is not None:
                runtime.entities.remove_entities([plan.source_entity_id, target_id])
            return {"review": applied, "target_entity_id": target_id}

    async def reopen_jev_identity_correction(
        self, project_id: str, review_id: str, *, expected_target_entity_id: int,
    ) -> dict:
        """Release one occurrence override while retaining the applied history."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=False)
            review = await self._maintenance_reviews.get(
                review_id, user_name=self.user_name, project_id=project_id,
            )
            if review is None or review.status != "applied" or not isinstance(
                review.proposed_plan, IdentityReassignmentPlan,
            ):
                raise ValueError("Applied identity correction not found")
            async with self.pg.transaction() as cur:
                await cur.execute(
                    """SELECT status FROM public.projects
                       WHERE user_name = %s AND project_id = %s FOR UPDATE""",
                    (self.user_name, project_id),
                )
                project = await cur.fetchone()
                if project is None or project["status"] != "active":
                    raise MaintenanceConflictError()
                await cur.execute(
                    """SELECT block_id FROM public.context_block_entity_occurrences
                       WHERE window_id = %s AND project_id = %s
                         AND identity_decision_key = %s
                         AND human_correction_review_id = %s
                         AND corrected_entity_id = %s FOR UPDATE""",
                    (review.proposed_plan.window_id, project_id,
                     review.proposed_plan.occurrence_key, review_id,
                     expected_target_entity_id),
                )
                rows = await cur.fetchall()
                if {str(row["block_id"]) for row in rows} != {
                    str(block_id) for block_id in review.proposed_plan.block_ids
                } or len(rows) != len(review.proposed_plan.block_ids):
                    raise MaintenanceConflictError()
                for table in (
                    "context_block_entity_occurrences",
                    "message_entity_occurrences",
                    "episode_entity_occurrences",
                ):
                    await cur.execute(
                        f"""UPDATE public.{table}
                            SET human_correction_review_id = NULL
                            WHERE project_id = %s AND human_correction_review_id = %s
                              AND corrected_entity_id = %s""",
                        (project_id, review_id, expected_target_entity_id),
                    )
            return {
                "review_id": review_id,
                "target_entity_id": expected_target_entity_id,
                "human_correction_active": False,
            }

    async def transition_maintenance_review(
        self,
        project_id: str,
        review_id: str,
        *,
        status: str,
        expected_state: dict | None = None,
        reason: str | None = None,
    ):
        """Apply or close a review after the caller has inspected its plan."""

        if status != "applied":
            await self._require_domain_project(project_id, allow_archived=True)
            review = await self._maintenance_reviews.get(
                review_id, user_name=self.user_name, project_id=project_id
            )
            if review is not None and isinstance(
                review.proposed_plan, RelationshipAdvisoryPlan
            ):
                raise ValueError("Use a relationship advisory action for this review")
            return await self._maintenance_reviews.transition(
                review_id=review_id,
                user_name=self.user_name,
                project_id=project_id,
                status=status,
                expected_state=expected_state,
                actor=self.user_name,
                reason=reason,
            )

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            return await self._apply_maintenance_review(
                project_id,
                review_id,
                expected_state=expected_state,
            )

    async def _apply_maintenance_review(
        self,
        project_id: str,
        review_id: str,
        *,
        expected_state: dict | None,
    ):
        """Dispatch one reviewed plan while the maintenance lock is held."""

        review = await self._maintenance_reviews.get(
            review_id,
            user_name=self.user_name,
            project_id=project_id,
        )
        if review is None or review.status != "open":
            raise ValueError("Unknown or already-resolved maintenance review")
        if expected_state is not None and review.expected_state != expected_state:
            raise MaintenanceConflictError()
        if isinstance(review.proposed_plan, EntityContextChangePlan):
            return await self._apply_jev_entity_context_review(project_id, review)
        if not isinstance(review.proposed_plan, RelationshipInterpretationPlan):
            raise ValueError(
                "This review requires its dedicated maintenance operation and "
                "cannot be applied as a status transition"
            )

        if review.evidence_snapshot.total_edges > 0:
            observation_ids = [
                int(pointer.identifier)
                for pointer in review.evidence_refs
                if pointer.kind == "relationship_observation"
            ]
            current_bundles = await self._require_knowledge_store().get_relationship_observations_evidence(
                observation_ids,
                user_name=self.user_name,
                project_id=project_id,
            )
            if (
                EvidenceService.snapshot(current_bundles).state_token
                != review.evidence_snapshot.state_token
            ):
                raise ValueError("Maintenance review is stale; evidence changed")

        domain = await self._domain_store.load(self.user_name, project_id)
        expected_domain_version = self._validate_expected_domain_version(
            review.expected_state.get("domain_version")
        )
        if domain.version != expected_domain_version:
            raise ValueError("Maintenance review is stale; project domain changed")
        result = await self._relationship_interpretation_writer.apply_plan(
            user_name=self.user_name,
            project_id=project_id,
            plan=review.proposed_plan,
            domain=domain.compile(),
            review_id=review.review_id,
            actor=self.user_name,
        )
        if result.conflicts:
            raise ValueError("Maintenance review could not be applied completely")
        return await self._maintenance_reviews.get(
            review_id,
            user_name=self.user_name,
            project_id=project_id,
        )

    async def _apply_jev_entity_context_review(self, project_id, review):
        plan = review.proposed_plan
        if review.kind != "jev_entity_context_correction" or plan.project_id != project_id:
            raise ValueError("Review is not an entity context correction")
        await self._require_domain_project(project_id, allow_archived=False)
        detail = await self.get_maintenance_review_detail(project_id, review.review_id)
        if detail.evidence_state != "current":
            raise ValueError("Maintenance review is stale; evidence changed")
        expected = review.expected_state
        decision_row = await self._require_knowledge_store().get_jev_decision(
            expected["capability"], expected["window_id"], expected["pass_number"],
            expected["occurrence_key"], user_name=self.user_name, project_id=project_id,
        )
        if decision_row is None or hashlib.sha256(json.dumps(
            decision_row["decision"], sort_keys=True, separators=(",", ":")
        ).encode()).hexdigest() != expected["decision_hash"]:
            raise MaintenanceConflictError()
        async with self.pg.transaction() as cur:
            await cur.execute(
                """SELECT status, domain_config ->> 'version' AS domain_version
                   FROM public.projects WHERE user_name = %s AND project_id = %s
                   FOR UPDATE""",
                (self.user_name, project_id),
            )
            project = await cur.fetchone()
            if (project is None or project["status"] != "active"
                    or int(project["domain_version"]) != expected["domain_version"]):
                raise MaintenanceConflictError()
            await cur.execute(
                """SELECT entity_type, topic, human_correction_review_id
                   FROM public.project_entity_contexts
                   WHERE user_name = %s AND project_id = %s AND entity_id = %s
                   FOR UPDATE""",
                (self.user_name, project_id, plan.entity_id),
            )
            current = await cur.fetchone()
            if (current is None or current["entity_type"] != expected["entity_type"]
                    or current["topic"] != expected["topic"]
                    or current["human_correction_review_id"] is not None):
                raise MaintenanceConflictError()
            await cur.execute(
                """UPDATE public.project_entity_contexts
                   SET entity_type = %s, topic = %s,
                       human_correction_review_id = %s,
                       updated_at_ms = floor(extract(epoch from clock_timestamp()) * 1000)::bigint
                   WHERE user_name = %s AND project_id = %s AND entity_id = %s""",
                (plan.entity_type, plan.topic, review.review_id,
                 self.user_name, project_id, plan.entity_id),
            )
            if plan.entity_type != current["entity_type"]:
                await cur.execute(
                    """SELECT observation_id FROM public.relationship_observations
                       WHERE user_name = %s AND project_id = %s AND retired_at IS NULL
                         AND (source_entity_id = %s OR target_entity_id = %s)""",
                    (self.user_name, project_id, plan.entity_id, plan.entity_id),
                )
                observation_ids = [int(row["observation_id"]) for row in await cur.fetchall()]
                await self._maintenance_reviews.mark_stale_for_observations(
                    user_name=self.user_name, project_id=project_id,
                    observation_ids=observation_ids,
                    reason="Human-corrected relationship endpoint type", cur=cur,
                )
            await self._maintenance_reviews.transition(
                review.review_id, user_name=self.user_name, project_id=project_id,
                status="applied", expected_state=expected, actor=self.user_name,
                reason="User confirmed JEV-linked entity correction", cur=cur,
            )
        runtime = self._active_projects.get(project_id)
        if runtime is not None:
            runtime.entities.remove_entities([plan.entity_id])
        return await self._maintenance_reviews.get(
            review.review_id, user_name=self.user_name, project_id=project_id,
        )

    async def get_conflict_group(self, project_id: str, conflict_id: str) -> dict:
        """Return the conflict workflow subject and immutable evidence snapshots."""

        await self._require_domain_project(project_id, allow_archived=True)
        detail = await self._conflict_reader.get_detail(
            conflict_id=conflict_id,
            user_name=self.user_name,
            project_id=project_id,
        )
        if detail is None:
            raise FileNotFoundError("Conflict group not found")
        return detail

    async def build_conflict_discovery_package(
        self,
        project_id: str,
        *,
        max_seed_span_days: int,
        max_package_tokens: int,
        token_counter: Callable[[str], int] | None = None,
    ) -> ConflictDiscoveryPackage | None:
        """Build bounded conflict evidence above the persistence facade."""
        await self._require_domain_project(project_id, allow_archived=True)
        cursor = await self._conflict_discovery_reader.get_cursor(
            user_name=self.user_name,
            project_id=project_id,
        )

        async def load_evidence(observation_ids: list[int]):
            return await load_conflict_evidence(
                self._require_knowledge_store(),
                observation_ids=observation_ids,
                user_name=self.user_name,
                project_id=project_id,
            )

        return await ConflictPacketBuilder(
            self._conflict_discovery_reader,
            token_counter=token_counter,
            evidence_loader=load_evidence,
        ).build(
            cursor,
            max_span_days=max_seed_span_days,
            max_tokens=max_package_tokens,
        )

    async def complete_conflict_discovery(
        self,
        package: ConflictDiscoveryPackage,
        *,
        candidates: Iterable[LLMConflictCandidate],
    ) -> int:
        """Persist grounded conflict reviews and advance the cursor atomically."""

        if package.cursor.user_name != self.user_name:
            raise ValueError("Conflict discovery package belongs to another user")
        async with self._lock:
            await self._require_domain_project(
                package.cursor.project_id,
                allow_archived=False,
            )
            results = await self._persist_conflict_discovery(
                package,
                candidates=tuple(candidates),
            )
        for result in results:
            try:
                await self._conflict_service.notify_detection(
                    user_name=package.cursor.user_name,
                    project_id=package.cursor.project_id,
                    origin="background_discovery",
                    result=result,
                )
            except Exception:
                logger.exception(
                    "Conflict {} was committed but its notification failed",
                    result.group.conflict_id,
                )
        return sum(int(result.should_notify) for result in results)

    async def _persist_conflict_discovery(
        self,
        package: ConflictDiscoveryPackage,
        *,
        candidates: tuple[LLMConflictCandidate, ...],
    ) -> list[ConflictWriteResult]:
        """Write one validated discovery package and cursor atomically."""

        candidate_items = candidates
        snapshots = {}
        for candidate in candidate_items:
            evidence_ids = tuple(sorted(set(candidate.evidence_ids)))
            if evidence_ids not in snapshots:
                snapshots[evidence_ids] = await snapshot_conflict_evidence(
                    self._require_knowledge_store(),
                    observation_ids=evidence_ids,
                    user_name=package.cursor.user_name,
                    project_id=package.cursor.project_id,
                )
        results = []
        async with self.pg.transaction() as cur:
            for candidate in candidate_items:
                evidence_ids = tuple(sorted(set(candidate.evidence_ids)))
                result = await self._conflict_writer.record_detection(
                    user_name=package.cursor.user_name,
                    project_id=package.cursor.project_id,
                    origin="background_discovery",
                    kind=candidate.kind,
                    rationale=candidate.rationale,
                    confidence=candidate.confidence,
                    evidence_ids=candidate.evidence_ids,
                    metadata={
                        "discovery_packet_tokens": package.estimated_tokens,
                    },
                    evidence_snapshot=snapshots[evidence_ids],
                    cur=cur,
                )
                results.append(result)
            await self._conflict_discovery_reader.advance(
                package.cursor,
                last_reviewed_observation_id=package.next_observation_id,
                cur=cur,
            )
        return results

    async def record_conflict_detection(
        self,
        project_id: str,
        *,
        origin: ConflictOrigin,
        kind: str,
        rationale: str,
        confidence: float | None,
        evidence_ids: list[int],
        metadata: dict | None = None,
        existing_conflict_id: str | None = None,
    ) -> ConflictWriteResult:
        """Record an agent/user conflict report at the maintenance boundary."""
        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            snapshot = await snapshot_conflict_evidence(
                self._require_knowledge_store(),
                observation_ids=evidence_ids,
                user_name=self.user_name,
                project_id=project_id,
            )
            return await self._conflict_service.record_detection(
                user_name=self.user_name,
                project_id=project_id,
                origin=origin,
                kind=kind,
                rationale=rationale,
                confidence=confidence,
                evidence_ids=evidence_ids,
                metadata=metadata,
                evidence_snapshot=snapshot,
                existing_conflict_id=existing_conflict_id,
            )

    async def resolve_conflict_group(
        self,
        project_id: str,
        conflict_id: str,
        *,
        resolution_kind: ConflictResolutionKind,
        resolution_note: str | None = None,
        resolved_by: str | None = None,
    ):
        """Apply a user-led classification without rewriting the evidence."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            return await self._conflict_service.resolve(
                conflict_id=conflict_id,
                user_name=self.user_name,
                project_id=project_id,
                resolution_kind=resolution_kind,
                resolved_by=resolved_by or self.user_name,
                resolution_note=resolution_note,
            )

    async def apply_relationship_advisory_action(
        self,
        project_id: str,
        review_id: str,
        action: str,
        *,
        relationship_type: str | None = None,
        note: str | None = None,
        decided_by: str | None = None,
    ) -> RelationshipAdvisoryDecision:
        """Persist an advisory decision without activating domain changes."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            detail = await self.get_maintenance_review_detail(project_id, review_id)
            review = detail.review
            if not isinstance(review.proposed_plan, RelationshipAdvisoryPlan):
                raise ValueError("Review is not a relationship advisory")
            if review.status == "stale" or detail.evidence_state != "current":
                raise ValueError("Relationship advisory evidence is stale")
            expected_version = review.expected_state.get("domain_version")
            if expected_version is not None:
                domain = await self._domain_store.load(self.user_name, project_id)
                if domain.version != self._validate_expected_domain_version(expected_version):
                    raise ValueError("Relationship advisory domain version is stale")
            return await self._relationship_advisory_writer.apply_action(
                user_name=self.user_name,
                project_id=project_id,
                pattern_key=review.proposed_plan.pattern_key,
                action=action,
                expected_review_id=review_id,
                relationship_type=relationship_type,
                note=note,
                decided_by=decided_by,
            )

    async def rebuild_project_embeddings(self, project_id: str) -> dict[str, int]:
        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            self._require_no_active_runtimes(
                "Embedding rebuild requires all project runtimes to be inactive"
            )
            knowledge_store = self._require_knowledge_store()
            return await knowledge_store.rebuild_project_embeddings(
                project_id,
                self.user_name,
            )

    async def preview_entity_cleanup(
        self,
        project_id: str,
        *,
        limit: int = 100,
    ) -> dict:
        """Preview project-owned derived entities for explicit user cleanup."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            knowledge_store = self._require_knowledge_store()
            return await EntityCleanupWorkflow(knowledge_store).preview(
                user_name=self.user_name,
                project_id=project_id,
                limit=limit,
            )

    async def apply_entity_cleanup(
        self,
        project_id: str,
        *,
        entity_ids: list[int],
    ) -> dict:
        """Delete user-selected derived entities while preserving messages."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            knowledge_store = self._require_knowledge_store()
            result = await EntityCleanupWorkflow(knowledge_store).apply(
                user_name=self.user_name,
                project_id=project_id,
                entity_ids=entity_ids,
            )
            runtime = self._active_projects.get(project_id)
            if runtime is not None:
                runtime.entities.remove_entities(result["deleted_entity_ids"])
            return result

    async def preview_historical_reclassification(
        self,
        project_id: str,
        *,
        limit: int | None = 1000,
    ) -> dict:
        """Preview deterministic historical entity changes for active domain."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            stored = await self._domain_store.load(self.user_name, project_id)
            knowledge_store = self._require_knowledge_store()
            return await knowledge_store.preview_historical_reclassification(
                user_name=self.user_name,
                project_id=project_id,
                domain=stored.compile(),
                limit=limit,
            )

    async def reclassify_historical_entities(
        self,
        project_id: str,
        *,
        expected_domain_version: int,
        batch_size: int = 100,
        max_entities: int | None = None,
    ) -> dict:
        """Apply explicit, bounded historical entity reclassification."""

        self._validate_expected_domain_version(expected_domain_version)

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            self._require_no_active_runtimes(
                "Historical reclassification requires all project runtimes to be inactive"
            )
            stored = await self._domain_store.load(self.user_name, project_id)
            if stored.version != expected_domain_version:
                raise DomainConfigConflict(expected_domain_version, stored.version)
            knowledge_store = self._require_knowledge_store()
            result = await knowledge_store.reclassify_historical_entities(
                user_name=self.user_name,
                project_id=project_id,
                domain=stored.compile(),
                batch_size=batch_size,
                max_entities=max_entities,
            )
            return result.to_dict()

    async def preview_historical_relationship_normalization(
        self,
        project_id: str,
        *,
        limit: int | None = 1000,
    ) -> dict:
        """Preview deterministic historical relationship normalization."""

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            stored = await self._domain_store.load(self.user_name, project_id)
            knowledge_store = self._require_knowledge_store()
            return await knowledge_store.preview_historical_relationship_normalization(
                user_name=self.user_name,
                project_id=project_id,
                domain=stored.compile(),
                limit=limit,
            )

    async def normalize_historical_relationships(
        self,
        project_id: str,
        *,
        expected_domain_version: int,
        batch_size: int = 100,
        max_relationships: int | None = None,
    ) -> dict:
        """Apply explicit, bounded historical relationship normalization."""

        self._validate_expected_domain_version(expected_domain_version)

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            self._require_no_active_runtimes(
                "Historical relationship normalization requires all project runtimes to be inactive"
            )
            stored = await self._domain_store.load(self.user_name, project_id)
            if stored.version != expected_domain_version:
                raise DomainConfigConflict(expected_domain_version, stored.version)
            knowledge_store = self._require_knowledge_store()
            result = await knowledge_store.normalize_historical_relationships(
                user_name=self.user_name,
                project_id=project_id,
                domain=stored.compile(),
                batch_size=batch_size,
                max_relationships=max_relationships,
            )
            return result.to_dict()

    def _require_no_active_runtimes(self, message: str) -> None:
        active_runtime_projects = [
            active_id
            for active_id in self._active_projects
            if self._project_leases.get(active_id)
        ]
        if active_runtime_projects:
            raise RuntimeError(f"{message}; active projects: {active_runtime_projects}")
