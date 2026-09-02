"""Project-scoped knowledge maintenance and repair workflows."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Iterable, Mapping

from core.knowledge.conflict_discovery import ConflictPacketBuilder
from core.knowledge.conflict_service import ConflictService
from core.knowledge.conflicts import (
    ConflictDiscoveryPackage,
    ConflictOrigin,
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
from core.knowledge.maintenance_reviews import RelationshipInterpretationPlan
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
        # facade remains focused on durable reads/writes and graph projections.
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

    @staticmethod
    def _validate_expected_domain_version(value: object) -> int:
        if not isinstance(value, int) or isinstance(value, bool) or value < 0:
            raise ValueError("expected_domain_version must be a non-negative integer")
        return value

    async def get_relationship_advisories(
        self,
        project_id: str,
        *,
        thresholds: AdvisoryThresholds | None = None,
    ) -> list[RelationshipAdvisory]:
        """Read evidence-backed relationship advisories with dispositions."""

        await self._require_domain_project(project_id, allow_archived=True)
        domain = await self._domain_store.load(self.user_name, project_id)
        advisories = await self._relationship_observation_reader.get_advisories(
            user_name=self.user_name,
            project_id=project_id,
            thresholds=thresholds,
        )
        for advisory in advisories:
            await self._relationship_advisory_writer.materialize_pending(
                user_name=self.user_name,
                project_id=project_id,
                advisory=advisory,
                domain_version=domain.version,
            )
        return advisories

    async def list_maintenance_reviews(self, project_id: str):
        """Return durable typed review history for this project."""

        await self._require_domain_project(project_id, allow_archived=True)
        return await self._maintenance_reviews.list(
            user_name=self.user_name,
            project_id=project_id,
        )

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
            raise ValueError("Maintenance review expected state no longer matches")
        if not isinstance(review.proposed_plan, RelationshipInterpretationPlan):
            raise ValueError(
                "This review requires its dedicated maintenance operation and "
                "cannot be applied as a status transition"
            )

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
        return await ConflictPacketBuilder(
            self._conflict_discovery_reader,
            token_counter=token_counter,
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
        results = []
        async with self.pg.transaction() as cur:
            for candidate in candidates:
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
                        "packet_compacted": package.compacted,
                    },
                    cur=cur,
                )
                results.append(result)
            await self._conflict_discovery_reader.advance(
                package.cursor,
                last_reviewed_observation_id=package.next_observation_id,
                cur=cur,
            )
        for result in results:
            await self._conflict_service.notify_detection(
                user_name=package.cursor.user_name,
                project_id=package.cursor.project_id,
                origin="background_discovery",
                result=result,
            )
        return sum(int(result.should_notify) for result in results)

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
        await self._require_domain_project(project_id, allow_archived=True)
        return await self._conflict_service.record_detection(
            user_name=self.user_name,
            project_id=project_id,
            origin=origin,
            kind=kind,
            rationale=rationale,
            confidence=confidence,
            evidence_ids=evidence_ids,
            metadata=metadata,
            existing_conflict_id=existing_conflict_id,
        )

    async def resolve_conflict_group(
        self,
        project_id: str,
        conflict_id: str,
        *,
        resolution_kind: str,
        resolution_note: str | None = None,
        resolved_by: str | None = None,
    ):
        """Apply a user-led classification without rewriting the evidence."""

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
        pattern_key: str,
        action: str,
        *,
        relationship_type: str | None = None,
        note: str | None = None,
        decided_by: str | None = None,
    ) -> RelationshipAdvisoryDecision:
        """Persist an advisory decision without activating domain changes."""

        await self._require_domain_project(project_id, allow_archived=True)
        return await self._relationship_advisory_writer.apply_action(
            user_name=self.user_name,
            project_id=project_id,
            pattern_key=pattern_key,
            action=action,
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
            summary = result.to_dict()
            summary["projection_rebuilt"] = False
            summary["embeddings_rebuilt"] = False
            if result.updated:
                summary["projection"] = await knowledge_store.rebuild_project_projection(
                    project_id,
                    self.user_name,
                )
                summary["projection_rebuilt"] = True
                summary["embeddings"] = await knowledge_store.rebuild_project_embeddings(
                    project_id,
                    self.user_name,
                )
                summary["embeddings_rebuilt"] = True
            return summary

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
            summary = result.to_dict()
            summary["projection_rebuilt"] = False
            if result.updated:
                summary["projection"] = await knowledge_store.rebuild_project_projection(
                    project_id,
                    self.user_name,
                )
                summary["projection_rebuilt"] = True
            return summary

    def _require_no_active_runtimes(self, message: str) -> None:
        active_runtime_projects = [
            active_id
            for active_id in self._active_projects
            if self._project_leases.get(active_id)
        ]
        if active_runtime_projects:
            raise RuntimeError(f"{message}; active projects: {active_runtime_projects}")
