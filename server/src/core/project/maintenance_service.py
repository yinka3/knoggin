"""Project-scoped knowledge maintenance and repair workflows."""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable, Mapping

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
                "domain configuration operations"
            )
        return project

    async def get_relationship_advisories(
        self,
        project_id: str,
        *,
        thresholds: AdvisoryThresholds | None = None,
    ) -> list[RelationshipAdvisory]:
        """Read evidence-backed relationship advisories with dispositions."""

        await self._require_domain_project(project_id, allow_archived=True)
        knowledge_store = self.resources.knowledge_store
        if knowledge_store is None:
            raise RuntimeError("Knowledge storage is unavailable")
        return await knowledge_store.get_relationship_advisories(
            user_name=self.user_name,
            project_id=project_id,
            thresholds=thresholds,
        )

    async def get_open_human_reviews(self, project_id: str):
        """Return workflow-neutral inbox entries for a project."""

        await self._require_domain_project(project_id, allow_archived=True)
        knowledge_store = self.resources.knowledge_store
        if knowledge_store is None:
            raise RuntimeError("Knowledge storage is unavailable")
        return await knowledge_store.get_open_human_reviews(
            user_name=self.user_name,
            project_id=project_id,
        )

    async def get_conflict_group(self, project_id: str, conflict_id: str) -> dict:
        """Return the conflict workflow subject and immutable evidence snapshots."""

        await self._require_domain_project(project_id, allow_archived=True)
        knowledge_store = self.resources.knowledge_store
        if knowledge_store is None:
            raise RuntimeError("Knowledge storage is unavailable")
        detail = await knowledge_store.get_conflict_group(
            conflict_id=conflict_id,
            user_name=self.user_name,
            project_id=project_id,
        )
        if detail is None:
            raise FileNotFoundError("Conflict group not found")
        return detail

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
        knowledge_store = self.resources.knowledge_store
        if knowledge_store is None:
            raise RuntimeError("Knowledge storage is unavailable")
        return await knowledge_store.resolve_conflict_group(
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
        knowledge_store = self.resources.knowledge_store
        if knowledge_store is None:
            raise RuntimeError("Knowledge storage is unavailable")
        return await knowledge_store.apply_relationship_advisory_action(
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
            project = await self._project_lookup(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")
            active_runtime_projects = [
                active_id
                for active_id in self._active_projects
                if self._project_leases.get(active_id)
            ]
            if active_runtime_projects:
                raise RuntimeError(
                    "Embedding rebuild requires all project runtimes to be "
                    f"inactive; active projects: {active_runtime_projects}"
                )
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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
            project = await self._project_lookup(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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
            project = await self._project_lookup(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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
            project = await self._project_lookup(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")
            stored = await DomainConfigStore(self.pg).load(self.user_name, project_id)
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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

        if (
            not isinstance(expected_domain_version, int)
            or isinstance(expected_domain_version, bool)
            or expected_domain_version < 0
        ):
            raise ValueError("expected_domain_version must be a non-negative integer")

        async with self._lock:
            project = await self._project_lookup(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")
            self._require_no_active_runtimes(
                "Historical reclassification requires all project runtimes to be inactive"
            )
            stored = await DomainConfigStore(self.pg).load(self.user_name, project_id)
            if stored.version != expected_domain_version:
                raise DomainConfigConflict(expected_domain_version, stored.version)
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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
            stored = await DomainConfigStore(self.pg).load(self.user_name, project_id)
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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

        if (
            not isinstance(expected_domain_version, int)
            or isinstance(expected_domain_version, bool)
            or expected_domain_version < 0
        ):
            raise ValueError("expected_domain_version must be a non-negative integer")

        async with self._lock:
            await self._require_domain_project(project_id, allow_archived=True)
            self._require_no_active_runtimes(
                "Historical relationship normalization requires all project runtimes to be inactive"
            )
            stored = await DomainConfigStore(self.pg).load(self.user_name, project_id)
            if stored.version != expected_domain_version:
                raise DomainConfigConflict(expected_domain_version, stored.version)
            knowledge_store = self.resources.knowledge_store
            if knowledge_store is None:
                raise RuntimeError("Knowledge storage is unavailable")
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
