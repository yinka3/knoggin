import asyncio
import hashlib
import json
import uuid
from collections.abc import AsyncIterator, Mapping
from contextlib import asynccontextmanager
from enum import Enum
from functools import partial
from typing import Dict, List, Optional

from loguru import logger

from common.conf.domain_config import DomainConfig
from common.conf.manager import ConfigManager
from common.scoping import IDENTITY_ENTITY_ID, build_readable_project_ids
from common.utils.time_utils import get_now_iso
from core.community.community_job import AACJob
from core.ingestion.jobs.cleaner_job import EntityCleanupJob
from core.ingestion.jobs.dlq_job import DLQReplayJob
from core.ingestion.jobs.episode_job import EpisodeJob
from core.ingestion.services.pipeline_service import IngestionPipeline
from core.ingestion.services.processor import TextProcessor
from core.knowledge.db.write_graph_db import write_batch_callback
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.documents.indexing_job import DocumentIndexingRecoveryJob
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.jobs.audit_retention_cleanup_job import (
    AuditRetentionCleanupJob,
)
from core.knowledge.jobs.conflict_discovery_job import ConflictDiscoveryJob
from core.knowledge.jobs.merge_rollback_cleanup_job import (
    MergeCleanupJob,
)
from core.knowledge.relationship_advisories import (
    AdvisoryThresholds,
    RelationshipAdvisory,
    RelationshipAdvisoryDecision,
)
from core.project.domain_config_operations import (
    DomainCandidate,
    DomainConfigOperations,
    DomainPreview,
    DomainValidation,
    parse_candidate,
)
from core.project.domain_config_operations import (
    preview_domain_config as build_domain_preview,
)
from core.project.domain_config_operations import (
    validate_domain_config as validate_domain_candidate,
)
from core.project.domain_config_store import (
    DomainActivation,
    DomainConfigConflict,
    DomainConfigStore,
)
from core.project.state import ProjectState
from core.project.workspace_service import PROJECT_FILE_PATH, build_project_markdown
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from infrastructure.resources import ResourceManager


class ProjectStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


def _parse_initial_domain(candidate: DomainConfig | Mapping[str, object]) -> DomainConfig:
    """Validate the complete domain required to create a new project."""

    config = parse_candidate(candidate)
    if config.version != 0:
        raise ValueError("A new project's domain_config.version must be 0")
    if not config.topics:
        raise ValueError("A new project requires at least one domain topic")
    if not config.entity_types:
        raise ValueError("A new project requires at least one domain entity type")

    topics = {topic.name.casefold(): topic for topic in config.topics}
    identity_topic = topics.get("identity")
    if identity_topic is None or not identity_topic.active:
        raise ValueError(
            "A new project's domain must include an active 'Identity' topic"
        )
    entity_types = {entity.name.casefold(): entity for entity in config.entity_types}
    identity_type = entity_types.get("identity")
    if (
        identity_type is None
        or identity_type.topic.casefold() != "identity"
        or not identity_topic.active
    ):
        raise ValueError(
            "A new project's domain must include an entity type named 'Identity'"
        )
    return config.with_version(1)


class ProjectManager:
    """Manages the lifecycle and storage of Projects."""

    def __init__(self, resources: ResourceManager, user_name: str):
        self.resources = resources
        self.user_name = user_name
        self.pg = resources.postgres
        self._project_deletion_writer = ProjectDeletionWriter(self.pg)
        self.active_projects: Dict[str, ProjectState] = {}
        self._identity_initialized = False
        self._maintenance_lock = asyncio.Lock()
        self._closed = False

    @property
    def config(self):
        return ConfigManager.get().config

    @property
    def dev_settings(self):
        return self.config.developer_settings

    async def create_project(
        self,
        name: str,
        domain_config: DomainConfig | Mapping[str, object],
        description: Optional[str] = None,
        access_mode: str = "open",
        allowed_projects: Optional[List[str]] = None,
    ) -> dict:
        """Create a project with its first active domain revision."""
        if not name or not name.strip():
            raise ValueError("create_project requires a non-empty project name")

        name = name.strip()
        active_domain = _parse_initial_domain(domain_config)
        project_id = str(uuid.uuid4())
        allowed_projects = await self._validate_allowed_project_ids(
            project_id, allowed_projects or []
        )
        project_query = """
            INSERT INTO public.projects (
                project_id, user_name, name, description, access_mode, status,
                domain_config
            ) VALUES (
                %(project_id)s, %(user_name)s, %(name)s, %(description)s,
                %(access_mode)s, %(status)s, %(domain_config)s
            )
        """
        scope_query = """
            INSERT INTO public.project_read_scopes (
                user_name, project_id, readable_project_id
            )
            VALUES (%(user_name)s, %(project_id)s, %(readable)s)
        """
        project_workspace_writer = DocumentWriter(self.pg, project_id)
        workspace_source_id = str(
            uuid.uuid5(
                uuid.NAMESPACE_URL,
                f"knoggin-project-workspace:{project_id}",
            )
        )
        project_file_content = build_project_markdown(name, description).encode(
            "utf-8"
        )
        project_file_hash = hashlib.sha256(project_file_content).hexdigest()
        async with self.pg.transaction() as cur:
            await cur.execute(
                project_query,
                {
                    "project_id": project_id,
                    "user_name": self.user_name,
                    "name": name,
                    "description": description,
                    "access_mode": access_mode,
                    "status": ProjectStatus.ACTIVE.value,
                    "domain_config": json.dumps(active_domain.to_dict()),
                },
            )
            for allowed_id in allowed_projects:
                await cur.execute(
                    scope_query,
                    {
                        "user_name": self.user_name,
                        "project_id": project_id,
                        "readable": allowed_id,
                    },
                )
            await project_workspace_writer.insert_managed_workspace_source_and_file(
                cursor=cur,
                source_id=workspace_source_id,
                display_name="Project Workspace",
                relative_path=PROJECT_FILE_PATH,
                original_name=PROJECT_FILE_PATH,
                extension=".md",
                content=project_file_content,
                content_hash=project_file_hash,
                created_at=get_now_iso(),
            )

        logger.info(f"Created project {project_id} ('{name}')")
        return await self.get_project(project_id)

    async def list_projects(self) -> List[dict]:
        """List all projects and enrich with session counts."""
        query = """
            SELECT p.*,
                   (SELECT COUNT(*) FROM public.sessions s WHERE s.project_id = p.project_id AND s.status <> 'deleted') as session_count,
                   (SELECT array_agg(readable_project_id) FROM public.project_read_scopes rs WHERE rs.project_id = p.project_id) as allowed_projects
            FROM public.projects p
            WHERE p.user_name = %(user_name)s
            ORDER BY p.updated_at DESC
        """
        rows = await self.pg.fetch_all(query, {"user_name": self.user_name})

        projects = []
        for row in rows:
            meta = dict(row)
            meta["id"] = meta.pop("project_id")
            meta["allowed_projects"] = meta["allowed_projects"] or []
            meta.pop("domain_config", None)
            projects.append(meta)

        return projects

    async def get_project(self, project_id: str) -> Optional[dict]:
        """Get project metadata."""
        query = """
            SELECT p.*,
                   (SELECT COUNT(*) FROM public.sessions s WHERE s.project_id = p.project_id AND s.status <> 'deleted') as session_count,
                   (SELECT array_agg(readable_project_id) FROM public.project_read_scopes rs WHERE rs.project_id = p.project_id) as allowed_projects
            FROM public.projects p
            WHERE p.user_name = %(user_name)s AND p.project_id = %(project_id)s
        """
        rows = await self.pg.fetch_all(
            query, {"user_name": self.user_name, "project_id": project_id}
        )
        if not rows:
            return None

        meta = dict(rows[0])
        meta["id"] = meta.pop("project_id")
        meta["allowed_projects"] = meta["allowed_projects"] or []
        meta.pop("domain_config", None)
        return meta

    async def get_episode_window_size(self, project_id: str) -> int:
        """Return the one project-owned episode window setting."""

        row = await self.pg.fetch_one(
            """
            SELECT episode_window_size
            FROM public.projects
            WHERE user_name = %s AND project_id = %s
            """,
            (self.user_name, project_id),
        )
        if row is None:
            raise ValueError("Episode settings require an existing project")
        return int(row["episode_window_size"])

    async def update_episode_window_size(
        self, project_id: str, episode_window_size: int
    ) -> int:
        """Persist and immediately apply a project's episode window size."""

        if not 8 <= episode_window_size <= 72:
            raise ValueError("episode_window_size must be between 8 and 72")
        row = await self.pg.fetch_one(
            """
            UPDATE public.projects
            SET episode_window_size = %s, updated_at = now()
            WHERE user_name = %s AND project_id = %s
            RETURNING episode_window_size
            """,
            (episode_window_size, self.user_name, project_id),
        )
        if row is None:
            raise ValueError("Episode settings require an existing project")
        active = self.active_projects.get(project_id)
        if active is not None and active.episode_job is not None:
            active.episode_job.update_episode_window_size(episode_window_size)
        return int(row["episode_window_size"])

    def validate_domain_config(self, candidate: DomainCandidate) -> DomainValidation:
        """Validate a complete candidate without touching project state."""

        return validate_domain_candidate(candidate)

    async def get_domain_config(self, project_id: str) -> DomainConfig:
        """Read the durable active domain configuration for one project."""

        await self._require_domain_project(project_id, allow_archived=True)
        return await DomainConfigStore(self.pg).load(self.user_name, project_id)

    async def preview_domain_config(
        self,
        project_id: str,
        candidate: DomainCandidate,
    ) -> DomainPreview:
        """Compare a complete candidate with the project's active revision."""

        await self._require_domain_project(project_id, allow_archived=True)
        current = await DomainConfigStore(self.pg).load(
            self.user_name,
            project_id,
        )
        return build_domain_preview(current, candidate)

    async def activate_domain_config(
        self,
        project_id: str,
        candidate: DomainCandidate,
        *,
        expected_version: int,
    ) -> DomainActivation:
        """Validate and optimistically activate a project's domain candidate.

        Active runtimes receive the new immutable snapshot through
        ``ProjectState``.  Projects without a loaded runtime update durable
        storage directly; their next runtime bootstrap reads the new revision.
        """

        async with self._maintenance_lock:
            await self._require_domain_project(project_id, allow_archived=False)
            active_state = self.active_projects.get(project_id)
            if active_state is not None:
                return await DomainConfigOperations.activate(
                    active_state,
                    candidate,
                    expected_version=expected_version,
                )

            parsed = parse_candidate(candidate)
            return await DomainConfigStore(self.pg).activate(
                user_name=self.user_name,
                project_id=project_id,
                candidate=parsed,
                expected_version=expected_version,
            )

    async def _require_domain_project(
        self,
        project_id: str,
        *,
        allow_archived: bool,
    ) -> dict:
        project = await self.get_project(project_id)
        if project is None:
            raise ValueError(f"Project '{project_id}' does not exist")
        allowed_statuses = {ProjectStatus.ACTIVE.value}
        if allow_archived:
            allowed_statuses.add(ProjectStatus.ARCHIVED.value)
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
        return await self.resources.knowledge_store.get_relationship_advisories(
            user_name=self.user_name,
            project_id=project_id,
            thresholds=thresholds,
        )

    async def get_open_human_reviews(self, project_id: str):
        """Return workflow-neutral inbox entries for a project."""

        await self._require_domain_project(project_id, allow_archived=True)
        return await self.resources.knowledge_store.get_open_human_reviews(
            user_name=self.user_name,
            project_id=project_id,
        )

    async def requeue_parked_dlq_item(self, project_id: str, dlq_id: str) -> bool:
        """Requeue a human-reviewed DLQ item through its active project runtime."""
        await self._require_domain_project(project_id, allow_archived=True)
        state = self.active_projects.get(project_id)
        if state is None or state.dlq_job is None:
            raise RuntimeError("Project runtime is not active for DLQ requeue")
        return await state.dlq_job.requeue_parked_dlq_item(
            user_name=self.user_name,
            project_id=project_id,
            dlq_id=dlq_id,
        )

    async def get_conflict_group(self, project_id: str, conflict_id: str) -> dict:
        """Return the conflict workflow subject and immutable evidence snapshots."""

        await self._require_domain_project(project_id, allow_archived=True)
        detail = await self.resources.knowledge_store.get_conflict_group(
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
        return await self.resources.knowledge_store.resolve_conflict_group(
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
        return await self.resources.knowledge_store.apply_relationship_advisory_action(
            user_name=self.user_name,
            project_id=project_id,
            pattern_key=pattern_key,
            action=action,
            relationship_type=relationship_type,
            note=note,
            decided_by=decided_by,
        )

    async def get_readable_project_ids(
        self,
        project_id: str,
        *,
        project_metadata: Optional[dict] = None,
    ) -> List[str]:
        """Return projects readable from project_id."""
        meta = project_metadata or await self.get_project(project_id)
        if not meta or meta["status"] == ProjectStatus.DELETED.value:
            return []

        allowed = meta.get("allowed_projects", [])
        if allowed:
            # Filter allowed to only ACTIVE or ARCHIVED projects
            valid_query = """
                SELECT project_id FROM public.projects
                WHERE user_name = %(user_name)s AND project_id = ANY(%(allowed)s)
                AND status IN ('active', 'archived')
            """
            rows = await self.pg.fetch_all(
                valid_query, {"user_name": self.user_name, "allowed": allowed}
            )
            allowed = [r["project_id"] for r in rows]

        return build_readable_project_ids(project_id, allowed)

    async def get_session_ids(self, project_id: str) -> List[str]:
        """Return durable session IDs currently associated with a project."""
        rows = await self.pg.fetch_all(
            """
            SELECT session_id
            FROM public.sessions
            WHERE user_name = %(user_name)s
              AND project_id = %(project_id)s
              AND status <> 'deleted'
            ORDER BY created_at ASC
            """,
            {"user_name": self.user_name, "project_id": project_id},
        )
        return [row["session_id"] for row in rows]

    async def get_episode_session_participation(self, project_id: str) -> List[dict]:
        """List the sessions currently allowed to feed future episode windows."""

        rows = await self.pg.fetch_all(
            """
            SELECT session_id, episode_participation_enabled,
                   episode_participation_after_message_id
            FROM public.sessions
            WHERE user_name = %(user_name)s
              AND project_id = %(project_id)s
              AND status <> 'deleted'
            ORDER BY created_at ASC
            """,
            {"user_name": self.user_name, "project_id": project_id},
        )
        return [
            {
                "session_id": str(row["session_id"]),
                "enabled": bool(row["episode_participation_enabled"]),
                "after_message_id": int(
                    row["episode_participation_after_message_id"]
                ),
            }
            for row in rows
        ]

    async def set_episode_participating_sessions(
        self, project_id: str, session_ids: List[str]
    ) -> List[dict]:
        """Select exactly which project sessions feed future episode windows.

        A state transition records the current message frontier.  Therefore a
        session enabled later contributes only messages made after that choice,
        rather than reviving material intentionally excluded while disabled.
        """

        selected = {session_id for session_id in session_ids if session_id}
        async with self.pg.transaction() as cur:
            await cur.execute(
                """
                SELECT session_id, episode_participation_enabled
                FROM public.sessions
                WHERE user_name = %s
                  AND project_id = %s
                  AND status <> 'deleted'
                FOR UPDATE
                """,
                (self.user_name, project_id),
            )
            rows = await cur.fetchall()
            available = {str(row["session_id"]) for row in rows}
            unknown = selected.difference(available)
            if unknown:
                raise ValueError(
                    "Episode participation includes sessions outside this project: "
                    + ", ".join(sorted(unknown))
                )
            for row in rows:
                session_id = str(row["session_id"])
                enabled = session_id in selected
                if enabled == bool(row["episode_participation_enabled"]):
                    continue
                await cur.execute(
                    """
                    UPDATE public.sessions
                    SET episode_participation_enabled = %s,
                        episode_participation_after_message_id = COALESCE(
                            (
                                SELECT MAX(message_id)
                                FROM public.messages
                                WHERE user_name = %s
                                  AND project_id = %s
                                  AND session_id = %s
                            ),
                            0
                        )
                    WHERE user_name = %s
                      AND project_id = %s
                      AND session_id = %s
                    """,
                    (
                        enabled,
                        self.user_name,
                        project_id,
                        session_id,
                        self.user_name,
                        project_id,
                        session_id,
                    ),
                )

        active = self.active_projects.get(project_id)
        if active is not None:
            await active.scheduler.record_activity()
        return await self.get_episode_session_participation(project_id)

    async def add_session(self, project_id: str, session_id: str) -> None:
        """Record project/session membership when a caller manages a session row."""
        await self.pg.execute(
            """
            INSERT INTO public.sessions (
                session_id, user_name, project_id, status
            ) VALUES (
                %(session_id)s, %(user_name)s, %(project_id)s, 'open'
            )
            ON CONFLICT (session_id) DO UPDATE
            SET last_active_at = now()
            WHERE public.sessions.user_name = EXCLUDED.user_name
              AND public.sessions.project_id = EXCLUDED.project_id
              AND public.sessions.status = 'open'
            """,
            {
                "session_id": session_id,
                "user_name": self.user_name,
                "project_id": project_id,
            },
        )

    async def remove_session(self, project_id: str, session_id: str) -> None:
        """Remove durable project/session membership for explicit session cleanup."""
        await self.pg.execute(
            """
            DELETE FROM public.sessions
            WHERE user_name = %(user_name)s
              AND project_id = %(project_id)s
              AND session_id = %(session_id)s
            """,
            {
                "user_name": self.user_name,
                "project_id": project_id,
                "session_id": session_id,
            },
        )

    async def _validate_allowed_project_ids(
        self, project_id: str, allowed_projects: List[str]
    ) -> List[str]:
        requested = list(
            dict.fromkeys(
                allowed_id
                for allowed_id in allowed_projects
                if allowed_id and allowed_id != project_id
            )
        )
        if not requested:
            return requested

        query = """
            SELECT project_id, status FROM public.projects
            WHERE user_name = %(user_name)s AND project_id = ANY(%(requested)s)
        """
        rows = await self.pg.fetch_all(
            query, {"user_name": self.user_name, "requested": requested}
        )
        stored_projects = {r["project_id"]: r["status"] for r in rows}

        readable_statuses = {ProjectStatus.ACTIVE.value, ProjectStatus.ARCHIVED.value}
        unavailable = []
        for allowed_id in requested:
            status = stored_projects.get(allowed_id)
            if status not in readable_statuses:
                unavailable.append(allowed_id)
        if unavailable:
            raise ValueError(
                f"Unavailable allowed project IDs for '{project_id}': {unavailable}"
            )
        return requested

    async def update_project(
        self,
        project_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        allowed_projects: Optional[List[str]] = None,
    ) -> Optional[dict]:
        """Update project metadata that does not control lifecycle status."""
        meta = await self.get_project(project_id)
        if not meta:
            return None
        if meta["status"] == ProjectStatus.DELETED.value:
            raise ValueError(f"Deleted project '{project_id}' cannot be updated")

        col_values = {}

        if name is not None:
            if not name.strip():
                raise ValueError("update_project requires a non-empty project name")
            col_values["name"] = name.strip()
        if description is not None:
            col_values["description"] = description

        if allowed_projects is not None:
            active_state = self.active_projects.get(project_id)
            if active_state and active_state.active_runtime_sessions_count > 0:
                raise RuntimeError(
                    f"Project '{project_id}' has active runtime sessions and "
                    "cannot change its readable project scope"
                )
            validated_allowed = await self._validate_allowed_project_ids(
                project_id, allowed_projects
            )
            if active_state:
                await active_state.shutdown()
                del self.active_projects[project_id]

        if allowed_projects is not None or col_values:
            async with self.pg.transaction() as cur:
                if allowed_projects is not None:
                    await cur.execute(
                        "DELETE FROM public.project_read_scopes "
                        "WHERE user_name = %(user_name)s "
                        "AND project_id = %(project_id)s",
                        {"user_name": self.user_name, "project_id": project_id},
                    )
                    for allowed_id in validated_allowed:
                        await cur.execute(
                            """
                            INSERT INTO public.project_read_scopes (
                                user_name, project_id, readable_project_id
                            )
                            VALUES (%(user_name)s, %(project_id)s, %(readable)s)
                            """,
                            {
                                "user_name": self.user_name,
                                "project_id": project_id,
                                "readable": allowed_id,
                            },
                        )

                if col_values:
                    fields = ", ".join(f"{column} = %s" for column in col_values)
                    await cur.execute(
                        f"UPDATE public.projects SET {fields}, updated_at = now()"
                        " WHERE user_name = %s AND project_id = %s",
                        [*col_values.values(), self.user_name, project_id],
                    )

        return await self.get_project(project_id)

    async def archive_project(self, project_id: str) -> Optional[dict]:
        """Retire a project while retaining its sessions and knowledge."""
        meta = await self.get_project(project_id)
        if not meta:
            return None
        if meta["status"] == ProjectStatus.DELETED.value:
            raise ValueError(f"Deleted project '{project_id}' cannot be archived")
        if meta["status"] == ProjectStatus.ARCHIVED.value:
            return meta

        active_state = self.active_projects.get(project_id)
        if active_state and active_state.active_runtime_sessions_count > 0:
            raise RuntimeError(
                f"Project '{project_id}' has active runtime sessions and cannot be archived"
            )
        if active_state:
            await active_state.shutdown()
            del self.active_projects[project_id]

        query = "UPDATE public.projects SET status = %(status)s, archived_at = now(), updated_at = now() WHERE user_name = %(user_name)s AND project_id = %(project_id)s"
        await self.pg.execute(
            query,
            {
                "status": ProjectStatus.ARCHIVED.value,
                "user_name": self.user_name,
                "project_id": project_id,
            },
        )
        logger.info(f"Archived project {project_id} with knowledge retained")
        return await self.get_project(project_id)

    async def reactivate_project(self, project_id: str) -> Optional[dict]:
        """Make an archived project eligible for sessions again."""
        meta = await self.get_project(project_id)
        if not meta:
            return None
        if meta["status"] == ProjectStatus.DELETED.value:
            raise ValueError(f"Deleted project '{project_id}' cannot be reactivated")
        if meta["status"] == ProjectStatus.ACTIVE.value:
            return meta

        query = "UPDATE public.projects SET status = %(status)s, archived_at = NULL, updated_at = now() WHERE user_name = %(user_name)s AND project_id = %(project_id)s"
        await self.pg.execute(
            query,
            {
                "status": ProjectStatus.ACTIVE.value,
                "user_name": self.user_name,
                "project_id": project_id,
            },
        )
        logger.info(f"Reactivated project {project_id}")
        return await self.get_project(project_id)

    async def delete_project(self, project_id: str) -> Optional[dict]:
        """Hard delete every PostgreSQL, AGE, and Redis record owned by a project."""
        async with self._maintenance_lock:
            meta = await self.get_project(project_id)
            if not meta:
                return None

            active_state = self.active_projects.get(project_id)
            if active_state and active_state.active_runtime_sessions_count > 0:
                raise RuntimeError(
                    f"Project '{project_id}' has active runtime sessions and "
                    "cannot be deleted"
                )
            if active_state:
                await active_state.shutdown()
                del self.active_projects[project_id]

            session_ids, agent_ids = await self._project_runtime_member_ids(project_id)
            await self._delete_project_redis_state(
                project_id,
                session_ids=session_ids,
                agent_ids=agent_ids,
            )
            deleted = await self._project_deletion_writer.delete_project(
                user_name=self.user_name,
                project_id=project_id,
            )
            if deleted is None:
                raise RuntimeError(
                    f"Project '{project_id}' disappeared during deletion"
                )

            logger.info(
                f"Hard deleted project {project_id} and all owned state: {deleted}"
            )
            meta["status"] = ProjectStatus.DELETED.value
            return meta

    async def _project_runtime_member_ids(
        self,
        project_id: str,
    ) -> tuple[List[str], List[str]]:
        if getattr(self.resources, "redis", None) is None:
            return [], []
        session_rows = await self.pg.fetch_all(
            """
            SELECT session_id
            FROM public.sessions
            WHERE user_name = %(user_name)s AND project_id = %(project_id)s
            """,
            {"user_name": self.user_name, "project_id": project_id},
        )
        agent_rows = await self.pg.fetch_all(
            """
            SELECT agent_id
            FROM public.agents
            WHERE user_name = %(user_name)s AND project_id = %(project_id)s
            """,
            {"user_name": self.user_name, "project_id": project_id},
        )
        return (
            [str(row["session_id"]) for row in session_rows],
            [str(row["agent_id"]) for row in agent_rows],
        )

    async def _delete_project_redis_state(
        self,
        project_id: str,
        *,
        session_ids: List[str],
        agent_ids: List[str],
    ) -> int:
        redis = getattr(self.resources, "redis", None)
        if redis is None:
            return 0

        keys = set(RedisKeys.project_cleanup_keys(self.user_name, project_id))
        patterns = list(RedisKeys.project_cleanup_patterns(self.user_name, project_id))
        for session_id in session_ids:
            keys.update(RedisKeys.session_keys(self.user_name, session_id))
            patterns.extend(
                (
                    RedisKeys.message_dedup_pattern(self.user_name, session_id),
                    RedisKeys.session_memory_pattern(self.user_name, session_id),
                )
            )
        for agent_id in agent_ids:
            keys.add(RedisKeys.agent_directives(self.user_name, agent_id))
            keys.add(RedisKeys.community_agent_memory(self.user_name, agent_id))

        for pattern in patterns:
            cursor = 0
            while True:
                cursor, matched = await redis.scan(
                    cursor,
                    match=pattern,
                    count=100,
                )
                keys.update(matched)
                if cursor == 0:
                    break

        deleted = int(await redis.delete(*sorted(keys))) if keys else 0
        await redis.hdel(RedisKeys.projects(self.user_name), project_id)
        if session_ids:
            await redis.hdel(RedisKeys.sessions(self.user_name), *session_ids)
        if agent_ids:
            await redis.hdel(RedisKeys.agents(self.user_name), *agent_ids)
            default_key = RedisKeys.agents_default(self.user_name)
            if await redis.get(default_key) in set(agent_ids):
                deleted += int(await redis.delete(default_key))
        return deleted

    async def acquire_project_for_session(
        self, project_id: str, session_id: str
    ) -> ProjectState:
        """Acquire runtime project state and record durable session membership."""
        async with self._maintenance_lock:
            if self._closed:
                raise RuntimeError("ProjectManager is shutting down")
            return await self._acquire_project_for_session(
                project_id,
                session_id,
            )

    @asynccontextmanager
    async def project_runtime(
        self,
        project_id: str,
        session_id: str,
    ) -> AsyncIterator[ProjectState]:
        """Own one session's lease on a project's in-memory runtime.

        A caller that successfully enters this context owns exactly one
        ``acquire_project_for_session`` reference.  Releasing that reference is
        guaranteed on normal completion, failure, and cancellation.
        """
        state = await self.acquire_project_for_session(
            project_id,
            session_id,
        )
        try:
            yield state
        finally:
            await self.release_project(project_id)

    async def _acquire_project_for_session(
        self, project_id: str, session_id: str
    ) -> ProjectState:
        if not project_id or not project_id.strip():
            raise ValueError("A persisted project_id is required to acquire a project")
        project = await self.get_project(project_id)
        if project is None:
            raise ValueError(
                f"Project '{project_id}' does not exist; "
                "create it before creating a session"
            )
        if project["status"] != ProjectStatus.ACTIVE.value:
            raise ValueError(
                f"Project '{project_id}' is {project['status']} and cannot "
                "create or resume sessions"
            )

        await self._ensure_identity_invariant()
        project_state = await self._get_or_start_project(
            project_id,
            project_metadata=project,
        )
        return project_state

    async def get_or_start_project(self, project_id: str) -> ProjectState:
        """Get an existing ProjectState or bootstrap a new one."""
        async with self._maintenance_lock:
            return await self._get_or_start_project(
                project_id,
            )

    async def _get_or_start_project(
        self,
        project_id: str,
        project_metadata: Optional[dict] = None,
    ) -> ProjectState:
        project = project_metadata or await self.get_project(project_id)
        if project is None:
            raise ValueError(
                f"Project '{project_id}' does not exist; "
                "create it before starting project runtime"
            )
        if project["status"] != ProjectStatus.ACTIVE.value:
            raise ValueError(
                f"Project '{project_id}' is {project['status']} and cannot "
                "start project runtime"
            )

        if project_id in self.active_projects:
            self.active_projects[project_id].active_runtime_sessions_count += 1
            return self.active_projects[project_id]

        logger.info(f"Bootstrapping ProjectState for project_id: {project_id}")
        readable_project_ids = await self.get_readable_project_ids(
            project_id,
            project_metadata=project,
        )

        domain_store = DomainConfigStore(self.pg)
        domain_config = await domain_store.load(self.user_name, project_id)
        compiled_domain = domain_config.compile()

        # Entity Manager
        er_cfg = self.dev_settings.entity_resolution
        entities = EntityResolver(
            project_id=project_id,
            readable_project_ids=readable_project_ids,
            knowledge_store=self.resources.knowledge_store,
            embedding_service=self.resources.embedding,
            fuzzy_substring_threshold=er_cfg.fuzzy_substring_threshold,
            fuzzy_non_substring_threshold=er_cfg.fuzzy_non_substring_threshold,
            generic_token_freq=er_cfg.generic_token_freq,
            candidate_fuzzy_threshold=er_cfg.candidate_fuzzy_threshold,
            candidate_vector_threshold=er_cfg.candidate_vector_threshold,
        )

        # NLP Pipeline
        nlp_cfg = self.dev_settings.nlp_pipeline
        pipeline = await asyncio.get_running_loop().run_in_executor(
            self.resources.executor,
            partial(
                TextProcessor,
                llm=self.resources.llm_service,
                get_known_aliases=entities.get_known_aliases,
                get_alias_version=entities.get_alias_version,
                get_profile=entities.get_profile,
                gliner=self.resources.gliner,
                spacy=self.resources.spacy,
                settings=nlp_cfg,
                model_work=getattr(self.resources, "model_work", None),
            ),
        )

        project_processor = IngestionPipeline(
            project_id=project_id,
            redis_client=self.resources.redis,
            llm=self.resources.llm_service,
            entities=entities,
            processor=pipeline,
            knowledge_store=self.resources.knowledge_store,
            cpu_executor=self.resources.executor,
            user_name=self.user_name,
            compiled_domain=compiled_domain,
            get_next_ent_id=self.resources.knowledge_store.allocate_entity_id,
            resolution_threshold=er_cfg.resolution_threshold,
            common_word_frequency_threshold=er_cfg.common_word_frequency_threshold,
            sparse_context_verbs=er_cfg.sparse_context_verbs,
        )

        await self._verify_user_entity(entities)

        scheduler = Scheduler(
            self.user_name,
            project_id,
            self.resources.redis,
            background_work=getattr(self.resources, "background_work", None),
        )
        episode_job = self._init_episode_job(project_id)

        project_state = ProjectState(
            project_id=project_id,
            entities=entities,
            pipeline=pipeline,
            scheduler=scheduler,
            user_name=self.user_name,
            redis_client=self.resources.redis,
            postgres_client=self.resources.postgres,
            embedding_service=self.resources.embedding,
            domain_config=domain_config,
            readable_project_ids=readable_project_ids,
            batch_processor=project_processor,
            background_work=getattr(self.resources, "background_work", None),
            domain_config_store=domain_store,
        )
        project_state.episode_job = episode_job

        self._register_background_jobs(
            project_state,
            entities,
            project_processor,
            episode_job,
        )
        project_state.active_runtime_sessions_count = 1
        self.active_projects[project_id] = project_state

        return project_state

    async def rebuild_project_search_indexes(self, project_id: str) -> Dict[str, int]:
        async with self._maintenance_lock:
            project = await self.get_project(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")

            active_runtime_projects = [
                active_id
                for active_id, state in self.active_projects.items()
                if state.active_runtime_sessions_count > 0
            ]
            if active_runtime_projects:
                raise RuntimeError(
                    "Search index repair requires all project runtimes to be "
                    f"inactive; active projects: {active_runtime_projects}"
                )

            rows = await self.pg.fetch_all(
                """
                SELECT project_id
                FROM public.projects
                WHERE user_name = %(user_name)s
                  AND status IN ('active', 'archived')
                ORDER BY project_id
                """,
                {"user_name": self.user_name},
            )
            identity_project_ids = [row["project_id"] for row in rows]
            return await self.resources.knowledge_store.rebuild_project_search_indexes(
                project_id,
                self.user_name,
                identity_project_ids,
            )

    async def preview_historical_reclassification(
        self,
        project_id: str,
        *,
        limit: int | None = 1000,
    ) -> dict:
        """Preview deterministic historical entity changes for active domain."""

        async with self._maintenance_lock:
            project = await self.get_project(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")

            stored = await DomainConfigStore(self.pg).load(
                self.user_name,
                project_id,
            )
            domain = stored.compile()
            return await self.resources.knowledge_store.preview_historical_reclassification(
                user_name=self.user_name,
                project_id=project_id,
                domain=domain,
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
        """Apply explicit, bounded historical entity reclassification.

        The project must be runtime-inactive. Reclassification changes only
        canonical entity type/topic fields; relationship observations retain
        the source types they had when the evidence was captured. Derived AGE
        and search projections are rebuilt after successful updates.
        """

        if (
            not isinstance(expected_domain_version, int)
            or isinstance(expected_domain_version, bool)
            or expected_domain_version < 0
        ):
            raise ValueError("expected_domain_version must be a non-negative integer")

        async with self._maintenance_lock:
            project = await self.get_project(project_id)
            if project is None:
                raise ValueError(f"Project '{project_id}' does not exist")

            active_runtime_projects = [
                active_id
                for active_id, state in self.active_projects.items()
                if state.active_runtime_sessions_count > 0
            ]
            if active_runtime_projects:
                raise RuntimeError(
                    "Historical reclassification requires all project runtimes "
                    "to be inactive; active projects: "
                    f"{active_runtime_projects}"
                )

            stored = await DomainConfigStore(self.pg).load(
                self.user_name,
                project_id,
            )
            actual_version = stored.version
            if actual_version != expected_domain_version:
                raise DomainConfigConflict(expected_domain_version, actual_version)

            domain = stored.compile()
            result = await self.resources.knowledge_store.reclassify_historical_entities(
                user_name=self.user_name,
                project_id=project_id,
                domain=domain,
                batch_size=batch_size,
                max_entities=max_entities,
            )
            summary = result.to_dict()
            summary["projection_rebuilt"] = False
            summary["search_index_rebuilt"] = False
            if result.updated:
                summary["projection"] = (
                    await self.resources.knowledge_store.rebuild_project_projection(
                        project_id,
                        self.user_name,
                    )
                )
                summary["projection_rebuilt"] = True

                rows = await self.pg.fetch_all(
                    """
                    SELECT project_id
                    FROM public.projects
                    WHERE user_name = %(user_name)s
                      AND status IN ('active', 'archived')
                    ORDER BY project_id
                    """,
                    {"user_name": self.user_name},
                )
                summary["search_index"] = (
                    await self.resources.knowledge_store.rebuild_project_search_indexes(
                        project_id,
                        self.user_name,
                        [row["project_id"] for row in rows],
                    )
                )
                summary["search_index_rebuilt"] = True
            return summary

    async def preview_historical_relationship_normalization(
        self,
        project_id: str,
        *,
        limit: int | None = 1000,
    ) -> dict:
        """Preview deterministic historical relationship normalization."""

        async with self._maintenance_lock:
            await self._require_domain_project(project_id, allow_archived=True)
            stored = await DomainConfigStore(self.pg).load(
                self.user_name,
                project_id,
            )
            return await self.resources.knowledge_store.preview_historical_relationship_normalization(
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
        """Apply explicit, bounded historical relationship normalization.

        This operation only rewrites already-persisted unrecognized
        relationships after an immutable domain-version check. It never runs
        automatically during domain activation.
        """

        if (
            not isinstance(expected_domain_version, int)
            or isinstance(expected_domain_version, bool)
            or expected_domain_version < 0
        ):
            raise ValueError("expected_domain_version must be a non-negative integer")

        async with self._maintenance_lock:
            await self._require_domain_project(project_id, allow_archived=True)
            active_runtime_projects = [
                active_id
                for active_id, state in self.active_projects.items()
                if state.active_runtime_sessions_count > 0
            ]
            if active_runtime_projects:
                raise RuntimeError(
                    "Historical relationship normalization requires all project "
                    "runtimes to be inactive; active projects: "
                    f"{active_runtime_projects}"
                )

            stored = await DomainConfigStore(self.pg).load(
                self.user_name,
                project_id,
            )
            if stored.version != expected_domain_version:
                raise DomainConfigConflict(expected_domain_version, stored.version)

            result = await self.resources.knowledge_store.normalize_historical_relationships(
                user_name=self.user_name,
                project_id=project_id,
                domain=stored.compile(),
                batch_size=batch_size,
                max_relationships=max_relationships,
            )
            summary = result.to_dict()
            summary["projection_rebuilt"] = False
            if result.updated:
                summary["projection"] = (
                    await self.resources.knowledge_store.rebuild_project_projection(
                        project_id,
                        self.user_name,
                    )
                )
                summary["projection_rebuilt"] = True
            return summary

    async def release_project(self, project_id: str):
        """Release runtime project state when an active session closes."""
        async with self._maintenance_lock:
            if project_id not in self.active_projects:
                return

            state = self.active_projects[project_id]
            state.active_runtime_sessions_count -= 1

            if state.active_runtime_sessions_count <= 0:
                await state.shutdown()
                del self.active_projects[project_id]
                logger.info(f"Released ProjectState for project_id: {project_id}")

    async def shutdown(self) -> None:
        """Stop every remaining project runtime before shared resources close."""

        async with self._maintenance_lock:
            if self._closed and not self.active_projects:
                return
            self._closed = True
            states = list(self.active_projects.values())
            self.active_projects.clear()
            for state in states:
                state.active_runtime_sessions_count = 0

        results = await asyncio.gather(
            *(state.shutdown() for state in states),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise RuntimeError(
                f"Failed to shut down {len(failures)} project runtime(s)"
            ) from failures[0]

    async def _verify_user_entity(self, entities: EntityResolver) -> None:
        user_id = await entities.get_id(self.user_name)
        if user_id != IDENTITY_ENTITY_ID:
            raise RuntimeError(
                f"Configured user '{self.user_name}' did not resolve to reserved "
                f"entity ID {IDENTITY_ENTITY_ID}"
            )
        logger.info(f"User entity verified: {self.user_name} (id={IDENTITY_ENTITY_ID})")

    async def _ensure_identity_invariant(self) -> None:
        if self._identity_initialized:
            return

        await self.resources.knowledge_store.ensure_identity_entity(
            self.user_name,
            getattr(self.config, "user_aliases", []),
        )
        self._identity_initialized = True

    def _init_episode_job(self, project_id: str) -> EpisodeJob:
        jobs_cfg = self.dev_settings.jobs
        return EpisodeJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs_cfg.episode,
            llm=self.resources.llm_service,
            embedding_service=self.resources.embedding,
            episode_window_size_provider=lambda: self.get_episode_window_size(project_id),
        )

    def _register_background_jobs(
        self,
        project_state: ProjectState,
        entities: EntityResolver,
        processor: IngestionPipeline,
        episode_job: Optional[EpisodeJob] = None,
    ):
        scheduler = project_state.scheduler
        project_id = project_state.project_id
        jobs_cfg = self.dev_settings.jobs
        episode_job = episode_job or self._init_episode_job(project_id)

        config_mgr = ConfigManager.get()

        async def _dlq_write_callback(result):
            if not result.scope or not result.scope.session_id:
                return False, "DLQ graph replay missing source session_id"
            return await write_batch_callback(
                result,
                knowledge_store=self.resources.knowledge_store,
                entities=entities,
                session_id=result.scope.session_id,
                project_id=project_id,
                user_name=self.user_name,
                redis_client=self.resources.redis,
            )

        def _entity_resolution_updated(config):
            entities.update_settings(config)
            processor.update_settings(config)

        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                _entity_resolution_updated, "developer_settings.entity_resolution"
            )
        )
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                processor.update_settings, "developer_settings.nlp_pipeline"
            )
        )
        scheduler.register(episode_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                lambda config: episode_job.update_settings(
                    config,
                ),
                "developer_settings.jobs.episode",
            )
        )

        document_index_job = DocumentIndexingRecoveryJob(
            project_state.document_service,
            jobs_cfg.document_indexing,
        )
        scheduler.register(document_index_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                document_index_job.update_settings,
                "developer_settings.jobs.document_indexing",
            )
        )

        dlq_cfg = jobs_cfg.dlq
        dlq_job = DLQReplayJob(
            entities=entities,
            processor=processor,
            write_to_graph=_dlq_write_callback,
            redis_client=self.resources.redis,
            settings=dlq_cfg,
        )
        project_state.dlq_job = dlq_job
        scheduler.register(dlq_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(dlq_job.update_settings, "developer_settings.jobs.dlq")
        )
        cleaner_job = EntityCleanupJob(
            user_name=self.user_name,
            knowledge_store=self.resources.knowledge_store,
            entities=entities,
            redis_client=self.resources.redis,
            settings=jobs_cfg.cleaner,
        )
        scheduler.register(cleaner_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                cleaner_job.update_settings, "developer_settings.jobs.cleaner"
            )
        )

        rollback_cleanup_job = MergeCleanupJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs_cfg.merge_rollback,
        )
        scheduler.register(rollback_cleanup_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                rollback_cleanup_job.update_settings,
                "developer_settings.jobs.merge_rollback",
            )
        )

        audit_retention_job = AuditRetentionCleanupJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs_cfg.audit_retention,
        )
        scheduler.register(audit_retention_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                audit_retention_job.update_settings,
                "developer_settings.jobs.audit_retention",
            )
        )

        conflict_discovery_job = ConflictDiscoveryJob(
            knowledge_store=self.resources.knowledge_store,
            settings=jobs_cfg.conflict_discovery,
            llm=self.resources.llm_service,
        )
        scheduler.register(conflict_discovery_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                conflict_discovery_job.update_settings,
                "developer_settings.jobs.conflict_discovery",
            )
        )

        scheduler.register(AACJob(project_state, self.resources))
