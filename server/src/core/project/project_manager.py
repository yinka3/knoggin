import asyncio
import json
import uuid
from collections.abc import Mapping
from enum import Enum
from typing import Dict, List, Optional

from loguru import logger

from common.conf.domain_config import DomainConfig
from common.conf.manager import ConfigManager
from common.scoping import build_readable_project_ids
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.documents.filesystem import ProjectFilesystemFactory
from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.jobs.application_maintenance_scheduler import (
    ApplicationMaintenanceScheduler,
)
from core.project.domain_config_operations import (
    DomainCandidate,
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
    DomainConfigStore,
)
from core.project.maintenance_service import ProjectMaintenanceService
from core.project.project_files import PROJECT_FILE_PATH, build_project_markdown
from runtime.project_factory import ProjectRuntimeFactory
from runtime.project_runtime import ProjectRuntime
from runtime.resources import RuntimeResources


class ProjectStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


def _parse_initial_domain(
    candidate: DomainConfig | Mapping[str, object],
) -> DomainConfig:
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

    def __init__(
        self,
        resources: RuntimeResources,
        user_name: str,
        *,
        filesystem_factory: ProjectFilesystemFactory | None = None,
    ):
        self.resources = resources
        self.user_name = user_name
        self.project_factory = ProjectRuntimeFactory(
            resources=resources,
            user_name=user_name,
            episode_window_size_provider=self.get_episode_window_size,
        )
        self.pg = resources.postgres
        self._filesystem_factory = filesystem_factory or ProjectFilesystemFactory(
            ConfigManager.get().config.developer_settings.documents.project_library_root
        )
        self._project_deletion_writer = ProjectDeletionWriter(self.pg)
        self.active_projects: Dict[str, ProjectRuntime] = {}
        self._project_leases: Dict[str, set[str]] = {}
        self.maintenance_service = ProjectMaintenanceService(
            resources=resources,
            user_name=user_name,
            project_lookup=self.get_project,
            active_projects=self.active_projects,
            project_leases=self._project_leases,
        )
        # Entity identity maintenance is user-global and must not be tied to a
        # loaded ProjectRuntime.  ProjectManager exposes the application-owned
        # service for the current local user while project maintenance remains
        # separately scoped above.
        self.entity_maintenance_service = EntityMaintenanceService(
            resources=resources,
            user_name=user_name,
        )
        self.maintenance_scheduler = ApplicationMaintenanceScheduler(
            maintenance_service=self.entity_maintenance_service,
            user_name=user_name,
            background_work=resources.background_work,
        )
        self._closed = False

    async def start(self) -> None:
        """Start application-owned maintenance triggers."""
        if self._closed:
            raise RuntimeError("ProjectManager is shut down")
        await self.maintenance_scheduler.start()

    async def create_project(
        self,
        name: str,
        domain_config: DomainConfig | Mapping[str, object],
        description: Optional[str] = None,
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
                project_id, user_name, name, description, status,
                domain_config
            ) VALUES (
                %(project_id)s, %(user_name)s, %(name)s, %(description)s,
                %(status)s, %(domain_config)s
            )
        """
        scope_query = """
            INSERT INTO public.project_read_scopes (
                user_name, project_id, readable_project_id
            )
            VALUES (%(user_name)s, %(project_id)s, %(readable)s)
        """
        project_file_content = build_project_markdown(name, description).encode("utf-8")
        filesystem = self._filesystem_factory.for_project(project_id)
        await asyncio.to_thread(
            filesystem.write_bytes,
            PROJECT_FILE_PATH,
            project_file_content,
        )
        try:
            async with self.pg.transaction() as cur:
                await cur.execute(
                    project_query,
                    {
                        "project_id": project_id,
                        "user_name": self.user_name,
                        "name": name,
                        "description": description,
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
        except Exception:
            try:
                await asyncio.to_thread(
                    filesystem.delete_file,
                    PROJECT_FILE_PATH,
                )
            except Exception:
                logger.exception("Could not roll back PROJECT.md after project creation failed")
            raise

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

    async def activate_domain_config(
        self,
        project_id: str,
        candidate: DomainCandidate,
        *,
        expected_version: int,
    ) -> DomainActivation:
        """Validate and optimistically activate a project's domain candidate.

        Active runtimes receive the new immutable snapshot through
        ``ProjectRuntime``.  Projects without a loaded runtime update durable
        storage directly; their next runtime bootstrap reads the new revision.
        """

        async with self.maintenance_service.lock:
            await self._require_domain_project(project_id, allow_archived=False)
            active_state = self.active_projects.get(project_id)
            parsed = parse_candidate(candidate)
            if active_state is not None:
                return await active_state.activate_domain_config(
                    parsed,
                    expected_version=expected_version,
                )

            return await DomainConfigStore(self.pg).activate(
                user_name=self.user_name,
                project_id=project_id,
                candidate=parsed,
                expected_version=expected_version,
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

    async def get_episode_sources(self, project_id: str) -> List[dict]:
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
                "after_message_id": int(row["episode_participation_after_message_id"]),
            }
            for row in rows
        ]

    async def set_episode_sources(
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

        return await self.get_episode_sources(project_id)

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
            if self._project_leases.get(project_id):
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
        if self._project_leases.get(project_id):
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
        """Hard delete every durable PostgreSQL and AGE record owned by a project."""
        async with self.maintenance_service.lock:
            meta = await self.get_project(project_id)
            if not meta:
                return None

            active_state = self.active_projects.get(project_id)
            if self._project_leases.get(project_id):
                raise RuntimeError(
                    f"Project '{project_id}' has active runtime sessions and "
                    "cannot be deleted"
                )
            if active_state:
                await active_state.shutdown()
                del self.active_projects[project_id]

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

    async def acquire_project_for_session(
        self, project_id: str, session_id: str
    ) -> ProjectRuntime:
        """Acquire one exact session lease on a live project runtime."""
        async with self.maintenance_service.lock:
            if self._closed:
                raise RuntimeError("ProjectManager is shutting down")
            return await self._acquire_project_for_session(
                project_id,
                session_id,
            )

    async def _acquire_project_for_session(
        self, project_id: str, session_id: str
    ) -> ProjectRuntime:
        if not project_id or not project_id.strip():
            raise ValueError("A persisted project_id is required to acquire a project")
        if not session_id or not session_id.strip():
            raise ValueError("A persisted session_id is required to acquire a project")

        leases = self._project_leases.setdefault(project_id, set())
        if session_id in leases:
            raise RuntimeError(
                f"Session '{session_id}' already holds a lease for project '{project_id}'"
            )
        project = await self.get_project(project_id)
        if project is None:
            if not leases:
                self._project_leases.pop(project_id, None)
            raise ValueError(
                f"Project '{project_id}' does not exist; "
                "create it before creating a session"
            )
        if project["status"] != ProjectStatus.ACTIVE.value:
            if not leases:
                self._project_leases.pop(project_id, None)
            raise ValueError(
                f"Project '{project_id}' is {project['status']} and cannot "
                "create or resume sessions"
            )

        project_state = self.active_projects.get(project_id)
        if project_state is None:
            logger.info(f"Bootstrapping ProjectRuntime for project_id: {project_id}")
            try:
                project_state = await self.project_factory.create(
                    project_id=project_id,
                    readable_project_ids=await self.get_readable_project_ids(
                        project_id,
                        project_metadata=project,
                    ),
                )
            except Exception:
                if not leases:
                    self._project_leases.pop(project_id, None)
                raise
            self.active_projects[project_id] = project_state

        leases.add(session_id)
        return project_state

    async def release_project_for_session(
        self, project_id: str, session_id: str
    ) -> None:
        """Release one exact session lease and stop the final project runtime."""
        async with self.maintenance_service.lock:
            leases = self._project_leases.get(project_id)
            if leases is None or session_id not in leases:
                raise RuntimeError(
                    f"Session '{session_id}' does not hold a lease for project '{project_id}'"
                )
            leases.remove(session_id)
            if leases:
                return

            self._project_leases.pop(project_id, None)
            state = self.active_projects.pop(project_id, None)
            if state is not None:
                await state.shutdown()
                logger.info(f"Released ProjectRuntime for project_id: {project_id}")

    async def shutdown(self) -> None:
        """Stop every remaining project runtime before shared resources close."""

        await self.maintenance_scheduler.stop()

        async with self.maintenance_service.lock:
            if self._closed and not self.active_projects:
                return
            self._closed = True
            states = list(self.active_projects.values())
            self.active_projects.clear()
            self._project_leases.clear()

        results = await asyncio.gather(
            *(state.shutdown() for state in states),
            return_exceptions=True,
        )
        failures = [result for result in results if isinstance(result, Exception)]
        if failures:
            raise RuntimeError(
                f"Failed to shut down {len(failures)} project runtime(s)"
            ) from failures[0]
