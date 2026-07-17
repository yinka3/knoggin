import asyncio
import json
import uuid
from enum import Enum
from functools import partial
from typing import Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.conf.topics_config import TopicConfig, load_topic_seed
from common.scoping import IDENTITY_ENTITY_ID, build_readable_project_ids
from core.community.community_job import AACJob
from core.ingestion.jobs.cleaner_job import EntityCleanupJob
from core.ingestion.jobs.dlq_job import DLQReplayJob
from core.ingestion.jobs.episode_job import EpisodeJob
from core.ingestion.services.pipeline_service import IngestionPipeline
from core.ingestion.services.processor import TextProcessor
from core.knowledge.db.write_graph_db import write_batch_callback
from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from core.knowledge.entity.resolver import EntityResolver
from core.knowledge.documents.indexing_job import DocumentIndexingRecoveryJob
from core.knowledge.jobs.merge_rollback_cleanup_job import (
    MergeCleanupJob,
)
from core.project.state import ProjectState
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from infrastructure.resources import ResourceManager


class ProjectStatus(str, Enum):
    ACTIVE = "active"
    ARCHIVED = "archived"
    DELETED = "deleted"


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

    @property
    def config(self):
        return ConfigManager.get().config

    @property
    def dev_settings(self):
        return self.config.developer_settings

    async def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        access_mode: str = "open",
        allowed_projects: Optional[List[str]] = None,
    ) -> dict:
        """Create a new project and store its metadata in Postgres."""
        if not name or not name.strip():
            raise ValueError("create_project requires a non-empty project name")

        name = name.strip()
        project_id = str(uuid.uuid4())
        allowed_projects = await self._validate_allowed_project_ids(
            project_id, allowed_projects or []
        )
        topic_seed = {
            topic: config.model_dump(mode="json")
            for topic, config in load_topic_seed().items()
        }

        query = """
            INSERT INTO public.projects (
                project_id, user_name, name, description, access_mode, status,
                topic_config
            ) VALUES (
                %(project_id)s, %(user_name)s, %(name)s, %(description)s,
                %(access_mode)s, %(status)s, %(topic_config)s
            ) RETURNING created_at, updated_at
        """
        await self.pg.fetch_all(
            query,
            {
                "project_id": project_id,
                "user_name": self.user_name,
                "name": name,
                "description": description,
                "access_mode": access_mode,
                "status": ProjectStatus.ACTIVE.value,
                "topic_config": json.dumps(topic_seed),
            },
        )
        for allowed_id in allowed_projects:
            scope_query = """
                INSERT INTO public.project_read_scopes (user_name, project_id, readable_project_id)
                VALUES (%(user_name)s, %(project_id)s, %(readable)s)
            """
            await self.pg.execute(
                scope_query,
                {
                    "user_name": self.user_name,
                    "project_id": project_id,
                    "readable": allowed_id,
                },
            )

        logger.info(f"Created project {project_id} ('{name}')")
        return await self.get_project(project_id)

    async def list_projects(self) -> List[dict]:
        """List all projects and enrich with session counts."""
        query = """
            SELECT p.*,
                   (SELECT COUNT(*) FROM public.sessions s WHERE s.project_id = p.project_id) as session_count,
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
            # Convert datetime to isoformat
            for time_field in [
                "created_at",
                "updated_at",
                "archived_at",
                "deleted_at",
                "last_activity_at",
            ]:
                if meta.get(time_field):
                    meta[time_field] = meta[time_field].isoformat()
            if "topic_config" in meta:
                del meta["topic_config"]
            projects.append(meta)

        return projects

    async def get_project(self, project_id: str) -> Optional[dict]:
        """Get project metadata."""
        query = """
            SELECT p.*,
                   (SELECT COUNT(*) FROM public.sessions s WHERE s.project_id = p.project_id) as session_count,
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
        for time_field in [
            "created_at",
            "updated_at",
            "archived_at",
            "deleted_at",
            "last_activity_at",
        ]:
            if meta.get(time_field):
                meta[time_field] = meta[time_field].isoformat()
        if "topic_config" in meta:
            del meta["topic_config"]
        return meta

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
            SET project_id = EXCLUDED.project_id,
                status = 'open',
                last_active_at = now()
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

            # Replace read scopes
            await self.pg.execute(
                "DELETE FROM public.project_read_scopes "
                "WHERE user_name = %(user_name)s "
                "AND project_id = %(project_id)s",
                {"user_name": self.user_name, "project_id": project_id},
            )
            for allowed_id in validated_allowed:
                await self.pg.execute(
                    """
                    INSERT INTO public.project_read_scopes (user_name, project_id, readable_project_id)
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
            await self.pg.execute(
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
        await redis.hdel(RedisKeys.project_topic_config(self.user_name), project_id)
        if session_ids:
            await redis.hdel(RedisKeys.sessions(self.user_name), *session_ids)
        if agent_ids:
            await redis.hdel(RedisKeys.agents(self.user_name), *agent_ids)
            default_key = RedisKeys.agents_default(self.user_name)
            if await redis.get(default_key) in set(agent_ids):
                deleted += int(await redis.delete(default_key))
        return deleted

    async def acquire_project_for_session(
        self, project_id: str, session_id: str, topics_config: Optional[dict] = None
    ) -> ProjectState:
        """Acquire runtime project state and record durable session membership."""
        async with self._maintenance_lock:
            return await self._acquire_project_for_session(
                project_id,
                session_id,
                topics_config=topics_config,
            )

    async def _acquire_project_for_session(
        self, project_id: str, session_id: str, topics_config: Optional[dict] = None
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
            initial_topics_config=topics_config,
            project_metadata=project,
        )
        return project_state

    async def _ensure_project_topics_config(
        self, project_id: str, initial_topics_config: Optional[dict] = None
    ) -> None:
        rows = await self.pg.fetch_all(
            """
            SELECT topic_config
            FROM public.projects
            WHERE user_name = %(user_name)s AND project_id = %(project_id)s
            """,
            {"user_name": self.user_name, "project_id": project_id},
        )
        if not rows:
            raise ValueError(f"Project not found while seeding topics: {project_id}")
        if rows[0].get("topic_config"):
            return

        topic_config = TopicConfig(
            initial_topics_config
            if initial_topics_config is not None
            else load_topic_seed()
        )
        await topic_config.save(
            self.pg,
            self.user_name,
            project_id,
        )

    async def get_or_start_project(
        self, project_id: str, initial_topics_config: Optional[dict] = None
    ) -> ProjectState:
        """Get an existing ProjectState or bootstrap a new one."""
        async with self._maintenance_lock:
            return await self._get_or_start_project(
                project_id,
                initial_topics_config=initial_topics_config,
            )

    async def _get_or_start_project(
        self,
        project_id: str,
        initial_topics_config: Optional[dict] = None,
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

        # Project topic config is the runtime source of truth. Seed it only when
        # this project has no persisted config yet.
        await self._ensure_project_topics_config(project_id, initial_topics_config)
        t_config = await TopicConfig.load(self.pg, self.user_name, project_id)

        # Entity Manager
        er_cfg = self.dev_settings.entity_resolution
        entities = EntityResolver(
            project_id=project_id,
            readable_project_ids=readable_project_ids,
            knowledge_store=self.resources.knowledge_store,
            embedding_service=self.resources.embedding,
            hierarchy_config=t_config.hierarchy,
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
                topic_config=t_config,
                get_known_aliases=entities.get_known_aliases,
                get_alias_version=entities.get_alias_version,
                get_profile=entities.get_profile,
                gliner=self.resources.gliner,
                spacy=self.resources.spacy,
                settings=nlp_cfg,
                local_reference_settings=self.dev_settings.local_references,
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
            topic_config=t_config,
            get_next_ent_id=self.resources.knowledge_store.allocate_entity_id,
            resolution_threshold=er_cfg.resolution_threshold,
            common_word_frequency_threshold=er_cfg.common_word_frequency_threshold,
            sparse_context_verbs=er_cfg.sparse_context_verbs,
            local_reference_settings=self.dev_settings.local_references,
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
            topic_config=t_config,
            entities=entities,
            pipeline=pipeline,
            scheduler=scheduler,
            user_name=self.user_name,
            redis_client=self.resources.redis,
            postgres_client=self.resources.postgres,
            embedding_service=self.resources.embedding,
            readable_project_ids=readable_project_ids,
            batch_processor=project_processor,
            background_work=getattr(self.resources, "background_work", None),
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
            ingestion_settings=self.dev_settings.ingestion,
            llm=self.resources.llm_service,
            embedding_service=self.resources.embedding,
            session_ids_provider=lambda: self.get_session_ids(project_id),
            local_reference_settings=self.dev_settings.local_references,
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
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                processor.update_local_reference_settings,
                "developer_settings.local_references",
            )
        )
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                episode_job.update_local_reference_settings,
                "developer_settings.local_references",
            )
        )
        scheduler.register(episode_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                lambda config: episode_job.update_settings(
                    config,
                    self.dev_settings.ingestion,
                ),
                "developer_settings.jobs.episode",
            )
        )
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                lambda config: episode_job.update_settings(
                    self.dev_settings.jobs.episode,
                    config,
                ),
                "developer_settings.ingestion",
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

        scheduler.register(AACJob(project_state, self.resources))
