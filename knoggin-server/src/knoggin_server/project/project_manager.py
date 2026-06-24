import asyncio
import json
import uuid
from functools import partial
from typing import Dict, List, Optional

from loguru import logger

from common.conf.manager import ConfigManager
from common.conf.topics_config import TopicConfig
from common.scoping import IDENTITY_ENTITY_ID, build_readable_project_ids
from common.utils.json_utils import safe_json_loads
from common.utils.time_utils import get_now_iso
from infrastructure.job.scheduler import Scheduler
from infrastructure.redis_client import RedisKeys
from infrastructure.resources import ResourceManager
from knoggin_server.community.community_job import AACJob
from knoggin_server.ingestion.jobs.archive_job import FactArchivalJob
from knoggin_server.ingestion.jobs.cleaner_job import EntityCleanupJob
from knoggin_server.ingestion.jobs.dlq_job import DLQReplayJob
from knoggin_server.ingestion.services.pipeline_service import BatchProcessor
from knoggin_server.ingestion.services.processor import TextProcessor
from knoggin_server.knowledge.db.write_graph_db import write_batch_callback
from knoggin_server.knowledge.jobs.merge_job import MergeDetectionJob
from knoggin_server.knowledge.jobs.profile_job import ProfileRefinementJob
from knoggin_server.knowledge.jobs.topics_job import TopicConfigJob
from knoggin_server.knowledge.services.entity_service import EntityManager
from knoggin_server.project.lifecycle import ProjectStatus
from knoggin_server.project.state import ProjectState


class ProjectManager:
    """Manages the lifecycle and storage of Projects."""

    def __init__(self, resources: ResourceManager, user_name: str):
        self.resources = resources
        self.user_name = user_name
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
        """Create a new project and store its metadata in Redis."""
        if not name or not name.strip():
            raise ValueError("create_project requires a non-empty project name")

        name = name.strip()
        project_id = str(uuid.uuid4())
        now = get_now_iso()
        allowed_projects = await self._validate_allowed_project_ids(
            project_id, allowed_projects or []
        )

        metadata = {
            "id": project_id,
            "name": name,
            "description": description,
            "access_mode": access_mode,
            "allowed_projects": allowed_projects,
            "status": ProjectStatus.ACTIVE.value,
            "archived_at": None,
            "deleted_at": None,
            "created_at": now,
            "updated_at": now,
        }

        key = RedisKeys.projects(self.user_name)
        await self.resources.redis.hset(key, project_id, json.dumps(metadata))
        logger.info(f"Created project {project_id} ('{name}')")
        return metadata

    async def list_projects(self) -> List[dict]:
        """List all projects and enrich with session counts."""
        key = RedisKeys.projects(self.user_name)
        raw_projects = await self.resources.redis.hgetall(key)

        projects = []
        for pid, data in raw_projects.items():
            try:
                meta = safe_json_loads(data)
                if not meta:
                    continue
                session_count = await self.resources.redis.scard(
                    RedisKeys.project_sessions(self.user_name, pid)
                )
                meta["session_count"] = session_count
                projects.append(meta)
            except Exception as e:
                logger.warning(f"Failed to parse project {pid}: {e}")

        # Sort by updated_at descending
        projects.sort(key=lambda x: x.get("updated_at", ""), reverse=True)
        return projects

    async def get_project(self, project_id: str) -> Optional[dict]:
        """Get project metadata."""
        key = RedisKeys.projects(self.user_name)
        data = await self.resources.redis.hget(key, project_id)
        if not data:
            return None
        meta = safe_json_loads(data)
        if not meta:
            return None
        meta["session_count"] = await self.resources.redis.scard(
            RedisKeys.project_sessions(self.user_name, project_id)
        )
        return meta

    async def get_readable_project_ids(self, project_id: str) -> List[str]:
        """Return projects readable from project_id."""
        meta = await self.get_project(project_id)
        if not meta or meta["status"] == ProjectStatus.DELETED.value:
            return []

        allowed = meta.get("allowed_projects", [])

        if allowed:
            stored_projects = await self.resources.redis.hgetall(
                RedisKeys.projects(self.user_name)
            )
            readable_statuses = {
                ProjectStatus.ACTIVE.value,
                ProjectStatus.ARCHIVED.value,
            }
            allowed = []
            for allowed_id in meta.get("allowed_projects", []):
                raw = stored_projects.get(allowed_id)
                metadata = safe_json_loads(raw, {}) if raw else {}
                if metadata.get("status") in readable_statuses:
                    allowed.append(allowed_id)

        return build_readable_project_ids(project_id, allowed)

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
        stored_projects = await self.resources.redis.hgetall(
            RedisKeys.projects(self.user_name)
        )
        readable_statuses = {
            ProjectStatus.ACTIVE.value,
            ProjectStatus.ARCHIVED.value,
        }
        unavailable = []
        for allowed_id in requested:
            raw = stored_projects.get(allowed_id)
            metadata = safe_json_loads(raw, {}) if raw else {}
            if metadata.get("status") not in readable_statuses:
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

        updated = False
        if name is not None:
            if not name.strip():
                raise ValueError("update_project requires a non-empty project name")
            meta["name"] = name.strip()
            updated = True
        if description is not None:
            meta["description"] = description
            updated = True
        if allowed_projects is not None:
            active_state = self.active_projects.get(project_id)
            if active_state and active_state.active_runtime_sessions_count > 0:
                raise RuntimeError(
                    f"Project '{project_id}' has active runtime sessions and "
                    "cannot change its readable project scope"
                )
            validated_allowed_projects = (
                await self._validate_allowed_project_ids(
                    project_id, allowed_projects
                )
            )
            if active_state:
                await active_state.shutdown()
                del self.active_projects[project_id]

            meta["allowed_projects"] = validated_allowed_projects
            updated = True

        if updated:
            meta["updated_at"] = get_now_iso()
            await self._save_project_metadata(meta)

        return meta

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
                f"Project '{project_id}' has active runtime sessions and "
                "cannot be archived"
            )
        if active_state:
            await active_state.shutdown()
            del self.active_projects[project_id]

        now = get_now_iso()
        meta["status"] = ProjectStatus.ARCHIVED.value
        meta["archived_at"] = now
        meta["updated_at"] = now
        await self._save_project_metadata(meta)
        logger.info(f"Archived project {project_id} with knowledge retained")
        return meta

    async def reactivate_project(self, project_id: str) -> Optional[dict]:
        """Make an archived project eligible for sessions again."""
        meta = await self.get_project(project_id)
        if not meta:
            return None
        if meta["status"] == ProjectStatus.DELETED.value:
            raise ValueError(f"Deleted project '{project_id}' cannot be reactivated")
        if meta["status"] == ProjectStatus.ACTIVE.value:
            return meta

        meta["status"] = ProjectStatus.ACTIVE.value
        meta["archived_at"] = None
        meta["updated_at"] = get_now_iso()
        await self._save_project_metadata(meta)
        logger.info(f"Reactivated project {project_id}")
        return meta

    async def _save_project_metadata(self, meta: dict) -> None:
        project_id = meta["id"]
        stored = {key: value for key, value in meta.items() if key != "session_count"}
        await self.resources.redis.hset(
            RedisKeys.projects(self.user_name),
            project_id,
            json.dumps(stored),
        )

    async def delete_project(self, project_id: str) -> Optional[dict]:
        """Mark a project deleted while retaining all project-owned data."""
        meta = await self.get_project(project_id)
        if not meta:
            return None
        if meta["status"] == ProjectStatus.DELETED.value:
            return meta

        active_state = self.active_projects.get(project_id)
        if active_state and active_state.active_runtime_sessions_count > 0:
            raise RuntimeError(
                f"Project '{project_id}' has active runtime sessions and "
                "cannot be deleted"
            )
        if active_state:
            await active_state.shutdown()
            del self.active_projects[project_id]

        now = get_now_iso()
        meta["status"] = ProjectStatus.DELETED.value
        meta["deleted_at"] = now
        meta["updated_at"] = now
        await self._save_project_metadata(meta)
        self._remove_project_from_active_read_scopes(project_id)
        logger.info(f"Marked project {project_id} deleted with data retained")
        return meta

    def _remove_project_from_active_read_scopes(self, project_id: str) -> None:
        for active_project_id, state in self.active_projects.items():
            if active_project_id == project_id:
                continue

            readable_scopes = [
                state.readable_project_ids,
                state.entities.readable_project_ids,
            ]
            for readable_project_ids in readable_scopes:
                if readable_project_ids and project_id in readable_project_ids:
                    readable_project_ids[:] = [
                        readable_id
                        for readable_id in readable_project_ids
                        if readable_id != project_id
                    ]

            cached_ids = [
                entity_id
                for entity_id, profile in state.entities.get_profiles().items()
                if profile.get("project_id") == project_id
            ]
            state.entities.remove_entities(cached_ids)

    async def add_session(self, project_id: str, session_id: str):
        """Add durable session membership to a project."""
        key = RedisKeys.project_sessions(self.user_name, project_id)
        await self.resources.redis.sadd(key, session_id)

    async def remove_session(self, project_id: str, session_id: str):
        """Remove durable session membership from a project."""
        key = RedisKeys.project_sessions(self.user_name, project_id)
        await self.resources.redis.srem(key, session_id)

    async def get_session_ids(self, project_id: str) -> List[str]:
        """Get all session IDs belonging to a project."""
        key = RedisKeys.project_sessions(self.user_name, project_id)
        return list(await self.resources.redis.smembers(key))

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
            project_id, initial_topics_config=topics_config
        )
        await self.add_session(project_id, session_id)
        return project_state

    def _serialize_topics_config(self, topics_config: dict) -> dict:
        return {
            name: cfg.model_dump() if hasattr(cfg, "model_dump") else cfg
            for name, cfg in topics_config.items()
        }

    async def _ensure_project_topics_config(
        self, project_id: str, initial_topics_config: Optional[dict] = None
    ) -> None:
        existing_topics = await self.resources.redis.hget(
            RedisKeys.project_topic_config(self.user_name), project_id
        )
        if existing_topics:
            return

        topics_config_dict = (
            initial_topics_config
            if initial_topics_config is not None
            else self.config.default_topics
        )
        topics_json = json.dumps(self._serialize_topics_config(topics_config_dict))
        await self.resources.redis.hset(
            RedisKeys.project_topic_config(self.user_name),
            project_id,
            topics_json,
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
        self, project_id: str, initial_topics_config: Optional[dict] = None
    ) -> ProjectState:
        project = await self.get_project(project_id)
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
        readable_project_ids = await self.get_readable_project_ids(project_id)

        # Project topic config is the runtime source of truth. Seed it only when
        # this project has no persisted config yet.
        await self._ensure_project_topics_config(project_id, initial_topics_config)
        t_config = await TopicConfig.load(
            self.resources.redis, self.user_name, project_id
        )
        await t_config.save(self.resources.redis, self.user_name, project_id)

        # Entity Manager
        er_cfg = self.dev_settings.entity_resolution
        entities = EntityManager(
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
                get_profile=entities.get_profile,
                gliner=self.resources.gliner,
                spacy=self.resources.spacy,
                gliner_threshold=nlp_cfg.gliner_threshold,
                vp01_min_confidence=nlp_cfg.vp01_min_confidence,
            ),
        )

        project_processor = BatchProcessor(
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
            context_support_epsilon=er_cfg.context_support_epsilon,
            sparse_context_verbs=er_cfg.sparse_context_verbs,
        )

        await self._verify_user_entity(entities)

        scheduler = Scheduler(self.user_name, project_id, self.resources.redis)
        profile_job = self._init_profile_job(entities)
        merge_job = self._init_merge_job(entities, t_config)

        project_state = ProjectState(
            project_id=project_id,
            topic_config=t_config,
            entities=entities,
            pipeline=pipeline,
            scheduler=scheduler,
            user_name=self.user_name,
            redis_client=self.resources.redis,
            readable_project_ids=readable_project_ids,
            batch_processor=project_processor,
        )
        project_state.profile_job = profile_job
        project_state.merge_job = merge_job

        self._register_background_jobs(
            project_state, entities, project_processor, profile_job, merge_job
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

            stored_projects = await self.resources.redis.hgetall(
                RedisKeys.projects(self.user_name)
            )
            retained_statuses = {
                ProjectStatus.ACTIVE.value,
                ProjectStatus.ARCHIVED.value,
            }
            identity_project_ids = sorted(
                stored_id
                for stored_id, raw in stored_projects.items()
                if safe_json_loads(raw, {}).get("status") in retained_statuses
            )
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

    async def _verify_user_entity(self, entities: EntityManager) -> None:
        user_id = await entities.get_id(self.user_name)
        if user_id != IDENTITY_ENTITY_ID:
            raise RuntimeError(
                f"Configured user '{self.user_name}' did not resolve to reserved "
                f"entity ID {IDENTITY_ENTITY_ID}"
            )
        logger.info(
            f"User entity verified: {self.user_name} (id={IDENTITY_ENTITY_ID})"
        )

    async def _ensure_identity_invariant(self) -> None:
        if self._identity_initialized:
            return

        await self.resources.knowledge_store.ensure_identity_entity(
            self.user_name,
            getattr(self.config, "user_aliases", []),
        )
        self._identity_initialized = True

    def _init_profile_job(self, entities: EntityManager) -> ProfileRefinementJob:
        jobs_cfg = self.dev_settings.jobs
        nlp_cfg = self.dev_settings.nlp_pipeline
        prof_cfg = jobs_cfg.profile

        return ProfileRefinementJob(
            llm=self.resources.llm_service,
            entities=entities,
            knowledge_store=self.resources.knowledge_store,
            executor=self.resources.executor,
            embedding_service=self.resources.embedding,
            redis_client=self.resources.redis,
            msg_window=prof_cfg.msg_window,
            volume_threshold=prof_cfg.volume_threshold,
            idle_threshold=prof_cfg.idle_threshold,
            profile_batch_size=prof_cfg.profile_batch_size,
            contradiction_sim_low=prof_cfg.contradiction_sim_low,
            contradiction_sim_high=prof_cfg.contradiction_sim_high,
            contradiction_batch_size=prof_cfg.contradiction_batch_size,
            profile_prompt=nlp_cfg.profile_prompt,
            contradiction_prompt=nlp_cfg.contradiction_prompt,
        )

    def _init_merge_job(
        self, entities: EntityManager, topic_config: TopicConfig
    ) -> MergeDetectionJob:
        jobs_cfg = self.dev_settings.jobs
        nlp_cfg = self.dev_settings.nlp_pipeline
        merge_cfg = jobs_cfg.merger

        return MergeDetectionJob(
            user_name=self.user_name,
            entities=entities,
            knowledge_store=self.resources.knowledge_store,
            llm_client=self.resources.llm_service,
            topic_config=topic_config,
            redis_client=self.resources.redis,
            auto_threshold=merge_cfg.auto_threshold,
            hitl_threshold=merge_cfg.hitl_threshold,
            cosine_threshold=merge_cfg.cosine_threshold,
            merge_prompt=nlp_cfg.merge_prompt,
        )

    def _register_background_jobs(
        self,
        project_state: ProjectState,
        entities: EntityManager,
        processor: BatchProcessor,
        profile_job: ProfileRefinementJob,
        merge_job: MergeDetectionJob,
    ):
        scheduler = project_state.scheduler
        project_id = project_state.project_id
        topic_config = project_state.topic_config
        jobs_cfg = self.dev_settings.jobs

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

        async def _update_topics_callback(new_config: dict):
            topic_config.update(new_config)
            await topic_config.save(self.resources.redis, self.user_name, project_id)
            entities.hierarchy_config = topic_config.hierarchy
            processor.refresh_topic_mappings()

        skip_initial_global_topics_update = True

        def _global_topics_updated_cb(new_topics: dict):
            nonlocal skip_initial_global_topics_update
            if skip_initial_global_topics_update:
                skip_initial_global_topics_update = False
                return
            asyncio.create_task(project_state.update_topics_config(new_topics))

        project_state.add_config_unsubscriber(
            config_mgr.subscribe(_global_topics_updated_cb, "default_topics")
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
                profile_job.update_settings, "developer_settings.jobs.profile"
            )
        )
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                merge_job.update_settings, "developer_settings.jobs.merger"
            )
        )

        scheduler.register(profile_job)
        scheduler.register(merge_job)

        dlq_cfg = jobs_cfg.dlq
        dlq_job = DLQReplayJob(
            entities=entities,
            processor=processor,
            write_to_graph=_dlq_write_callback,
            redis_client=self.resources.redis,
            interval=dlq_cfg.interval_seconds,
            batch_size=dlq_cfg.batch_size,
            max_attempts=dlq_cfg.max_attempts,
        )
        scheduler.register(dlq_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(dlq_job.update_settings, "developer_settings.jobs.dlq")
        )

        clean_cfg = jobs_cfg.cleaner
        cleaner_job = EntityCleanupJob(
            user_name=self.user_name,
            knowledge_store=self.resources.knowledge_store,
            entities=entities,
            redis_client=self.resources.redis,
            interval_hours=clean_cfg.interval_hours,
            orphan_age_hours=clean_cfg.orphan_age_hours,
            stale_junk_days=clean_cfg.stale_junk_days,
        )
        scheduler.register(cleaner_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                cleaner_job.update_settings, "developer_settings.jobs.cleaner"
            )
        )

        arch_cfg = jobs_cfg.archival
        archival_job = FactArchivalJob(
            user_name=self.user_name,
            knowledge_store=self.resources.knowledge_store,
            redis_client=self.resources.redis,
            retention_days=arch_cfg.retention_days,
            fallback_interval_hours=arch_cfg.fallback_interval_hours,
        )
        scheduler.register(archival_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                archival_job.update_settings, "developer_settings.jobs.archival"
            )
        )

        topic_cfg = jobs_cfg.topic_config
        topic_job = TopicConfigJob(
            llm=self.resources.llm_service,
            topic_config=topic_config,
            update_callback=_update_topics_callback,
            redis_client=self.resources.redis,
            knowledge_store=self.resources.knowledge_store,
            interval_msgs=topic_cfg.interval_msgs,
            conversation_window=topic_cfg.conversation_window,
        )
        scheduler.register(topic_job)
        project_state.add_config_unsubscriber(
            config_mgr.subscribe(
                topic_job.update_settings, "developer_settings.jobs.topic_config"
            )
        )

        scheduler.register(AACJob(project_state, self.resources))
