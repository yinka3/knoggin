import json
import uuid
from datetime import datetime, timezone
from typing import Dict, List, Optional

from loguru import logger

from infrastructure.redis_client import RedisKeys
from infrastructure.resources import ResourceManager


class ProjectManager:
    """Manages the lifecycle and storage of Projects."""

    def __init__(self, resources: ResourceManager, user_name: str):
        self.resources = resources
        self.user_name = user_name

    async def create_project(
        self,
        name: str,
        description: Optional[str] = None,
        access_mode: str = "open",
        allowed_projects: Optional[List[str]] = None,
    ) -> dict:
        """Create a new project and store its metadata in Redis."""
        project_id = str(uuid.uuid4())
        
        metadata = {
            "id": project_id,
            "name": name,
            "description": description,
            "access_mode": access_mode,
            "allowed_projects": allowed_projects or [],
            "created_at": datetime.now(timezone.utc).isoformat(),
            "updated_at": datetime.now(timezone.utc).isoformat(),
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
                meta = json.loads(data)
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
        meta = json.loads(data)
        meta["session_count"] = await self.resources.redis.scard(
            RedisKeys.project_sessions(self.user_name, project_id)
        )
        return meta

    async def update_project(
        self, project_id: str, name: Optional[str] = None, description: Optional[str] = None
    ) -> Optional[dict]:
        """Update project name or description."""
        meta = await self.get_project(project_id)
        if not meta:
            return None

        updated = False
        if name is not None:
            meta["name"] = name
            updated = True
        if description is not None:
            meta["description"] = description
            updated = True

        if updated:
            meta["updated_at"] = datetime.now(timezone.utc).isoformat()
            # session_count is dynamic, don't store it in hash
            meta_to_save = {k: v for k, v in meta.items() if k != "session_count"}
            key = RedisKeys.projects(self.user_name)
            await self.resources.redis.hset(key, project_id, json.dumps(meta_to_save))

        return meta

    async def delete_project(self, project_id: str) -> List[str]:
        """Delete project metadata and return orphaned session IDs for caller to clean up."""
        # 1. Get orphaned sessions
        session_ids = await self.get_session_ids(project_id)

        # 2. Delete from projects hash
        key = RedisKeys.projects(self.user_name)
        await self.resources.redis.hdel(key, project_id)

        # 3. Delete the project_sessions set
        sessions_key = RedisKeys.project_sessions(self.user_name, project_id)
        await self.resources.redis.delete(sessions_key)

        logger.info(f"Deleted project {project_id}, orphaned {len(session_ids)} sessions")
        return session_ids

    async def add_session(self, project_id: str, session_id: str):
        """Add a session to a project."""
        key = RedisKeys.project_sessions(self.user_name, project_id)
        await self.resources.redis.sadd(key, session_id)

    async def remove_session(self, project_id: str, session_id: str):
        """Remove a session from a project."""
        key = RedisKeys.project_sessions(self.user_name, project_id)
        await self.resources.redis.srem(key, session_id)

    async def get_session_ids(self, project_id: str) -> List[str]:
        """Get all session IDs belonging to a project."""
        key = RedisKeys.project_sessions(self.user_name, project_id)
        return list(await self.resources.redis.smembers(key))
