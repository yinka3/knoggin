import os
import asyncio
from typing import Dict, Optional
from loguru import logger

from core.session.context import Context
from common.infra.resources import ResourceManager
from services.session_manager import SessionManager
from services.agent_manager import AgentManager

class AppState:
    def __init__(self, resources: ResourceManager, active_sessions: Dict[str, Context], user_name: str):
        self.resources = resources
        self.user_name = user_name
        self.active_sessions = active_sessions
        
        self.session_manager = SessionManager(resources, user_name, active_sessions)
        self.agent_manager = AgentManager(resources, user_name, active_sessions)
        
        self.global_scheduler = None

    async def start_scheduler(self):
        """Lazy-start the global scheduler if user_name is present."""
        if not self.user_name or self.global_scheduler:
            return

        from jobs.scheduler import Scheduler
        from jobs.aac_job import AACJob
        
        self.global_scheduler = Scheduler(self.user_name, "global", self.resources.redis, self.resources)
        self.global_scheduler.register(AACJob(resources=self.resources))
        await self.global_scheduler.start()
        logger.info(f"Global scheduler started lazily for user: {self.user_name}")

    async def get_or_resume_session(self, session_id: str) -> Optional[Context]:
        """Delegate shortcut for backwards compatibility in deps."""
        return await self.session_manager.get_or_resume_session(session_id)
        
    async def list_sessions(self) -> Dict[str, dict]:
        """Delegate shortcut for backwards compatibility in deps."""
        return await self.session_manager.list_sessions()
        
    async def get_default_agent_id(self) -> str:
        """Delegate shortcut for backwards compatibility in deps."""
        return await self.agent_manager.get_default_agent_id()
        
    async def get_agent(self, agent_id: str):
        """Delegate shortcut for backwards compatibility."""
        return await self.agent_manager.get_agent(agent_id)

    async def shutdown(self):
        if self.global_scheduler:
            await self.global_scheduler.stop()
            self.global_scheduler = None

        for session_id in list(self.active_sessions.keys()):
            try:
                await self.session_manager.close_session(session_id)
            except Exception as e:
                logger.error(f"Failed to close session {session_id} during shutdown: {e}")
        
        await self.resources.shutdown()
        logger.info("AppState shutdown complete")
