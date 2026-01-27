from datetime import datetime, timezone
import json
import os
from typing import Dict, Optional
import uuid

from loguru import logger
from main.context import Context
from shared.resource import ResourceManager
from dotenv import load_dotenv

load_dotenv()

# USER_NAME = os.environ

class AppState:

    def __init__(self, resources: ResourceManager, active_sessions: Dict[str, Context], user_name: str):
        self.resources = resources
        self.active_sessions = active_sessions
        self.user_name = user_name
    
    async def list_sessions(self) -> Dict[str, dict]:
        try:
            raw = await self.resources.redis.hgetall(f"sessions:{self.user_name}")
            return {sid: json.loads(data) for sid, data in raw.items()}
        except Exception as e:
            logger.warning(f"Failed to list sessions: {e}")
            return {}

    async def create_session(self, topics_config: dict = None) -> Context:
        session_id = str(uuid.uuid4())
        
        context = await Context.create(
            user_name=self.user_name,
            resources=self.resources,
            topics_config=topics_config,
            session_id=session_id
        )
        
        metadata = {
            "created_at": datetime.now(timezone.utc).isoformat(),
            "topics_config": topics_config or {"General": {"labels": [], "hierarchy": {}}},
            "last_active": datetime.now(timezone.utc).isoformat()
        }
        
        await self.resources.redis.hset(
            f"sessions:{self.user_name}",
            session_id,
            json.dumps(metadata)
        )
        
        self.active_sessions[session_id] = context
        logger.info(f"Created session: {session_id}")
        return context

    async def get_session(self, session_id: str) -> Optional[Context]:
        if session_id in self.active_sessions:
            return self.active_sessions[session_id]
        
        raw = await self.resources.redis.hget(f"sessions:{self.user_name}", session_id)
        if not raw:
            return None
        
        metadata = json.loads(raw)
        
        context = await Context.create(
            user_name=self.user_name,
            resources=self.resources,
            topics_config=metadata.get("topics_config"),
            session_id=session_id
        )
        
        self.active_sessions[session_id] = context
        
        metadata["last_active"] = datetime.now(timezone.utc).isoformat()
        await self.resources.redis.hset(
            f"sessions:{self.user_name}",
            session_id,
            json.dumps(metadata)
        )
        
        logger.info(f"Resumed session: {session_id}")
        return context
    
    async def close_session(self, session_id: str) -> bool:
        if session_id not in self.active_sessions:
            return False
        
        context = self.active_sessions[session_id]
        await context.shutdown()
        del self.active_sessions[session_id]
        
        raw = await self.resources.redis.hget(f"sessions:{self.user_name}", session_id)
        if raw:
            metadata = json.loads(raw)
            metadata["last_active"] = datetime.now(timezone.utc).isoformat()
            await self.resources.redis.hset(
                f"sessions:{self.user_name}",
                session_id,
                json.dumps(metadata)
            )
        
        logger.info(f"Closed session: {session_id}")
        return True
    
    async def shutdown(self):
        for session_id in list(self.active_sessions.keys()):
            await self.close_session(session_id)
        
        self.resources.executor.shutdown(wait=True)
        await self.resources.redis.aclose()
        self.resources.store.close()
        
        logger.info("AppState shutdown complete")

    

    


