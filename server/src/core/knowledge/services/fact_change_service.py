import uuid
from typing import Optional

from loguru import logger

from common.schema.primitives import FactRecord
from common.scoping import require_scope_value
from common.utils.events import emit
from common.utils.time_utils import get_now
from infrastructure.redis_client import RedisKeys

REMOVE_CHANGE_TYPES = frozenset(
    {"manual_remove", "bad_extraction_report", "admin_recovery"}
)
REPLACE_CHANGE_TYPES = frozenset(
    {"manual_correction", "fact_merge", "bad_extraction_report", "admin_recovery"}
)


class FactChangeService:
    """Coordinates user/admin fact changes around the durable audit boundary."""

    def __init__(self, knowledge_store, embedding_service, redis=None):
        self.knowledge_store = knowledge_store
        self.embedding_service = embedding_service
        self.redis = redis

    @staticmethod
    def _require_entity_id(entity_id: int, operation: str) -> int:
        if entity_id <= 0:
            raise ValueError(f"{operation} requires positive entity_id")
        return entity_id

    @staticmethod
    def _require_change_type(
        change_type: str, allowed: frozenset[str], operation: str
    ) -> str:
        change_type = require_scope_value(change_type, "change_type", operation)
        if change_type not in allowed:
            raise ValueError(f"{operation} does not support change_type={change_type}")
        return change_type

    @staticmethod
    def _require_fact_ids(fact_ids: list[str], operation: str) -> list[str]:
        if not fact_ids:
            raise ValueError(f"{operation} requires fact_ids")
        scoped_fact_ids = [
            require_scope_value(fact_id, "fact_id", operation) for fact_id in fact_ids
        ]
        if len(set(scoped_fact_ids)) != len(scoped_fact_ids):
            raise ValueError(f"{operation} rejects duplicate fact_ids")
        return scoped_fact_ids

    async def remove_fact(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_id: int,
        fact_id: str,
        actor: str,
        reason: str,
        session_id: Optional[str] = None,
        change_type: str = "manual_remove",
    ) -> dict:
        operation = "remove_fact"
        user_name = require_scope_value(user_name, "user_name", operation)
        project_id = require_scope_value(project_id, "project_id", operation)
        actor = require_scope_value(actor, "actor", operation)
        reason = require_scope_value(reason, "reason", operation)
        fact_id = require_scope_value(fact_id, "fact_id", operation)
        entity_id = self._require_entity_id(entity_id, operation)
        change_type = self._require_change_type(
            change_type, REMOVE_CHANGE_TYPES, operation
        )
        result = await self.knowledge_store.remove_fact_with_audit(
            fact_change_id=str(uuid.uuid4()),
            user_name=user_name,
            project_id=project_id,
            entity_id=entity_id,
            fact_id=fact_id,
            actor=actor,
            change_type=change_type,
            reason=reason,
            session_id=session_id,
        )
        result["dirty_marked"] = await self._mark_entity_dirty(
            user_name=user_name,
            project_id=project_id,
            entity_id=entity_id,
        )
        return result

    async def replace_facts(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_id: int,
        fact_ids: list[str],
        replacement_content: str,
        actor: str,
        reason: str,
        session_id: Optional[str] = None,
        change_type: str = "manual_correction",
    ) -> dict:
        operation = "replace_facts"
        user_name = require_scope_value(user_name, "user_name", operation)
        project_id = require_scope_value(project_id, "project_id", operation)
        actor = require_scope_value(actor, "actor", operation)
        reason = require_scope_value(reason, "reason", operation)
        entity_id = self._require_entity_id(entity_id, operation)
        fact_ids = self._require_fact_ids(fact_ids, operation)
        replacement_content = require_scope_value(
            replacement_content, "replacement_content", operation
        )
        replacement_content = replacement_content.strip()
        change_type = self._require_change_type(
            change_type, REPLACE_CHANGE_TYPES, operation
        )
        embedding = await self.embedding_service.encode_single(replacement_content)
        replacement_fact = FactRecord(
            id=str(uuid.uuid4()),
            content=replacement_content,
            valid_at=get_now(),
            source_entity_id=entity_id,
            source="user",
            embedding=embedding,
        )
        result = await self.knowledge_store.replace_facts_with_audit(
            fact_change_id=str(uuid.uuid4()),
            user_name=user_name,
            project_id=project_id,
            entity_id=entity_id,
            fact_ids=fact_ids,
            actor=actor,
            change_type=change_type,
            reason=reason,
            replacement_fact=replacement_fact,
            replacement_content=replacement_content,
            session_id=session_id,
        )
        result["dirty_marked"] = await self._mark_entity_dirty(
            user_name=user_name,
            project_id=project_id,
            entity_id=entity_id,
        )
        return result

    async def _mark_entity_dirty(
        self, *, user_name: str, project_id: str, entity_id: int
    ) -> bool:
        if not self.redis:
            return False
        dirty_key = RedisKeys.dirty_entities(user_name, project_id)
        try:
            await self.redis.sadd(dirty_key, str(entity_id))
            await self.redis.delete(
                RedisKeys.project_profile_complete(user_name, project_id)
            )
            await emit(
                project_id,
                "job",
                "dirty_entities_marked",
                {
                    "user_name": user_name,
                    "project_id": project_id,
                    "dirty_key": dirty_key,
                    "entity_ids": [entity_id],
                    "marked_count": 1,
                    "reason": "fact_change",
                },
            )
            return True
        except Exception as exc:
            logger.warning(f"Failed to mark fact change entity dirty: {exc}")
            return False
