"""Explicit project-scoped cleanup of derived entity state."""

from __future__ import annotations

from collections.abc import Iterable
from typing import Any

from common.scoping import IDENTITY_ENTITY_ID, require_scope_value
from core.knowledge.store import KnowledgeStore


class EntityCleanupWorkflow:
    """Preview and apply deliberate entity deletion without background heuristics."""

    def __init__(self, knowledge_store: KnowledgeStore) -> None:
        self.knowledge_store = knowledge_store

    async def preview(
        self,
        *,
        user_name: str,
        project_id: str,
        limit: int = 100,
    ) -> dict[str, Any]:
        candidates = await self.knowledge_store.preview_project_entity_cleanup(
            user_name=user_name,
            project_id=require_scope_value(
                project_id,
                "project_id",
                "EntityCleanupWorkflow.preview",
            ),
            limit=limit,
        )
        return {"project_id": project_id, "candidates": candidates}

    async def apply(
        self,
        *,
        user_name: str,
        project_id: str,
        entity_ids: Iterable[int],
    ) -> dict[str, Any]:
        selected_ids = self._normalize_selected_ids(entity_ids)
        deleted_ids = await self.knowledge_store.delete_selected_project_entities(
            selected_ids,
            user_name=user_name,
            project_id=require_scope_value(
                project_id,
                "project_id",
                "EntityCleanupWorkflow.apply",
            ),
        )
        return {"project_id": project_id, "deleted_entity_ids": deleted_ids}

    @staticmethod
    def _normalize_selected_ids(entity_ids: Iterable[int]) -> list[int]:
        selected_ids: list[int] = []
        for entity_id in entity_ids:
            if not isinstance(entity_id, int) or isinstance(entity_id, bool):
                raise TypeError("entity_ids must contain integer IDs")
            if entity_id <= 0:
                raise ValueError("entity_ids must contain positive IDs")
            selected_ids.append(entity_id)
        selected_ids = sorted(set(selected_ids))
        if not selected_ids:
            raise ValueError("Entity cleanup requires at least one selected entity")
        if IDENTITY_ENTITY_ID in selected_ids:
            raise ValueError("The reserved identity entity cannot be deleted")
        return selected_ids
