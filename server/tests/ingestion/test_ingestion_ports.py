from typing import Any

from common.schema.contracts import CandidateSuggestion, ExecutionScope
from core.ingestion.ports import IngestionGraphPersistence, IngestionPersistence


class _IngestionStore:
    async def save_message_logs(self, _messages: list[dict[str, Any]]) -> bool:
        return True

    async def save_candidate_suggestions(
        self,
        _scope: ExecutionScope,
        _suggestions: list[CandidateSuggestion],
    ) -> int:
        return 0


class _GraphStore(_IngestionStore):
    async def validate_existing_ids(
        self,
        _ids: list[int],
        *,
        visible_project_ids: list[str],
    ) -> set[int] | None:
        return set(visible_project_ids and _ids)

    async def update_entity_aliases(
        self,
        _alias_updates: dict[int, list[str]],
        *,
        project_id: str,
    ) -> None:
        assert project_id

    async def write_batch(self, *_args, **_kwargs) -> bool:
        return True


def test_ingestion_store_fakes_match_their_narrow_runtime_protocols():
    assert isinstance(_IngestionStore(), IngestionPersistence)
    assert isinstance(_GraphStore(), IngestionGraphPersistence)
