from common.schema.ingestion.contracts import (
    GraphWriteSummary,
    IngestionCommit,
)
from core.ingestion.ports import IngestionGraphPersistence, IngestionPersistence


class _IngestionStore:
    async def seal_due_user_messages(self, **_kwargs):
        return 0

    async def claim_next_ingestion_batch(self, **_kwargs):
        return None

    async def release_ingestion_claim(self, **_kwargs):
        return True

    async def fail_ingestion_claim(self, **_kwargs):
        return True


class _GraphStore(_IngestionStore):
    async def validate_existing_ids(
        self,
        _ids: list[int],
        *,
        visible_project_ids: list[str],
    ) -> set[int] | None:
        return set(visible_project_ids and _ids)

    async def commit_ingestion(self, _commit: IngestionCommit) -> GraphWriteSummary:
        return GraphWriteSummary()


def test_ingestion_store_fakes_match_their_narrow_runtime_protocols():
    assert isinstance(_IngestionStore(), IngestionPersistence)
    assert isinstance(_GraphStore(), IngestionGraphPersistence)
