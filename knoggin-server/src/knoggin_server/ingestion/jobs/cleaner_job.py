import asyncio

import redis.asyncio as aioredis
from loguru import logger

from common.schema.settings import CleanerSettings
from common.utils.events import emit
from common.utils.time_utils import get_now_ms
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.knowledge_store import KnowledgeStore
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.entity.resolver import EntityResolver


class EntityCleanupJob(BaseJob):
    """
    Removes 'orphan' entities (no relationships) that have been
    stagnant for >X hours.

    Trigger: Time-based (Default Every 24h)
    Safety: Only deletes if last_mentioned < 24h ago.
    """

    def __init__(
        self,
        user_name: str,
        knowledge_store: KnowledgeStore,
        entities: EntityResolver,
        redis_client: aioredis.Redis,
        interval_hours: int = 24,
        orphan_age_hours: int = 24,
        stale_junk_days: int = 30,
    ):
        self.user_name = user_name
        self.knowledge_store = knowledge_store
        self.redis = redis_client
        self.entities = entities

        self.run_interval_seconds = interval_hours * 3600
        self.orphan_cutoff_ms = orphan_age_hours * 3600 * 1000
        self.stale_cutoff_ms = stale_junk_days * 24 * 3600 * 1000

        logger.info(f"Cleaner Job initialized. Interval: {interval_hours}h")

    @property
    def name(self) -> str:
        return "entity_cleanup"

    @property
    def cadence_seconds(self) -> float:
        return self.run_interval_seconds

    async def should_run(self, ctx: JobContext) -> bool:
        return False

    async def execute(self, ctx: JobContext) -> JobResult:
        with logger.contextualize(
            user=ctx.user_name, job=self.name, project=ctx.project_id
        ):
            project_id = ctx.project_id
            null_deleted_ids = await self.knowledge_store.cleanup_null_entities(
                project_id=project_id
            )
            if null_deleted_ids:
                self.entities.remove_entities(null_deleted_ids)

            now_ms = get_now_ms()
            orphan_cutoff = now_ms - self.orphan_cutoff_ms
            junk_cutoff = now_ms - self.stale_cutoff_ms

            user_id = await self.entities.get_id(self.user_name)
            if user_id is None:
                return JobResult(success=True, summary="User entity not initialized")

            orphan_ids = await self.knowledge_store.get_orphan_entities(
                user_id, orphan_cutoff, junk_cutoff, project_id=project_id
            )

            merge_key = RedisKeys.merge_queue(self.user_name, ctx.project_id)
            pending_merge = await self.redis.smembers(merge_key)
            if pending_merge:
                pending_ids = {int(eid) for eid in pending_merge}
                protected = set(orphan_ids) & pending_ids
                if protected:
                    logger.info(
                        "Cleanup: Skipping "
                        f"{len(protected)} orphans pending merge evaluation"
                    )
                    orphan_ids = [eid for eid in orphan_ids if eid not in pending_ids]

            if not orphan_ids:
                if null_deleted_ids:
                    return JobResult(
                        success=True,
                        summary=f"Cleaned {len(null_deleted_ids)} entities",
                    )
                return JobResult(success=True, summary="No orphans found")

            logger.info(
                f"Cleanup trigger: Found {len(orphan_ids)} entities "
                "(Orphans >24h or Junk >30d)"
            )
            for eid in orphan_ids:
                # We don't fetch names to avoid slow DB calls, but we log the IDs
                logger.debug(f"Cleaning entity ID: {eid}")

            batch_size = 100
            deleted_ids = list(null_deleted_ids)
            for i in range(0, len(orphan_ids), batch_size):
                batch = orphan_ids[i : i + batch_size]
                batch_deleted_ids = await self.knowledge_store.bulk_delete_entities(
                    batch, project_id=project_id
                )
                deleted_ids.extend(batch_deleted_ids)
                self.entities.remove_entities(batch_deleted_ids)
                await asyncio.sleep(0.1)  # Yield to other tasks

            deleted_count = len(deleted_ids)

            await emit(
                ctx.project_id,
                "job",
                "entities_cleaned",
                {"orphan_count": len(orphan_ids), "deleted_count": deleted_count},
            )
            return JobResult(success=True, summary=f"Cleaned {deleted_count} entities")

    def update_settings(self, settings: CleanerSettings) -> None:
        self.enabled = settings.enabled
        self.run_interval_seconds = settings.interval_hours * 3600
        self.orphan_cutoff_ms = settings.orphan_age_hours * 3600 * 1000
        self.stale_cutoff_ms = settings.stale_junk_days * 24 * 3600 * 1000
        logger.info(
            "EntityCleanupJob settings updated: "
            f"enabled={self.enabled}, interval={settings.interval_hours}h, "
            f"orphan_age={settings.orphan_age_hours}h, "
            f"stale_age={settings.stale_junk_days}d"
        )
