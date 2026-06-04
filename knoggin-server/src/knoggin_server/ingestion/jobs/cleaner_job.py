import asyncio

import redis.asyncio as aioredis
from loguru import logger

from common.utils.events import emit
from common.utils.time_utils import get_now_ms, get_now_unix
from infrastructure.graph_client import GraphClient
from infrastructure.job.base import BaseJob, JobContext, JobResult
from infrastructure.redis_client import RedisKeys
from knoggin_server.knowledge.services.entity_service import EntityManager


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
        graph_client: GraphClient,
        entities: EntityManager,
        redis_client: aioredis.Redis,
        interval_hours: int = 24,
        orphan_age_hours: int = 24,
        stale_junk_days: int = 30,
    ):
        self.user_name = user_name
        self.graph_client = graph_client
        self.redis = redis_client
        self.entities = entities

        self.run_interval_seconds = interval_hours * 3600
        self.orphan_cutoff_ms = orphan_age_hours * 3600 * 1000
        self.stale_cutoff_ms = stale_junk_days * 24 * 3600 * 1000

        logger.info(f"Cleaner Job initialized. Interval: {interval_hours}h")

    @property
    def name(self) -> str:
        return "entity_cleanup"

    async def should_run(self, ctx: JobContext) -> bool:
        """Run if we haven't run in X hours."""
        last_run_key = RedisKeys.job_last_run(self.name, self.user_name, ctx.scope_id)
        last_run_ts = await self.redis.get(last_run_key)

        if not last_run_ts:
            await self.redis.set(last_run_key, get_now_unix())
            return False

        try:
            elapsed = get_now_unix() - float(last_run_ts)
        except ValueError:
            await self.redis.set(last_run_key, get_now_unix())
            return False

        return elapsed >= self.run_interval_seconds

    async def execute(self, ctx: JobContext) -> JobResult:
        with logger.contextualize(
            user=ctx.user_name, job=self.name, session=ctx.scope_id
        ):
            project_id = ctx.project_id or ctx.scope_id
            await self.graph_client.cleanup_null_entities(project_id=project_id)

            now_ms = get_now_ms()
            orphan_cutoff = now_ms - self.orphan_cutoff_ms
            junk_cutoff = now_ms - self.stale_cutoff_ms

            user_id = await self.entities.get_id(self.user_name)
            if user_id is None:
                await self.redis.set(
                    RedisKeys.job_last_run(self.name, self.user_name, ctx.scope_id),
                    get_now_unix(),
                )
                return JobResult(success=True, summary="User entity not initialized")

            orphan_ids = await self.graph_client.get_orphan_entities(
                user_id, orphan_cutoff, junk_cutoff, project_id=project_id
            )

            merge_key = RedisKeys.merge_queue(self.user_name, ctx.scope_id)
            pending_merge = await self.redis.smembers(merge_key)
            if pending_merge:
                pending_ids = {int(eid) for eid in pending_merge}
                protected = set(orphan_ids) & pending_ids
                if protected:
                    logger.info(
                        f"Cleanup: Skipping {len(protected)} orphans pending merge evaluation"
                    )
                    orphan_ids = [eid for eid in orphan_ids if eid not in pending_ids]

            if not orphan_ids:
                await self.redis.set(
                    RedisKeys.job_last_run(self.name, self.user_name, ctx.scope_id),
                    get_now_unix(),
                )
                return JobResult(success=True, summary="No orphans found")

            logger.info(
                f"Cleanup trigger: Found {len(orphan_ids)} entities (Orphans >24h or Junk >30d)"
            )
            for eid in orphan_ids:
                # We don't fetch names to avoid slow DB calls, but we log the IDs
                logger.debug(f"Cleaning entity ID: {eid}")

            batch_size = 100
            deleted_count = 0
            for i in range(0, len(orphan_ids), batch_size):
                batch = orphan_ids[i : i + batch_size]
                deleted_count += await self.graph_client.bulk_delete_entities(
                    batch, project_id=project_id
                )
                self.entities.remove_entities(batch)
                await asyncio.sleep(0.1)  # Yield to other tasks

            await self.redis.set(
                RedisKeys.job_last_run(self.name, self.user_name, ctx.scope_id),
                get_now_unix(),
            )

            await emit(
                ctx.scope_id,
                "job",
                "entities_cleaned",
                {"orphan_count": len(orphan_ids), "deleted_count": deleted_count},
            )
            return JobResult(success=True, summary=f"Cleaned {deleted_count} entities")

    def update_settings(
        self,
        interval_hours: int = None,
        orphan_age_hours: int = None,
        stale_junk_days: int = None,
    ):
        """
        Override BaseJob to convert hours/days into milliseconds.
        """
        updates = []

        if interval_hours is not None:
            self.run_interval_seconds = interval_hours * 3600
            updates.append(f"interval={interval_hours}h")

        if orphan_age_hours is not None:
            self.orphan_cutoff_ms = orphan_age_hours * 3600 * 1000
            updates.append(f"orphan_age={orphan_age_hours}h")

        if stale_junk_days is not None:
            self.stale_cutoff_ms = stale_junk_days * 24 * 3600 * 1000
            updates.append(f"stale_age={stale_junk_days}d")

        if updates:
            logger.info(f"Cleaner Job reconfigured: {', '.join(updates)}")

    async def on_shutdown(self, ctx: JobContext) -> None:
        # No state to flush
        pass
