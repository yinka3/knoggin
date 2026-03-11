"""Tests for jobs/cleaner.py and jobs/archive.py.

Deterministic jobs — no LLM calls. Store and Redis mocked.
"""

import time
import pytest
from unittest.mock import MagicMock, AsyncMock, patch

from jobs.cleaner import EntityCleanupJob
from jobs.archive import FactArchivalJob
from jobs.base import JobContext


# ── Helpers ─────────────────────────────────────────────

def make_redis(**overrides):
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    redis.scard = AsyncMock(return_value=0)
    redis.srandmember = AsyncMock(return_value=[])
    for k, v in overrides.items():
        setattr(redis, k, v)
    return redis


def make_ctx(redis=None, idle_seconds=0.0):
    return JobContext(
        user_name="Yinka",
        session_id="test-session",
        redis=redis or make_redis(),
        idle_seconds=idle_seconds,
    )


def make_cleaner(**overrides):
    store = MagicMock()
    store.cleanup_null_entities.return_value = 0
    store.get_orphan_entities.return_value = []
    store.bulk_delete_entities.return_value = 0

    resolver = MagicMock()
    resolver.get_id.return_value = 1
    resolver.remove_entities.return_value = 0

    defaults = dict(
        user_name="Yinka",
        store=store,
        ent_resolver=resolver,
        interval_hours=24,
        orphan_age_hours=24,
        stale_junk_days=30,
    )
    defaults.update(overrides)
    job = EntityCleanupJob(**defaults)
    return job, store, resolver


def make_archival(**overrides):
    store = MagicMock()
    store.delete_old_invalidated_facts.return_value = 0

    defaults = dict(
        user_name="Yinka",
        store=store,
        retention_days=14,
        fallback_interval_hours=24,
    )
    defaults.update(overrides)
    job = FactArchivalJob(**defaults)
    return job, store


class TestEntityCleanupJob:

    # ════════════════════════════════════════════════════════
    #  should_run
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_first_run_sets_timestamp_returns_false(self):
        """No last_run key → seed timestamp, don't run yet."""
        redis = make_redis(get=AsyncMock(return_value=None))
        ctx = make_ctx(redis)
        job, _, _ = make_cleaner()

        result = await job.should_run(ctx)

        assert result is False
        redis.set.assert_called_once()

    @pytest.mark.asyncio
    async def test_interval_elapsed_returns_true(self):
        """Last run was 25 hours ago, interval is 24h → should run."""
        old_ts = str(time.time() - (25 * 3600))
        redis = make_redis(get=AsyncMock(return_value=old_ts))
        ctx = make_ctx(redis)
        job, _, _ = make_cleaner(interval_hours=24)

        result = await job.should_run(ctx)
        assert result is True

    @pytest.mark.asyncio
    async def test_interval_not_elapsed_returns_false(self):
        """Last run was 1 hour ago, interval is 24h → don't run."""
        recent_ts = str(time.time() - 3600)
        redis = make_redis(get=AsyncMock(return_value=recent_ts))
        ctx = make_ctx(redis)
        job, _, _ = make_cleaner(interval_hours=24)

        result = await job.should_run(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_corrupted_timestamp_resets(self):
        """Non-numeric value in Redis → reset and return False."""
        redis = make_redis(get=AsyncMock(return_value="not_a_number"))
        ctx = make_ctx(redis)
        job, _, _ = make_cleaner()

        result = await job.should_run(ctx)

        assert result is False
        # Should have reset the timestamp
        redis.set.assert_called_once()


    # ════════════════════════════════════════════════════════
    #  execute
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_no_user_entity(self):
        """If user entity doesn't exist yet, early return."""
        job, store, resolver = make_cleaner()
        resolver.get_id.return_value = None
        redis = make_redis()
        ctx = make_ctx(redis)

        with patch("jobs.cleaner.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "not initialized" in result.summary
        store.get_orphan_entities.assert_not_called()
        # Should still update last_run
        redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_no_orphans_found(self):
        job, store, resolver = make_cleaner()
        store.get_orphan_entities.return_value = []
        redis = make_redis()
        ctx = make_ctx(redis)

        with patch("jobs.cleaner.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "No orphans" in result.summary
        store.bulk_delete_entities.assert_not_called()
        redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_orphans_deleted(self):
        job, store, resolver = make_cleaner()
        store.get_orphan_entities.return_value = [10, 11, 12]
        store.bulk_delete_entities.return_value = 3
        redis = make_redis()
        ctx = make_ctx(redis)

        with patch("jobs.cleaner.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "3" in result.summary
        store.bulk_delete_entities.assert_called_once_with([10, 11, 12])
        resolver.remove_entities.assert_called_once_with([10, 11, 12])
        redis.set.assert_called()

    @pytest.mark.asyncio
    async def test_null_entities_cleaned_first(self):
        """cleanup_null_entities should always run before orphan detection."""
        job, store, _ = make_cleaner()
        redis = make_redis()
        ctx = make_ctx(redis)

        call_order = []
        store.cleanup_null_entities.side_effect = lambda: call_order.append("null_cleanup")
        store.get_orphan_entities.side_effect = lambda *a: (call_order.append("orphan_detect"), [])[1]

        with patch("jobs.cleaner.emit", new_callable=AsyncMock):
            await job.execute(ctx)

        assert call_order == ["null_cleanup", "orphan_detect"]


    # ════════════════════════════════════════════════════════
    #  update_settings
    # ════════════════════════════════════════════════════════

    def test_converts_hours_to_seconds(self):
        job, _, _ = make_cleaner(interval_hours=24)
        job.update_settings(interval_hours=12)
        assert job.run_interval_seconds == 12 * 3600

    def test_converts_orphan_hours_to_ms(self):
        job, _, _ = make_cleaner()
        job.update_settings(orphan_age_hours=48)
        assert job.orphan_cutoff_ms == 48 * 3600 * 1000

    def test_converts_stale_days_to_ms(self):
        job, _, _ = make_cleaner()
        job.update_settings(stale_junk_days=60)
        assert job.stale_cutoff_ms == 60 * 24 * 3600 * 1000

    def test_partial_update(self):
        job, _, _ = make_cleaner(interval_hours=24, orphan_age_hours=24)
        original_orphan = job.orphan_cutoff_ms

        job.update_settings(interval_hours=12)

        assert job.run_interval_seconds == 12 * 3600
        assert job.orphan_cutoff_ms == original_orphan  # unchanged


class TestFactArchivalJob:

    # ════════════════════════════════════════════════════════
    #  should_run
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_profile_complete_flag_triggers(self):
        """If profile_complete key exists, always run regardless of time."""
        async def key_aware_get(key):
            if "profile_complete" in key:
                return "1"
            return None

        redis = make_redis(get=AsyncMock(side_effect=key_aware_get))
        ctx = make_ctx(redis)
        job, _ = make_archival()

        result = await job.should_run(ctx)
        assert result is True

    @pytest.mark.asyncio
    async def test_no_flag_interval_elapsed(self):
        """No profile_complete, but fallback interval has elapsed."""
        old_ts = str(time.time() - (25 * 3600))  # 25 hours ago

        async def key_aware_get(key):
            if "profile_complete" in key:
                return None
            if "last_run" in key:
                return old_ts
            return None

        redis = make_redis(get=AsyncMock(side_effect=key_aware_get))
        ctx = make_ctx(redis)
        job, _ = make_archival(fallback_interval_hours=24)

        result = await job.should_run(ctx)
        assert result is True

    @pytest.mark.asyncio
    async def test_no_flag_interval_not_elapsed(self):
        recent_ts = str(time.time() - 3600)  # 1 hour ago

        async def key_aware_get(key):
            if "profile_complete" in key:
                return None
            if "last_run" in key:
                return recent_ts
            return None

        redis = make_redis(get=AsyncMock(side_effect=key_aware_get))
        ctx = make_ctx(redis)
        job, _ = make_archival(fallback_interval_hours=24)

        result = await job.should_run(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_no_flag_no_last_run(self):
        """No profile_complete, no last_run → don't run."""
        redis = make_redis(get=AsyncMock(return_value=None))
        ctx = make_ctx(redis)
        job, _ = make_archival()

        result = await job.should_run(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_corrupted_last_run_returns_false(self):
        async def key_aware_get(key):
            if "profile_complete" in key:
                return None
            if "last_run" in key:
                return "garbage_value"
            return None

        redis = make_redis(get=AsyncMock(side_effect=key_aware_get))
        ctx = make_ctx(redis)
        job, _ = make_archival()

        result = await job.should_run(ctx)
        assert result is False


    # ════════════════════════════════════════════════════════
    #  execute
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_facts_deleted(self):
        job, store = make_archival(retention_days=14)
        store.delete_old_invalidated_facts.return_value = 5
        ctx = make_ctx()

        with patch("jobs.archive.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "5" in result.summary
        store.delete_old_invalidated_facts.assert_called_once()

    @pytest.mark.asyncio
    async def test_no_facts_to_delete(self):
        job, store = make_archival()
        store.delete_old_invalidated_facts.return_value = 0
        ctx = make_ctx()

        with patch("jobs.archive.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "0" in result.summary


    # ════════════════════════════════════════════════════════
    #  update_settings
    # ════════════════════════════════════════════════════════

    def test_updates_retention_days(self):
        job, _ = make_archival(retention_days=14)
        job.update_settings(retention_days=30)
        assert job.retention_days == 30

    def test_converts_hours_to_seconds(self):
        job, _ = make_archival(fallback_interval_hours=24)
        job.update_settings(fallback_interval_hours=12)
        assert job._fallback_interval_seconds == 12 * 3600

    def test_partial_update(self):
        job, _ = make_archival(retention_days=14, fallback_interval_hours=24)
        original_interval = job._fallback_interval_seconds

        job.update_settings(retention_days=7)

        assert job.retention_days == 7
        assert job._fallback_interval_seconds == original_interval