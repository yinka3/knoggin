"""Tests for LLM-dependent jobs: ProfileRefinementJob, MergeDetectionJob, TopicConfigJob.

Covers should_run trigger logic, sanitize guards, and execute orchestration.
All LLM/Redis/store calls mocked.
"""

import json
import pytest
from concurrent.futures import ThreadPoolExecutor
from unittest.mock import MagicMock, AsyncMock, patch

from jobs.profile import ProfileRefinementJob
from jobs.merger import MergeDetectionJob
from jobs.topics import TopicConfigJob
from jobs.base import JobContext, JobResult
from shared.config.topics_config import TopicConfig


# ── Shared helpers ──────────────────────────────────────

def make_redis(**overrides):
    redis = MagicMock()
    redis.get = AsyncMock(return_value=None)
    redis.set = AsyncMock()
    redis.scard = AsyncMock(return_value=0)
    redis.srandmember = AsyncMock(return_value=[])
    redis.sadd = AsyncMock()
    redis.srem = AsyncMock()
    redis.llen = AsyncMock(return_value=0)
    redis.zrevrange = AsyncMock(return_value=[])
    redis.hmget = AsyncMock(return_value=[])
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


TOPICS_RAW = {
    "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
    "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
    "Work": {"active": True, "labels": ["company", "project"], "hierarchy": {}, "aliases": ["career"]},
    "Education": {"active": True, "labels": ["university"], "hierarchy": {}, "aliases": []},
}


class TestProfileRefinementJob:

    # ════════════════════════════════════════════════════════
    #  should_run
    # ════════════════════════════════════════════════════════

    def _make_profile_job(self):
        return ProfileRefinementJob(
            llm=MagicMock(),
            resolver=MagicMock(),
            store=MagicMock(),
            executor=ThreadPoolExecutor(max_workers=1),
            embedding_service=MagicMock(),
            volume_threshold=15,
            idle_threshold=90,
        )

    @pytest.mark.asyncio
    async def test_no_dirty_entities(self):
        job = self._make_profile_job()
        redis = make_redis(scard=AsyncMock(return_value=0))
        ctx = make_ctx(redis, idle_seconds=200)

        with patch("jobs.profile.emit", new_callable=AsyncMock):
            result = await job.should_run(ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_volume_trigger(self):
        """Dirty count >= threshold → run regardless of idle time."""
        job = self._make_profile_job()
        redis = make_redis(scard=AsyncMock(return_value=20))
        ctx = make_ctx(redis, idle_seconds=10)

        with patch("jobs.profile.emit", new_callable=AsyncMock):
            result = await job.should_run(ctx)

        assert result is True

    @pytest.mark.asyncio
    async def test_idle_trigger(self):
        """Some dirty entities + idle past threshold → run."""
        job = self._make_profile_job()
        redis = make_redis(scard=AsyncMock(return_value=3))
        ctx = make_ctx(redis, idle_seconds=120)

        with patch("jobs.profile.emit", new_callable=AsyncMock):
            result = await job.should_run(ctx)

        assert result is True

    @pytest.mark.asyncio
    async def test_below_both_thresholds(self):
        """Some dirty but not enough, and not idle enough."""
        job = self._make_profile_job()
        redis = make_redis(scard=AsyncMock(return_value=3))
        ctx = make_ctx(redis, idle_seconds=30)

        with patch("jobs.profile.emit", new_callable=AsyncMock):
            result = await job.should_run(ctx)

        assert result is False

    @pytest.mark.asyncio
    async def test_volume_exactly_at_threshold(self):
        """Dirty count == threshold → should trigger."""
        job = self._make_profile_job()
        redis = make_redis(scard=AsyncMock(return_value=15))
        ctx = make_ctx(redis, idle_seconds=0)

        with patch("jobs.profile.emit", new_callable=AsyncMock):
            result = await job.should_run(ctx)

        assert result is True


class TestMergeDetectionJob:

    # ════════════════════════════════════════════════════════
    #  should_run
    # ════════════════════════════════════════════════════════

    def _make_merge_job(self):
        return MergeDetectionJob(
            user_name="Yinka",
            ent_resolver=MagicMock(),
            store=MagicMock(),
            llm_client=MagicMock(),
            topic_config=TopicConfig(TOPICS_RAW),
            executor=ThreadPoolExecutor(max_workers=1),
        )

    @pytest.mark.asyncio
    async def test_empty_queue(self):
        job = self._make_merge_job()
        redis = make_redis(scard=AsyncMock(return_value=0))
        ctx = make_ctx(redis)

        result = await job.should_run(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_queue_has_items(self):
        job = self._make_merge_job()
        redis = make_redis(scard=AsyncMock(return_value=5))
        ctx = make_ctx(redis)

        result = await job.should_run(ctx)
        assert result is True


    # ════════════════════════════════════════════════════════
    #  execute
    # ════════════════════════════════════════════════════════

    @pytest.mark.asyncio
    async def test_no_dirty_entities(self):
        job = self._make_merge_job()
        redis = make_redis(srandmember=AsyncMock(return_value=[]))
        ctx = make_ctx(redis)

        with patch("jobs.merger.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "No dirty" in result.summary

    @pytest.mark.asyncio
    async def test_no_candidates_found(self):
        job = self._make_merge_job()
        job.ent_resolver.detect_merge_entity_candidates.return_value = []
        redis = make_redis(
            srandmember=AsyncMock(return_value=["1", "2"]),
            srem=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.merger.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "No candidates" in result.summary
        # Dirty IDs should be cleared from queue
        redis.srem.assert_called()


    # ════════════════════════════════════════════════════════
    #  _same_topic
    # ════════════════════════════════════════════════════════

    def test_same_topic(self):
        job = self._make_merge_job()
        assert job._same_topic("Work", "Work") is True

    def test_alias_resolves_same(self):
        job = self._make_merge_job()
        assert job._same_topic("career", "Work") is True

    def test_different_topics(self):
        job = self._make_merge_job()
        assert job._same_topic("Work", "Education") is False

    def test_none_defaults_to_general(self):
        job = self._make_merge_job()
        assert job._same_topic(None, "General") is True

    def test_both_none(self):
        job = self._make_merge_job()
        assert job._same_topic(None, None) is True


class TestTopicConfigJob:

    # ════════════════════════════════════════════════════════
    #  should_run
    # ════════════════════════════════════════════════════════

    def _make_topic_job(self):
        return TopicConfigJob(
            llm=MagicMock(),
            topic_config=TopicConfig(TOPICS_RAW),
            update_callback=AsyncMock(),
            interval_msgs=40,
            conversation_window=50,
        )

    @pytest.mark.asyncio
    async def test_below_message_threshold(self):
        job = self._make_topic_job()

        async def key_aware_get(key):
            if "heartbeat" in key:
                return "10"
            return None

        redis = make_redis(get=AsyncMock(side_effect=key_aware_get))
        ctx = make_ctx(redis)

        result = await job.should_run(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_above_threshold_buffer_empty(self):
        job = self._make_topic_job()

        async def key_aware_get(key):
            if "heartbeat" in key:
                return "50"
            return None

        redis = make_redis(
            get=AsyncMock(side_effect=key_aware_get),
            llen=AsyncMock(return_value=0),
        )
        ctx = make_ctx(redis)

        result = await job.should_run(ctx)
        assert result is True

    @pytest.mark.asyncio
    async def test_above_threshold_buffer_not_empty(self):
        """Don't evolve while extraction is still pending."""
        job = self._make_topic_job()

        async def key_aware_get(key):
            if "heartbeat" in key:
                return "50"
            return None

        redis = make_redis(
            get=AsyncMock(side_effect=key_aware_get),
            llen=AsyncMock(return_value=3),
        )
        ctx = make_ctx(redis)

        result = await job.should_run(ctx)
        assert result is False

    @pytest.mark.asyncio
    async def test_no_heartbeat_key(self):
        job = self._make_topic_job()
        redis = make_redis(get=AsyncMock(return_value=None))
        ctx = make_ctx(redis)

        result = await job.should_run(ctx)
        assert result is False


    # ════════════════════════════════════════════════════════
    #  execute
    # ════════════════════════════════════════════════════════

    def _make_topic_job_with_mock(self, llm_response=None):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value=llm_response)
        llm.merge_model = "test-model"

        update_callback = AsyncMock()
        job = TopicConfigJob(
            llm=llm,
            topic_config=TopicConfig(TOPICS_RAW),
            update_callback=update_callback,
            interval_msgs=40,
            conversation_window=50,
        )
        return job, update_callback

    @pytest.mark.asyncio
    async def test_no_conversation(self):
        job, callback = self._make_topic_job_with_mock()
        redis = make_redis(
            zrevrange=AsyncMock(return_value=[]),
            set=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.topics.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "No conversation" in result.summary
        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_llm_returns_none(self):
        job, callback = self._make_topic_job_with_mock(llm_response=None)
        redis = make_redis(
            zrevrange=AsyncMock(return_value=["turn_1"]),
            hmget=AsyncMock(return_value=[json.dumps({"role": "user", "content": "test"})]),
            set=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.topics.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is False
        assert "LLM failed" in result.summary

    @pytest.mark.asyncio
    async def test_llm_returns_invalid_json(self):
        job, callback = self._make_topic_job_with_mock(llm_response="This is not JSON at all")
        redis = make_redis(
            zrevrange=AsyncMock(return_value=["turn_1"]),
            hmget=AsyncMock(return_value=[json.dumps({"role": "user", "content": "test"})]),
            set=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.topics.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is False
        assert "Invalid JSON" in result.summary

    @pytest.mark.asyncio
    async def test_successful_evolution(self):
        new_config = {
            **TOPICS_RAW,
            "Fitness": {"active": True, "labels": ["exercise", "sport"], "hierarchy": {}, "aliases": []},
        }
        job, callback = self._make_topic_job_with_mock(llm_response=json.dumps(new_config))
        redis = make_redis(
            zrevrange=AsyncMock(return_value=["turn_1", "turn_2"]),
            hmget=AsyncMock(return_value=[
                json.dumps({"role": "user", "content": "I've been going to the gym a lot"}),
                json.dumps({"role": "assistant", "content": "That's great!"}),
            ]),
            set=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.topics.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "Fitness" in result.summary
        callback.assert_called_once()

    @pytest.mark.asyncio
    async def test_destructive_changes_rejected(self):
        """Deactivating more than half of active topics → rejected.
        Need enough non-protected topics to trigger the guard after
        General/Identity are restored."""
        old_with_more = {
            **TOPICS_RAW,
            "Health": {"active": True, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Hobbies": {"active": True, "labels": ["game"], "hierarchy": {}, "aliases": []},
        }
        destructive = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
            "Work": {"active": False, "labels": ["company"], "hierarchy": {}, "aliases": []},
            "Education": {"active": False, "labels": ["university"], "hierarchy": {}, "aliases": []},
            "Health": {"active": False, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Hobbies": {"active": False, "labels": ["game"], "hierarchy": {}, "aliases": []},
        }

        job, callback = self._make_topic_job_with_mock(llm_response=json.dumps(destructive))
        # Override the topic_config to have the larger set
        job.topic_config = TopicConfig(old_with_more)

        redis = make_redis(
            zrevrange=AsyncMock(return_value=["turn_1"]),
            hmget=AsyncMock(return_value=[json.dumps({"role": "user", "content": "test"})]),
            set=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.topics.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is False
        assert "destructive" in result.summary.lower() or "Rejected" in result.summary
        callback.assert_not_called()

    @pytest.mark.asyncio
    async def test_no_changes_needed(self):
        """LLM returns identical config → no update triggered."""
        job, callback = self._make_topic_job_with_mock(llm_response=json.dumps(TOPICS_RAW))
        redis = make_redis(
            zrevrange=AsyncMock(return_value=["turn_1"]),
            hmget=AsyncMock(return_value=[json.dumps({"role": "user", "content": "test"})]),
            set=AsyncMock(),
        )
        ctx = make_ctx(redis)

        with patch("jobs.topics.emit", new_callable=AsyncMock):
            result = await job.execute(ctx)

        assert result.success is True
        assert "No changes" in result.summary
        callback.assert_not_called()


    # ════════════════════════════════════════════════════════
    #  sanitize_topic_evolution
    # ════════════════════════════════════════════════════════

    def test_removed_topic_restored(self):
        """LLM removes a topic → sanitizer restores it."""
        old = TOPICS_RAW.copy()
        new = {k: v for k, v in TOPICS_RAW.items() if k != "Work"}

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert "Work" in result

    def test_protected_topics_preserved(self):
        """LLM modifies General/Identity → old values kept."""
        old = TOPICS_RAW.copy()
        new = {
            **TOPICS_RAW,
            "General": {"active": False, "labels": ["misc"], "hierarchy": {}, "aliases": []},
            "Identity": {"active": True, "labels": ["human"], "hierarchy": {}, "aliases": []},
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert result["General"] == old["General"]
        assert result["Identity"] == old["Identity"]

    def test_hierarchy_preserved(self):
        """LLM changes hierarchy → old hierarchy restored."""
        old = {
            **TOPICS_RAW,
            "Work": {"active": True, "labels": ["company"], "hierarchy": {"company": ["project"]}, "aliases": []},
        }
        new = {
            **TOPICS_RAW,
            "Work": {"active": True, "labels": ["company"], "hierarchy": {"company": ["team"]}, "aliases": []},
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert result["Work"]["hierarchy"] == {"company": ["project"]}

    def test_new_topics_added(self):
        old = TOPICS_RAW.copy()
        new = {
            **TOPICS_RAW,
            "Fitness": {"active": True, "labels": ["exercise"], "hierarchy": {}, "aliases": []},
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert "Fitness" in result

    def test_new_topics_capped_at_three(self):
        old = TOPICS_RAW.copy()
        new = {
            **TOPICS_RAW,
            "Topic1": {"active": True, "labels": ["t1"], "hierarchy": {}, "aliases": []},
            "Topic2": {"active": True, "labels": ["t2"], "hierarchy": {}, "aliases": []},
            "Topic3": {"active": True, "labels": ["t3"], "hierarchy": {}, "aliases": []},
            "Topic4": {"active": True, "labels": ["t4"], "hierarchy": {}, "aliases": []},
            "Topic5": {"active": True, "labels": ["t5"], "hierarchy": {}, "aliases": []},
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        new_topic_names = [t for t in result if t not in old]
        assert len(new_topic_names) <= 3

    def test_bulk_deactivation_rejected(self):
        """Deactivating > half of active topics (after protected restoration) → returns None.
        General/Identity are restored from old config, so we need enough
        non-protected deactivations to trigger the guard."""
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
            "Education": {"active": True, "labels": ["university"], "hierarchy": {}, "aliases": []},
            "Health": {"active": True, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Hobbies": {"active": True, "labels": ["game"], "hierarchy": {}, "aliases": []},
        }
        # Deactivate 4 non-protected out of 6 total active → >50% after restoration
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
            "Work": {"active": False, "labels": ["company"], "hierarchy": {}, "aliases": []},
            "Education": {"active": False, "labels": ["university"], "hierarchy": {}, "aliases": []},
            "Health": {"active": False, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Hobbies": {"active": False, "labels": ["game"], "hierarchy": {}, "aliases": []},
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert result is None

    def test_label_sanitization(self):
        """Invalid labels cleaned: non-string, too long, invalid chars."""
        old = TOPICS_RAW.copy()
        new = {
            **TOPICS_RAW,
            "Fitness": {
                "active": True,
                "labels": [
                    "exercise",           # valid
                    "UPPER_CASE",         # invalid: uppercase
                    "a" * 50,             # invalid: too long
                    123,                  # invalid: not string
                    "good-label",         # valid
                    "",                   # invalid: empty
                    "has spaces ok",      # valid
                ],
                "hierarchy": {},
                "aliases": [],
            },
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        labels = result["Fitness"]["labels"]
        assert "exercise" in labels
        assert "good-label" in labels
        assert "has spaces ok" in labels
        assert "UPPER_CASE" not in labels
        assert 123 not in labels

    def test_new_topic_missing_fields_defaulted(self):
        old = TOPICS_RAW.copy()
        new = {
            **TOPICS_RAW,
            "Fitness": {"labels": ["exercise"]},  # missing active, aliases, hierarchy
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert result["Fitness"]["active"] is True
        assert result["Fitness"]["aliases"] == []
        assert result["Fitness"]["hierarchy"] == {}

    def test_single_deactivation_allowed(self):
        """Deactivating one topic out of four is fine."""
        old = TOPICS_RAW.copy()
        new = {
            **TOPICS_RAW,
            "Education": {"active": False, "labels": ["university"], "hierarchy": {}, "aliases": []},
        }

        result = TopicConfigJob.sanitize_topic_evolution(old, new)
        assert result is not None
        assert result["Education"]["active"] is False