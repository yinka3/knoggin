import json

import pytest
from unittest.mock import AsyncMock, MagicMock, patch

from src.jobs.base import JobContext
from src.jobs.topics import TopicConfigJob
from src.common.config.topics_config import (
    TopicConfig,
    build_label_block,
    build_topic_alias_lookup,
    get_active_topic_names
)
from src.common.services.topic_manager import generate_topics




@pytest.fixture
def config_dict():
    return {
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": ["people"]},
        "Work": {
            "active": True,
            "labels": ["company", "project"],
            "hierarchy": {"company": ["project"]},
            "aliases": ["career", "job"],
            "hot": True,
        },
        "Cooking": {"active": False, "labels": ["recipe", "ingredient"], "hierarchy": {}, "aliases": ["food"]},
    }


@pytest.fixture
def topic_config(config_dict):
    return TopicConfig(config_dict)


class TestModuleHelpers:

    def test_build_label_block_active_topics_included(self, config_dict):
        block = build_label_block(config_dict)
        assert "Work" in block
        assert "company" in block
        assert "project" in block

    def test_build_label_block_identity_excluded(self, config_dict):
        block = build_label_block(config_dict)
        assert "Identity" not in block

    def test_build_label_block_inactive_excluded(self, config_dict):
        block = build_label_block(config_dict)
        assert "Cooking" not in block
        assert "recipe" not in block

    def test_build_label_block_empty_labels_topic_excluded(self, config_dict):
        block = build_label_block(config_dict)
        assert "General" not in block

    def test_build_label_block_empty_config(self):
        assert build_label_block({}) == ""

    def test_build_alias_lookup_canonical_names_mapped(self, config_dict):
        lookup = build_topic_alias_lookup(config_dict)
        assert lookup["general"] == "General"
        assert lookup["work"] == "Work"

    def test_build_alias_lookup_aliases_mapped(self, config_dict):
        lookup = build_topic_alias_lookup(config_dict)
        assert lookup["career"] == "Work"
        assert lookup["job"] == "Work"
        assert lookup["food"] == "Cooking"
        assert lookup["people"] == "Identity"

    def test_build_alias_lookup_case_insensitive(self, config_dict):
        lookup = build_topic_alias_lookup(config_dict)
        assert "work" in lookup
        assert "Work" not in lookup

    def test_build_alias_lookup_no_aliases_key(self):
        config = {"Work": {"active": True, "labels": ["company"]}}
        lookup = build_topic_alias_lookup(config)
        assert lookup["work"] == "Work"

    def test_get_active_topic_names_returns_active_only(self, config_dict):
        names = get_active_topic_names(config_dict)
        assert "General" in names
        assert "Work" in names
        assert "Cooking" not in names


class TestTopicConfig:

    def test_raw(self, topic_config, config_dict):
        assert topic_config.raw is config_dict

    def test_alias_lookup(self, topic_config):
        assert topic_config.alias_lookup["career"] == "Work"
        assert topic_config.alias_lookup["general"] == "General"

    def test_label_block(self, topic_config):
        block = topic_config.label_block
        assert "Work" in block
        assert "company" in block
        assert "Identity" not in block

    def test_hierarchy(self, topic_config):
        assert topic_config.hierarchy["Work"] == {"company": ["project"]}
        assert topic_config.hierarchy["General"] == {}

    def test_active_topics(self, topic_config):
        active = topic_config.active_topics
        assert "General" in active
        assert "Work" in active
        assert "Cooking" not in active

    def test_hot_topics(self, topic_config):
        hot = topic_config.hot_topics
        assert "Work" in hot
        assert "General" not in hot

    def test_hot_topics_excludes_inactive(self):
        tc = TopicConfig({
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Stale": {"active": False, "labels": ["thing"], "hierarchy": {}, "aliases": [], "hot": True},
        })
        assert tc.hot_topics == []

    def test_properties_are_cached(self, topic_config):
        a = topic_config.alias_lookup
        b = topic_config.alias_lookup
        assert a is b

        a2 = topic_config.active_topics
        b2 = topic_config.active_topics
        assert a2 is b2

    def test_normalize_canonical(self, topic_config):
        assert topic_config.normalize_topic("Work") == "Work"

    def test_normalize_alias(self, topic_config):
        assert topic_config.normalize_topic("career") == "Work"
        assert topic_config.normalize_topic("Job") == "Work"

    def test_normalize_unknown_falls_to_general(self, topic_config):
        assert topic_config.normalize_topic("NonExistent") == "General"

    def test_normalize_unknown_no_general(self):
        tc = TopicConfig({
            "General": {"active": False, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        })
        assert tc.normalize_topic("garbage") is None

    def test_normalize_empty(self, topic_config):
        assert topic_config.normalize_topic("") is None
        assert topic_config.normalize_topic(None) is None

    def test_inactive_topic_alias_still_resolves(self, topic_config):
        result = topic_config.normalize_topic("food")
        assert result == "Cooking"

    def test_inactive_canonical_still_resolves(self, topic_config):
        result = topic_config.normalize_topic("Cooking")
        assert result == "Cooking"

    def test_is_active_true(self, topic_config):
        assert topic_config.is_active("Work") is True

    def test_is_active_false(self, topic_config):
        assert topic_config.is_active("Cooking") is False

    def test_is_active_missing_defaults_true(self, topic_config):
        assert topic_config.is_active("DoesNotExist") is True

    def test_get_labels(self, topic_config):
        assert topic_config.get_labels_for_topic("Work") == ["company", "project"]

    def test_get_labels_empty(self, topic_config):
        assert topic_config.get_labels_for_topic("General") == []

    def test_get_labels_missing_topic(self, topic_config):
        assert topic_config.get_labels_for_topic("NoSuchTopic") == []

    def test_update_invalidates_cache(self, topic_config):
        old_active = topic_config.active_topics
        assert "Cooking" not in old_active

        topic_config.update({
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Cooking": {"active": True, "labels": ["recipe"], "hierarchy": {}, "aliases": []},
        })

        new_active = topic_config.active_topics
        assert "Cooking" in new_active
        assert old_active is not new_active

    def test_update_refreshes_alias_lookup(self, topic_config):
        old_lookup = topic_config.alias_lookup
        assert "newstuff" not in old_lookup

        topic_config.update({
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "NewTopic": {"active": True, "labels": ["widget"], "hierarchy": {}, "aliases": ["newstuff"]},
        })

        assert topic_config.alias_lookup["newstuff"] == "NewTopic"

    def test_add_new_topic(self, topic_config):
        topic_config.add_topic("Health", {
            "active": True, "labels": ["doctor"], "hierarchy": {}, "aliases": ["medical"]
        })
        assert "Health" in topic_config.raw
        assert topic_config.is_active("Health") is True
        assert "medical" in topic_config.alias_lookup

    def test_add_existing_topic_is_noop(self, topic_config):
        original_labels = topic_config.raw["Work"]["labels"][:]
        topic_config.add_topic("Work", {
            "active": True, "labels": ["different"], "hierarchy": {}, "aliases": []
        })
        assert topic_config.raw["Work"]["labels"] == original_labels

    def test_add_topic_clears_cache(self, topic_config):
        old_active = topic_config.active_topics
        topic_config.add_topic("NewTopic", {
            "active": True, "labels": ["widget"], "hierarchy": {}, "aliases": []
        })
        new_active = topic_config.active_topics
        assert "NewTopic" in new_active
        assert old_active is not new_active

    def test_remove_existing_topic(self, topic_config):
        assert "Cooking" in topic_config.raw
        topic_config.remove_topic("Cooking")
        assert "Cooking" not in topic_config.raw
        assert "food" not in topic_config.alias_lookup

    def test_remove_nonexistent_topic(self, topic_config):
        topic_config.remove_topic("DoesNotExist")
        assert "Work" in topic_config.raw

    def test_remove_clears_cache(self, topic_config):
        old_active = topic_config.active_topics
        topic_config.remove_topic("Work")
        new_active = topic_config.active_topics
        assert "Work" not in new_active
        assert old_active is not new_active

    def test_deactivate_topic(self, topic_config):
        assert topic_config.is_active("Work") is True
        topic_config.toggle_active("Work", False)
        assert topic_config.is_active("Work") is False
        assert "Work" not in topic_config.active_topics

    def test_activate_topic(self, topic_config):
        assert topic_config.is_active("Cooking") is False
        topic_config.toggle_active("Cooking", True)
        assert topic_config.is_active("Cooking") is True
        assert "Cooking" in topic_config.active_topics

    def test_toggle_nonexistent_topic(self, topic_config):
        topic_config.toggle_active("Ghost", True)
        assert "Ghost" not in topic_config.raw

    def test_validate_hot_topics_filters_to_active(self, topic_config):
        result = topic_config.validate_hot_topics(["Work", "Cooking"])
        assert "Work" in result
        assert "Cooking" not in result

    def test_validate_hot_topics_normalizes_aliases(self, topic_config):
        result = topic_config.validate_hot_topics(["career", "job"])
        assert "Work" in result
        assert result.count("Work") == 1

    def test_validate_hot_topics_empty_input(self, topic_config):
        assert topic_config.validate_hot_topics([]) == []

    def test_validate_hot_topics_all_invalid(self, topic_config):
        result = topic_config.validate_hot_topics(["Nonexistent", "AlsoFake"])
        assert result == ["General"]

    def test_validate_hot_topics_unknown_normalizes_to_general(self, topic_config):
        result = topic_config.validate_hot_topics(["Work", "TotallyMadeUp"])
        assert "Work" in result
        assert "General" in result
        assert len(result) == 2

    @pytest.mark.asyncio
    async def test_save_writes_to_redis(self, topic_config):
        redis = MagicMock()
        redis.hset = AsyncMock()

        await topic_config.save(redis, "Yinka", "session-1")

        redis.hset.assert_called_once()
        call_args = redis.hset.call_args[0]
        stored = json.loads(call_args[2])
        assert "Work" in stored

    @pytest.mark.asyncio
    async def test_load_from_redis(self):
        config = {"Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []}}
        redis = MagicMock()
        redis.hget = AsyncMock(return_value=json.dumps(config))

        tc = await TopicConfig.load(redis, "Yinka", "session-1")
        assert "Work" in tc.raw

    @pytest.mark.asyncio
    async def test_load_missing_key_uses_default(self):
        redis = MagicMock()
        redis.hget = AsyncMock(return_value=None)

        tc = await TopicConfig.load(redis, "Yinka", "session-1")
        assert "General" in tc.raw
        assert len(tc.raw) == 1


class TestGenerateTopics:

    @pytest.mark.asyncio
    async def test_clean_json_response(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='{"Work": {"labels": ["company", "project"], "aliases": ["career"]}}')

        result = await generate_topics(llm, "I work in tech")
        assert "Work" in result
        assert "General" in result
        assert "Identity" in result
        assert result["Work"]["labels"] == ["company", "project"]

    @pytest.mark.asyncio
    async def test_json_wrapped_in_code_fence(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='```json\n{"Work": {"labels": ["company"]}}\n```')

        result = await generate_topics(llm, "I work in tech")
        assert "Work" in result

    @pytest.mark.asyncio
    async def test_strips_general_and_identity(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='{"General": {"labels": ["misc"]}, "Identity": {"labels": ["human"]}, "Work": {"labels": ["company"]}}')

        result = await generate_topics(llm, "test")
        assert result["General"]["labels"] == []
        assert result["Identity"]["labels"] == ["person"]
        assert result["Work"]["labels"] == ["company"]

    @pytest.mark.asyncio
    async def test_caps_at_max_topics(self):
        llm = MagicMock()
        many = {f"Topic{i}": {"labels": [f"label{i}"]} for i in range(10)}
        llm.call_llm = AsyncMock(return_value=json.dumps(many))

        result = await generate_topics(llm, "test", max_topics=3)
        generated = {k: v for k, v in result.items() if k not in ("General", "Identity")}
        assert len(generated) <= 3

    @pytest.mark.asyncio
    async def test_missing_fields_get_defaults(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='{"Work": {"labels": ["company"]}}')

        result = await generate_topics(llm, "test")
        assert result["Work"]["aliases"] == []
        assert result["Work"]["hierarchy"] == {}
        assert result["Work"]["active"] is True

    @pytest.mark.asyncio
    async def test_invalid_json_raises(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value="This is not JSON at all, sorry!")

        with pytest.raises(ValueError, match="Failed to parse"):
            await generate_topics(llm, "test")

    @pytest.mark.asyncio
    async def test_empty_llm_response_raises(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value=None)

        with pytest.raises(ValueError, match="empty response"):
            await generate_topics(llm, "test")

    @pytest.mark.asyncio
    async def test_json_with_trailing_explanation(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='```json\n{"Work": {"labels": ["company"]}}\n```\n\nI generated one topic based on your description.')

        result = await generate_topics(llm, "test")
        assert "Work" in result

    @pytest.mark.asyncio
    async def test_whitespace_only_response_raises(self):
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value="   \n\n   ")

        with pytest.raises(ValueError):
            await generate_topics(llm, "test")


class TestTopicConfigJob:

    @pytest.fixture
    def mock_redis(self):
        return AsyncMock()

    @pytest.fixture
    def job_context(self, mock_redis):
        ctx = MagicMock(spec=JobContext)
        ctx.user_name = "test_user"
        ctx.session_id = "user-123"
        ctx.redis = mock_redis
        return ctx

    @pytest.fixture
    def topic_job(self, topic_config):
        llm = AsyncMock()
        update_cb = AsyncMock()
        return TopicConfigJob(llm, topic_config, update_cb, interval_msgs=10)

    def _sanitize(self, old, new):
        return TopicConfigJob.sanitize_topic_evolution(old, new)

    def test_removed_topics_restored(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        }
        result = self._sanitize(old, new)
        assert "Work" in result

    def test_protected_topics_preserved(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": ["misc"], "hierarchy": {}, "aliases": []},
            "Identity": {"active": False, "labels": ["human"], "hierarchy": {}, "aliases": []},
        }
        result = self._sanitize(old, new)
        assert result["General"]["labels"] == []
        assert result["Identity"]["labels"] == ["person"]
        assert result["Identity"]["active"] is True

    def test_hierarchy_preserved_from_old(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {"company": ["project"]}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        }
        result = self._sanitize(old, new)
        assert result["Work"]["hierarchy"] == {"company": ["project"]}

    def test_bulk_deactivation_rejected(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
            "Health": {"active": True, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Education": {"active": True, "labels": ["university"], "hierarchy": {}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": False, "labels": ["company"], "hierarchy": {}, "aliases": []},
            "Health": {"active": False, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Education": {"active": False, "labels": ["university"], "hierarchy": {}, "aliases": []},
        }
        result = self._sanitize(old, new)
        assert result is None

    def test_new_topics_capped_at_three(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Topic1": {"active": True, "labels": ["a"], "aliases": [], "hierarchy": {}},
            "Topic2": {"active": True, "labels": ["b"], "aliases": [], "hierarchy": {}},
            "Topic3": {"active": True, "labels": ["c"], "aliases": [], "hierarchy": {}},
            "Topic4": {"active": True, "labels": ["d"], "aliases": [], "hierarchy": {}},
            "Topic5": {"active": True, "labels": ["e"], "aliases": [], "hierarchy": {}},
        }
        result = self._sanitize(old, new)
        new_topics = [t for t in result if t not in old]
        assert len(new_topics) <= 3

    def test_label_sanitization(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {
                "active": True,
                "hierarchy": {},
                "aliases": [],
                "labels": [
                    "COMPANY",
                    "valid-label",
                    "",
                    "x" * 31,
                    "123invalid",
                    "good_label",
                    42,
                ],
            },
        }
        result = self._sanitize(old, new)
        labels = result["Work"]["labels"]
        assert "company" in labels
        assert "valid-label" in labels
        assert "good_label" in labels
        assert "" not in labels
        assert "123invalid" not in labels
        assert 42 not in labels

    def test_new_topic_field_defaults(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        }
        new = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "NewTopic": {"labels": ["widget"]},
        }
        result = self._sanitize(old, new)
        nt = result["NewTopic"]
        assert nt["aliases"] == []
        assert nt["hierarchy"] == {}
        assert nt["active"] is True

    def test_no_changes_passthrough(self):
        config = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        }
        result = self._sanitize(config, dict(config))
        assert result is not None
        assert "Work" in result

    def test_single_deactivation_allowed(self):
        old = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
            "Health": {"active": True, "labels": ["doctor"], "hierarchy": {}, "aliases": []},
            "Education": {"active": True, "labels": ["university"], "hierarchy": {}, "aliases": []},
        }
        new = dict(old)
        new["Health"] = {**old["Health"], "active": False}
        result = self._sanitize(old, new)
        assert result is not None
        assert result["Health"]["active"] is False

    @pytest.mark.asyncio
    async def test_should_run_interval_not_met(self, topic_job, job_context, mock_redis):
        mock_redis.get.return_value = "5"
        assert await topic_job.should_run(job_context) is False

    @pytest.mark.asyncio
    async def test_should_run_buffer_not_empty(self, topic_job, job_context, mock_redis):
        mock_redis.get.return_value = "15"
        mock_redis.llen.return_value = 2
        assert await topic_job.should_run(job_context) is False

    @pytest.mark.asyncio
    async def test_should_run_true(self, topic_job, job_context, mock_redis):
        mock_redis.get.return_value = "15"
        mock_redis.llen.return_value = 0
        assert await topic_job.should_run(job_context) is True

    @pytest.mark.asyncio
    async def test_execute_no_conversation(self, topic_job, job_context, mock_redis):
        mock_redis.zrevrange.return_value = []
        res = await topic_job.execute(job_context)
        assert res.success is True
        assert "No conversation" in res.summary
        mock_redis.set.assert_called_with("heartbeat_counter:test_user:user-123", 0)

    @pytest.mark.asyncio
    @patch("src.jobs.topics.emit")
    async def test_execute_success(self, mock_emit, topic_job, job_context, mock_redis):
        mock_redis.zrevrange.return_value = ["1", "2"]
        mock_redis.hmget.return_value = [
            '{"role": "user", "content": "hello"}',
            '{"role": "agent", "content": "hi"}'
        ]
        topic_job.llm.call_llm.return_value = """```json
{"General": {"active": true, "labels": [], "hierarchy": {}, "aliases": []}, "NewTopic": {"active": true, "labels": ["test"]}}
```"""
        res = await topic_job.execute(job_context)

        assert res.success is True
        topic_job.update_callback.assert_called_once()
        assert mock_emit.call_count == 2