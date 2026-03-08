"""Tests for shared/config/topics_config.py"""

import json

import pytest
from unittest.mock import AsyncMock, MagicMock


from shared.config.topics_config import (
    TopicConfig,
    build_label_block,
    build_topic_alias_lookup,
    get_active_topic_names
)
from shared.services.topics import generate_topics


# --- Shared Fixtures ---

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


# --- Module-Level Helpers ---

class TestBuildLabelBlock:
    """Tests the logic for building the GLiNER label block from configuration."""

    def test_active_topics_included(self, config_dict):
        block = build_label_block(config_dict)
        assert "Work" in block
        assert "company" in block
        assert "project" in block

    def test_identity_excluded(self, config_dict):
        """Identity is always skipped in the label block."""
        block = build_label_block(config_dict)
        assert "Identity" not in block

    def test_inactive_excluded(self, config_dict):
        block = build_label_block(config_dict)
        assert "Cooking" not in block
        assert "recipe" not in block

    def test_empty_labels_topic_excluded(self, config_dict):
        """General has no labels, so it shouldn't appear."""
        block = build_label_block(config_dict)
        assert "General" not in block


class TestBuildTopicAliasLookup:
    """Tests the construction of the topic alias reverse lookup map."""

    def test_canonical_names_mapped(self, config_dict):
        lookup = build_topic_alias_lookup(config_dict)
        assert lookup["general"] == "General"
        assert lookup["work"] == "Work"

    def test_aliases_mapped(self, config_dict):
        lookup = build_topic_alias_lookup(config_dict)
        assert lookup["career"] == "Work"
        assert lookup["job"] == "Work"
        assert lookup["food"] == "Cooking"
        assert lookup["people"] == "Identity"

    def test_case_insensitive(self, config_dict):
        lookup = build_topic_alias_lookup(config_dict)
        assert "work" in lookup
        assert "Work" not in lookup  # all keys are lowercased


class TestGetActiveTopicNames:
    """Tests filtering the topic configuration for active topics."""

    def test_returns_active_only(self, config_dict):
        names = get_active_topic_names(config_dict)
        assert "General" in names
        assert "Work" in names
        assert "Cooking" not in names


# --- TopicConfig Class ---

class TestTopicConfig:
    """Validates the behavior and caching of the TopicConfig class."""

    # ── Properties ──────────────────────────────────────

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
        """A topic with hot=True but active=False should not be hot."""
        tc = TopicConfig({
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Stale": {"active": False, "labels": ["thing"], "hierarchy": {}, "aliases": [], "hot": True},
        })
        assert tc.hot_topics == []

    # ── Lazy caching ────────────────────────────────────

    def test_properties_are_cached(self, topic_config):
        """Repeated access should return the same object (cached)."""
        a = topic_config.alias_lookup
        b = topic_config.alias_lookup
        assert a is b

        a2 = topic_config.active_topics
        b2 = topic_config.active_topics
        assert a2 is b2

    # ── normalize_topic ─────────────────────────────────

    def test_normalize_canonical(self, topic_config):
        assert topic_config.normalize_topic("Work") == "Work"

    def test_normalize_alias(self, topic_config):
        assert topic_config.normalize_topic("career") == "Work"
        assert topic_config.normalize_topic("Job") == "Work"

    def test_normalize_unknown_falls_to_general(self, topic_config):
        assert topic_config.normalize_topic("NonExistent") == "General"

    def test_normalize_unknown_no_general(self):
        """If General is inactive, unknown topics return None."""
        tc = TopicConfig({
            "General": {"active": False, "labels": [], "hierarchy": {}, "aliases": []},
            "Work": {"active": True, "labels": ["company"], "hierarchy": {}, "aliases": []},
        })
        assert tc.normalize_topic("garbage") is None

    def test_normalize_empty(self, topic_config):
        assert topic_config.normalize_topic("") is None
        assert topic_config.normalize_topic(None) is None

    # ── is_active ───────────────────────────────────────

    def test_is_active_true(self, topic_config):
        assert topic_config.is_active("Work") is True

    def test_is_active_false(self, topic_config):
        assert topic_config.is_active("Cooking") is False

    def test_is_active_missing_defaults_true(self, topic_config):
        """A topic not in config at all defaults to active=True per the implementation."""
        assert topic_config.is_active("DoesNotExist") is True

    # ── get_labels_for_topic ────────────────────────────

    def test_get_labels(self, topic_config):
        assert topic_config.get_labels_for_topic("Work") == ["company", "project"]

    def test_get_labels_empty(self, topic_config):
        assert topic_config.get_labels_for_topic("General") == []

    def test_get_labels_missing_topic(self, topic_config):
        assert topic_config.get_labels_for_topic("NoSuchTopic") == []

    # ── update + cache invalidation ─────────────────────

    def test_update_invalidates_cache(self, topic_config):
        old_active = topic_config.active_topics
        assert "Cooking" not in old_active

        topic_config.update({
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Cooking": {"active": True, "labels": ["recipe"], "hierarchy": {}, "aliases": []},
        })

        new_active = topic_config.active_topics
        assert "Cooking" in new_active
        # Cache must be a new object, not the stale one
        assert old_active is not new_active

    def test_update_refreshes_alias_lookup(self, topic_config):
        old_lookup = topic_config.alias_lookup
        assert "newstuff" not in old_lookup

        topic_config.update({
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "NewTopic": {"active": True, "labels": ["widget"], "hierarchy": {}, "aliases": ["newstuff"]},
        })

        assert topic_config.alias_lookup["newstuff"] == "NewTopic"


# --- Topic Generation (LLM Recovery) ---

class TestGenerateTopics:
    """Tests the recovery and parsing of topics generated by the LLM."""

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
        """LLM might emit General/Identity — they should be replaced by system defaults."""
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='{"General": {"labels": ["misc"]}, "Identity": {"labels": ["human"]}, "Work": {"labels": ["company"]}}')

        result = await generate_topics(llm, "test")
        # General/Identity should be system defaults, not LLM's version
        assert result["General"]["labels"] == []
        assert result["Identity"]["labels"] == ["person"]
        assert result["Work"]["labels"] == ["company"]

    @pytest.mark.asyncio
    async def test_caps_at_max_topics(self):
        llm = MagicMock()
        many = {f"Topic{i}": {"labels": [f"label{i}"]} for i in range(10)}
        llm.call_llm = AsyncMock(return_value=json.dumps(many))

        result = await generate_topics(llm, "test", max_topics=3)
        # General + Identity + 3 generated = 5 total
        generated = {k: v for k, v in result.items() if k not in ("General", "Identity")}
        assert len(generated) <= 3

    @pytest.mark.asyncio
    async def test_missing_fields_get_defaults(self):
        """LLM omits aliases/hierarchy — should be backfilled."""
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
        """LLM returns valid JSON followed by a text explanation."""
        llm = MagicMock()
        llm.call_llm = AsyncMock(return_value='```json\n{"Work": {"labels": ["company"]}}\n```\n\nI generated one topic based on your description.')

        result = await generate_topics(llm, "test")
        assert "Work" in result