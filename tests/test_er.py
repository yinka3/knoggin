"""Tests for main/entity_resolve.py — Chunk 1: Index management.

This module focuses on testing the internal indexing and management logic of the EntityResolver class.
It includes tests for entity registration, profile management, alias handling, and hydration.
All store and embedding services are mocked.
"""

from unittest.mock import MagicMock, AsyncMock, patch
import pytest
from main.entity_resolve import EntityResolver


# --- Fixtures and Helpers ---

FAKE_EMBEDDING = [0.1] * 1024


def make_mock_store(hydration_data: list[dict] = None) -> MagicMock:
    """Create a mock MemGraphStore with configurable hydration data."""
    store = MagicMock()
    store.get_all_entities_for_hydration.return_value = hydration_data or []
    store.get_entities_by_names.return_value = []
    store.get_entity_embedding.return_value = FAKE_EMBEDDING
    store.search_entities_by_embedding.return_value = []
    return store


def make_mock_embedding() -> MagicMock:
    """Create a mock EmbeddingService with async encoding methods."""
    svc = MagicMock()
    svc.encode_single = AsyncMock(return_value=FAKE_EMBEDDING)
    svc.encode = AsyncMock(return_value=[FAKE_EMBEDDING])
    return svc


@pytest.fixture
def resolver():
    """Fixture providing a fresh resolver with an empty store."""
    store = make_mock_store()
    embedding = make_mock_embedding()

    with patch("main.entity_resolve.emit_sync"):
        r = EntityResolver(
            store=store,
            embedding_service=embedding,
            session_id="test-session",
        )
    return r, store, embedding


@pytest.fixture
def hydrated_resolver():
    """Fixture providing a resolver pre-loaded with sample entities."""
    hydration_data = [
        {
            "id": 1,
            "canonical_name": "Alice Johnson",
            "aliases": ["alice", "alice j"],
            "type": "person",
            "topic": "Identity",
            "session_id": "s1",
        },
        {
            "id": 2,
            "canonical_name": "Acme Corp",
            "aliases": ["acme"],
            "type": "company",
            "topic": "Work",
            "session_id": "s1",
        },
    ]
    store = make_mock_store(hydration_data)
    embedding = make_mock_embedding()

    with patch("main.entity_resolve.emit_sync"):
        r = EntityResolver(
            store=store,
            embedding_service=embedding,
            session_id="test-session",
        )
    return r, store, embedding


class TestRegisterEntity:
    """Tests for the register_entity method."""

    def test_register_populates_all_indexes(self, resolver):
        """Verify that registration correctly populates name, profile, and mention indexes."""
        r, store, emb = resolver

        r.register_entity(
            entity_id=10,
            canonical_name="Bob Smith",
            mentions=["Bob Smith", "Bobby"],
            entity_type="person",
            topic="Identity",
            session_id="s1",
        )

        # name_to_id
        assert r.get_id("Bob Smith") == 10
        assert r.get_id("Bobby") == 10

        # entity_profiles
        profiles = r.get_profiles()
        assert 10 in profiles
        assert profiles[10]["canonical_name"] == "Bob Smith"
        assert profiles[10]["type"] == "person"
        assert profiles[10]["topic"] == "Identity"

        # id_to_names
        mentions = r.get_mentions_for_id(10)
        assert "bob smith" in mentions
        assert "bobby" in mentions

    def test_register_with_context_embeds_context(self, resolver):
        """Context should be included in the text sent for embedding."""
        r, store, emb = resolver

        r.register_entity(
            entity_id=11,
            canonical_name="Eve",
            mentions=["Eve"],
            entity_type="person",
            topic="Identity",
            source_context="She works at Anthropic",
        )

        call_args = emb.encode_single.call_args[0][0]
        assert "Eve" in call_args
        assert "Anthropic" in call_args

    def test_register_without_context(self, resolver):
        """Standard registration should embed just the name and type if no context is provided."""
        r, store, emb = resolver

        r.register_entity(
            entity_id=12,
            canonical_name="Charlie",
            mentions=["Charlie"],
            entity_type="person",
            topic="Identity",
        )

        call_args = emb.encode_single.call_args[0][0]
        assert "Charlie" in call_args
        assert "person" in call_args


class TestGetId:
    """Tests for the get_id method."""

    def test_cache_hit(self, hydrated_resolver):
        """Pre-hydrated entities should be found in the local cache, avoiding store calls."""
        r, store, _ = hydrated_resolver

        result = r.get_id("Alice Johnson")
        assert result == 1
        store.get_entities_by_names.assert_not_called()

    def test_alias_cache_hit(self, hydrated_resolver):
        """Aliases should also be resolvable via the cache."""
        r, store, _ = hydrated_resolver

        assert r.get_id("alice") == 1
        store.get_entities_by_names.assert_not_called()

    def test_store_fallback(self, resolver):
        """If an entity is not in cache, the resolver should fallback to searching in the store."""
        r, store, _ = resolver

        store.get_entities_by_names.return_value = [{
            "id": 99,
            "canonical_name": "New Person",
            "type": "person",
            "aliases": ["np"],
        }]

        result = r.get_id("New Person")
        assert result == 99
        store.get_entities_by_names.assert_called_once()

        # Now should be cached
        store.get_entities_by_names.reset_mock()
        assert r.get_id("New Person") == 99
        store.get_entities_by_names.assert_not_called()

        # Alias from store should also be cached
        assert r.get_id("np") == 99

    def test_not_found(self, resolver):
        """Return None if entity is not in cache or store."""
        r, store, _ = resolver

        store.get_entities_by_names.return_value = []
        assert r.get_id("Nobody") is None

    def test_empty_name(self, resolver):
        """Handle empty or None names gracefully."""
        r, _, _ = resolver
        assert r.get_id("") is None
        assert r.get_id(None) is None


class TestValidateAndCommit:
    """Tests for validate_existing and commit_new_aliases."""

    def test_validate_found_with_new_aliases(self, hydrated_resolver):
        """Detect when an entity exists but some provided names are unknown as aliases."""
        r, _, _ = hydrated_resolver

        eid, has_new, new_aliases = r.validate_existing("Alice Johnson", ["Alice Johnson", "AJ"])
        assert eid == 1
        assert has_new is True
        assert "AJ" in new_aliases
        assert "Alice Johnson" not in new_aliases

    def test_validate_found_no_new_aliases(self, hydrated_resolver):
        """Detect when an entity exists and all provided names are already known."""
        r, _, _ = hydrated_resolver

        eid, has_new, new_aliases = r.validate_existing("Alice Johnson", ["alice"])
        assert eid == 1
        assert has_new is False
        assert new_aliases == []

    def test_validate_not_found(self, resolver):
        """Return None if entity name is entirely unknown."""
        r, _, _ = resolver

        eid, has_new, new_aliases = r.validate_existing("Unknown Person", ["Unknown"])
        assert eid is None
        assert has_new is False

    def test_validate_empty_name(self, resolver):
        """Handle validation for empty input."""
        r, _, _ = resolver

        eid, _, _ = r.validate_existing("", [])
        assert eid is None

    def test_commit_aliases_registers_in_indexes(self, hydrated_resolver):
        """Committing aliases should update both name-to-id and id-to-names indexes."""
        r, _, _ = hydrated_resolver

        assert r.get_id("AJ") is None

        r.commit_new_aliases(1, ["AJ"])

        assert r.get_id("AJ") == 1
        assert "aj" in r.get_mentions_for_id(1)

    def test_commit_aliases_nonexistent_entity(self, resolver):
        """Committing aliases for an entity not in profiles should be a no-op."""
        r, _, _ = resolver

        r.commit_new_aliases(999, ["ghost"])
        assert r.get_id("ghost") is None


class TestRemoveEntities:
    """Tests for entity removal logic."""

    def test_remove_clears_all_indexes(self, hydrated_resolver):
        """Removing an entity should clear it from all internal indexes."""
        r, _, _ = hydrated_resolver

        assert r.get_id("Alice Johnson") == 1

        with patch("main.entity_resolve.emit_sync"):
            removed = r.remove_entities([1])

        assert removed == 1
        assert r.get_id("Alice Johnson") is None
        assert r.get_id("alice") is None
        assert r.get_id("alice j") is None
        assert 1 not in r.get_profiles()
        assert r.get_mentions_for_id(1) == []

    def test_remove_preserves_other_entities(self, hydrated_resolver):
        """Removing one entity should not affect others in the index."""
        r, _, _ = hydrated_resolver

        with patch("main.entity_resolve.emit_sync"):
            r.remove_entities([1])

        assert r.get_id("Acme Corp") == 2
        assert 2 in r.get_profiles()

    def test_remove_empty_list(self, resolver):
        """Handling of an empty removal list."""
        r, _, _ = resolver
        assert r.remove_entities([]) == 0

    def test_remove_nonexistent(self, resolver):
        """Handling of removal requests for unknown entity IDs."""
        r, _, _ = resolver

        with patch("main.entity_resolve.emit_sync"):
            assert r.remove_entities([999]) == 0


class TestHydration:
    """Tests for the initial hydration process from the database store."""

    def test_hydrate_populates_indexes(self, hydrated_resolver):
        """Verify that all entities and aliases from hydration data correctly populate indexes."""
        r, _, _ = hydrated_resolver

        assert r.get_id("Alice Johnson") == 1
        assert r.get_id("alice") == 1
        assert r.get_id("alice j") == 1
        assert r.get_id("Acme Corp") == 2
        assert r.get_id("acme") == 2

        profiles = r.get_profiles()
        assert len(profiles) == 2
        assert profiles[1]["canonical_name"] == "Alice Johnson"
        assert profiles[2]["type"] == "company"

        alice_mentions = r.get_mentions_for_id(1)
        assert "alice johnson" in alice_mentions
        assert "alice" in alice_mentions
        assert "alice j" in alice_mentions

    def test_hydrate_empty_store(self, resolver):
        """Resolver should be initialized empty if the store provides no records."""
        r, _, _ = resolver

        assert r.get_profiles() == {}
        assert r.get_known_aliases() == {}


class TestSnapshotIsolation:
    """Tests for ensuring results returned from getters are copies, protecting internal state."""

    def test_get_known_aliases_returns_copy(self, hydrated_resolver):
        """Verify get_known_aliases returns a copy."""
        r, _, _ = hydrated_resolver

        aliases_a = r.get_known_aliases()
        aliases_b = r.get_known_aliases()
        assert aliases_a is not aliases_b
        assert aliases_a == aliases_b

    def test_get_profiles_returns_copy(self, hydrated_resolver):
        """Verify get_profiles returns a copy."""
        r, _, _ = hydrated_resolver

        profiles_a = r.get_profiles()
        profiles_b = r.get_profiles()
        assert profiles_a is not profiles_b

        profiles_a[999] = {"canonical_name": "Injected"}
        assert 999 not in r.get_profiles()