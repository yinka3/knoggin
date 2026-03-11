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

    def test_alias_collision_skips_conflicting_alias(self, resolver):
        """If alias already belongs to another entity, it should be skipped silently."""
        r, store, emb = resolver

        r.register_entity(10, "Alice Johnson", ["Alice Johnson", "alice"], "person", "Identity", "s1")
        r.register_entity(20, "Alice Cooper", ["Alice Cooper", "alice"], "person", "Identity", "s1")

        # "alice" should still point to entity 10 (first registered)
        assert r.get_id("alice") == 10
        # "alice cooper" should point to entity 20
        assert r.get_id("alice cooper") == 20
        # Entity 20 should still be registered with its canonical name
        assert 20 in r.get_profiles()

    def test_canonical_name_collision(self, resolver):
        """Two entities with the same canonical_name lowercase — second one overwrites."""
        r, store, emb = resolver

        r.register_entity(10, "Alice", ["Alice"], "person", "Identity", "s1")
        r.register_entity(20, "ALICE", ["ALICE"], "person", "Identity", "s1")

        # "alice" now points to 20 (last write wins for canonical)
        assert r.get_id("alice") == 20


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

    def test_store_fallback_profile_has_defaults(self, resolver):
        """Profile created from store fallback should have topic='General' and session_id=None."""
        r, store, _ = resolver

        store.get_entities_by_names.return_value = [{
            "id": 50,
            "canonical_name": "New Entity",
            "type": "company",
            "aliases": [],
        }]

        r.get_id("New Entity")

        profile = r.get_profile(50)
        assert profile is not None
        assert profile["topic"] == "General"
        assert profile["session_id"] is None
        assert profile["canonical_name"] == "New Entity"
        assert profile["type"] == "company"


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

    def test_mixed_known_and_unknown(self, hydrated_resolver):
        """Remove list with one known entity and one unknown — should return 1."""
        r, _, _ = hydrated_resolver

        with patch("main.entity_resolve.emit_sync"):
            removed = r.remove_entities([1, 999])

        assert removed == 1
        assert r.get_id("Alice Johnson") is None
        assert r.get_id("Acme Corp") == 2


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






# ════════════════════════════════════════════════════════
#  get_profile
# ════════════════════════════════════════════════════════

class TestGetProfile:

    def test_get_profile_found(self, hydrated_resolver):
        r, _, _ = hydrated_resolver
        profile = r.get_profile(1)
        assert profile["canonical_name"] == "Alice Johnson"
        assert profile["type"] == "person"

    def test_get_profile_not_found(self, resolver):
        r, _, _ = resolver
        assert r.get_profile(999) is None


# ════════════════════════════════════════════════════════
#  compute_embedding
# ════════════════════════════════════════════════════════

class TestComputeEmbedding:

    def test_compute_embedding_updates_profile(self, hydrated_resolver):
        r, _, emb = hydrated_resolver
        new_emb = [0.5] * 1024
        emb.encode_single = AsyncMock(return_value=new_emb)

        result = r.compute_embedding(1, "Alice Johnson. Works at Anthropic.")
        assert result == new_emb
        assert r.get_profile(1)["embedding"] == new_emb

    def test_compute_embedding_unknown_entity(self, resolver):
        r, _, _ = resolver
        result = r.compute_embedding(999, "Nobody")
        assert result == []


# ════════════════════════════════════════════════════════
#  get_embedding_for_id
# ════════════════════════════════════════════════════════

class TestGetEmbeddingForId:

    def test_returns_profile_embedding_if_available(self, hydrated_resolver):
        r, store, _ = hydrated_resolver
        # Hydrated entities get embedding set during register
        # Force an embedding into the profile to test this path
        with r._lock:
            r.entity_profiles[1]["embedding"] = FAKE_EMBEDDING
        result = r.get_embedding_for_id(1)
        assert result == FAKE_EMBEDDING
        store.get_entity_embedding.assert_not_called()

    def test_falls_back_to_store(self, resolver):
        """If profile has no embedding, should call store.get_entity_embedding."""
        r, store, _ = resolver
        with r._lock:
            r.entity_profiles[50] = {
                "canonical_name": "Test",
                "type": "person",
                "topic": "General",
                "session_id": "s1",
            }
        store.get_entity_embedding.return_value = [0.9] * 1024

        result = r.get_embedding_for_id(50)
        assert result == [0.9] * 1024
        store.get_entity_embedding.assert_called_once_with(50)

    def test_no_profile_falls_back_to_store(self, resolver):
        r, store, _ = resolver
        store.get_entity_embedding.return_value = [0.8] * 1024

        result = r.get_embedding_for_id(999)
        assert result == [0.8] * 1024
        store.get_entity_embedding.assert_called_once_with(999)


# ════════════════════════════════════════════════════════
#  update_settings
# ════════════════════════════════════════════════════════

class TestUpdateSettings:

    def test_updates_all_fields(self, resolver):
        r, _, _ = resolver
        r.update_settings(
            fuzzy_substring_threshold=80,
            fuzzy_non_substring_threshold=95,
            generic_token_freq=5,
            candidate_fuzzy_threshold=90,
            candidate_vector_threshold=0.90,
        )
        assert r.fuzzy_substring_threshold == 80
        assert r.fuzzy_non_substring_threshold == 95
        assert r.generic_token_freq == 5
        assert r.candidate_fuzzy_threshold == 90
        assert r.candidate_vector_threshold == 0.90

    def test_partial_update(self, resolver):
        r, _, _ = resolver
        original_non_sub = r.fuzzy_non_substring_threshold

        r.update_settings(fuzzy_substring_threshold=80)

        assert r.fuzzy_substring_threshold == 80
        assert r.fuzzy_non_substring_threshold == original_non_sub

    def test_none_values_ignored(self, resolver):
        r, _, _ = resolver
        original = r.fuzzy_substring_threshold

        r.update_settings(fuzzy_substring_threshold=None)
        assert r.fuzzy_substring_threshold == original





# ════════════════════════════════════════════════════════
#  compute_batch_embeddings
# ════════════════════════════════════════════════════════

class TestComputeBatchEmbeddings:

    def test_compute_batch_embeddings_empty(self, resolver):
        """Empty list should return empty list."""
        r, _, _ = resolver
        result = r.compute_batch_embeddings([])
        assert result == []

    def test_compute_batch_embeddings_success(self, resolver):
        """Valid list of texts should return list of embeddings."""
        r, _, emb = resolver
        expected = [[0.1] * 1024, [0.2] * 1024]
        emb.encode = AsyncMock(return_value=expected)

        result = r.compute_batch_embeddings(["text 1", "text 2"])
        assert result == expected
        emb.encode.assert_called_once_with(["text 1", "text 2"])


# ════════════════════════════════════════════════════════
#  detect_merge_entity_candidates
# ════════════════════════════════════════════════════════

class TestDetectMergeEntityCandidates:

    @patch("main.entity_resolve.EntityResolver._collect_candidate_pairs")
    @patch("main.entity_resolve.EntityResolver._classify_pair")
    def test_detect_merge_entity_candidates_no_targets(self, mock_classify, mock_collect, resolver):
        """Should return empty list if no valid targets."""
        r, _, _ = resolver
        # Resolver has no profiles by default
        assert r.detect_merge_entity_candidates({99}) == []
        mock_collect.assert_not_called()

    @patch("main.entity_resolve.EntityResolver._collect_candidate_pairs")
    @patch("main.entity_resolve.EntityResolver._classify_pair")
    def test_detect_merge_entity_candidates_no_pairs(self, mock_classify, mock_collect, hydrated_resolver):
        """Should return empty list if _collect_candidate_pairs returns empty."""
        r, _, _ = hydrated_resolver
        mock_collect.return_value = {}

        result = r.detect_merge_entity_candidates()
        assert result == []
        mock_collect.assert_called_once()
        mock_classify.assert_not_called()

    @patch("main.entity_resolve.EntityResolver._collect_candidate_pairs")
    @patch("main.entity_resolve.EntityResolver._classify_pair")
    def test_detect_merge_entity_candidates_success(self, mock_classify, mock_collect, hydrated_resolver):
        """Should query store for facts and return classified pairs."""
        r, store, _ = hydrated_resolver
        mock_collect.return_value = {(1, 2): 95}
        
        # Mock store facts and classification
        store.get_facts_for_entities.return_value = {1: [], 2: []}
        mock_classify.return_value = {"primary_id": 1, "secondary_id": 2, "fuzz_score": 95}

        result = r.detect_merge_entity_candidates({1, 2})
        
        assert len(result) == 1
        assert result[0] == {"primary_id": 1, "secondary_id": 2, "fuzz_score": 95}
        store.get_facts_for_entities.assert_called_once()
        # The exact order of list(set([1, 2])) might vary, but both elements are there.
        called_args = store.get_facts_for_entities.call_args[0][0]
        assert set(called_args) == {1, 2}
        mock_classify.assert_called_once()