"""Tests for main/entity_resolve.py — Chunk 2: Candidate scoring & merge.

This module tests the scoring, merging, and candidate discovery logic of the EntityResolver.
It uses real fuzzy matching (rapidfuzz) and mocked store/embedding services.
Resolver indexes are populated directly to test scoring logic in isolation.
"""

from unittest.mock import MagicMock, patch
import pytest
from main.entity_resolve import EntityResolver


# --- Helpers and Fixtures ---

FAKE_EMBEDDING = [0.1] * 1024


def make_mock_store() -> MagicMock:
    """Create a mock MemGraphStore with common response patterns."""
    store = MagicMock()
    store.get_all_entities_for_hydration.return_value = []
    store.get_entities_by_names.return_value = []
    store.get_entity_embedding.return_value = FAKE_EMBEDDING
    store.search_entities_by_embedding.return_value = []
    store.search_similar_entities.return_value = []
    store.has_direct_edge.return_value = False
    store.has_hierarchy_edge.return_value = False
    store.get_neighbor_ids.return_value = set()
    return store


def populate_entity(resolver: EntityResolver, eid: int, canonical: str, aliases: list[str], etype: str = "person", topic: str = "General"):
    """Directly inject an entity into resolver indexes to bypass async registration for scoring tests."""
    with resolver._lock:
        resolver.entity_profiles[eid] = {
            "canonical_name": canonical,
            "type": etype,
            "topic": topic,
            "session_id": "test",
        }
        all_names = [canonical] + aliases
        for name in all_names:
            resolver._name_to_id[name.lower()] = eid
            if eid not in resolver._id_to_names:
                resolver._id_to_names[eid] = set()
            resolver._id_to_names[eid].add(name.lower())


@pytest.fixture
def resolver():
    """Fixture providing a fresh EntityResolver with mocked dependencies."""
    store = make_mock_store()
    embedding = MagicMock()

    with patch("main.entity_resolve.emit_sync"):
        r = EntityResolver(
            store=store,
            embedding_service=embedding,
            session_id="test-session",
        )
    return r, store


@pytest.fixture
def populated_resolver(resolver):
    """Fixture providing a resolver pre-loaded with a variety of entity profiles for scoring evaluation."""
    r, store = resolver

    populate_entity(r, 1, "Alice Johnson", ["alice", "alice j"])
    populate_entity(r, 2, "Bob Smith", ["bob", "bobby"])
    populate_entity(r, 3, "Acme Corp", ["acme"], etype="company", topic="Work")
    populate_entity(r, 4, "Acme Corporation", ["acme corp"], etype="company", topic="Work")
    populate_entity(r, 5, "Google", [], etype="company", topic="Work")
    populate_entity(r, 6, "Alice Cooper", ["alice c"])
    populate_entity(r, 7, "Dr. Alice Johnson", ["dr. alice", "dr alice johnson"])

    return r, store


class TestGetCandidateIds:
    """Tests for the candidate discovery and scoring algorithm in get_candidate_ids."""

    def test_exact_match_scores_one(self, populated_resolver):
        """Exact canonical name match should return a score of 1.0."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Alice Johnson", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[1] == 1.0

    def test_exact_alias_match(self, populated_resolver):
        """Exact alias match should return a score of 1.0."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("bobby", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[2] == 1.0

    def test_typo_matches_fuzzy(self, populated_resolver):
        """Minor typos should be caught by fuzzy matching with a high score."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Alice Jonson", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 1 in scores
        assert scores[1] >= 0.85

    def test_first_name_substring_matches(self, populated_resolver):
        """Common first names defined as aliases should be resolvable."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Bob", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[2] == 1.0

    def test_unrelated_name_no_fuzzy_match(self, populated_resolver):
        """Names with low similarity should not return candidates above the default threshold."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Robert", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 2 not in scores

    def test_below_threshold_excluded(self, populated_resolver):
        """Verify that the default fuzzy similarity threshold is respected."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Zachary", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert len(scores) == 0

    def test_vector_search_adds_candidates(self, populated_resolver):
        """Vector search should surface candidates based on semantic similarity even if string similarity is low."""
        r, store = populated_resolver
        store.search_entities_by_embedding.return_value = [(5, 0.92)]

        results = r.get_candidate_ids("search engine company", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 5 in scores
        assert scores[5] == 0.92

    def test_combined_fuzzy_and_vector_max_wins(self, populated_resolver):
        """When multiple scoring methods find the same entity, the highest score should be kept."""
        r, store = populated_resolver
        store.search_entities_by_embedding.return_value = [(1, 0.88)]

        results = r.get_candidate_ids("Alice Johnson", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[1] == 1.0

    def test_vector_beats_fuzzy_when_higher(self, populated_resolver):
        """Semantic vector score should override fuzzy score if it's higher."""
        r, store = populated_resolver
        store.search_entities_by_embedding.return_value = [(5, 0.95)]

        results = r.get_candidate_ids("tech giant", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[5] == 0.95

    def test_results_sorted_descending(self, populated_resolver):
        """Candidate IDs should be returned sorted by score in descending order."""
        r, store = populated_resolver
        store.search_entities_by_embedding.return_value = [(5, 0.87)]

        results = r.get_candidate_ids("Alice Johnson", precomputed_embedding=FAKE_EMBEDDING)
        scores_list = [score for _, score in results]
        assert scores_list == sorted(scores_list, reverse=True)

    def test_empty_mention(self, populated_resolver):
        """Empty mentions should return no candidates."""
        r, _ = populated_resolver
        assert r.get_candidate_ids("", precomputed_embedding=FAKE_EMBEDDING) == []

    def test_case_insensitive(self, populated_resolver):
        """Matching should be case-insensitive."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("ALICE JOHNSON", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 1 in scores

    def test_multiple_aliases_best_score_wins(self, populated_resolver):
        """An entity with multiple aliases should be matched against all of them, keeping the best score."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("bob", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[2] == 1.0


class TestResolveEntityName:
    """Tests for the resolve_entity_name high-level API."""

    def test_resolves_to_canonical(self, populated_resolver):
        """Resolve an alias to its canonical name."""
        r, _ = populated_resolver
        r.get_candidate_ids = lambda mention, **kw: [(1, 1.0)]
        assert r.resolve_entity_name("alice") == "Alice Johnson"

    def test_no_match_returns_none(self, populated_resolver):
        """Return None if no entity can be resolved."""
        r, _ = populated_resolver
        r.get_candidate_ids = lambda mention, **kw: []
        assert r.resolve_entity_name("Nobody At All") is None

    def test_resolves_via_fuzzy(self, populated_resolver):
        """Resolve a name variant using fuzzy logic to the canonical name."""
        r, _ = populated_resolver
        r.get_candidate_ids = lambda mention, **kw: [(2, 0.91)]
        assert r.resolve_entity_name("Bobby S") == "Bob Smith"


class TestMergeEntities:
    """Tests for merging two entities together."""

    def test_merge_transfers_aliases(self, populated_resolver):
        """Merging should move all aliases from the secondary entity to the primary one."""
        r, _ = populated_resolver

        with patch("main.entity_resolve.emit_sync"):
            r.merge_into(primary_id=1, secondary_id=6)

        assert r.get_id("alice cooper") == 1
        assert r.get_id("alice c") == 1

    def test_merge_removes_secondary_profile(self, populated_resolver):
        """The secondary entity profile should be deleted after a successful merge."""
        r, _ = populated_resolver

        with patch("main.entity_resolve.emit_sync"):
            r.merge_into(primary_id=1, secondary_id=6)

        assert 6 not in r.get_profiles()

    def test_merge_preserves_primary(self, populated_resolver):
        """The primary entity profile should remain intact after a merge."""
        r, _ = populated_resolver

        with patch("main.entity_resolve.emit_sync"):
            r.merge_into(primary_id=1, secondary_id=6)

        assert r.get_id("Alice Johnson") == 1
        profiles = r.get_profiles()
        assert profiles[1]["canonical_name"] == "Alice Johnson"

    def test_merge_primary_gets_all_secondary_names(self, populated_resolver):
        """Primary entity should aggregate all names and mentions from both entities."""
        r, _ = populated_resolver

        with patch("main.entity_resolve.emit_sync"):
            r.merge_into(primary_id=1, secondary_id=6)

        mentions = set(r.get_mentions_for_id(1))
        assert "alice johnson" in mentions
        assert "alice" in mentions
        assert "alice cooper" in mentions
        assert "alice c" in mentions


class TestNameVariations:
    """Stress tests for various natural language name variations using real fuzzy scoring defaults."""

    def test_dr_prefix_matches(self, populated_resolver):
        """Prefixes like 'Dr.' should still allow a match with the base name."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Dr. Alice Johnson", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 7 in scores
        assert 1 in scores

    def test_first_name_only_matches_multiple(self, populated_resolver):
        """First name queries should hit entities where that name is defined as an alias."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("alice", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 1 in scores
        assert scores[1] == 1.0

    def test_initials_do_not_fuzzy_match(self, populated_resolver):
        """Initials (e.g., 'AJ') should not fuzzy match full names at default thresholds."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("AJ", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 1 not in scores

    def test_acme_variants_cross_match(self, populated_resolver):
        """Verify cross-resolution of similar company names/aliases."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Acme Corp", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 4 in scores
        assert scores[4] == 1.0
        assert 3 in scores

    def test_short_string_uses_strict_ratio(self, populated_resolver):
        """Very short strings should use strict matching to avoid false positives."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("MIT", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert len(scores) == 0

    def test_same_first_name_different_people(self, populated_resolver):
        """Disambiguation: Ranking should prioritize the entity that matches more of the mention string."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Alice Johnson", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores.get(1, 0) > scores.get(6, 0)

    def test_typo_in_company_name(self, populated_resolver):
        """Typo-resilience for company names."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Gogle", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        # Assuming WRatio('gogle', 'google') >= 85
        assert 5 in scores

    def test_name_reordering(self, populated_resolver):
        """Handle reordered names (e.g., 'Last, First')."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Johnson, Alice", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert 1 in scores
        assert scores[1] >= 0.85

    def test_partial_company_name(self, populated_resolver):
        """Match on partial company name defined as an alias."""
        r, _ = populated_resolver
        results = r.get_candidate_ids("Acme", precomputed_embedding=FAKE_EMBEDDING)
        scores = dict(results)
        assert scores[3] == 1.0


class TestAliasCollisions:
    """Tests for detecting and managing alias collisions (different entities sharing the same name)."""

    def test_detects_shared_alias(self):
        """Shared aliases between two entities should be flagged as a collision."""
        store = make_mock_store()
        embedding = MagicMock()

        with patch("main.entity_resolve.emit_sync"):
            r = EntityResolver(store=store, embedding_service=embedding, session_id="t")

        populate_entity(r, 1, "Alice Johnson", ["alice"])
        populate_entity(r, 2, "Alice Cooper", ["alice"])

        collisions = r.find_alias_collisions_targeted({1, 2})
        pair = tuple(sorted((1, 2)))
        assert pair in collisions

    def test_no_collision(self, populated_resolver):
        """Unrelated entities should not result in collisions."""
        r, _ = populated_resolver

        collisions = r.find_alias_collisions_targeted({2, 5})
        assert collisions == []