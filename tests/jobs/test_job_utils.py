"""Tests for jobs/jobs_utils.py — parsers, formatters, and helpers.

Pure logic tests. No async, no mocks needed (except find_duplicate_facts uses numpy).
"""

import pytest
from datetime import datetime, timezone

from jobs.jobs_utils import (
    cosine_similarity,
    extract_fact_with_source,
    find_duplicate_facts,
    format_vp04_input,
    format_vp05_input,
    has_sufficient_facts,
    parse_merge_score,
    parse_new_facts,
    process_extracted_facts,
)
from shared.models.schema.dtypes import Fact, ProfileUpdate


# ── Helpers ─────────────────────────────────────────────

def make_fact(id, content, embedding=None, invalid_at=None):
    return Fact(
        id=id,
        source_entity_id=1,
        content=content,
        valid_at=datetime.now(timezone.utc),
        invalid_at=invalid_at,
        embedding=embedding or [],
    )


# ════════════════════════════════════════════════════════
#  parse_new_facts
# ════════════════════════════════════════════════════════

class TestParseNewFacts:

    def test_happy_path(self):
        reasoning = """
        Some preamble text.
        <new_facts>
        Alice Johnson: Works at Anthropic [MSG_5] | Lives in San Francisco [MSG_5]
        Bob Smith: Studying CS at Stanford [MSG_7]
        </new_facts>
        """
        result = parse_new_facts(reasoning)
        assert len(result) == 2
        assert result[0].canonical_name == "Alice Johnson"
        assert len(result[0].facts) == 2
        assert "Anthropic" in result[0].facts[0]
        assert result[1].canonical_name == "Bob Smith"

    def test_missing_close_tag(self):
        reasoning = """
        <new_facts>
        Alice: Some fact [MSG_1]
        """
        result = parse_new_facts(reasoning)
        assert len(result) == 1
        assert result[0].canonical_name == "Alice"

    def test_no_open_tag(self):
        assert parse_new_facts("No tags here at all") is None

    def test_empty_block(self):
        assert parse_new_facts("<new_facts>\n</new_facts>") is None

    def test_empty_input(self):
        assert parse_new_facts("") is None
        assert parse_new_facts(None) is None

    def test_malformed_lines_skipped(self):
        reasoning = """
        <new_facts>
        This line has no colon
        Alice: Valid fact [MSG_1]
        </new_facts>
        """
        result = parse_new_facts(reasoning)
        assert len(result) == 1
        assert result[0].canonical_name == "Alice"

    def test_multiple_facts_pipe_separated(self):
        reasoning = """
        <new_facts>
        Alice: Fact one [MSG_1] | Fact two [MSG_2] | Fact three [MSG_3]
        </new_facts>
        """
        result = parse_new_facts(reasoning)
        assert len(result[0].facts) == 3

    def test_supersedes_format_preserved(self):
        reasoning = """
        <new_facts>
        Alice: [SUPERSEDES: Works at Google] Works at Anthropic [MSG_5]
        </new_facts>
        """
        result = parse_new_facts(reasoning)
        assert "SUPERSEDES" in result[0].facts[0]
        assert "Anthropic" in result[0].facts[0]


# ════════════════════════════════════════════════════════
#  parse_merge_score
# ════════════════════════════════════════════════════════

class TestParseMergeScore:

    def test_xml_tags(self):
        assert parse_merge_score("<score>0.95</score>") == pytest.approx(0.95)

    def test_xml_with_preamble(self):
        text = "After careful analysis...\n<score>0.42</score>\nSome explanation."
        assert parse_merge_score(text) == pytest.approx(0.42)

    def test_missing_close_tag(self):
        assert parse_merge_score("<score>0.88") == pytest.approx(0.88)

    def test_bare_float(self):
        assert parse_merge_score("0.75") == pytest.approx(0.75)

    def test_bare_float_with_text(self):
        text = "The entities are likely the same.\n0.92\n"
        assert parse_merge_score(text) == pytest.approx(0.92)

    def test_score_1_0(self):
        assert parse_merge_score("<score>1.0</score>") == pytest.approx(1.0)

    def test_score_0_0(self):
        assert parse_merge_score("<score>0.0</score>") == pytest.approx(0.0)

    def test_out_of_range_rejected(self):
        assert parse_merge_score("<score>1.5</score>") is None
        assert parse_merge_score("<score>-0.3</score>") is None

    def test_multi_dot_rejected(self):
        assert parse_merge_score("<score>0.9.5</score>") is None

    def test_non_numeric_rejected(self):
        assert parse_merge_score("<score>high</score>") is None

    def test_empty_input(self):
        assert parse_merge_score("") is None
        assert parse_merge_score(None) is None

    def test_no_score_found(self):
        assert parse_merge_score("I think they might be the same person.") is None


# ════════════════════════════════════════════════════════
#  process_extracted_facts
# ════════════════════════════════════════════════════════

class TestProcessExtractedFacts:

    def test_new_fact_added(self):
        existing = [make_fact("f1", "Works at Google")]
        new = ["Lives in San Francisco [MSG_5]"]

        result = process_extracted_facts(existing, new)
        assert len(result.new_contents) == 1
        assert "San Francisco" in result.new_contents[0]
        assert len(result.to_invalidate) == 0

    def test_supersedes_invalidates_old(self):
        existing = [make_fact("f1", "Works at Google")]
        new = ["[SUPERSEDES: Works at Google] Works at Anthropic [MSG_5]"]

        result = process_extracted_facts(existing, new)
        assert "f1" in result.to_invalidate
        assert len(result.new_contents) == 1
        assert "Anthropic" in result.new_contents[0]

    def test_supersedes_target_not_found(self):
        """If SUPERSEDES target doesn't match any fact, still add new fact."""
        existing = [make_fact("f1", "Works at Google")]
        new = ["[SUPERSEDES: Lives in NYC] Lives in SF [MSG_5]"]

        result = process_extracted_facts(existing, new)
        assert len(result.to_invalidate) == 0
        assert len(result.new_contents) == 1

    def test_invalidates_removes_old(self):
        existing = [make_fact("f1", "Works at Google")]
        new = ["[INVALIDATES: Works at Google] [MSG_5]"]

        result = process_extracted_facts(existing, new)
        assert "f1" in result.to_invalidate
        assert len(result.new_contents) == 0

    def test_duplicate_not_added(self):
        existing = [make_fact("f1", "Works at Google")]
        new = ["Works at Google"]

        result = process_extracted_facts(existing, new)
        assert len(result.new_contents) == 0

    def test_empty_new_facts(self):
        existing = [make_fact("f1", "Works at Google")]
        result = process_extracted_facts(existing, [])
        assert result.to_invalidate == []
        assert result.new_contents == []

    def test_invalidated_facts_ignored(self):
        """Only active facts should be matched against."""
        invalidated = make_fact("f1", "Works at Google",
                               invalid_at=datetime.now(timezone.utc))
        existing = [invalidated]
        new = ["Works at Google"]

        result = process_extracted_facts(existing, new)
        # f1 is already invalidated, so "Works at Google" is treated as new
        assert len(result.new_contents) == 1

    def test_mixed_operations(self):
        existing = [
            make_fact("f1", "Works at Google"),
            make_fact("f2", "Lives in NYC"),
            make_fact("f3", "Has 2 kids"),
        ]
        new = [
            "[SUPERSEDES: Works at Google] Works at Anthropic [MSG_5]",
            "[INVALIDATES: Lives in NYC] [MSG_6]",
            "Plays piano [MSG_7]",
            "Has 2 kids",  # duplicate
        ]

        result = process_extracted_facts(existing, new)
        assert "f1" in result.to_invalidate  # superseded
        assert "f2" in result.to_invalidate  # invalidated
        assert len(result.new_contents) == 2  # Anthropic + piano, not kids


# ════════════════════════════════════════════════════════
#  extract_fact_with_source
# ════════════════════════════════════════════════════════

class TestExtractFactWithSource:

    def test_with_msg_tag(self):
        content, msg_id = extract_fact_with_source("Works at Anthropic [MSG_5]")
        assert content == "Works at Anthropic"
        assert msg_id == 5

    def test_with_msg_underscore_variant(self):
        content, msg_id = extract_fact_with_source("Lives in SF [MSG5]")
        assert content == "Lives in SF"
        assert msg_id == 5

    def test_no_msg_tag(self):
        content, msg_id = extract_fact_with_source("Works at Anthropic")
        assert content == "Works at Anthropic"
        assert msg_id is None

    def test_supersedes_prefix_stripped(self):
        content, msg_id = extract_fact_with_source(
            "[SUPERSEDES: old fact] Works at Anthropic [MSG_5]"
        )
        assert content == "Works at Anthropic"
        assert msg_id == 5

    def test_invalidates_prefix_stripped(self):
        content, msg_id = extract_fact_with_source(
            "[INVALIDATES: old fact] [MSG_5]"
        )
        assert msg_id == 5


# ════════════════════════════════════════════════════════
#  cosine_similarity
# ════════════════════════════════════════════════════════

class TestCosineSimilarity:

    def test_identical_vectors(self):
        vec = [1.0, 0.0, 0.0]
        assert cosine_similarity(vec, vec) == pytest.approx(1.0)

    def test_orthogonal_vectors(self):
        assert cosine_similarity([1, 0, 0], [0, 1, 0]) == pytest.approx(0.0)

    def test_opposite_vectors(self):
        assert cosine_similarity([1, 0], [-1, 0]) == pytest.approx(-1.0)

    def test_empty_vectors(self):
        assert cosine_similarity([], [1, 2]) == 0.0
        assert cosine_similarity([1, 2], []) == 0.0

    def test_none_in_vector(self):
        assert cosine_similarity([1, None], [1, 2]) == 0.0


# ════════════════════════════════════════════════════════
#  has_sufficient_facts
# ════════════════════════════════════════════════════════

class TestHasSufficientFacts:

    def test_both_have_facts(self):
        candidate = {"facts_a": [make_fact("f1", "a")], "facts_b": [make_fact("f2", "b")]}
        assert has_sufficient_facts(candidate) is True

    def test_one_empty(self):
        candidate = {"facts_a": [make_fact("f1", "a")], "facts_b": []}
        assert has_sufficient_facts(candidate) is False

    def test_both_empty(self):
        candidate = {"facts_a": [], "facts_b": []}
        assert has_sufficient_facts(candidate) is False

    def test_missing_keys(self):
        assert has_sufficient_facts({}) is False


# ════════════════════════════════════════════════════════
#  find_duplicate_facts
# ════════════════════════════════════════════════════════

class TestFindDuplicateFacts:

    def test_identical_embeddings_marked(self):
        emb = [0.1, 0.2, 0.3, 0.4]
        facts_a = [make_fact("fa1", "fact A", embedding=emb)]
        facts_b = [make_fact("fb1", "fact B", embedding=emb)]

        result = find_duplicate_facts(facts_a, facts_b, threshold=0.96)
        assert "fb1" in result

    def test_different_embeddings_not_marked(self):
        facts_a = [make_fact("fa1", "fact A", embedding=[1, 0, 0, 0])]
        facts_b = [make_fact("fb1", "fact B", embedding=[0, 0, 0, 1])]

        result = find_duplicate_facts(facts_a, facts_b, threshold=0.96)
        assert result == []

    def test_empty_inputs(self):
        assert find_duplicate_facts([], [make_fact("f1", "a")]) == []
        assert find_duplicate_facts([make_fact("f1", "a")], []) == []
        assert find_duplicate_facts([], []) == []

    def test_invalidated_facts_excluded(self):
        """Only active facts should be compared."""
        emb = [0.1, 0.2, 0.3, 0.4]
        facts_a = [make_fact("fa1", "fact A", embedding=emb)]
        facts_b = [make_fact("fb1", "fact B", embedding=emb,
                             invalid_at=datetime.now(timezone.utc))]

        result = find_duplicate_facts(facts_a, facts_b, threshold=0.96)
        assert result == []

    def test_no_embeddings_excluded(self):
        facts_a = [make_fact("fa1", "fact A", embedding=[])]
        facts_b = [make_fact("fb1", "fact B", embedding=[0.1, 0.2])]

        result = find_duplicate_facts(facts_a, facts_b, threshold=0.96)
        assert result == []


# ════════════════════════════════════════════════════════
#  format_vp04_input / format_vp05_input
# ════════════════════════════════════════════════════════

class TestFormatters:

    def test_vp04_includes_entities_and_conversation(self):
        entities = [
            {"entity_name": "Alice", "entity_type": "person", "existing_facts": [], "known_aliases": ["alice"]}
        ]
        result = format_vp04_input(entities, "[USER]: test conversation")
        assert "Alice" in result
        assert "person" in result
        assert "test conversation" in result

    def test_vp04_empty_entities(self):
        result = format_vp04_input([], "some text")
        assert "some text" in result

    def test_vp05_includes_both_entities(self):
        a = {"canonical_name": "Alice", "type": "person", "aliases": ["alice"], "facts": []}
        b = {"canonical_name": "Alice J", "type": "person", "aliases": ["aj"], "facts": []}
        result = format_vp05_input(a, b)
        assert "Entity A" in result
        assert "Entity B" in result
        assert "Alice" in result
        assert "Alice J" in result

    def test_vp05_with_facts(self):
        a = {
            "canonical_name": "Alice",
            "type": "person",
            "aliases": [],
            "facts": [{"content": "Works at Anthropic", "recorded_at": "2025-01-01T00:00:00+00:00", "source_message": None}],
        }
        b = {"canonical_name": "Alice J", "type": "person", "aliases": [], "facts": []}
        result = format_vp05_input(a, b)
        assert "Anthropic" in result
        assert "2025-01-01" in result