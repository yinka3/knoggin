"""
Tests for main/utils.py.

This module validates various NLP utility functions including XML parsing,
entity validation, and string matching logic.
"""

import pytest


from main.utils import (
    extract_xml_content,
    is_covered,
    is_generic_phrase,
    is_substring_match,
    parse_connection_response,
    parse_entities,
    validate_entity,
)
from shared.config.topics_config import TopicConfig
from shared.services.topics import _strip_code_fences


# --- Shared Fixtures ---

@pytest.fixture
def topic_config():
    """Minimal TopicConfig used across multiple test classes."""
    return TopicConfig({
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
        "Work": {"active": True, "labels": ["company", "project"], "hierarchy": {}, "aliases": ["career", "job"]},
        "Cooking": {"active": False, "labels": ["recipe"], "hierarchy": {}, "aliases": []},
    })


# --- XML Extraction ---

class TestExtractXmlContent:
    """Tests the semi-robust XML content extractor for LLM responses."""

    def test_happy_path(self):
        text = "Some preamble <entities>1 | Alice | person | General | 0.9</entities> trailing"
        result = extract_xml_content(text, "entities")
        assert result == "1 | Alice | person | General | 0.9"

    def test_missing_close_tag(self):
        """Should capture everything after the open tag."""
        text = "<entities>1 | Bob | person | General | 0.85\n2 | Eve | person | General | 0.9"
        result = extract_xml_content(text, "entities")
        assert "Bob" in result
        assert "Eve" in result

    def test_no_open_tag(self):
        assert extract_xml_content("no tags here", "entities") is None

    def test_case_insensitive(self):
        text = "<ENTITIES>data</ENTITIES>"
        assert extract_xml_content(text, "entities") == "data"

    def test_empty_input(self):
        assert extract_xml_content("", "entities") is None
        assert extract_xml_content(None, "entities") is None

    def test_empty_block(self):
        text = "<entities></entities>"
        assert extract_xml_content(text, "entities") == ""

    def test_nested_content_preserved(self):
        """Content between tags should not be stripped of internal structure."""
        text = "<facts>\n  <fact subject=\"Alice\">Works at Anthropic</fact>\n</facts>"
        result = extract_xml_content(text, "facts")
        assert "<fact subject=" in result


# --- Entity Parsing ---

class TestParseEntities:
    """Tests parsing extracted entity blocks from LLM output."""

    def test_five_field_format(self, topic_config):
        reasoning = """
        <entities>
        1 | Alice Johnson | person | Identity | 0.95
        2 | Acme Corp | company | Work | 0.88
        </entities>
        """
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 2
        assert result[0].name == "Alice Johnson"
        assert result[0].label == "person"
        assert result[0].topic == "Identity"
        assert result[0].msg_id == 1
        assert result[1].name == "Acme Corp"
        assert result[1].confidence == 0.88

    def test_four_field_fallback_topic_general(self, topic_config):
        """4 fields with field3 not a known topic alias -> label=field3, topic=General."""
        reasoning = "<entities>\n1 | Bob | person | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].label == "person"
        assert result[0].topic == "General"

    def test_four_field_fallback_topic_inferred(self, topic_config):
        """4 fields with field3 matching a topic alias -> label='', topic=field3."""
        reasoning = "<entities>\n1 | Acme Corp | career | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].label == ""
        assert result[0].topic == "career"

    def test_below_confidence_filtered(self, topic_config):
        reasoning = """
        <entities>
        1 | Maybe Entity | thing | General | 0.5
        2 | Definite Entity | person | Identity | 0.95
        </entities>
        """
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].name == "Definite Entity"

    def test_all_below_confidence_returns_none(self, topic_config):
        reasoning = "<entities>\n1 | Weak | thing | General | 0.3\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_malformed_lines_skipped(self, topic_config):
        reasoning = """
        <entities>
        this is garbage
        1 | Alice | person | Identity | 0.9
        just two | fields
        </entities>
        """
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].name == "Alice"

    def test_header_row_skipped(self, topic_config):
        reasoning = """
        <entities>
        msg_id | name | label | topic | confidence
        1 | Alice | person | Identity | 0.9
        </entities>
        """
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1

    def test_msg_prefix_stripped(self, topic_config):
        """LLMs sometimes emit 'MSG 1' or 'msg 1' instead of just '1'."""
        reasoning = "<entities>\nMSG 3 | Alice | person | Identity | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result[0].msg_id == 3

    def test_no_entities_block(self):
        assert parse_entities("No XML here at all") is None

    def test_none_topic_config(self):
        """topic_config=None should not crash; 4-field lines default to General."""
        reasoning = "<entities>\n1 | Alice | person | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=None)
        assert len(result) == 1
        assert result[0].topic == "General"

    def test_empty_name_skipped(self, topic_config):
        reasoning = "<entities>\n1 |  | person | Identity | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None


# --- Connection Parsing ---

class TestParseConnectionResponse:
    """Tests parsing message-to-entity connection responses from the LLM."""

    def test_happy_path(self):
        text = """
        <connections>
        MSG 1 | Alice; Bob | 0.9 | They work together
        MSG 2 | Eve; Charlie | 0.85 | Met at conference
        </connections>
        """
        result = parse_connection_response(text)
        assert len(result) == 2

        by_msg = {mc.message_id: mc for mc in result}
        assert 1 in by_msg
        assert by_msg[1].entity_pairs[0].entity_a == "Alice"
        assert by_msg[1].entity_pairs[0].entity_b == "Bob"
        assert by_msg[1].entity_pairs[0].confidence == 0.9
        assert by_msg[1].entity_pairs[0].context == "They work together"

    def test_no_connections_line(self):
        text = "<connections>\nMSG 1 | NO CONNECTIONS\n</connections>"
        result = parse_connection_response(text)
        assert result == []

    def test_no_connections_block(self):
        result = parse_connection_response("No XML here")
        assert result == []

    def test_malformed_lines_skipped(self):
        text = """
        <connections>
        garbage line
        MSG 1 | Alice; Bob | 0.9 | colleagues
        only two fields | here
        </connections>
        """
        result = parse_connection_response(text)
        assert len(result) == 1
        assert result[0].entity_pairs[0].entity_a == "Alice"

    def test_confidence_clamped(self):
        """Confidence outside [0, 1] should be clamped."""
        text = "<connections>\nMSG 1 | A; B | 1.5 | reason\n</connections>"
        result = parse_connection_response(text)
        assert result[0].entity_pairs[0].confidence == 1.0

    def test_invalid_confidence_defaults(self):
        """Non-numeric confidence should default to 0.8."""
        text = "<connections>\nMSG 1 | A; B | high | reason\n</connections>"
        result = parse_connection_response(text)
        assert result[0].entity_pairs[0].confidence == 0.8

    def test_multiple_pairs_same_message(self):
        text = """
        <connections>
        MSG 5 | Alice; Bob | 0.9 | friends
        MSG 5 | Alice; Eve | 0.7 | roommates
        </connections>
        """
        result = parse_connection_response(text)
        assert len(result) == 1
        assert len(result[0].entity_pairs) == 2


# --- Entity Validation ---

class TestValidateEntity:
    """Validates entity names and labels against configuration rules."""

    def test_valid_entity(self, topic_config):
        assert validate_entity("Alice Johnson", "Identity", topic_config) is True

    def test_valid_general_topic(self, topic_config):
        assert validate_entity("Alice Johnson", "General", topic_config) is True

    def test_empty_name(self, topic_config):
        assert validate_entity("", "General", topic_config) is False

    def test_short_name(self, topic_config):
        assert validate_entity("A", "General", topic_config) is False

    def test_long_name(self, topic_config):
        assert validate_entity("x" * 101, "General", topic_config) is False

    def test_pronoun_rejected(self, topic_config):
        assert validate_entity("they", "General", topic_config) is False

    def test_numeric_only(self, topic_config):
        assert validate_entity("12345", "General", topic_config) is False

    def test_invalid_topic(self, topic_config):
        """Topic that doesn't exist and isn't a known alias should reject."""
        assert validate_entity("Alice", "NonExistent", topic_config) is False

    def test_inactive_topic_via_alias(self, topic_config):
        """'Cooking' is inactive — but validate_entity checks topic normalization, not active state."""
        # normalize_topic("Cooking") -> "Cooking" (exact match in alias_lookup)
        # so it doesn't fall to General, meaning it passes the normalize check
        result = validate_entity("Gordon Ramsay", "Cooking", topic_config)
        assert result is True

    def test_none_topic(self, topic_config):
        """topic=None should skip the topic validation branch."""
        assert validate_entity("Adeyinka", None, topic_config) is True

    def test_person_label_bypasses_generic_filter(self, topic_config):
        """Common first names like 'Alice' should pass when label='person'."""
        assert validate_entity("Alice", "General", topic_config, label="person") is True

    def test_common_name_without_person_label_filtered(self, topic_config):
        """Same common name without person label should be caught by generic filter."""
        assert validate_entity("Alice", "General", topic_config) is False


# --- Generic Phrase Filtering ---

class TestIsGenericPhrase:
    """Tests the logic for identifying and filtering common, generic phrases."""

    def test_proper_noun_passes(self):
        assert is_generic_phrase("Anthropic") is False

    def test_common_single_word_filtered(self):
        assert is_generic_phrase("the") is True
        assert is_generic_phrase("good") is True

    def test_rare_word_in_multiword_passes(self):
        """If any word is rare, the phrase passes."""
        assert is_generic_phrase("Anthropic research") is False

    def test_all_common_multiword_filtered(self):
        assert is_generic_phrase("the big one") is True


# --- Coverage and Substring Matching ---

class TestIsCovered:
    """Tests the overlap and coverage logic for entity names."""

    def test_exact_match(self):
        assert is_covered("Alice", {"alice"}) is True

    def test_candidate_substring_of_covered(self):
        assert is_covered("Bob", {"bob smith"}) is True

    def test_covered_substring_of_candidate(self):
        assert is_covered("Bob Smith", {"bob"}) is True

    def test_no_overlap(self):
        assert is_covered("Alice", {"bob", "eve"}) is False

    def test_empty_covered_set(self):
        assert is_covered("Alice", set()) is False


# ════════════════════════════════════════════════════════
#  is_substring_match
# ════════════════════════════════════════════════════════

class TestIsSubstringMatch:

    def test_a_in_b(self):
        assert is_substring_match("Bob", "Bob Smith") is True

    def test_b_in_a(self):
        assert is_substring_match("Bob Smith", "Bob") is True

    def test_case_insensitive(self):
        assert is_substring_match("bob", "BOB SMITH") is True

    def test_no_match(self):
        assert is_substring_match("Alice", "Bob") is False


# --- LLM Response Cleanup ---

class TestStripCodeFences:
    """Tests the utility for removing Markdown code fences from LLM output."""

    def test_json_fence(self):
        text = '```json\n{"Work": {"labels": ["company"]}}\n```'
        assert _strip_code_fences(text) == '{"Work": {"labels": ["company"]}}'

    def test_plain_fence(self):
        text = '```\n{"Work": {"labels": ["company"]}}\n```'
        assert _strip_code_fences(text) == '{"Work": {"labels": ["company"]}}'

    def test_no_fence(self):
        text = '{"Work": {"labels": ["company"]}}'
        assert _strip_code_fences(text) == '{"Work": {"labels": ["company"]}}'

    def test_only_opening_fence(self):
        """LLM sometimes forgets the closing fence."""
        text = '```json\n{"Work": {"labels": ["company"]}}'
        result = _strip_code_fences(text)
        assert result == '{"Work": {"labels": ["company"]}}'

    def test_only_closing_fence(self):
        text = '{"Work": {"labels": ["company"]}}\n```'
        result = _strip_code_fences(text)
        assert result == '{"Work": {"labels": ["company"]}}'

    def test_whitespace_padding(self):
        text = '  \n```json\n{"a": 1}\n```\n  '
        assert _strip_code_fences(text) == '{"a": 1}'