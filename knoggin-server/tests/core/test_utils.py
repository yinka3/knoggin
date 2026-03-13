"""Tests for src/core/utils.py — XML parsing, entity validation, and string matching."""

import pytest

from src.core.utils import (
    extract_xml_content,
    is_covered,
    is_generic_phrase,
    is_substring_match,
    parse_connection_response,
    parse_entities,
    validate_entity,
    format_vp01_input,
    format_vp02_input,
    handle_background_task_result,
)
from src.common.config.topics_config import TopicConfig
from src.common.services.topic_manager import _strip_code_fences




@pytest.fixture
def topic_config():
    return TopicConfig({
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []},
        "Work": {"active": True, "labels": ["company", "project"], "hierarchy": {}, "aliases": ["career", "job"]},
        "Cooking": {"active": False, "labels": ["recipe"], "hierarchy": {}, "aliases": []},
    })


class TestExtractXmlContent:

    def test_happy_path(self):
        text = "Some preamble <entities>1 | Alice | person | General | 0.9</entities> trailing"
        result = extract_xml_content(text, "entities")
        assert result == "1 | Alice | person | General | 0.9"

    def test_missing_close_tag(self):
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
        text = "<facts>\n  <fact subject=\"Alice\">Works at Anthropic</fact>\n</facts>"
        result = extract_xml_content(text, "facts")
        assert "<fact subject=" in result


class TestParseEntities:

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
        reasoning = "<entities>\n1 | Bob | person | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].label == "person"
        assert result[0].topic == "General"

    def test_four_field_fallback_topic_inferred(self, topic_config):
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
        reasoning = "<entities>\nMSG 3 | Alice | person | Identity | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result[0].msg_id == 3

    def test_no_entities_block(self):
        assert parse_entities("No XML here at all") is None

    def test_none_topic_config(self):
        reasoning = "<entities>\n1 | Alice | person | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=None)
        assert len(result) == 1
        assert result[0].topic == "General"

    def test_empty_name_skipped(self, topic_config):
        reasoning = "<entities>\n1 |  | person | Identity | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_duplicate_names_both_returned(self, topic_config):
        reasoning = """
        <entities>
        1 | Alice | person | Identity | 0.95
        2 | Alice | person | Identity | 0.90
        </entities>
        """
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 2
        assert all(e.name == "Alice" for e in result)
        assert result[0].msg_id == 1
        assert result[1].msg_id == 2

    def test_confidence_exactly_at_threshold(self, topic_config):
        reasoning = "<entities>\n1 | Alice | person | Identity | 0.8\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].confidence == 0.8

    def test_confidence_just_below_threshold(self, topic_config):
        reasoning = "<entities>\n1 | Alice | person | Identity | 0.79\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_non_numeric_msg_id(self, topic_config):
        reasoning = "<entities>\nabc | Alice | person | Identity | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_non_numeric_confidence(self, topic_config):
        reasoning = "<entities>\n1 | Alice | person | Identity | high\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_extra_pipes_in_five_field_line(self, topic_config):
        reasoning = "<entities>\n1 | Alice | person | Identity | 0.9 | extra stuff\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_whitespace_only_name(self, topic_config):
        reasoning = "<entities>\n1 | \t  | person | Identity | 0.9\n</entities>"
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert result is None

    def test_mixed_valid_and_invalid_lines(self, topic_config):
        reasoning = """
        <entities>
        abc | Bad ID | person | Identity | 0.9
        1 | | person | Identity | 0.9
        2 | Low Conf | person | Identity | 0.5
        3 | Good Entity | person | Identity | 0.95
        1 | Bad Conf | person | Identity | nope
        </entities>
        """
        result = parse_entities(reasoning, min_confidence=0.8, topic_config=topic_config)
        assert len(result) == 1
        assert result[0].name == "Good Entity"
        assert result[0].msg_id == 3


class TestParseConnectionResponse:

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
        text = "<connections>\nMSG 1 | A; B | 1.5 | reason\n</connections>"
        result = parse_connection_response(text)
        assert result[0].entity_pairs[0].confidence == 1.0

    def test_invalid_confidence_defaults(self):
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

    def test_non_adjacent_duplicate_msg_ids_merge(self):
        text = """
        <connections>
        MSG 5 | Alice; Bob | 0.9 | friends
        MSG 6 | Eve; Charlie | 0.8 | coworkers
        MSG 5 | Alice; Eve | 0.7 | roommates
        </connections>
        """
        result = parse_connection_response(text)
        by_msg = {mc.message_id: mc for mc in result}
        assert len(by_msg[5].entity_pairs) == 2
        assert len(by_msg[6].entity_pairs) == 1

    def test_empty_entity_b_after_semicolon(self):
        text = "<connections>\nMSG 1 | Alice; | 0.9 | reason\n</connections>"
        result = parse_connection_response(text)
        assert len(result) == 1
        assert result[0].entity_pairs[0].entity_a == "Alice"
        assert result[0].entity_pairs[0].entity_b == ""

    def test_negative_confidence_clamped_to_zero(self):
        text = "<connections>\nMSG 1 | A; B | -0.5 | reason\n</connections>"
        result = parse_connection_response(text)
        assert result[0].entity_pairs[0].confidence == 0.0

    def test_three_plus_entities_only_first_two_used(self):
        text = "<connections>\nMSG 1 | A; B; C | 0.9 | reason\n</connections>"
        result = parse_connection_response(text)
        assert len(result) == 1
        pair = result[0].entity_pairs[0]
        assert pair.entity_a == "A"
        assert pair.entity_b == "B"

    def test_msg_id_without_msg_prefix_skipped(self):
        text = "<connections>\n5 | Alice; Bob | 0.9 | reason\n</connections>"
        result = parse_connection_response(text)
        assert result == []

    def test_no_semicolon_in_entity_pair_skipped(self):
        text = "<connections>\nMSG 1 | Alice and Bob | 0.9 | reason\n</connections>"
        result = parse_connection_response(text)
        assert result == []


class TestValidateEntity:

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
        assert validate_entity("Alice", "NonExistent", topic_config) is False

    def test_inactive_topic_via_alias(self, topic_config):
        result = validate_entity("Gordon Ramsay", "Cooking", topic_config)
        assert result is True

    def test_none_topic(self, topic_config):
        assert validate_entity("Adeyinka", None, topic_config) is True

    def test_person_label_bypasses_generic_filter(self, topic_config):
        assert validate_entity("Alice", "General", topic_config, label="person") is True

    def test_common_name_without_person_label_filtered(self, topic_config):
        assert validate_entity("Alice", "General", topic_config) is True

    def test_stop_word_rejected(self, topic_config):
        assert validate_entity("however", "General", topic_config) is False
        assert validate_entity("although", "General", topic_config) is False

    def test_unicode_name_passes(self, topic_config):
        assert validate_entity("José García", "General", topic_config) is True
        assert validate_entity("Ólafur", "Identity", topic_config) is True

    def test_cjk_name_passes(self, topic_config):
        assert validate_entity("明美", "General", topic_config) is True

    def test_topic_config_none_with_non_general_topic(self):
        with pytest.raises(AttributeError):
            validate_entity("Alice", "Work", None, label="person")

    def test_topic_config_none_with_general_topic(self):
        assert validate_entity("Alice Johnson", "General", None) is True

    def test_topic_config_none_with_none_topic(self):
        assert validate_entity("Alice Johnson", None, None) is True

    def test_mixed_alphanumeric_name(self, topic_config):
        assert validate_entity("Agent007", "General", topic_config) is True
        assert validate_entity("R2D2", "General", topic_config) is True

    def test_label_general_lowercase_does_not_bypass_filter(self, topic_config):
        assert validate_entity("the big one", "General", topic_config, label="general") is False

    def test_label_empty_string_does_not_bypass_filter(self, topic_config):
        assert validate_entity("the big one", "General", topic_config, label="") is False

    def test_name_with_special_chars_but_has_alpha(self, topic_config):
        assert validate_entity("C++", "General", topic_config) is True

    def test_whitespace_only_name(self, topic_config):
        assert validate_entity("  ", "General", topic_config) is False


class TestIsGenericPhrase:

    def test_proper_noun_passes(self):
        assert is_generic_phrase("Anthropic") is False

    def test_common_single_word_filtered(self):
        assert is_generic_phrase("the") is False
        assert is_generic_phrase("good") is False

    def test_rare_word_in_multiword_passes(self):
        assert is_generic_phrase("Anthropic research") is False

    def test_all_common_multiword_filtered(self):
        assert is_generic_phrase("the big one") is True

    def test_empty_string_passes_as_non_generic(self):
        assert is_generic_phrase("") is False

    def test_single_word_always_passes(self):
        assert is_generic_phrase("Knoggin") is False
        assert is_generic_phrase("time") is False
        assert is_generic_phrase("good") is False


class TestIsCovered:

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

    def test_candidate_with_leading_trailing_whitespace(self):
        assert is_covered("  Alice  ", {"alice"}) is True

    def test_covered_set_with_whitespace_entries_still_matches(self):
        assert is_covered("alice", {"  alice  "}) is True


class TestIsSubstringMatch:

    def test_a_in_b(self):
        assert is_substring_match("Bob", "Bob Smith") is True

    def test_b_in_a(self):
        assert is_substring_match("Bob Smith", "Bob") is True

    def test_case_insensitive(self):
        assert is_substring_match("bob", "BOB SMITH") is True

    def test_no_match(self):
        assert is_substring_match("Alice", "Bob") is False


class TestStripCodeFences:

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

    def test_multiple_fence_blocks_only_first_preserved(self):
        text = '```json\n{"a": 1}\n```\nsome text\n```json\n{"b": 2}\n```'
        result = _strip_code_fences(text)
        assert '{"a": 1}' in result
        assert '{"b": 2}' not in result

    def test_python_language_tag(self):
        text = '```python\nprint("hello")\n```'
        result = _strip_code_fences(text)
        assert result == 'print("hello")'

    def test_yaml_language_tag(self):
        text = '```yaml\nkey: value\n```'
        result = _strip_code_fences(text)
        assert result == 'key: value'


class TestFormatVp01Input:

    def test_format_vp01_input(self):
        messages = [{"id": 1, "role": "user", "content": "Hello Alice"}]
        known_ents = [("Alice", 101)]
        gliner_ents = [(1, "Alice", "person")]
        ambiguous = [(1, "Apple", "company", ["Work", "Food"])]
        covered_texts = {1: {"alice"}}
        label_block = "Topic: Identity\n  Labels: person"

        result = format_vp01_input(
            messages, known_ents, gliner_ents, ambiguous, covered_texts, label_block
        )

        assert "## Label Schema" in result
        assert "Topic: Identity" in result
        assert "[MSG 1] [USER]: \"Hello Alice\"" in result
        assert "\"Alice\" -> entity_id=101" in result
        assert "MSG 1: \"Alice\" -> person" in result
        assert "MSG 1: \"Apple\" (company) -> choose from: ['Work', 'Food']" in result

    def test_format_vp01_input_empty(self):
        result = format_vp01_input([], [], [], [], {}, "")
        assert "## Messages" in result
        assert "(none)" in result


class TestFormatVp02Input:

    def test_format_vp02_input(self):
        candidates = [
            {"canonical_name": "Alice", "type": "person", "source_msgs": [1], "mentions": ["Al"]}
        ]
        messages = [{"id": 1, "role": "user", "content": "Hello Alice"}]
        session_context = "Previous context"

        result = format_vp02_input(candidates, messages, session_context)

        assert "Alice [person] (from MSG 1)" in result
        assert "Mentions: Al" in result
        assert "[MSG 1] [USER]: \"Hello Alice\"" in result
        assert "Previous context" in result

    def test_format_vp02_input_empty(self):
        result = format_vp02_input([], [], "")
        assert "(none)" in result


class TestHandleBackgroundTaskResult:

    def test_cancelled_task(self):
        from unittest.mock import MagicMock
        task = MagicMock()
        task.cancelled.return_value = True
        handle_background_task_result(task)

    def test_task_with_exception(self):
        from unittest.mock import MagicMock, patch
        task = MagicMock()
        task.cancelled.return_value = False
        task.exception.return_value = ValueError("Test error")
        with patch("src.core.utils.logger.error") as mock_logger:
            handle_background_task_result(task)
            mock_logger.assert_called_once_with("Background task failed: Test error")

    def test_successful_task(self):
        from unittest.mock import MagicMock
        task = MagicMock()
        task.cancelled.return_value = False
        task.exception.return_value = None
        handle_background_task_result(task)