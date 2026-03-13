"""Tests for VP-02 connection extraction — parser and formatting. No LLM calls."""

from src.core.utils import format_vp02_input, parse_connection_response


class TestParseConnectionResponse:

    def test_single_connection(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | worked out together
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 1
        assert result[0].message_id == 1
        assert len(result[0].entity_pairs) == 1
        assert result[0].entity_pairs[0].entity_a == "Marcus"
        assert result[0].entity_pairs[0].entity_b == "Priya"
        assert result[0].entity_pairs[0].confidence == 0.9
        assert result[0].entity_pairs[0].context == "worked out together"

    def test_multiple_connections_same_message(self):
        raw = """<connections>
MSG 5 | Des; Ty | 0.85 | same workout session
MSG 5 | Des; Yinka | 0.85 | same workout session
MSG 5 | Ty; Yinka | 0.85 | same workout session
</connections>"""
        result = parse_connection_response(raw)

        msg_5_results = [r for r in result if r.message_id == 5]
        assert len(msg_5_results) == 1
        assert len(msg_5_results[0].entity_pairs) == 3

    def test_multiple_messages(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | dinner together
MSG 3 | Derek; Sophie | 0.95 | stated girlfriend
MSG 5 | Tariq; Jasmine | 0.8 | meeting together
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 3
        msg_ids = {r.message_id for r in result}
        assert msg_ids == {1, 3, 5}

    def test_no_connections_message(self):
        raw = """<connections>
MSG 1 | NO CONNECTIONS
MSG 2 | Marcus; Priya | 0.9 | lunch together
MSG 3 | NO CONNECTIONS
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 1
        assert result[0].message_id == 2

    def test_all_no_connections(self):
        raw = """<connections>
MSG 1 | NO CONNECTIONS
MSG 2 | NO CONNECTIONS
MSG 3 | NO CONNECTIONS
</connections>"""
        result = parse_connection_response(raw)
        assert result == []

    def test_confidence_clamping(self):
        raw = """<connections>
MSG 1 | A; B | 1.5 | over confidence
MSG 2 | C; D | -0.5 | negative confidence
MSG 3 | E; F | 0.75 | normal
</connections>"""
        result = parse_connection_response(raw)

        confidences = {}
        for r in result:
            for pair in r.entity_pairs:
                confidences[(pair.entity_a, pair.entity_b)] = pair.confidence

        assert confidences[("A", "B")] == 1.0
        assert confidences[("C", "D")] == 0.0
        assert confidences[("E", "F")] == 0.75

    def test_invalid_confidence_defaults(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | high | dinner together
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 1
        assert result[0].entity_pairs[0].confidence == 0.8

    def test_malformed_lines_skipped(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | valid connection
this is garbage
MSG 2 | only two parts
| missing msg id | A; B | 0.5 | reason
MSG 3 | Derek; Sophie | 0.85 | also valid
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 2
        msg_ids = {r.message_id for r in result}
        assert msg_ids == {1, 3}

    def test_empty_connections_block(self):
        raw = """<connections>
</connections>"""
        result = parse_connection_response(raw)
        assert result == []

    def test_missing_connections_tag(self):
        raw = """Some random text without the expected tags."""
        result = parse_connection_response(raw)
        assert result == []

    def test_whitespace_handling(self):
        raw = """<connections>
MSG 1 |   Marcus  ;   Priya   |  0.9  |  dinner together  
</connections>"""
        result = parse_connection_response(raw)

        assert result[0].entity_pairs[0].entity_a == "Marcus"
        assert result[0].entity_pairs[0].entity_b == "Priya"
        assert result[0].entity_pairs[0].context == "dinner together"

    def test_msg_id_variations(self):
        raw = """<connections>
MSG 1 | A; B | 0.9 | reason1
msg 2 | C; D | 0.8 | reason2
MSG  3 | E; F | 0.7 | reason3
</connections>"""
        result = parse_connection_response(raw)

        msg_ids = {r.message_id for r in result}
        assert msg_ids == {1, 2, 3}

    def test_entity_names_with_spaces(self):
        raw = """<connections>
MSG 1 | Dr. Sarah Chen; Mount Sinai Hospital | 0.85 | works at
MSG 2 | The Museum of Modern Art; Kenji Tanaka | 0.7 | visited together
</connections>"""
        result = parse_connection_response(raw)

        pairs = {(p.entity_a, p.entity_b) for r in result for p in r.entity_pairs}
        assert ("Dr. Sarah Chen", "Mount Sinai Hospital") in pairs
        assert ("The Museum of Modern Art", "Kenji Tanaka") in pairs

    def test_special_characters_in_reason(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | they're co-workers (same team)
</connections>"""
        result = parse_connection_response(raw)
        assert result[0].entity_pairs[0].context == "they're co-workers (same team)"

    def test_multi_pipe_reason_preserved(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | worked on Project X | also collaborated on Y
</connections>"""
        result = parse_connection_response(raw)
        assert len(result) == 1
        reason = result[0].entity_pairs[0].context
        assert "Project X" in reason
        assert "collaborated on Y" in reason

    def test_single_pipe_reason_unchanged(self):
        raw = """<connections>
MSG 1 | Alice; Bob | 0.9 | colleagues at work
</connections>"""
        result = parse_connection_response(raw)
        assert result[0].entity_pairs[0].context == "colleagues at work"

    def test_three_pipe_reason_all_parts_joined(self):
        raw = """<connections>
MSG 1 | A; B | 0.85 | part one | part two | part three
</connections>"""
        result = parse_connection_response(raw)
        reason = result[0].entity_pairs[0].context
        assert "part one" in reason
        assert "part two" in reason
        assert "part three" in reason

    def test_reason_with_semicolons(self):
        raw = """<connections>
MSG 1 | Alice; Bob | 0.9 | worked together; also friends outside work
</connections>"""
        result = parse_connection_response(raw)
        assert len(result) == 1
        assert "friends outside work" in result[0].entity_pairs[0].context


class TestFormatVP03Input:

    def test_basic_formatting(self):
        candidates = [
            {"canonical_name": "Marcus", "type": "person", "mentions": ["marcus"], "source_msgs": [1]},
            {"canonical_name": "Stripe", "type": "company", "mentions": ["stripe"], "source_msgs": [2]},
        ]
        messages = [
            {"id": 1, "role": "user", "message": "Had lunch with Marcus today."},
            {"id": 2, "role": "user", "message": "Then stopped by the Stripe office."},
        ]

        result = format_vp02_input(candidates, messages, "")

        assert "## Candidate Entities" in result
        assert "Marcus [person]" in result
        assert "Stripe [company]" in result
        assert "## Messages" in result
        assert "[MSG 1]" in result
        assert "[MSG 2]" in result
        assert "Had lunch with Marcus today." in result

    def test_source_msgs_included(self):
        candidates = [
            {"canonical_name": "Tariq", "type": "person", "mentions": ["tariq"], "source_msgs": [1, 3, 5]},
        ]
        messages = [{"id": 1, "role": "user", "message": "Test"}]

        result = format_vp02_input(candidates, messages, "")
        assert "(from MSG 1, 3, 5)" in result

    def test_mentions_included(self):
        candidates = [
            {"canonical_name": "Dr. Sarah Chen", "type": "person",
             "mentions": ["sarah", "dr. chen", "sarah chen"], "source_msgs": [1]},
        ]
        messages = [{"id": 1, "role": "user", "message": "Test"}]

        result = format_vp02_input(candidates, messages, "")
        assert "Mentions: sarah, dr. chen, sarah chen" in result

    def test_role_labels(self):
        candidates = []
        messages = [
            {"id": 1, "role": "user", "message": "User message"},
            {"id": 2, "role": "assistant", "message": "Agent response"},
        ]

        result = format_vp02_input(candidates, messages, "")
        assert "[USER]" in result
        assert "[AGENT]" in result

    def test_session_context(self):
        candidates = []
        messages = [{"id": 1, "role": "user", "message": "Test"}]
        session_context = "Earlier, user mentioned working at Anthropic."

        result = format_vp02_input(candidates, messages, session_context)
        assert "## Session Context" in result
        assert "Earlier, user mentioned working at Anthropic." in result

    def test_empty_session_context(self):
        candidates = []
        messages = [{"id": 1, "role": "user", "message": "Test"}]

        result = format_vp02_input(candidates, messages, "")
        assert "(none)" in result

    def test_empty_candidates(self):
        candidates = []
        messages = [{"id": 1, "role": "user", "message": "Test"}]

        result = format_vp02_input(candidates, messages, "")
        assert "## Candidate Entities" in result

    def test_empty_messages(self):
        candidates = [{"canonical_name": "Test", "type": "person", "mentions": [], "source_msgs": []}]
        messages = []

        result = format_vp02_input(candidates, messages, "")
        assert "## Messages" in result

    def test_role_label_takes_precedence_over_role(self):
        candidates = []
        messages = [
            {"id": 1, "role": "user", "role_label": "CUSTOM_USER", "message": "Hello"},
            {"id": 2, "role": "assistant", "role_label": "CUSTOM_AGENT", "message": "Hi there"},
        ]
        result = format_vp02_input(candidates, messages, "")
        assert "[CUSTOM_USER]" in result
        assert "[CUSTOM_AGENT]" in result
        assert "[USER]" not in result
        assert "[AGENT]" not in result

    def test_content_key_fallback(self):
        candidates = []
        messages = [{"id": 1, "role": "user", "content": "Hello from content key"}]
        result = format_vp02_input(candidates, messages, "")
        assert "Hello from content key" in result

    def test_text_key_fallback(self):
        candidates = []
        messages = [{"id": 1, "role": "user", "text": "Hello from text key"}]
        result = format_vp02_input(candidates, messages, "")
        assert "Hello from text key" in result

    def test_no_content_keys_empty_string(self):
        candidates = []
        messages = [{"id": 1, "role": "user"}]
        result = format_vp02_input(candidates, messages, "")
        assert "[MSG 1]" in result

    def test_message_key_takes_precedence(self):
        candidates = []
        messages = [{"id": 1, "role": "user", "message": "from message", "content": "from content"}]
        result = format_vp02_input(candidates, messages, "")
        assert "from message" in result

    def test_no_source_msgs_no_parenthetical(self):
        candidates = [
            {"canonical_name": "Alice", "type": "person", "mentions": [], "source_msgs": []},
        ]
        messages = [{"id": 1, "role": "user", "message": "test"}]
        result = format_vp02_input(candidates, messages, "")
        assert "Alice [person]" in result
        assert "(from MSG" not in result


class TestConnectionRuleCoverage:
    """Validates parsing of simulated LLM responses that follow VP-02 connection rules."""

    def test_rule_explicit_joint_activity(self):
        raw = """<connections>
MSG 1 | Marcus; Yinka | 0.95 | grabbed dinner together
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 1
        pair = result[0].entity_pairs[0]
        assert pair.entity_a == "Marcus"
        assert pair.entity_b == "Yinka"
        assert pair.confidence >= 0.9

    def test_rule_stated_relationship(self):
        raw = """<connections>
MSG 1 | Yinka; Priya | 0.95 | stated coworker relationship
MSG 2 | Derek; Sophie | 0.95 | stated girlfriend relationship
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 2
        pairs = {(p.entity_a, p.entity_b) for r in result for p in r.entity_pairs}
        assert ("Yinka", "Priya") in pairs
        assert ("Derek", "Sophie") in pairs

    def test_rule_same_event_group(self):
        raw = """<connections>
MSG 1 | Tariq; Jasmine | 0.9 | same meeting
MSG 1 | Tariq; Yinka | 0.9 | same meeting
MSG 1 | Jasmine; Yinka | 0.9 | same meeting
</connections>"""
        result = parse_connection_response(raw)

        msg_1 = [r for r in result if r.message_id == 1][0]
        assert len(msg_1.entity_pairs) == 3

    def test_rule_sequential_no_connection(self):
        raw = """<connections>
MSG 1 | Omar; Yinka | 0.85 | phone call
MSG 1 | Stripe; Yinka | 0.7 | visited office
</connections>"""
        result = parse_connection_response(raw)

        pairs = {(p.entity_a, p.entity_b) for r in result for p in r.entity_pairs}
        assert ("Omar", "Yinka") in pairs
        assert ("Stripe", "Yinka") in pairs
        assert ("Omar", "Stripe") not in pairs

    def test_rule_different_days_no_connection(self):
        raw = """<connections>
MSG 1 | Mike; Yinka | 0.9 | met yesterday
MSG 2 | Sarah; Yinka | 0.85 | meeting planned
</connections>"""
        result = parse_connection_response(raw)

        pairs = {(p.entity_a, p.entity_b) for r in result for p in r.entity_pairs}
        assert ("Mike", "Yinka") in pairs
        assert ("Sarah", "Yinka") in pairs
        assert ("Mike", "Sarah") not in pairs

    def test_rule_co_mention_not_connection(self):
        raw = """<connections>
MSG 1 | NO CONNECTIONS
</connections>"""
        result = parse_connection_response(raw)
        assert result == []

    def test_rule_hierarchical_relationship(self):
        raw = """<connections>
MSG 1 | Jasmine; Michael | 0.95 | reports to (org hierarchy)
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 1
        assert result[0].entity_pairs[0].entity_a == "Jasmine"
        assert result[0].entity_pairs[0].entity_b == "Michael"

    def test_rule_introduction(self):
        raw = """<connections>
MSG 1 | Sarah; Yinka | 0.9 | introduced
MSG 1 | Sarah; David | 0.9 | colleague relationship
MSG 1 | Yinka; David | 0.85 | introduced by Sarah
</connections>"""
        result = parse_connection_response(raw)

        msg_1 = [r for r in result if r.message_id == 1][0]
        assert len(msg_1.entity_pairs) == 3

    def test_rule_entity_to_org(self):
        raw = """<connections>
MSG 1 | Marcus; Stripe | 0.95 | employment relationship
</connections>"""
        result = parse_connection_response(raw)

        pair = result[0].entity_pairs[0]
        assert pair.entity_a == "Marcus"
        assert pair.entity_b == "Stripe"

    def test_mixed_connections_and_no_connections(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | lunch together
MSG 2 | NO CONNECTIONS
MSG 3 | Derek; Sophie | 0.95 | relationship stated
MSG 4 | NO CONNECTIONS
MSG 5 | Tariq; Jasmine | 0.8 | same meeting
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 3
        msg_ids = {r.message_id for r in result}
        assert msg_ids == {1, 3, 5}


class TestConnectionEdgeCases:

    def test_self_connection_parsed(self):
        raw = """<connections>
MSG 1 | Marcus; Marcus | 0.9 | self reference?
</connections>"""
        result = parse_connection_response(raw)
        # Parser doesn't filter self-connections — downstream concern
        assert len(result) == 1

    def test_very_long_entity_names(self):
        raw = """<connections>
MSG 1 | The Massachusetts Institute of Technology Department of Computer Science; Dr. Alexandra Konstantinidis-Papadopoulos | 0.75 | faculty member
</connections>"""
        result = parse_connection_response(raw)

        pair = result[0].entity_pairs[0]
        assert "Massachusetts Institute" in pair.entity_a
        assert "Konstantinidis" in pair.entity_b

    def test_unicode_in_names(self):
        raw = """<connections>
MSG 1 | José García; Müller GmbH | 0.85 | business relationship
MSG 2 | 田中太郎; Sony株式会社 | 0.9 | employment
</connections>"""
        result = parse_connection_response(raw)

        assert len(result) == 2
        pairs = {(p.entity_a, p.entity_b) for r in result for p in r.entity_pairs}
        assert ("José García", "Müller GmbH") in pairs
        assert ("田中太郎", "Sony株式会社") in pairs

    def test_numeric_entity_names(self):
        raw = """<connections>
MSG 1 | 3M Company; John Smith | 0.8 | works at
MSG 2 | Y Combinator; Startup123 | 0.75 | invested in
</connections>"""
        result = parse_connection_response(raw)

        pairs = {(p.entity_a, p.entity_b) for r in result for p in r.entity_pairs}
        assert ("3M Company", "John Smith") in pairs
        assert ("Y Combinator", "Startup123") in pairs

    def test_pipe_in_reason_field(self):
        raw = """<connections>
MSG 1 | Marcus; Priya | 0.9 | worked on Project X | also collaborated on Y
</connections>"""
        result = parse_connection_response(raw)
        assert len(result) == 1

    def test_large_message_ids(self):
        raw = """<connections>
MSG 999999 | A; B | 0.9 | reason
MSG 1000000000 | C; D | 0.8 | reason
</connections>"""
        result = parse_connection_response(raw)

        msg_ids = {r.message_id for r in result}
        assert 999999 in msg_ids
        assert 1000000000 in msg_ids

    def test_newlines_in_output(self):
        raw = """<connections>

MSG 1 | Marcus; Priya | 0.9 | reason


MSG 2 | Derek; Sophie | 0.85 | reason

</connections>"""
        result = parse_connection_response(raw)
        assert len(result) == 2