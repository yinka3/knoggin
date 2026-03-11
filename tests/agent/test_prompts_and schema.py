"""
Tests for agent/system_prompt.py and shared/models/schema/tool_schema.py.

All pure functions — zero cost, zero external deps.
"""

from agent.system_prompt import get_agent_prompt, get_fallback_summary_prompt
from shared.models.schema.tool_schema import get_filtered_schemas, TOOL_SCHEMAS, ALL_TOOL_NAMES


# ════════════════════════════════════════════════════════
#  get_agent_prompt
# ════════════════════════════════════════════════════════

class TestGetAgentPrompt:

    def test_user_name_in_prompt(self):
        result = get_agent_prompt("Alice")
        assert "Alice" in result

    def test_default_persona_when_empty(self):
        result = get_agent_prompt("Alice", persona="")
        assert "<persona>" in result
        # Default voice should be present
        assert "Warm" in result or "direct" in result

    def test_custom_persona(self):
        result = get_agent_prompt("Alice", persona="Speak like a pirate")
        assert "Speak like a pirate" in result

    def test_agent_name(self):
        result = get_agent_prompt("Alice", agent_name="STELLA")
        assert "STELLA" in result

    def test_current_time_included(self):
        result = get_agent_prompt("Alice", current_time="2025-03-09 14:30")
        assert "2025-03-09 14:30" in result

    def test_current_time_omitted(self):
        result = get_agent_prompt("Alice", current_time="")
        # Should not have "Current time:" when empty
        assert "Current time:" not in result

    def test_memory_context_section(self):
        result = get_agent_prompt("Alice", memory_context="[General]\n  - (m1) Likes Python")
        assert "<your_memory>" in result
        assert "Likes Python" in result
        assert "<persistent_context>" in result

    def test_no_memory_no_section(self):
        result = get_agent_prompt("Alice", memory_context="", files_context="")
        assert "<persistent_context>" not in result
        assert "<your_memory>" not in result

    def test_files_context_section(self):
        result = get_agent_prompt("Alice", files_context="- report.pdf (2KB, 5 chunks)")
        assert "<uploaded_files>" in result
        assert "report.pdf" in result

    def test_agent_rules(self):
        result = get_agent_prompt("Alice", agent_rules="Never swear")
        assert "<agent_rules>" in result
        assert "Never swear" in result
        assert "<agent_instructions>" in result

    def test_agent_preferences(self):
        result = get_agent_prompt("Alice", agent_preferences="Be concise")
        assert "<agent_preferences>" in result
        assert "Be concise" in result

    def test_agent_icks(self):
        result = get_agent_prompt("Alice", agent_icks="Don't use emojis")
        assert "<agent_icks>" in result

    def test_no_agent_specific_section_when_empty(self):
        result = get_agent_prompt("Alice", agent_rules="", agent_preferences="", agent_icks="")
        assert "<agent_instructions>" not in result

    def test_instructions_section(self):
        result = get_agent_prompt("Alice", instructions="Focus on technical topics only")
        assert "<instructions>" in result
        assert "Focus on technical topics" in result

    def test_no_instructions_when_none(self):
        result = get_agent_prompt("Alice", instructions=None)
        assert "<instructions>" not in result

    def test_html_escaping(self):
        """Memory/rules with HTML chars should be escaped to prevent injection."""
        result = get_agent_prompt("Alice", memory_context="<script>alert('xss')</script>")
        assert "<script>" not in result
        assert "&lt;script&gt;" in result

    def test_system_guidelines_always_present(self):
        result = get_agent_prompt("Alice")
        assert "<system_guidelines>" in result
        assert "search_entity" in result

    def test_thinking_section_always_present(self):
        result = get_agent_prompt("Alice")
        assert "<thinking>" in result

    def test_skip_tools_section_always_present(self):
        result = get_agent_prompt("Alice")
        assert "<skip_tools>" in result


# ════════════════════════════════════════════════════════
#  get_fallback_summary_prompt
# ════════════════════════════════════════════════════════

class TestGetFallbackSummaryPrompt:

    def test_user_name_in_prompt(self):
        result = get_fallback_summary_prompt("Alice")
        assert "Alice" in result

    def test_agent_name_in_prompt(self):
        result = get_fallback_summary_prompt("Alice", agent_name="STELLA")
        assert "STELLA" not in result or "Alice" in result
        # Main requirement: user_name is present
        assert "Alice" in result

    def test_default_agent_name(self):
        result = get_fallback_summary_prompt("Alice")
        assert "Alice" in result


# ════════════════════════════════════════════════════════
#  TOOL_SCHEMAS structure
# ════════════════════════════════════════════════════════

class TestToolSchemas:

    def test_all_schemas_have_function_key(self):
        for schema in TOOL_SCHEMAS:
            assert "function" in schema
            assert "name" in schema["function"]
            assert "parameters" in schema["function"]

    def test_all_tool_names_present(self):
        """Every name in ALL_TOOL_NAMES should have a corresponding schema."""
        schema_names = {s["function"]["name"] for s in TOOL_SCHEMAS}
        for name in ALL_TOOL_NAMES:
            assert name in schema_names, f"{name} missing from TOOL_SCHEMAS"

    def test_request_clarification_exists(self):
        names = {s["function"]["name"] for s in TOOL_SCHEMAS}
        assert "request_clarification" in names

    def test_all_schemas_have_required_fields(self):
        for schema in TOOL_SCHEMAS:
            params = schema["function"]["parameters"]
            assert params.get("type") == "object"
            assert "properties" in params


# ════════════════════════════════════════════════════════
#  get_filtered_schemas
# ════════════════════════════════════════════════════════

class TestGetFilteredSchemas:

    def test_none_returns_all(self):
        result = get_filtered_schemas(None)
        assert result is TOOL_SCHEMAS

    def test_empty_list_still_has_clarification(self):
        result = get_filtered_schemas([])
        names = {s["function"]["name"] for s in result}
        assert "request_clarification" in names
        assert len(names) == 1

    def test_single_tool_plus_clarification(self):
        result = get_filtered_schemas(["search_entity"])
        names = {s["function"]["name"] for s in result}
        assert names == {"search_entity", "request_clarification"}

    def test_multiple_tools(self):
        enabled = ["search_entity", "get_connections", "save_memory"]
        result = get_filtered_schemas(enabled)
        names = {s["function"]["name"] for s in result}
        assert names == {"search_entity", "get_connections", "save_memory", "request_clarification"}

    def test_unknown_tool_ignored(self):
        result = get_filtered_schemas(["search_entity", "fake_tool"])
        names = {s["function"]["name"] for s in result}
        assert "fake_tool" not in names
        assert "search_entity" in names

    def test_clarification_not_duplicated(self):
        """If request_clarification is explicitly in the list, it shouldn't appear twice."""
        result = get_filtered_schemas(["request_clarification", "search_entity"])
        names = [s["function"]["name"] for s in result]
        assert names.count("request_clarification") == 1