import pytest

from knoggin_server.agent.system_prompt import (
    get_agent_prompt,
    get_fallback_summary_prompt,
)


@pytest.mark.no_network
def test_agent_prompt_renders_core_identity_mode_and_tool_policy():
    prompt = get_agent_prompt(
        user_name="Ada",
        current_time="2026-04-05 10:30 UTC",
        persona="Precise, skeptical, and warm.",
        agent_name="STELLA",
        current_mode="Architect",
    )

    assert prompt.startswith(
        "You are STELLA, operating within the Knoggin knowledge system for Ada."
    )
    assert "<persona>Precise, skeptical, and warm.</persona>" in prompt
    assert "YOUR CURRENT MODE: Architect" in prompt
    assert "Current time: 2026-04-05 10:30 UTC." in prompt
    assert 'fact_check("Ada")' in prompt
    assert "search_messages — use only as a last resort" in prompt


@pytest.mark.no_network
def test_agent_prompt_uses_default_voice_without_custom_persona():
    prompt = get_agent_prompt(user_name="Ada")

    assert (
        "<persona>Warm and direct. Match their energy. No corporate filler.</persona>"
        in prompt
    )


@pytest.mark.no_network
def test_agent_prompt_renders_memory_with_active_topics_and_saved_guidance():
    prompt = get_agent_prompt(
        user_name="Ada",
        memory_context="[Identity]\n- Ada prefers explicit test coverage.",
        active_topics=["Identity", "Testing"],
    )

    assert "<persistent_context>" in prompt
    assert "<your_memory>" in prompt
    assert (
        "Active topics you can categorize memories under: Identity, Testing"
        in prompt
    )
    assert "[Identity]\n- Ada prefers explicit test coverage." in prompt
    assert "Do not save things already here." in prompt


@pytest.mark.no_network
def test_agent_prompt_omits_persistent_context_when_no_memory_or_files():
    prompt = get_agent_prompt(
        user_name="Ada",
        active_topics=["Identity"],
    )

    assert "<persistent_context>" not in prompt
    assert "<your_memory>" not in prompt
    assert "<uploaded_documents>" not in prompt
    assert "Active topics you can categorize memories under" not in prompt


@pytest.mark.no_network
def test_agent_prompt_renders_files_without_memory_section():
    prompt = get_agent_prompt(
        user_name="Ada",
        documents_context="- profile-plan.md (2KB, 3 chunks)",
        active_topics=["Identity"],
    )

    assert "<persistent_context>" in prompt
    assert "<uploaded_documents>" in prompt
    assert (
        "Indexed documents visible in this project context. "
        "Use search_documents to query them."
    ) in prompt
    assert "- profile-plan.md (2KB, 3 chunks)" in prompt
    assert "<your_memory>" not in prompt
    assert "Active topics you can categorize memories under" not in prompt


@pytest.mark.no_network
def test_agent_prompt_renders_compact_document_focus_without_contents():
    prompt = get_agent_prompt(
        user_name="Ada",
        document_focus_context=(
            "Active document focus:\n"
            "- mode: pinned\n"
            "- expires: this session\n"
            "- path_prefix: src"
        ),
    )

    assert "<document_focus>" in prompt
    assert "use_focus=false" in prompt
    assert "- path_prefix: src" in prompt
    assert "document contents" not in prompt


@pytest.mark.no_network
def test_agent_prompt_renders_agent_and_community_contexts():
    prompt = get_agent_prompt(
        user_name="Ada",
        agent_directives=(
            "Required:\n"
            "- Stay grounded.\n"
            "- Cite evidence.\n\n"
            "Preferred:\n"
            "- Prefer concise answers.\n\n"
            "Avoid:\n"
            "- Do not overstate weak evidence."
        ),
        instructions="Use the available evidence before answering.",
        is_community=True,
        participants=["planner", "critic"],
        current_mode="Librarian",
    )

    assert "<agent_instructions>" in prompt
    assert "<agent_directives>" in prompt
    assert "Required:\n- Stay grounded.\n- Cite evidence." in prompt
    assert "Preferred:\n- Prefer concise answers." in prompt
    assert "Avoid:\n- Do not overstate weak evidence." in prompt
    assert "<community_context>" in prompt
    assert "Current participants: planner, critic" in prompt
    assert "<instructions>\nUse the available evidence before answering." in prompt
    assert "YOUR CURRENT MODE: Librarian" in prompt
    assert "request_replanning" in prompt


@pytest.mark.no_network
def test_agent_prompt_uses_none_for_empty_community_participants():
    prompt = get_agent_prompt(
        user_name="Ada",
        is_community=True,
        participants=[],
    )

    assert "Current participants: None" in prompt


@pytest.mark.no_network
def test_fallback_summary_prompt_renders_query_and_evidence():
    prompt = get_fallback_summary_prompt(
        user_name="Ada",
        user_query="What did we learn about retrieval tests?",
        evidence_context="Relevant Messages:\n- retrieval ranking passed",
    )

    assert 'The user Ada asked: "What did we learn about retrieval tests?"' in prompt
    assert "Here is the evidence gathered:" in prompt
    assert "Relevant Messages:\n- retrieval ranking passed" in prompt
    assert "State facts found or explicitly state what is missing." in prompt
