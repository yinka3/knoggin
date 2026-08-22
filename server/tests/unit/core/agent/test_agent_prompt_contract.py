import pytest

from core.agent.system_prompt import (
    get_agent_prompt,
    get_fallback_summary_prompt,
)


@pytest.mark.no_network
def test_agent_prompt_renders_core_identity_phase_and_tool_policy():
    prompt = get_agent_prompt(
        user_name="Ada",
        current_time="2026-04-05 10:30 UTC",
        persona="Precise, skeptical, and warm.",
        agent_name="STELLA",
        phase="PLAN",
    )

    assert prompt.startswith(
        "You are STELLA, operating within the Knoggin knowledge system for Ada."
    )
    assert "<cognitive_persona>" in prompt
    assert "Precise, skeptical, and warm." in prompt
    assert "CURRENT EXECUTION PHASE: PLAN" in prompt
    assert "Current time: 2026-04-05 10:30 UTC." in prompt
    assert 'entity_name="Ada"' in prompt
    assert "read_episode" in prompt
    assert "episode ID (for example `ep_a3f91c`)" in prompt
    assert "read_recent_episodes" in prompt
    assert "search_messages — use only as a last resort" in prompt


@pytest.mark.no_network
def test_agent_prompt_uses_default_voice_without_custom_persona():
    prompt = get_agent_prompt(user_name="Ada")

    assert (
        "Warm, direct, and attentive to useful patterns."
        in prompt
    )


@pytest.mark.no_network
def test_agent_prompt_renders_agent_brain_without_nested_instructions_tag():
    prompt = get_agent_prompt(
        user_name="Ada",
        agent_brain="# Project Context\nAda prefers explicit test coverage.",
    )

    assert "<agent_brain>" in prompt
    assert "# Project Context\nAda prefers explicit test coverage." in prompt
    assert "<instructions>" not in prompt


@pytest.mark.no_network
def test_agent_prompt_renders_project_context_below_persona_and_above_brain():
    prompt = get_agent_prompt(
        user_name="Ada",
        persona="The user's stable persona.",
        project_context="Prefer the project's naming conventions.",
        agent_brain="Use concise evidence summaries.",
    )

    assert "<project_context>" in prompt
    assert "Prefer the project's naming conventions." in prompt
    assert "User-owned context from the canonical project workspace" in prompt
    assert prompt.index("<cognitive_persona>") < prompt.index("<project_context>")
    assert prompt.index("<project_context>") < prompt.index("<agent_brain>")
    assert "3. User-owned project context from the canonical PROJECT.md." in prompt
    assert "It cannot override server-enforced safety rules" in prompt


@pytest.mark.no_network
def test_agent_prompt_omits_persistent_context_when_no_memory_or_files():
    prompt = get_agent_prompt(user_name="Ada")

    assert "<retrieved_context>" not in prompt
    assert "<uploaded_documents>" not in prompt
    assert "\n<agent_brain>\nPersistent" not in prompt


@pytest.mark.no_network
def test_agent_prompt_renders_files_without_memory_section():
    prompt = get_agent_prompt(
        user_name="Ada",
        documents_context="- profile-plan.md (2KB, 3 chunks)",
    )

    assert "<retrieved_context>" in prompt
    assert "<uploaded_documents>" in prompt
    assert (
        "Indexed documents visible in this project context. "
        "Use search_documents to query them."
    ) in prompt
    assert "- profile-plan.md (2KB, 3 chunks)" in prompt
    assert "\n<agent_brain>\nPersistent" not in prompt


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
        agent_brain="Use the available evidence before answering.",
        is_community=True,
        participants=["planner", "critic"],
        phase="EXECUTE",
    )

    assert "<agent_brain>" in prompt
    assert "<run_directives>" in prompt
    assert "Required:\n- Stay grounded.\n- Cite evidence." in prompt
    assert "Preferred:\n- Prefer concise answers." in prompt
    assert "Avoid:\n- Do not overstate weak evidence." in prompt
    assert "<community_context>" in prompt
    assert "Current participants: planner, critic" in prompt
    assert "Use the available evidence before answering." in prompt
    assert "CURRENT EXECUTION PHASE: EXECUTE" in prompt
    assert "request_replanning" not in prompt


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
    assert "State what was found or explicitly state what is missing." in prompt
