from typing import Optional

from common.utils.prompt_loader import render_prompt


def get_agent_prompt(
    user_name: str,
    current_time: str = "",
    persona: str = "",
    agent_name: str = "Agent",
    memory_context: str = "",
    files_context: str = "",
    agent_directives: str = "",
    instructions: str = "",
    is_community: bool = False,
    participants: Optional[list[str]] = None,
    current_mode: str = "Architect",
    active_topics: Optional[list[str]] = None,
) -> str:
    date_context = f"Current time: {current_time}." if current_time else ""
    voice = (
        persona
        if persona
        else "Warm and direct. Match their energy. No corporate filler."
    )
    topics_str = ", ".join(active_topics) if active_topics else "None"
    participants_list = ", ".join(participants) if participants else "None"

    return render_prompt(
        "agent/system_prompt.j2",
        user_name=user_name,
        current_time=current_time,
        agent_name=agent_name,
        memory_context=memory_context,
        files_context=files_context,
        agent_directives=agent_directives,
        instructions=instructions,
        is_community=is_community,
        current_mode=current_mode,
        voice=voice,
        date_context=date_context,
        topics_str=topics_str,
        participants_list=participants_list,
    )


def get_fallback_summary_prompt(
    user_name: str, user_query: str, evidence_context: str
) -> str:
    return render_prompt(
        "agent/fallback_summary.j2",
        user_name=user_name,
        user_query=user_query,
        evidence_context=evidence_context,
    )
