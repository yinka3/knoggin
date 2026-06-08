from __future__ import annotations

from typing import Any, Optional

from common.conf.manager import ConfigManager
from common.schema.primitives import Message
from common.utils.time_utils import get_now_iso


class OnboardingService:
    """Small onboarding facade for identity setup and project quickstart."""

    def __init__(
        self,
        *,
        project_manager: Any = None,
        session_manager: Any = None,
        config_manager: Any = None,
    ):
        self.project_manager = project_manager
        self.session_manager = session_manager
        self.config_manager = config_manager or ConfigManager.get()

    def complete_user_onboarding(
        self,
        display_name: str,
        aliases: Optional[list[str]] = None,
    ) -> dict:
        """Set the storage-scoped user identity once, before real data exists."""
        name = _clean_required(display_name, "display_name")
        config = self.config_manager.config
        existing_name = (config.user_name or "").strip()

        if existing_name and existing_name != name:
            raise ValueError(
                "Changing user_name after onboarding is not supported. "
                "Create a migration before renaming stored user data."
            )

        updates: dict[str, Any] = {
            "user_name": name,
            "configured_at": config.configured_at or get_now_iso(),
        }

        if aliases is not None:
            updates["user_aliases"] = _clean_list(aliases)
        elif not existing_name:
            updates["user_aliases"] = []

        if hasattr(self.config_manager, "update_settings"):
            saved = self.config_manager.update_settings(updates)
            if saved is False:
                raise RuntimeError("Failed to save onboarding settings")
        else:
            for key, value in updates.items():
                setattr(config, key, value)

        updated = self.config_manager.config
        return {
            "user_name": updated.user_name,
            "user_aliases": list(updated.user_aliases),
            "configured_at": updated.configured_at,
        }

    async def create_project_quickstart(
        self,
        name: str,
        description: Optional[str] = None,
        kickoff_note: Optional[str] = None,
        facts: Optional[list[str]] = None,
        preferences: Optional[list[str]] = None,
        model: Optional[str] = None,
        agent_id: Optional[str] = None,
        enabled_tools: Optional[list[str]] = None,
        seed_user_profile: bool = True,
    ) -> dict:
        """Create a project/session and seed optional context through ingestion."""
        if self.project_manager is None or self.session_manager is None:
            raise RuntimeError(
                "create_project_quickstart requires project_manager and session_manager"
            )

        project_name = _clean_required(name, "name")
        project_description = _clean_optional(description)
        project = await self.project_manager.create_project(
            project_name,
            description=project_description,
        )
        ctx = await self.session_manager.create_session(
            model=model,
            agent_id=agent_id,
            enabled_tools=enabled_tools,
            project_id=project["id"],
        )

        seed_text = self._build_seed_message(
            project_name=project_name,
            description=project_description,
            kickoff_note=kickoff_note,
            facts=facts,
            preferences=preferences,
            seed_user_profile=seed_user_profile,
        )

        seeded = False
        seed_error = None
        if seed_text:
            try:
                await ctx.add(Message(content=seed_text))
                seeded = True
            except Exception as exc:
                seed_error = str(exc)

        return {
            "project": project,
            "session_id": ctx.session_id,
            "project_id": project["id"],
            "seeded": seeded,
            "seed_error": seed_error,
        }

    def _build_seed_message(
        self,
        *,
        project_name: str,
        description: Optional[str],
        kickoff_note: Optional[str],
        facts: Optional[list[str]],
        preferences: Optional[list[str]],
        seed_user_profile: bool,
    ) -> str:
        sections = []

        if seed_user_profile:
            cleaned_facts = _clean_list(facts or [])
            if cleaned_facts:
                sections.append(
                    "Project-scoped user facts:\n"
                    + "\n".join(f"- {fact}" for fact in cleaned_facts)
                )

            cleaned_preferences = _clean_list(preferences or [])
            if cleaned_preferences:
                sections.append(
                    "Project-scoped user preferences:\n"
                    + "\n".join(f"- {preference}" for preference in cleaned_preferences)
                )

        if description:
            sections.append(f"Project description for {project_name}:\n{description}")

        kickoff = _clean_optional(kickoff_note)
        if kickoff:
            sections.append(f"Project kickoff note:\n{kickoff}")

        return "\n\n".join(sections)


def _clean_required(value: str, field_name: str) -> str:
    cleaned = _clean_optional(value)
    if not cleaned:
        raise ValueError(f"{field_name} is required")
    return cleaned


def _clean_optional(value: Optional[str]) -> Optional[str]:
    if value is None:
        return None
    cleaned = value.strip()
    return cleaned or None


def _clean_list(values: list[str]) -> list[str]:
    cleaned = []
    seen = set()
    for value in values:
        item = value.strip()
        if not item or item in seen:
            continue
        cleaned.append(item)
        seen.add(item)
    return cleaned
