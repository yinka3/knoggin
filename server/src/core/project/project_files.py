"""Small project-file conventions shared by project creation and agent tools."""

from typing import Iterable, Optional

from core.knowledge.documents.scanning import normalize_relative_path

PROJECT_FILE_PATH = "PROJECT.md"
CONTEXT_FILE_PATH = "CONTEXT.md"


def is_controlled_context_file(path: str) -> bool:
    """Whether a normalized workspace path is the engine-owned Context file."""

    return isinstance(path, str) and path.casefold() == CONTEXT_FILE_PATH.casefold()


def workspace_name(name: str) -> str:
    """Validate a workspace name: one top-level folder, not a hidden one."""

    name = normalize_relative_path(name, name)
    if "/" in name or name.startswith(".") or len(name) > 100:
        raise ValueError("workspace name must be one folder name of at most 100 characters")
    return name


def workspace_of(path: str, workspaces: Iterable[str]) -> Optional[str]:
    """The workspace a project path sits in, or None for files outside one."""

    top, separator, _ = path.partition("/")
    return top if separator and top in workspaces else None


def build_project_markdown(name: str, description: Optional[str] = None) -> str:
    """Build the small trusted seed for a newly-created project."""
    clean_name = " ".join(name.split())
    clean_description = description.strip() if description else ""
    sections = [f"# {clean_name}", ""]
    if clean_description:
        sections.extend([clean_description, ""])
    sections.extend(
        [
            "## Project Context",
            "",
            "Add project-specific context and instructions here.",
            "",
        ]
    )
    return "\n".join(sections)
