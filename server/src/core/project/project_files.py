"""Small project-file conventions shared by project creation and agent tools."""

from typing import Optional

PROJECT_FILE_PATH = "PROJECT.md"


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
