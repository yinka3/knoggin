import pytest

from common.utils.agent_identity import (
    MAX_BRAIN_CHARS,
    MAX_BRAIN_SECTION_CHARS,
    normalize_agent_brain,
    replace_brain_section,
)


def _sectioned_brain(*, self_conception="Identity", directives="Check sources"):
    return (
        f"# Self-Conception\n{self_conception}\n\n"
        f"# Behavioral Directives\n{directives}\n\n"
        "# Project Context\nProject facts\n\n"
        "# User Preferences & Lessons Learned\nUser preferences"
    )


def test_normalize_brain_rejects_oversized_section_in_full_brain():
    brain = _sectioned_brain(directives="x" * (MAX_BRAIN_SECTION_CHARS + 1))

    with pytest.raises(ValueError, match="Brain section exceeds"):
        normalize_agent_brain(brain)


def test_normalize_brain_rejects_oversized_total_even_when_sections_fit():
    brain = _sectioned_brain(self_conception="x" * MAX_BRAIN_CHARS)

    with pytest.raises(ValueError, match="Brain exceeds"):
        normalize_agent_brain(brain)


def test_normalize_brain_rejects_duplicate_required_heading():
    brain = _sectioned_brain() + "\n\n# Behavioral Directives\nSecond copy"

    with pytest.raises(ValueError, match="exactly one section: Behavioral Directives"):
        normalize_agent_brain(brain)


def test_normalized_brain_still_accepts_a_bounded_section_edit():
    brain = normalize_agent_brain(_sectioned_brain())

    updated = replace_brain_section(brain, "Behavioral Directives", "Check citations")

    assert "# Behavioral Directives\nCheck citations" in updated
    assert "# Project Context\nProject facts" in updated
