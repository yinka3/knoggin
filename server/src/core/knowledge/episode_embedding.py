"""Canonical text used to embed episodic-memory records."""

from common.schema.episode.models import Episode


def build_episode_embedding_text_from_fields(
    summary: str,
    new_developments: list[str],
    updates: list[str],
    unresolved: list[str],
) -> str:
    """Return a stable semantic representation of an episode's current memory.

    Keep this independent of message evidence and operational metadata: embedding
    should represent the maintained episode narrative, while raw messages remain
    available for proof and expansion.
    """

    sections = [("Summary", summary.strip())]
    for label, values in (
        ("New developments", new_developments),
        ("Updates", updates),
        ("Unresolved", unresolved),
    ):
        normalized = [value.strip() for value in values if value and value.strip()]
        if normalized:
            sections.append((label, "\n".join(f"- {value}" for value in normalized)))
    return "\n\n".join(f"{label}:\n{content}" for label, content in sections)


def build_episode_embedding_text(episode: Episode) -> str:
    """Build canonical embedding text from an in-memory episode aggregate."""

    return build_episode_embedding_text_from_fields(
        episode.summary,
        episode.new_developments,
        episode.updates,
        episode.unresolved,
    )
