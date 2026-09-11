"""Canonical text used to embed user-global entity identities."""


def build_entity_embedding_text(canonical_name: str) -> str:
    """Return the normalized canonical identity representation.

    Entity type and topic belong to project-local classifications, so they must
    not affect the vector stored on the user-global identity.
    """

    return str(canonical_name or "").strip()
