def build_entity_embedding_text(
    canonical_name: str,
    entity_type: str,
) -> str:
    name = str(canonical_name or "").strip()
    normalized_type = str(entity_type or "unknown").strip() or "unknown"
    base = f"{name} ({normalized_type})"

    return base
