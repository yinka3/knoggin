"""Format Context evidence for relationship extraction."""

from typing import Dict, List


def format_context_vp02_input(
    candidates: List[Dict],
    blocks: List[Dict],
    *,
    relationship_block: str = "",
) -> str:
    """Format Context-native VP-02 evidence with stable local block handles."""

    lines = ["## Candidate Entities"]
    handles = [candidate["handle"] for candidate in candidates]
    lines.append(f"Allowed entity handles: {handles}")
    for candidate in candidates:
        source_blocks = candidate.get("source_blocks", [])
        source = f" (introduced in {', '.join(source_blocks)})" if source_blocks else ""
        identity = " (reserved user identity)" if candidate.get("is_identity") else ""
        lines.append(
            f"{candidate['handle']} = {candidate['canonical_name']} "
            f"[{candidate['type']}]{identity}{source}"
        )
        if candidate.get("mentions"):
            lines.append(f"  Mentions: {', '.join(candidate['mentions'])}")
    if not candidates:
        lines.append("(none)")

    lines.append("\n## Configured Canonical Relationships")
    lines.append(relationship_block or "(none configured)")

    lines.append("\n## Current Context Blocks")
    valid_blocks = [block["local_id"] for block in blocks]
    lines.append(f"Valid block_ids: {valid_blocks}")
    for block in blocks:
        lines.append(
            f"[BLOCK {block['local_id']}] [{block['section_key']}]: \"{block['markdown']}\""
        )
    if not blocks:
        lines.append("(none)")

    lines.append("\n## Output Constraints")
    lines.append("Use only Allowed entity handles (`eN`) for entity_a and entity_b.")
    lines.append(
        "Canonical names, mentions, types, and source blocks are descriptive only; "
        "never use a name, alias, or durable entity ID as an endpoint."
    )
    lines.append("Each connection must cite one or more Valid block_ids.")
    lines.append(
        "Cite the smallest current block set that proves the relation; neighboring "
        "blocks may resolve pronouns but never invent a relation."
    )
    lines.append(
        "Return no relationship for co-mentions, unstated implications, or agent-derived claims."
    )
    return "\n".join(lines)
