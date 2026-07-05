import re
from typing import List

import numpy as np
from loguru import logger

from common.schema.contracts import FactMergeResult, SkippedFactChange
from common.schema.primitives import Fact, FactRecord


def cosine_similarity(vec_a: List[float], vec_b: List[float]) -> float:
    if not vec_a or not vec_b or None in vec_a or None in vec_b:
        return 0.0

    a = np.asarray(vec_a, dtype=float)
    b = np.asarray(vec_b, dtype=float)

    if a.shape != b.shape:
        return 0.0

    denominator = np.linalg.norm(a) * np.linalg.norm(b)
    if denominator == 0:
        return 0.0

    return float(np.dot(a, b) / denominator)


def find_duplicate_facts(
    facts_a: List[FactRecord], facts_b: List[FactRecord], threshold: float = 0.96
) -> List[str]:
    """
    Find facts in B that are semantic duplicates of facts in A.
    Returns fact IDs from B to invalidate after merge.
    """
    if not facts_a or not facts_b:
        return []

    active_a = [f for f in facts_a if f.invalid_at is None and f.embedding]
    active_b = [f for f in facts_b if f.invalid_at is None and f.embedding]

    if not active_a or not active_b:
        return []

    emb_a = np.asarray([f.embedding for f in active_a], dtype=float)
    emb_b = np.asarray([f.embedding for f in active_b], dtype=float)

    dot_products = emb_b @ emb_a.T
    norms = np.outer(np.linalg.norm(emb_b, axis=1), np.linalg.norm(emb_a, axis=1))
    similarity_matrix = np.divide(
        dot_products,
        norms,
        out=np.zeros_like(dot_products, dtype=float),
        where=norms != 0,
    )

    to_invalidate = []

    for i, fact_b in enumerate(active_b):
        max_sim = similarity_matrix[i].max()

        if max_sim >= threshold:
            to_invalidate.append(fact_b.id)
            logger.info(
                "Marked duplicate fact for invalidation: "
                f"'{fact_b.content[:50]}...' (sim={max_sim:.3f})"
            )

    return to_invalidate


def has_sufficient_facts(candidate: dict, min_facts: int = 1) -> bool:
    facts_a = candidate.get("facts_a", [])
    facts_b = candidate.get("facts_b", [])
    return len(facts_a) >= min_facts and len(facts_b) >= min_facts


def process_extracted_facts(
    existing_facts: List[FactRecord], new_facts: List[Fact]
) -> FactMergeResult:
    """
    Process structured LLM-extracted facts against existing FactRecord nodes.
    Returns IDs to invalidate and new Fact objects to be inserted.
    """
    if not new_facts:
        return FactMergeResult()

    to_invalidate = []
    updates_to_keep = []
    skipped = []
    missing_targets = []

    active_facts = [f for f in existing_facts if f.invalid_at is None]

    for fact_update in new_facts:
        content = fact_update.content.strip()

        # Handle supersedes
        if fact_update.supersedes:
            old_text = fact_update.supersedes.strip()
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
                active_facts = [f for f in active_facts if f.id != matched_fact.id]
            else:
                logger.warning(f"SUPERSEDES target not found: '{old_text}'")
                missing_targets.append(
                    SkippedFactChange(
                        content=old_text,
                        reason="supersedes_target_not_found",
                        metadata={"new_content": content},
                    )
                )

            if not _is_duplicate(content, active_facts):
                updates_to_keep.append(fact_update)
            else:
                skipped.append(
                    SkippedFactChange(
                        content=content,
                        reason="duplicate",
                        metadata={"operation": "supersedes"},
                    )
                )
            continue

        # Handle invalidates
        if fact_update.invalidates:
            old_text = fact_update.invalidates.strip()
            matched_fact = _find_matching_fact(old_text, active_facts)
            if matched_fact:
                to_invalidate.append(matched_fact.id)
                active_facts = [f for f in active_facts if f.id != matched_fact.id]
            else:
                logger.warning(f"INVALIDATES target not found: '{old_text}'")
                missing_targets.append(
                    SkippedFactChange(
                        content=old_text,
                        reason="invalidates_target_not_found",
                        metadata={"new_content": content},
                    )
                )
            continue

        # Normal new fact
        if not _is_duplicate(content, active_facts):
            updates_to_keep.append(fact_update)
            logger.debug(f"Adding new fact: {content}")
        else:
            skipped.append(
                SkippedFactChange(
                    content=content,
                    reason="duplicate",
                    metadata={"operation": "create"},
                )
            )

    return FactMergeResult(
        to_invalidate=to_invalidate,
        new_contents=updates_to_keep,
        skipped=skipped,
        missing_targets=missing_targets,
    )


def _normalize_fact_target(text: str) -> str:
    normalized = re.sub(r"\s+", " ", text).strip().casefold()
    return normalized.rstrip(".!?")


def _find_matching_fact(text: str, facts: List[FactRecord]) -> FactRecord | None:
    """Find existing fact by exact normalized text."""
    target = _normalize_fact_target(text)
    for fact in facts:
        if _normalize_fact_target(fact.content) == target:
            return fact

    return None


def _is_duplicate(content: str, facts: List[FactRecord]) -> bool:
    """Check if content already exists in facts (exact match)."""
    target = _normalize_fact_target(content)
    return any(_normalize_fact_target(f.content) == target for f in facts)
