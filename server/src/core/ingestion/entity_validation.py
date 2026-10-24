"""Validate extracted entity mentions before resolution."""

from functools import lru_cache
from typing import Optional

from loguru import logger
from wordfreq import word_frequency

from common.conf.domain_config import CompiledDomain

PRONOUNS = {
    "my",
    "his",
    "her",
    "their",
    "our",
    "your",
    "its",
    "he",
    "she",
    "they",
    "we",
    "i",
    "me",
    "him",
    "them",
    "this",
    "that",
    "these",
    "those",
}

STOP_WORDS = {
    "a",
    "an",
    "and",
    "are",
    "as",
    "at",
    "be",
    "by",
    "for",
    "from",
    "in",
    "is",
    "it",
    "of",
    "on",
    "or",
    "that",
    "the",
    "to",
    "was",
    "with",
}


@lru_cache(maxsize=1)
def _stop_words() -> set[str]:
    stop_words = set(STOP_WORDS)
    try:
        from spacy.lang.en.stop_words import STOP_WORDS as SPACY_STOP_WORDS

        stop_words |= set(SPACY_STOP_WORDS)
    except Exception as exc:
        logger.debug(f"Falling back without spaCy stop words: {exc}")

    try:
        from sklearn.feature_extraction.text import ENGLISH_STOP_WORDS

        stop_words |= set(ENGLISH_STOP_WORDS)
    except Exception as exc:
        logger.debug(f"Falling back without sklearn stop words: {exc}")

    return stop_words


def is_generic_phrase(text: str, threshold: float = 5e-6) -> bool:
    """
    Returns True if phrase is generic (should filter).
    - Any rare word (< threshold) → pass (likely proper noun)
    - Single common word → filter
    - Multi-word all common → sum and check scaled threshold
    """
    words = text.lower().split()
    freqs = [word_frequency(w, "en") for w in words]

    # Any rare word = likely name/proper noun → pass
    if any(f < threshold for f in freqs):
        return False

    # Single common word shouldn't be blocked here (handled by global Stop Word filters)
    if len(words) <= 1:
        return False

    # Multi-word, all common: sum frequencies
    total = sum(freqs)
    return total > threshold * 100


def validate_entity(
    name: str, topic: str, domain: CompiledDomain, label: Optional[str] = None
) -> bool:
    """Filter invalid mentions before resolution."""

    if not name or len(name) < 2:
        return False

    if len(name) > 100:
        return False

    if name.lower() in _stop_words():
        return False

    if name.lower() in PRONOUNS:
        return False

    has_specific_label = label and label.lower() not in ("", "general")
    if not has_specific_label and is_generic_phrase(name):
        return False

    if not any(c.isalpha() for c in name):
        return False

    if topic:
        normalized = domain.normalize_topic(topic)
        if not normalized:
            logger.debug(f"Invalid topic '{topic}' for entity '{name}'")
            return False

    return True
