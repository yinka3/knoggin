"""Canonical text used to embed episodic-memory records."""

import math
from typing import Protocol

from common.schema.episode.models import Episode

MAX_EPISODE_EMBEDDING_CHUNKS = 256


class EpisodeEmbeddingEncoder(Protocol):
    def split_for_embedding(self, text: str) -> list[str]: ...

    async def encode(self, texts: list[str]) -> list[list[float]]: ...


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


async def encode_episode_embedding_texts(
    encoder: EpisodeEmbeddingEncoder, texts: list[str]
) -> list[list[float]]:
    """Embed every part of each narrative and pool long ones into one vector.

    Single-chunk vectors retain the model's original result. Long narratives
    receive a unit-normalized mean of their chunk vectors for cosine search.
    """

    if not texts:
        return []
    groups = [encoder.split_for_embedding(text) for text in texts]
    for text, chunks in zip(texts, groups):
        if not chunks or len(chunks) > MAX_EPISODE_EMBEDDING_CHUNKS:
            raise ValueError("episode embedding exceeds the chunk budget")
        if "".join(chunks) != text:
            raise ValueError("episode embedding chunks must preserve the full narrative")
    vectors = await encoder.encode([chunk for chunks in groups for chunk in chunks])
    expected = sum(len(chunks) for chunks in groups)
    if len(vectors) != expected:
        raise RuntimeError("episode embedding service returned an invalid result")
    result: list[list[float]] = []
    offset = 0
    for chunks in groups:
        chunk_vectors = vectors[offset : offset + len(chunks)]
        offset += len(chunks)
        if len(chunk_vectors) == 1:
            result.append(chunk_vectors[0])
            continue
        width = len(chunk_vectors[0])
        if not width or any(len(vector) != width for vector in chunk_vectors):
            raise RuntimeError("episode embedding chunks have inconsistent dimensions")
        pooled = [0.0] * width
        for vector in chunk_vectors:
            norm = math.sqrt(sum(value * value for value in vector))
            if not math.isfinite(norm) or norm == 0:
                raise RuntimeError("episode embedding chunks require finite nonzero vectors")
            for index, value in enumerate(vector):
                pooled[index] += value / norm
        norm = math.sqrt(sum(value * value for value in pooled))
        if not math.isfinite(norm) or norm == 0:
            raise RuntimeError("episode embedding chunks cannot be pooled")
        result.append([value / norm for value in pooled])
    return result
