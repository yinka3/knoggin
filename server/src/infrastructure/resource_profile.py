"""Explicit, user-selected local resource budgets."""

import os
from dataclasses import dataclass, replace


@dataclass(frozen=True)
class ResourceProfile:
    """Resolved startup limits for local model and indexing work."""

    name: str
    worker_count: int
    embedding_batch_size: int
    workspace_prepare_concurrency: int
    background_job_workers: int
    foreground_model_workers: int
    background_model_workers: int

    @classmethod
    def from_environment(cls) -> "ResourceProfile":
        """Resolve a named profile, with explicit environment overrides."""
        name = os.getenv("KNOGGIN_RESOURCE_PROFILE", "balanced").strip().lower()
        profiles = {
            "conservative": cls(
                name="conservative",
                worker_count=2,
                embedding_batch_size=8,
                workspace_prepare_concurrency=2,
                background_job_workers=1,
                foreground_model_workers=1,
                background_model_workers=1,
            ),
            "balanced": cls(
                name="balanced",
                worker_count=4,
                embedding_batch_size=32,
                workspace_prepare_concurrency=4,
                background_job_workers=1,
                foreground_model_workers=1,
                background_model_workers=1,
            ),
            "performance": cls(
                name="performance",
                worker_count=8,
                embedding_batch_size=64,
                workspace_prepare_concurrency=8,
                background_job_workers=2,
                foreground_model_workers=1,
                background_model_workers=1,
            ),
        }
        if name not in profiles:
            raise ValueError(
                "KNOGGIN_RESOURCE_PROFILE must be one of: "
                + ", ".join(sorted(profiles))
            )
        profile = profiles[name]
        return replace(
            profile,
            worker_count=_positive_int("KNOGGIN_WORKERS", profile.worker_count),
            embedding_batch_size=_positive_int(
                "KNOGGIN_EMBEDDING_BATCH_SIZE", profile.embedding_batch_size
            ),
            workspace_prepare_concurrency=_positive_int(
                "KNOGGIN_WORKSPACE_PREPARE_CONCURRENCY",
                profile.workspace_prepare_concurrency,
            ),
            background_job_workers=_positive_int(
                "KNOGGIN_BACKGROUND_JOB_WORKERS",
                profile.background_job_workers,
            ),
            foreground_model_workers=_positive_int(
                "KNOGGIN_FOREGROUND_MODEL_WORKERS",
                profile.foreground_model_workers,
            ),
            background_model_workers=_positive_int(
                "KNOGGIN_BACKGROUND_MODEL_WORKERS",
                profile.background_model_workers,
            ),
        )


def _positive_int(name: str, default: int) -> int:
    value = os.getenv(name)
    if value is None or not value.strip():
        return default
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be a positive integer") from exc
    if parsed < 1:
        raise ValueError(f"{name} must be a positive integer")
    return parsed
