"""Load Hugging Face models from the local cache when they are already there."""

from __future__ import annotations

from pathlib import Path

from huggingface_hub import snapshot_download


def cached_model_path(model_id: str, revision: str | None = None) -> str | None:
    """The local folder for a model, or None when it is not cached yet.

    Loaders given a Hub name ask huggingface.co for the file list on every
    load, which fails offline even with every file cached. Passing this folder
    instead keeps a cached model local; prefetch_models.py downloads updates.
    """
    if Path(model_id).is_dir():
        return model_id
    try:
        return snapshot_download(model_id, revision=revision, local_files_only=True)
    except Exception:
        return None
