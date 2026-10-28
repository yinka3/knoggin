"""Load Hugging Face models from the local cache when they are already there."""

from __future__ import annotations

import os
from pathlib import Path

from huggingface_hub import snapshot_download

# The exact Hub commit of each default model, so a download gets the version
# Knoggin was tested with rather than whatever is newest. The Dockerfile reads
# this list too. Update a pin on purpose, after testing the new version.
PINNED_REVISIONS = {
    "BAAI/bge-base-en-v1.5": "a5beb1e3e68b9ab74eb54cfd186867f64f240e1a",
    "BAAI/bge-reranker-base": "2cfc18c9415c912f9d8155881c133215df768a70",
    "fastino/gliner2.5-base-v1": "54785d51df8d86a0d8f2eb212fec15871aad2c4d",
    "fastino/gliner2.5-multi-v1": "cf5593a5d45e3bbf204b9df621b13b1c0cf25ee3",
}


def model_revision(model_id: str, revision_variable: str) -> str | None:
    """The revision to load: the user's setting, else the pin for a default
    model. A model Knoggin does not ship with stays unpinned."""

    return os.getenv(revision_variable) or PINNED_REVISIONS.get(model_id)


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
