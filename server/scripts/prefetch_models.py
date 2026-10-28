"""Download, export, and validate Knoggin's complete local model set.

Normal starts load cached models without asking the Hub. Run this to fetch
models that are missing or newer, or the pinned *_MODEL_REVISION versions.
"""

import asyncio
import os
import subprocess
import sys
from pathlib import Path

import torch
from dotenv import load_dotenv
from huggingface_hub import snapshot_download
from loguru import logger

SERVER_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SERVER_ROOT.parent
sys.path.insert(0, str(SERVER_ROOT / "src"))

from common.embedding_contract import (  # noqa: E402
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_RERANKER_MODEL,
)
from common.model_cache import cached_model_path  # noqa: E402
from core.ingestion.vp01 import (  # noqa: E402
    GLINER25_ENGLISH_MODEL,
    GLINER25_MULTILINGUAL_MODEL,
    GLiNER25VP01Adapter,
)
from core.knowledge.embedding_service import EmbeddingService  # noqa: E402
from infrastructure.model_work import ModelWorkPriority  # noqa: E402
from infrastructure.resource_profile import ResourceProfile  # noqa: E402


def _device() -> torch.device:
    prefer_gpu = os.getenv("PREFER_GPU", "false").lower() == "true"
    if prefer_gpu and torch.cuda.is_available():
        return torch.device("cuda")
    if prefer_gpu and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


async def prefetch_models() -> None:
    """Prepare every local model without starting the server or data services."""
    load_dotenv(REPOSITORY_ROOT / ".env")
    profile = ResourceProfile.from_environment()
    device = _device()
    embedding_model = os.getenv("EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL)
    reranker_model = os.getenv("RERANKER_MODEL", DEFAULT_RERANKER_MODEL)
    models = [
        (embedding_model, "EMBEDDING_MODEL_REVISION"),
        (reranker_model, "RERANKER_MODEL_REVISION"),
        (GLINER25_ENGLISH_MODEL, "GLINER_EN_MODEL_REVISION"),
    ]
    # The multilingual model loads on first use; only refresh it once present.
    if cached_model_path(GLINER25_MULTILINGUAL_MODEL) is not None:
        models.append((GLINER25_MULTILINGUAL_MODEL, "GLINER_MULTI_MODEL_REVISION"))
    for model, revision_variable in models:
        logger.info(f"Fetching {model}")
        await asyncio.to_thread(
            snapshot_download, model, revision=os.getenv(revision_variable)
        )
    embedding = EmbeddingService(
        embedding_model=embedding_model,
        reranker_model=reranker_model,
        device=device,
        batch_size=profile.embedding_batch_size,
    )
    try:
        logger.info(
            "Prefetching all local models | "
            f"profile={profile.name} | device={device} | "
            f"provider={embedding.onnx_provider or 'torch'}"
        )
        await embedding.load_models()
        await embedding.load_reranker(priority=ModelWorkPriority.BACKGROUND)
        await asyncio.to_thread(
            GLiNER25VP01Adapter.load,
            language="en",
            device=str(device),
        )
        logger.info("Prefetching Docling PDF layout, table, and OCR artifacts")
        await asyncio.to_thread(
            subprocess.run,
            [
                "docling-tools",
                "models",
                "download",
                "layout",
                "tableformer",
                "rapidocr",
                "--quiet",
            ],
            check=True,
        )
        logger.info("All local models are ready in the local cache")
    finally:
        embedding.cleanup()


if __name__ == "__main__":
    asyncio.run(prefetch_models())
