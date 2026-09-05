"""Download, export, and validate Knoggin's complete local model set."""

import asyncio
import os
import sys
from pathlib import Path

import torch
from dotenv import load_dotenv
from loguru import logger

SERVER_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SERVER_ROOT.parent
sys.path.insert(0, str(SERVER_ROOT / "src"))

from core.ingestion.vp01 import GLiNER25VP01Adapter  # noqa: E402
from core.knowledge.services.embedding_service import EmbeddingService  # noqa: E402
from infrastructure.model_work import ModelWorkPriority  # noqa: E402
from infrastructure.resource_profile import ResourceProfile  # noqa: E402


def _device() -> torch.device:
    use_gpu = os.getenv("KNOGGIN_GPU", "false").lower() == "true"
    if use_gpu and torch.cuda.is_available():
        return torch.device("cuda")
    if use_gpu and hasattr(torch.backends, "mps") and torch.backends.mps.is_available():
        return torch.device("mps")
    return torch.device("cpu")


async def prefetch_models() -> None:
    """Prepare every local model without starting the server or data services."""
    load_dotenv(REPOSITORY_ROOT / ".env")
    profile = ResourceProfile.from_environment()
    device = _device()
    embedding = EmbeddingService(
        embedding_model=os.getenv(
            "KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"
        ),
        reranker_model=os.getenv(
            "KNOGGIN_RERANKER_MODEL", "BAAI/bge-reranker-large"
        ),
        nli_model=os.getenv(
            "KNOGGIN_NLI_MODEL",
            "MoritzLaurer/DeBERTa-v3-large-mnli-fever-anli-ling-wanli",
        ),
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
        await embedding.load_nli_model(priority=ModelWorkPriority.BACKGROUND)
        await asyncio.to_thread(
            GLiNER25VP01Adapter.load,
            language="en",
            device=str(device),
        )
        logger.info("All local models are ready in the local cache")
    finally:
        embedding.cleanup()


if __name__ == "__main__":
    asyncio.run(prefetch_models())
