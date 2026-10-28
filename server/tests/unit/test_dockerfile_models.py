from pathlib import Path

from common.embedding_contract import DEFAULT_EMBEDDING_MODEL, DEFAULT_RERANKER_MODEL
from core.ingestion.vp01 import GLINER25_ENGLISH_MODEL, GLINER25_MULTILINGUAL_MODEL

DOCKERFILE = Path(__file__).resolve().parents[3] / "Dockerfile"


def test_the_image_bakes_the_models_the_code_loads_by_default():
    baked = DOCKERFILE.read_text(encoding="utf-8")
    for model in (
        DEFAULT_EMBEDDING_MODEL, DEFAULT_RERANKER_MODEL,
        GLINER25_ENGLISH_MODEL, GLINER25_MULTILINGUAL_MODEL,
    ):
        assert f'"{model}"' in baked, model
