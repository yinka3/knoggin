from common.embedding_contract import DEFAULT_EMBEDDING_MODEL, DEFAULT_RERANKER_MODEL
from common.model_cache import PINNED_REVISIONS, model_revision
from core.ingestion.vp01 import GLINER25_ENGLISH_MODEL, GLINER25_MULTILINGUAL_MODEL


def test_every_default_model_is_pinned_to_a_full_commit():
    # The Dockerfile bakes exactly this list.
    assert set(PINNED_REVISIONS) == {
        DEFAULT_EMBEDDING_MODEL, DEFAULT_RERANKER_MODEL,
        GLINER25_ENGLISH_MODEL, GLINER25_MULTILINGUAL_MODEL,
    }
    assert all(len(revision) == 40 for revision in PINNED_REVISIONS.values())


def test_a_revision_setting_overrides_the_pin_and_other_models_stay_unpinned(monkeypatch):
    monkeypatch.delenv("EMBEDDING_MODEL_REVISION", raising=False)
    assert model_revision(DEFAULT_EMBEDDING_MODEL, "EMBEDDING_MODEL_REVISION") == (
        PINNED_REVISIONS[DEFAULT_EMBEDDING_MODEL]
    )
    assert model_revision("someone/else", "EMBEDDING_MODEL_REVISION") is None
    monkeypatch.setenv("EMBEDDING_MODEL_REVISION", "v2")
    assert model_revision(DEFAULT_EMBEDDING_MODEL, "EMBEDDING_MODEL_REVISION") == "v2"
