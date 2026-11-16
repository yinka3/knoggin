import json

import pytest

from common import docling_models


def test_any_changed_missing_or_extra_model_file_is_refused(tmp_path, monkeypatch):
    root = tmp_path / "models"
    (root / "layout").mkdir(parents=True)
    (root / "layout" / "model.onnx").write_bytes(b"weights")
    (root / "ocr.txt").write_bytes(b"dict")
    (root / "layout" / ".cache").mkdir()
    (root / "layout" / ".cache" / "download.metadata").write_text("timestamp")
    manifest = tmp_path / "docling_models.json"
    monkeypatch.setattr(docling_models, "MANIFEST", manifest)

    docling_models.write_manifest(root)
    assert set(json.loads(manifest.read_text())) == {"layout/model.onnx", "ocr.txt"}
    docling_models.verify(root)  # download bookkeeping does not count

    (root / "layout" / "model.onnx").write_bytes(b"swapped")
    with pytest.raises(ValueError, match="changed layout/model.onnx"):
        docling_models.verify(root)
    (root / "layout" / "model.onnx").write_bytes(b"weights")
    (root / "extra.py").write_text("print('hi')")
    with pytest.raises(ValueError, match="unexpected extra.py"):
        docling_models.verify(root)
    (root / "extra.py").unlink()
    (root / "ocr.txt").unlink()
    with pytest.raises(ValueError, match="missing ocr.txt"):
        docling_models.verify(root)
