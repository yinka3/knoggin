"""Docling's PDF models: one folder, checked against a committed manifest.

Docling downloads its layout models from each repository's newest commit and
its OCR files from plain URLs. Checking every file's SHA-256 against
docling_models.json makes any upstream change fail the download instead of
being used unnoticed. The Dockerfile copies this file on its own, so it uses
only the standard library.
"""

from __future__ import annotations

import hashlib
import json
import os
from pathlib import Path

MANIFEST = Path(__file__).with_name("docling_models.json")


def artifacts_path() -> Path:
    """Where Docling's models live; Docling is pointed here at runtime."""

    configured = os.getenv("DOCLING_ARTIFACTS_PATH")
    return Path(configured).expanduser() if configured else Path.home() / ".cache/knoggin/docling"


def file_hashes(root: Path) -> dict[str, str]:
    """SHA-256 of every model file, skipping download bookkeeping."""

    hashes = {}
    for path in sorted(root.rglob("*")):
        relative = path.relative_to(root)
        if not path.is_file() or ".cache" in relative.parts:
            continue
        digest = hashlib.sha256()
        with path.open("rb") as handle:
            for block in iter(lambda: handle.read(1 << 20), b""):
                digest.update(block)
        hashes[relative.as_posix()] = digest.hexdigest()
    return hashes


def verify(root: Path) -> None:
    """Raise ValueError unless the folder holds exactly the manifest's files."""

    expected = json.loads(MANIFEST.read_text(encoding="utf-8"))
    actual = file_hashes(root)
    problems = [f"missing {name}" for name in expected if name not in actual]
    problems += [f"unexpected {name}" for name in actual if name not in expected]
    problems += [
        f"changed {name}" for name, value in actual.items()
        if name in expected and expected[name] != value
    ]
    if problems:
        raise ValueError(
            "Docling model files do not match docling_models.json: "
            + "; ".join(problems[:10])
            + ". If you upgraded Docling on purpose, rerun prefetch_models.py "
            "with --update-docling-manifest after testing."
        )


def write_manifest(root: Path) -> None:
    MANIFEST.write_text(json.dumps(file_hashes(root), indent=2) + "\n", encoding="utf-8")
