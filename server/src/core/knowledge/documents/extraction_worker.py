"""Private subprocess entry point for disposable Docling extraction."""

from __future__ import annotations

import os
import pickle
import sys
from pathlib import Path

from common.exceptions import DependencyError
from core.knowledge.documents.extraction import extract_and_split_document


def main(argv: list[str]) -> int:
    if len(argv) != 4:
        return 2
    input_path, result_path, extension, extraction_mode = argv
    try:
        extraction = extract_and_split_document(
            Path(input_path).read_bytes(),
            extension,
            extraction_mode,
        )
        result: tuple[str, object] = ("ok", extraction)
    except DependencyError as exc:
        result = ("dependency_error", str(exc))
    except ValueError as exc:
        result = ("value_error", str(exc))
    except BaseException as exc:
        detail = str(exc).strip() or type(exc).__name__
        result = ("worker_error", detail)
    completed_path = Path(result_path)
    partial_path = completed_path.with_suffix(".partial")
    with partial_path.open("wb") as result_file:
        pickle.dump(result, result_file)
    partial_path.replace(completed_path)
    return 0


if __name__ == "__main__":
    os._exit(main(sys.argv[1:]))
