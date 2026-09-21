#!/usr/bin/env python3
"""Run a bounded, database-free PDF parser evaluation."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import threading
import time
from pathlib import Path

import psutil
from pypdf import PdfReader, PdfWriter

SERVER_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(SERVER_ROOT / "src"))

from core.knowledge.documents import storage  # noqa: E402

DEFAULT_LONG_PAGES = "1-40,95-114,370-389,745-764"


def parse_pages(spec: str, page_count: int) -> list[int]:
    pages: list[int] = []
    for part in spec.split(","):
        bounds = part.strip().split("-", 1)
        start = int(bounds[0])
        end = int(bounds[-1])
        if start < 1 or end < start or end > page_count:
            raise ValueError(f"invalid page range {part!r} for {page_count} pages")
        pages.extend(range(start, end + 1))
    return list(dict.fromkeys(pages))


def make_sample(source: Path, destination: Path, page_spec: str) -> list[int]:
    reader = PdfReader(source)
    pages = parse_pages(page_spec, len(reader.pages))
    writer = PdfWriter()
    for page_number in pages:
        writer.add_page(reader.pages[page_number - 1])
    destination.parent.mkdir(parents=True, exist_ok=True)
    with destination.open("wb") as stream:
        writer.write(stream)
    return pages


def evaluate(
    content: bytes,
    label: str,
    *,
    original_pages: list[int],
    output: Path,
) -> dict[str, object]:
    process = psutil.Process(os.getpid())
    peak_rss = process.memory_info().rss
    stop = threading.Event()

    def sample_memory() -> None:
        nonlocal peak_rss
        while not stop.wait(0.1):
            peak_rss = max(peak_rss, process.memory_info().rss)

    monitor = threading.Thread(target=sample_memory, daemon=True)
    monitor.start()
    started = time.perf_counter()
    try:
        extraction = storage.extract_and_split_document(content, ".pdf")
    finally:
        elapsed = time.perf_counter() - started
        stop.set()
        monitor.join()

    snapshot = extraction.snapshot
    if snapshot is None:
        raise RuntimeError("PDF extraction did not return a parse snapshot")
    methods: dict[str, int] = {}
    region_count = 0
    page_results = []
    page_text_directory = output / f"page-text-{label}"
    page_text_directory.mkdir(parents=True, exist_ok=True)
    for page in snapshot.pages:
        page_methods: dict[str, int] = {}
        element_types: dict[str, int] = {}
        for region in page.regions:
            region_count += 1
            methods[region.extraction_method] = methods.get(region.extraction_method, 0) + 1
            page_methods[region.extraction_method] = (
                page_methods.get(region.extraction_method, 0) + 1
            )
            element_types[region.element_type] = element_types.get(region.element_type, 0) + 1
        sample_page = page.page_number
        original_page = (
            original_pages[sample_page - 1]
            if 1 <= sample_page <= len(original_pages)
            else None
        )
        page_results.append(
            {
                "sample_page": sample_page,
                "original_page": original_page,
                "characters": len(page.text),
                "text_sha256": hashlib.sha256(page.text.encode()).hexdigest(),
                "extraction_methods": dict(sorted(page_methods.items())),
                "element_types": dict(sorted(element_types.items())),
            }
        )
        (page_text_directory / f"original-page-{original_page}.md").write_text(
            page.text + "\n",
            encoding="utf-8",
        )
    return {
        "label": label,
        "seconds": round(elapsed, 3),
        "peak_rss_mib": round(peak_rss / 1024 / 1024, 1),
        "pages": len(snapshot.pages),
        "regions": region_count,
        "chunks": len(extraction.chunks),
        "characters": len(extraction.text),
        "text_sha256": hashlib.sha256(extraction.text.encode()).hexdigest(),
        "extraction_methods": dict(sorted(methods.items())),
        "page_results": page_results,
        "page_text_directory": str(page_text_directory),
    }


def install_pdf_converter(*, table_structure: bool) -> None:
    """Install an evaluation-only converter without changing production config."""

    from docling.datamodel.accelerator_options import (
        AcceleratorDevice,
        AcceleratorOptions,
    )
    from docling.datamodel.base_models import InputFormat
    from docling.datamodel.pipeline_options import PdfPipelineOptions
    from docling.document_converter import DocumentConverter, PdfFormatOption

    options = PdfPipelineOptions(
        accelerator_options=AcceleratorOptions(
            device=AcceleratorDevice.CPU,
            num_threads=2,
        ),
        do_ocr=True,
        do_table_structure=table_structure,
        do_code_enrichment=False,
        do_formula_enrichment=False,
        do_picture_description=False,
        do_picture_classification=False,
        enable_remote_services=False,
        allow_external_plugins=False,
    )
    converter = DocumentConverter(
        allowed_formats=[InputFormat.PDF],
        format_options={InputFormat.PDF: PdfFormatOption(pipeline_options=options)},
    )
    storage._docling_converter = lambda: converter


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", type=Path, help="long source PDF")
    parser.add_argument("--pages", default=DEFAULT_LONG_PAGES)
    parser.add_argument("--output", type=Path, default=Path("/tmp/knoggin-parser-evaluation"))
    parser.add_argument("--skip-warm", action="store_true")
    parser.add_argument("--disable-table-structure", action="store_true")
    args = parser.parse_args()

    source = args.pdf.resolve()
    if not source.is_file():
        parser.error(f"PDF does not exist: {source}")
    args.output.mkdir(parents=True, exist_ok=True)
    table_structure = not args.disable_table_structure
    install_pdf_converter(table_structure=table_structure)
    sample = args.output / f"{source.stem}-sample.pdf"
    original_pages = make_sample(source, sample, args.pages)
    content = sample.read_bytes()
    report = {
        "source": str(source),
        "source_sha256": hashlib.sha256(source.read_bytes()).hexdigest(),
        "sample": str(sample),
        "sample_sha256": hashlib.sha256(content).hexdigest(),
        "original_pages": original_pages,
        "table_structure": table_structure,
        "runs": [
            evaluate(
                content,
                "cold",
                original_pages=original_pages,
                output=args.output,
            )
        ],
    }
    if not args.skip_warm:
        report["runs"].append(
            evaluate(
                content,
                "warm",
                original_pages=original_pages,
                output=args.output,
            )
        )
    report_path = args.output / f"{source.stem}-report.json"
    report_path.write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
    print(json.dumps(report, indent=2))
    print(f"Report written to {report_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
