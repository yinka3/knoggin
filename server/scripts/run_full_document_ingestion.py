#!/usr/bin/env python3
"""Run one isolated, full-document ingestion and retrieval evaluation.

The runner creates a disposable PostgreSQL database, uses production Docling
parsing and embedding services, submits one PDF through ``DocumentService``,
checks durable output, runs fixed retrieval queries, and writes a JSON report.
Model downloads are disabled by default so a run cannot quietly depend on the
network. Use ``scripts/prefetch_models.py`` first or pass
``--allow-model-downloads`` explicitly.
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import re
import shutil
import sys
import tempfile
import threading
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from uuid import uuid4

import psutil
import psycopg
from dotenv import load_dotenv
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from pypdf import PdfReader

SERVER_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SERVER_ROOT.parent
sys.path.insert(0, str(SERVER_ROOT / "src"))

from common.embedding_contract import (  # noqa: E402
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_RERANKER_MODEL,
    EMBEDDING_DIMENSION,
)

DEFAULT_DATABASE_URL = "postgresql://knoggin:knoggin@127.0.0.1:5432/knoggin_db"
DEFAULT_QUERIES = (
    "How do B-tree indexes organize and locate data?",
    "What is write-ahead logging and why is it used?",
    "How do distributed systems handle replication and consistency?",
)


@dataclass
class PeakMemoryMonitor:
    """Sample server and child-process resident memory while work runs."""

    interval_seconds: float = 0.1

    def __post_init__(self) -> None:
        self._process = psutil.Process(os.getpid())
        self._stop = threading.Event()
        self._thread: threading.Thread | None = None
        self._lock = threading.Lock()
        self._current_phase: str | None = None
        self._phase_start_rss_bytes = 0
        self._phase_peak_rss_bytes = 0
        self._phase_stats: dict[str, dict[str, int]] = {}
        self.peak_rss_bytes = self._process.memory_info().rss

    def _sample_rss(self) -> int:
        rss = self._process.memory_info().rss
        for child in self._process.children(recursive=True):
            try:
                rss += child.memory_info().rss
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                continue
        with self._lock:
            self.peak_rss_bytes = max(self.peak_rss_bytes, rss)
            if self._current_phase is not None:
                self._phase_peak_rss_bytes = max(self._phase_peak_rss_bytes, rss)
        return rss

    def start_phase(self, name: str) -> None:
        """Finish the current phase and begin a named memory interval."""
        if not isinstance(name, str) or not name.strip():
            raise ValueError("memory phase name must not be blank")
        rss = self._sample_rss()
        with self._lock:
            if self._current_phase is not None:
                self._phase_stats[self._current_phase] = {
                    "start_rss_bytes": self._phase_start_rss_bytes,
                    "end_rss_bytes": rss,
                    "peak_rss_bytes": self._phase_peak_rss_bytes,
                }
            self._current_phase = name
            self._phase_start_rss_bytes = rss
            self._phase_peak_rss_bytes = rss

    def start(self) -> None:
        def sample() -> None:
            while not self._stop.wait(self.interval_seconds):
                self._sample_rss()

        self._thread = threading.Thread(target=sample, daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop.set()
        if self._thread is not None:
            self._thread.join()
        rss = self._sample_rss()
        with self._lock:
            if self._current_phase is not None:
                self._phase_stats[self._current_phase] = {
                    "start_rss_bytes": self._phase_start_rss_bytes,
                    "end_rss_bytes": rss,
                    "peak_rss_bytes": self._phase_peak_rss_bytes,
                }
                self._current_phase = None

    def phase_stats_mib(self) -> dict[str, dict[str, float]]:
        """Return completed phase RSS measurements in MiB."""
        mib = 1024 * 1024
        with self._lock:
            return {
                name: {
                    "start_rss_mib": round(values["start_rss_bytes"] / mib, 1),
                    "end_rss_mib": round(values["end_rss_bytes"] / mib, 1),
                    "peak_rss_mib": round(values["peak_rss_bytes"] / mib, 1),
                }
                for name, values in self._phase_stats.items()
            }


def _database_url(source_url: str, database: str) -> str:
    params = conninfo_to_dict(source_url)
    params["dbname"] = database
    params.setdefault("connect_timeout", "5")
    return make_conninfo(**params)


def _model_slug(model: str) -> str:
    slug = re.sub(r"[^a-z0-9]+", "-", model.casefold()).strip("-")
    return slug or "model"


def _default_output(pdf: Path, embedding_model: str) -> Path:
    stamp = time.strftime("%Y%m%d-%H%M%S")
    return REPOSITORY_ROOT / "doc_testing" / "reports" / (
        f"{pdf.stem}-{_model_slug(embedding_model)}-{stamp}.json"
    )


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("pdf", type=Path, help="PDF to ingest in full")
    parser.add_argument(
        "--database-url",
        default=os.getenv("TEST_DATABASE_URL", DEFAULT_DATABASE_URL),
        help="PostgreSQL server used to create the isolated database",
    )
    parser.add_argument(
        "--embedding-model",
        default=os.getenv("EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
    )
    parser.add_argument(
        "--reranker-model",
        default=os.getenv("RERANKER_MODEL", DEFAULT_RERANKER_MODEL),
    )
    parser.add_argument(
        "--embedding-backend",
        choices=("onnx", "torch"),
        default=os.getenv("EMBEDDING_BACKEND", "onnx"),
    )
    parser.add_argument(
        "--onnx-provider",
        default=os.getenv("ONNX_PROVIDER", "auto"),
    )
    parser.add_argument(
        "--extraction-mode",
        choices=("standard", "ocr"),
        default="standard",
        help="PDF extraction mode; OCR is slower and uses more memory",
    )
    parser.add_argument(
        "--extraction-strategy",
        choices=("auto", "full", "bounded"),
        default="auto",
        help="PDF sizing strategy; auto bypasses grouping at 100 pages or fewer",
    )
    parser.add_argument(
        "--query",
        action="append",
        dest="queries",
        help="Retrieval query; repeat for multiple queries",
    )
    parser.add_argument("--results-per-query", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument(
        "--allow-model-downloads",
        action="store_true",
        help="Permit Hugging Face/Docling downloads during the run",
    )
    parser.add_argument(
        "--keep-artifacts",
        action="store_true",
        help="Keep the isolated database and project directory",
    )
    args = parser.parse_args(argv)
    args.pdf = args.pdf.expanduser().resolve()
    if not args.pdf.is_file():
        parser.error(f"PDF does not exist: {args.pdf}")
    if args.pdf.suffix.casefold() != ".pdf":
        parser.error("the full ingestion runner accepts a PDF")
    if not 1 <= args.results_per_query <= 50:
        parser.error("--results-per-query must be between 1 and 50")
    args.queries = tuple(args.queries or DEFAULT_QUERIES)
    args.output = (args.output or _default_output(args.pdf, args.embedding_model)).resolve()
    return args


def _create_database(source_url: str, database: str) -> str:
    admin_url = _database_url(source_url, "postgres")
    database_url = _database_url(source_url, database)
    schema_text = (SERVER_ROOT / "src" / "infrastructure" / "schema.sql").read_text(
        encoding="utf-8"
    )
    with psycopg.connect(admin_url, autocommit=True) as admin:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    try:
        with psycopg.connect(database_url, autocommit=True) as connection:
            connection.execute("CREATE EXTENSION vector")
            connection.execute(schema_text)
    except BaseException:
        _drop_database(source_url, database)
        raise
    return database_url


def _drop_database(source_url: str, database: str) -> None:
    with psycopg.connect(_database_url(source_url, "postgres"), autocommit=True) as admin:
        admin.execute(
            "SELECT pg_terminate_backend(pid) FROM pg_stat_activity "
            "WHERE datname = %s AND pid <> pg_backend_pid()",
            (database,),
        )
        admin.execute(sql.SQL("DROP DATABASE IF EXISTS {}").format(sql.Identifier(database)))


def _page_summary(page: dict[str, Any]) -> dict[str, Any]:
    methods: dict[str, int] = {}
    for region in page.get("regions", []):
        method = str(region.get("extraction_method", "unknown"))
        methods[method] = methods.get(method, 0) + 1
    text = str(page.get("text", ""))
    return {
        "page_number": page.get("page_number"),
        "characters": len(text),
        "text_sha256": hashlib.sha256(text.encode()).hexdigest(),
        "region_count": len(page.get("regions", [])),
        "extraction_methods": dict(sorted(methods.items())),
    }


def _retrieval_result(row: dict[str, Any]) -> dict[str, Any]:
    content = str(row.get("content", ""))
    return {
        "document_id": str(row.get("document_id")),
        "snapshot_id": str(row.get("snapshot_id")),
        "relative_path": row.get("relative_path"),
        "page_number": row.get("page_number"),
        "chunk_kind": row.get("chunk_kind"),
        "locator": row.get("locator"),
        "content_preview": content[:500],
        "content_sha256": hashlib.sha256(content.encode()).hexdigest(),
    }


def _configure_model_downloads(*, allowed: bool) -> None:
    """Make the CLI switch authoritative over inherited offline settings."""
    if allowed:
        os.environ.pop("HF_HUB_OFFLINE", None)
        os.environ.pop("TRANSFORMERS_OFFLINE", None)
        return
    os.environ["HF_HUB_OFFLINE"] = "1"
    os.environ["TRANSFORMERS_OFFLINE"] = "1"


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
    from core.knowledge.documents import DocumentService, ProjectFilesystemFactory
    from core.knowledge.documents.extraction import inspect_pdf_ocr_need
    from core.knowledge.embedding_service import EmbeddingService
    from infrastructure.postgres_client import PostgresClient

    _configure_model_downloads(allowed=args.allow_model_downloads)
    os.environ["ONNX_PROVIDER"] = args.onnx_provider

    database = f"knoggin_full_ingestion_{uuid4().hex[:12]}"
    project_id = f"full-ingestion-{uuid4().hex[:12]}"
    user_name = "full_ingestion_runner"
    project_root = Path(tempfile.mkdtemp(prefix="knoggin-full-ingestion-"))
    monitor = PeakMemoryMonitor()
    report: dict[str, Any] = {
        "status": "running",
        "source": {
            "path": str(args.pdf),
            "bytes": args.pdf.stat().st_size,
            "sha256": hashlib.sha256(args.pdf.read_bytes()).hexdigest(),
            "expected_pages": len(PdfReader(args.pdf).pages),
        },
        "models": {
            "embedding": args.embedding_model,
            "reranker": args.reranker_model,
            "embedding_backend": args.embedding_backend,
            "onnx_provider_requested": args.onnx_provider,
            "device": "cpu",
            "downloads_allowed": args.allow_model_downloads,
        },
        "extraction": {
            "mode": args.extraction_mode,
            "strategy": args.extraction_strategy,
            "ocr_enabled": args.extraction_mode == "ocr",
            "docling_cpu_threads": 4,
            "process_isolated": True,
        },
        "isolation": {
            "database": database,
            "project_id": project_id,
            "project_root": str(project_root),
            "artifacts_retained": args.keep_artifacts,
        },
    }
    ocr_preflight = inspect_pdf_ocr_need(args.pdf.read_bytes())
    report["extraction"]["ocr_preflight"] = {
        "page_count": ocr_preflight.page_count,
        "low_text_page_count": ocr_preflight.low_text_page_count,
        "scanned_page_count": ocr_preflight.scanned_page_count,
        "ocr_recommended": ocr_preflight.ocr_recommended,
        "reason": ocr_preflight.reason,
    }
    if ocr_preflight.ocr_recommended and args.extraction_mode != "ocr":
        print(
            f"Warning: OCR is recommended before ingestion: {ocr_preflight.reason}",
            file=sys.stderr,
        )
    timings: dict[str, float] = {}
    started = time.perf_counter()
    database_created = False
    client: PostgresClient | None = None
    embedding: EmbeddingService | None = None
    monitor.start_phase("setup")
    monitor.start()
    try:
        database_url = _create_database(args.database_url, database)
        database_created = True
        client = PostgresClient(database_url, min_size=1, max_size=2)
        embedding = EmbeddingService(
            embedding_model=args.embedding_model,
            reranker_model=args.reranker_model,
            device="cpu",
            embedding_backend=args.embedding_backend,
        )
        await client.connect()
        await client.execute(
            """
            INSERT INTO public.projects (project_id, user_name, name, domain_config)
            VALUES (%s, %s, %s, %s::jsonb)
            """,
            (project_id, user_name, "Full ingestion evaluation", '{"version": 1}'),
        )

        monitor.start_phase("embedding_model_load")
        phase = time.perf_counter()
        await embedding.load_models()
        timings["embedding_model_load_seconds"] = time.perf_counter() - phase
        if embedding.embedding_dim != EMBEDDING_DIMENSION:
            raise RuntimeError(
                "embedding model produced "
                f"{embedding.embedding_dim} dimensions; expected {EMBEDDING_DIMENSION}"
            )
        await EmbeddingRebuilder(client, embedding).ensure_configuration()
        report["models"].update(
            {
                "embedding_dimensions": embedding.embedding_dim,
                "configuration_fingerprint": embedding.configuration_fingerprint,
                "onnx_provider_resolved": embedding.onnx_provider,
            }
        )

        monitor.start_phase("indexing_setup")
        service = DocumentService(
            project_id=project_id,
            postgres_client=client,
            embedding_service=embedding,
            readable_project_ids=[project_id],
            filesystem_factory=ProjectFilesystemFactory(project_root),
            document_rerank_enabled=True,
        )
        content = args.pdf.read_bytes()
        monitor.start_phase("submission_and_indexing")
        phase = time.perf_counter()
        document = await service.submit_document(
            content=content,
            original_name=args.pdf.name,
            extraction_mode=args.extraction_mode,
            extraction_strategy=args.extraction_strategy,
        )
        timings["submission_and_indexing_seconds"] = time.perf_counter() - phase
        if document["status"] != "indexed":
            raise RuntimeError(
                "document did not reach indexed status: "
                f"{document['status']} ({document.get('error_message')})"
            )

        monitor.start_phase("verification")
        snapshot_row = await client.fetch_one(
            """
            SELECT parser_name, parser_version, parser_fingerprint, snapshot
            FROM public.document_parse_snapshots
            WHERE snapshot_id = %s AND document_id = %s
            """,
            (document["current_snapshot_id"], document["document_id"]),
        )
        if snapshot_row is None:
            raise RuntimeError("indexed document has no current parse snapshot")
        snapshot = snapshot_row["snapshot"]
        if isinstance(snapshot, str):
            snapshot = json.loads(snapshot)
        pages = list(snapshot.get("pages", []))
        expected_pages = report["source"]["expected_pages"]
        if len(pages) != expected_pages:
            raise RuntimeError(
                f"parse snapshot has {len(pages)} pages; expected {expected_pages}"
            )
        snapshot_structure = snapshot.get("structure", {})
        if snapshot_structure.get("schema_name") == "KnogginGroupedDoclingDocument":
            report["extraction"].update(
                {
                    "page_group_target": snapshot_structure.get("page_group_target"),
                    "grouping_method": snapshot_structure.get("grouping_method"),
                    "page_groups": [
                        {
                            "start_page": group.get("start_page"),
                            "end_page": group.get("end_page"),
                        }
                        for group in snapshot_structure.get("groups", [])
                    ],
                }
            )

        chunk_stats = await client.fetch_one(
            """
            SELECT count(*)::integer AS chunks,
                   count(*) FILTER (WHERE page_number IS NOT NULL)::integer AS located_chunks,
                   min(vector_dims(embedding))::integer AS min_dimensions,
                   max(vector_dims(embedding))::integer AS max_dimensions
            FROM public.document_chunks
            WHERE document_id = %s
            """,
            (document["document_id"],),
        )
        selected_pages = sorted({1, (expected_pages + 1) // 2, expected_pages})
        located_pages = await client.fetch_all(
            """
            SELECT page_number, count(*)::integer AS chunks,
                   count(*) FILTER (WHERE layout_region IS NOT NULL)::integer AS locators
            FROM public.document_chunks
            WHERE document_id = %s AND page_number = ANY(%s)
            GROUP BY page_number
            ORDER BY page_number
            """,
            (document["document_id"], selected_pages),
        )
        located_by_page = {int(row["page_number"]): row for row in located_pages}
        missing = [page for page in selected_pages if page not in located_by_page]
        if missing:
            raise RuntimeError(f"selected pages have no stored chunks: {missing}")
        if any(int(located_by_page[page]["locators"]) < 1 for page in selected_pages):
            raise RuntimeError("selected page chunks are missing layout locators")
        if (
            chunk_stats["min_dimensions"] != EMBEDDING_DIMENSION
            or chunk_stats["max_dimensions"] != EMBEDDING_DIMENSION
        ):
            raise RuntimeError(f"stored embedding dimensions are invalid: {chunk_stats}")

        monitor.start_phase("retrieval")
        phase = time.perf_counter()
        retrieval = []
        for query in args.queries:
            rows = await service.search(query, n_results=args.results_per_query)
            if not rows:
                raise RuntimeError(f"retrieval returned no results for query: {query}")
            retrieval.append(
                {
                    "query": query,
                    "results": [_retrieval_result(dict(row)) for row in rows],
                }
            )
        timings["retrieval_seconds"] = time.perf_counter() - phase

        monitor.start_phase("report_assembly")
        report.update(
            {
                "status": "passed",
                "document": document,
                "snapshot": {
                    "parser_name": snapshot_row["parser_name"],
                    "parser_version": snapshot_row["parser_version"],
                    "parser_fingerprint": snapshot_row["parser_fingerprint"],
                    "page_count": len(pages),
                    "selected_pages": [
                        _page_summary(pages[page - 1]) for page in selected_pages
                    ],
                },
                "chunks": dict(chunk_stats),
                "selected_page_chunks": [dict(row) for row in located_pages],
                "retrieval": retrieval,
            }
        )
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
        try:
            timings["total_seconds"] = time.perf_counter() - started
            report["timings"] = {key: round(value, 3) for key, value in timings.items()}
            monitor.stop()
            report["peak_rss_mib"] = round(monitor.peak_rss_bytes / 1024 / 1024, 1)
            report["memory_phases"] = monitor.phase_stats_mib()
            if embedding is not None:
                embedding.cleanup()
            if client is not None:
                await client.close()
            args.output.parent.mkdir(parents=True, exist_ok=True)
            args.output.write_text(
                json.dumps(report, indent=2, default=str) + "\n",
                encoding="utf-8",
            )
        finally:
            # Remove artifacts even when an earlier cleanup step fails.
            if not args.keep_artifacts and database_created:
                _drop_database(args.database_url, database)
            if not args.keep_artifacts:
                shutil.rmtree(project_root, ignore_errors=True)
    return report


def main(argv: list[str] | None = None) -> int:
    load_dotenv(REPOSITORY_ROOT / ".env")
    args = _parse_args(argv)
    try:
        report = asyncio.run(_run(args))
    except BaseException as exc:
        print(f"Full ingestion failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(f"Report written to {args.output}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, default=str))
    print(f"Report written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
