#!/usr/bin/env python3
"""Run isolated end-to-end ingestion and retrieval for one source-code file."""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import os
import shutil
import sys
import tempfile
import time
from pathlib import Path
from typing import Any
from uuid import uuid4

from dotenv import load_dotenv

SERVER_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SERVER_ROOT.parent
sys.path.insert(0, str(SERVER_ROOT / "src"))

from run_full_document_ingestion import (  # noqa: E402
    DEFAULT_DATABASE_URL,
    PeakMemoryMonitor,
    _configure_model_downloads,
    _create_database,
    _drop_database,
    _model_slug,
)

from common.embedding_contract import (  # noqa: E402
    DEFAULT_EMBEDDING_MODEL,
    DEFAULT_RERANKER_MODEL,
    EMBEDDING_DIMENSION,
)
from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder  # noqa: E402
from core.knowledge.documents import (  # noqa: E402
    DocumentService,
    ProjectFilesystemFactory,
)
from core.knowledge.documents.extraction import split_document  # noqa: E402
from core.knowledge.services.embedding_service import EmbeddingService  # noqa: E402
from infrastructure.postgres_client import PostgresClient  # noqa: E402

DEFAULT_QUERIES = (
    "wake_pending_indexes",
    "How does document indexing recover unfinished work after the server restarts?",
    "How does the indexing admission loop retry after a temporary database failure?",
)
EXPECTED_QUERY_TERMS = {
    "wake_pending_indexes": "def wake_pending_indexes",
    "How does document indexing recover unfinished work after the server restarts?": (
        "async def recover_pending_indexes"
    ),
    "How does the indexing admission loop retry after a temporary database failure?": (
        "Document index admission loop failed"
    ),
}


def _parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("source", type=Path)
    parser.add_argument(
        "--database-url",
        default=os.getenv("KNOGGIN_TEST_DATABASE_URL", DEFAULT_DATABASE_URL),
    )
    parser.add_argument(
        "--embedding-model",
        default=os.getenv("KNOGGIN_EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
    )
    parser.add_argument(
        "--reranker-model",
        default=os.getenv("KNOGGIN_RERANKER_MODEL", DEFAULT_RERANKER_MODEL),
    )
    parser.add_argument(
        "--embedding-backend",
        choices=("onnx", "torch"),
        default=os.getenv("KNOGGIN_EMBEDDING_BACKEND", "onnx"),
    )
    parser.add_argument(
        "--onnx-provider",
        default=os.getenv("KNOGGIN_ONNX_PROVIDER", "auto"),
    )
    parser.add_argument("--query", action="append", dest="queries")
    parser.add_argument(
        "--expected-term", action="append", dest="expected_terms",
        help="Expected source text for the corresponding --query (repeat in query order)",
    )
    parser.add_argument("--results-per-query", type=int, default=5)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--allow-model-downloads", action="store_true")
    parser.add_argument("--keep-artifacts", action="store_true")
    args = parser.parse_args(argv)
    args.source = args.source.expanduser().resolve()
    if not args.source.is_file():
        parser.error(f"source file does not exist: {args.source}")
    if not 1 <= args.results_per_query <= 50:
        parser.error("--results-per-query must be between 1 and 50")
    args.queries = tuple(args.queries or DEFAULT_QUERIES)
    if args.expected_terms and len(args.expected_terms) != len(args.queries):
        parser.error("provide one --expected-term per --query")
    args.expected_terms = tuple(args.expected_terms or ())
    if args.output is None:
        stamp = time.strftime("%Y%m%d-%H%M%S")
        args.output = REPOSITORY_ROOT / "doc_testing" / "reports" / (
            f"code-{args.source.stem}-{_model_slug(args.embedding_model)}-{stamp}.json"
        )
    args.output = args.output.resolve()
    return args


def _result(row: dict[str, Any]) -> dict[str, Any]:
    content = str(row.get("content", ""))
    return {
        "relative_path": row.get("relative_path"),
        "symbol_name": row.get("symbol_name"),
        "language": row.get("language"),
        "chunk_kind": row.get("chunk_kind"),
        "locator": row.get("locator"),
        "start_line": row.get("start_line"),
        "end_line": row.get("end_line"),
        "score": row.get("score"),
        "content_preview": content[:800],
        "content_sha256": hashlib.sha256(content.encode()).hexdigest(),
    }


async def _run(args: argparse.Namespace) -> dict[str, Any]:
    _configure_model_downloads(allowed=args.allow_model_downloads)
    os.environ["KNOGGIN_ONNX_PROVIDER"] = args.onnx_provider
    database = f"knoggin_code_retrieval_{uuid4().hex[:12]}"
    project_id = f"code-retrieval-{uuid4().hex[:12]}"
    user_name = "code_retrieval_runner"
    project_root = Path(tempfile.mkdtemp(prefix="knoggin-code-retrieval-"))
    content = args.source.read_bytes()
    expected_language = split_document(
        content.decode("utf-8"), extension=args.source.suffix
    )[0].language
    report: dict[str, Any] = {
        "status": "running",
        "source": {
            "path": str(args.source),
            "bytes": len(content),
            "sha256": hashlib.sha256(content).hexdigest(),
        },
        "models": {
            "embedding": args.embedding_model,
            "reranker": args.reranker_model,
            "embedding_backend": args.embedding_backend,
            "onnx_provider_requested": args.onnx_provider,
        },
        "isolation": {
            "database": database,
            "project_id": project_id,
            "project_root": str(project_root),
            "artifacts_retained": args.keep_artifacts,
        },
    }
    monitor = PeakMemoryMonitor()
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
            (project_id, user_name, "Code retrieval evaluation", '{"version": 1}'),
        )

        monitor.start_phase("embedding_model_load")
        phase = time.perf_counter()
        await embedding.load_models()
        timings["embedding_model_load_seconds"] = time.perf_counter() - phase
        if embedding.embedding_dim != EMBEDDING_DIMENSION:
            raise RuntimeError(
                f"embedding model produced {embedding.embedding_dim} dimensions; "
                f"expected {EMBEDDING_DIMENSION}"
            )
        await EmbeddingRebuilder(client, embedding).ensure_configuration()
        report["models"].update(
            {
                "embedding_dimensions": embedding.embedding_dim,
                "configuration_fingerprint": embedding.configuration_fingerprint,
                "onnx_provider_resolved": embedding.onnx_provider,
            }
        )

        service = DocumentService(
            project_id=project_id,
            postgres_client=client,
            embedding_service=embedding,
            readable_project_ids=[project_id],
            filesystem_factory=ProjectFilesystemFactory(project_root),
            document_rerank_enabled=True,
        )
        monitor.start_phase("submission_and_indexing")
        phase = time.perf_counter()
        document = await service.submit_document(
            content=content,
            original_name=args.source.name,
        )
        timings["submission_and_indexing_seconds"] = time.perf_counter() - phase
        if document["status"] != "indexed":
            raise RuntimeError(
                f"document did not reach indexed status: {document['status']} "
                f"({document.get('error_message')})"
            )

        chunks = await client.fetch_all(
            """
            SELECT chunk_index, chunk_kind, language, symbol_name,
                   start_line, end_line, vector_dims(embedding) AS dimensions
            FROM public.document_chunks
            WHERE document_id = %s
            ORDER BY chunk_index
            """,
            (document["document_id"],),
        )
        if not chunks:
            raise RuntimeError("indexed source file produced no chunks")
        if any(row["dimensions"] != EMBEDDING_DIMENSION for row in chunks):
            raise RuntimeError("stored code embeddings have the wrong dimensions")
        if any(row["chunk_kind"] != "code" for row in chunks):
            raise RuntimeError("source file produced a non-code chunk")
        if any(row["language"] != expected_language for row in chunks):
            raise RuntimeError("Source chunks lost their language metadata")
        if any(row["start_line"] is None or row["end_line"] is None for row in chunks):
            raise RuntimeError("code chunks are missing line locators")

        monitor.start_phase("retrieval")
        phase = time.perf_counter()
        retrieval = []
        for query_index, query in enumerate(args.queries):
            rows = await service.search(
                query,
                n_results=args.results_per_query,
                relative_path=args.source.name,
            )
            if not rows:
                raise RuntimeError(f"retrieval returned no results for query: {query}")
            expected_term = (
                args.expected_terms[query_index]
                if args.expected_terms else EXPECTED_QUERY_TERMS.get(query)
            )
            expected_rank = next(
                (
                    rank
                    for rank, row in enumerate(rows, start=1)
                    if expected_term and expected_term in str(row.get("content", ""))
                ),
                None,
            )
            retrieval.append(
                {
                    "query": query,
                    "expected_term": expected_term,
                    "expected_match_rank": expected_rank,
                    "results": [_result(dict(row)) for row in rows],
                }
            )
        timings["retrieval_seconds"] = time.perf_counter() - phase
        report.update(
            {
                "status": "passed",
                "document": document,
                "chunks": [dict(row) for row in chunks],
                "retrieval": retrieval,
            }
        )
    except BaseException as exc:
        report["status"] = "failed"
        report["error"] = {"type": type(exc).__name__, "message": str(exc)}
        raise
    finally:
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
            json.dumps(report, indent=2, default=str) + "\n", encoding="utf-8"
        )
        if database_created and not args.keep_artifacts:
            _drop_database(args.database_url, database)
        if not args.keep_artifacts:
            shutil.rmtree(project_root)
    return report


def main(argv: list[str] | None = None) -> int:
    load_dotenv(REPOSITORY_ROOT / ".env")
    args = _parse_args(argv)
    try:
        report = asyncio.run(_run(args))
    except BaseException as exc:
        print(f"Code retrieval failed: {type(exc).__name__}: {exc}", file=sys.stderr)
        print(f"Report written to {args.output}", file=sys.stderr)
        return 1
    print(json.dumps(report, indent=2, default=str))
    print(f"Report written to {args.output}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
