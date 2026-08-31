"""Project-scoped durable document indexing execution."""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List, Optional

from loguru import logger

from common.schema.health import sanitize_health_details
from common.utils.time_utils import get_now_iso
from core.knowledge.db.readers.document_reader import DocumentReader
from core.knowledge.db.writers.document_writer import DocumentWriter
from core.knowledge.documents.constants import (
    EXPECTED_EMBEDDING_DIMENSION,
    MAX_ERROR_MESSAGE_LENGTH,
)
from core.knowledge.documents.filesystem import ProjectFilesystem
from core.knowledge.documents.policy import DocumentIndexPolicy
from core.knowledge.documents.storage import (
    DocumentChunk,
    embedding_text,
    extract_and_split_document,
)
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.background_work import (
    BackgroundWorkCoordinator,
    BackgroundWorkRejected,
)

BlockingRunner = Callable[..., Awaitable[Any]]

_DRAIN_BATCH_SIZE = 16
_DRAIN_RETRY_SECONDS = 0.1


class DocumentIndexer:
    """Owns extraction and atomic derived-data publication for one project."""

    def __init__(
        self,
        *,
        project_id: str,
        reader: DocumentReader,
        writer: DocumentWriter,
        embedding_service: EmbeddingService,
        policy: DocumentIndexPolicy,
        blocking_runner: BlockingRunner,
        background_work: Optional[BackgroundWorkCoordinator] = None,
        filesystem: ProjectFilesystem | None = None,
    ) -> None:
        self.project_id = project_id
        self._reader = reader
        self._writer = writer
        self._embedding = embedding_service
        self._policy = policy
        self._run_blocking = blocking_runner
        self._background_work = background_work
        self._filesystem = filesystem
        self._background_tasks: set[asyncio.Task] = set()
        self._document_tasks: dict[str, asyncio.Task] = {}
        self._reconciliation_callback: Callable[[], Awaitable[Dict]] | None = None
        self._reconciliation_interval_seconds: int | None = None
        self._reconciliation_task: asyncio.Task | None = None
        self._drain_task: asyncio.Task | None = None
        self._drain_wakeup = asyncio.Event()
        self._recovered_count = 0
        self._last_recovery_requeued = 0
        self._started = False
        self._stopping = False

    @property
    def policy(self) -> DocumentIndexPolicy:
        return self._policy

    def update_policy(self, policy: DocumentIndexPolicy) -> None:
        self._policy = policy

    def set_reconciliation_callback(
        self,
        callback: Callable[[], Awaitable[Dict]],
        *,
        interval_seconds: int,
    ) -> None:
        """Install the project-local reconciliation hook owned by DocumentService."""
        if (
            not isinstance(interval_seconds, int)
            or isinstance(interval_seconds, bool)
            or interval_seconds < 10
        ):
            raise ValueError("reconciliation interval must be at least 10 seconds")
        self._reconciliation_callback = callback
        self._reconciliation_interval_seconds = interval_seconds

    async def index_document(
        self,
        *,
        document_id: str,
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> Dict:
        """Claim, derive, and atomically publish one document's index."""

        policy = policy or self._policy
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=None,
        )
        metadata = rows[0] if rows else None
        if metadata is None:
            raise FileNotFoundError("Document not found")
        if metadata["status"] == "indexed":
            return metadata

        claimed = await self._writer.transition_index_status(
            document_id=document_id,
            status="indexing",
            allowed_statuses=("queued", "failed"),
            updated_at=get_now_iso(),
        )
        if claimed is None:
            refreshed = await self._reader.fetch_documents_by_reference(
                document_id=document_id,
                relative_path=None,
            )
            if not refreshed:
                raise FileNotFoundError("Document not found")
            return refreshed[0]

        try:
            async with self._index_claim(
                document_id=document_id,
            ):
                raw_bytes = await self._source_bytes(claimed)
                if raw_bytes is None:
                    raise FileNotFoundError("Document content is missing")
                extraction = await self._run_blocking(
                    extract_and_split_document,
                    raw_bytes,
                    claimed["extension"],
                )
                chunks = extraction.chunks
                embeddings = await self._encode_embeddings(
                    [
                        embedding_text(chunk, claimed["relative_path"])
                        for chunk in chunks
                    ],
                    policy=policy,
                )
                self._validate_embeddings(embeddings, chunks)
                row = await self._writer.persist_indexed_chunks(
                    document_id=document_id,
                    chunks=chunks,
                    embeddings=embeddings,
                    extracted_text=extraction.text,
                    indexed_at=get_now_iso(),
                    expected_content_hash=claimed["content_hash"],
                )
                if row is None:
                    refreshed = await self._reader.fetch_documents_by_reference(
                        document_id=document_id,
                        relative_path=None,
                    )
                    if not refreshed:
                        raise FileNotFoundError("Document not found")
                    return refreshed[0]
                return row
        except asyncio.CancelledError:
            raise
        except FileNotFoundError:
            raise
        except Exception as exc:
            detail = str(exc).strip() or type(exc).__name__
            raise RuntimeError(
                f"Failed to index document: {detail[:MAX_ERROR_MESSAGE_LENGTH]}"
            ) from exc

    async def _source_bytes(
        self,
        document: Dict,
    ) -> bytes | None:
        """Read current document bytes from the local project tree.

        All project documents currently resolve through the durable source route.
        """
        if self._filesystem is not None:
            return await self._run_blocking(
                self._filesystem.read_bytes,
                document["relative_path"],
            )
        return await self._reader.fetch_document_content(
            document_id=str(document["document_id"]),
        )

    @staticmethod
    def _validate_embeddings(
        embeddings: List[List[float]],
        chunks: List[DocumentChunk],
    ) -> None:
        if len(embeddings) != len(chunks):
            raise ValueError("Embedding count does not match chunk count")
        if any(
            len(embedding) != EXPECTED_EMBEDDING_DIMENSION
            for embedding in embeddings
        ):
            raise ValueError(
                "Document chunk embeddings must have exactly "
                f"{EXPECTED_EMBEDDING_DIMENSION} dimensions"
            )

    async def _encode_embeddings(
        self,
        values: List[str],
        *,
        policy: DocumentIndexPolicy,
    ) -> List[List[float]]:
        embeddings: List[List[float]] = []
        for start in range(0, len(values), policy.embedding_chunk_batch_size):
            embeddings.extend(
                await self._embedding.encode(
                    values[start : start + policy.embedding_chunk_batch_size]
                )
            )
        return embeddings

    @asynccontextmanager
    async def _index_claim(
        self,
        *,
        document_id: str,
    ) -> AsyncIterator[None]:
        try:
            yield
        except asyncio.CancelledError:
            await self._writer.requeue_index_claims(
                document_ids=[document_id],
                updated_at=get_now_iso(),
            )
            raise
        except Exception as exc:
            detail = str(exc).strip() or type(exc).__name__
            await self._writer.record_index_failure(
                document_id=document_id,
                error_message=detail[:MAX_ERROR_MESSAGE_LENGTH],
                updated_at=get_now_iso(),
            )
            raise

    async def schedule_document_index(
        self,
        *,
        document_id: str,
    ) -> Dict:
        """Admit one durable document into inline or bounded background work."""
        if self._stopping:
            raise RuntimeError("Document indexer is stopping")
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=None,
        )
        if not rows:
            raise FileNotFoundError("Document not found")
        document = rows[0]
        if document["status"] in {"indexed", "indexing"}:
            return document
        if document["status"] == "queued":
            queued = document
        else:
            queued = await self._writer.transition_index_status(
                document_id=document_id,
                status="queued",
                allowed_statuses=("failed",),
                updated_at=get_now_iso(),
            )
            if queued is None:
                return document

        if (
            document["size_bytes"] <= self._policy.inline_index_max_bytes
            or self._background_work is None
        ):
            return await self.index_document(
                document_id=document_id,
                policy=self._policy,
            )

        existing = self._document_tasks.get(document_id)
        if existing is not None and not existing.done():
            return queued

        task = asyncio.create_task(
            self._background_work.submit(
                self.project_id,
                lambda: self.index_document(
                    document_id=document_id,
                    policy=self._policy,
                ),
                name="document-index",
                coalesce_key=f"document-index:{document_id}",
            ),
            name=f"document-index:{self.project_id}:{document_id}",
        )
        self._background_tasks.add(task)
        self._document_tasks[document_id] = task
        task.add_done_callback(
            lambda completed: self._observe_background_task(
                completed,
                document_id=document_id,
            )
        )
        return queued

    async def _release_index_claims(self, document_ids: List[str]) -> None:
        if not document_ids:
            return
        try:
            await self._writer.requeue_index_claims(
                document_ids=document_ids,
                updated_at=get_now_iso(),
            )
        except Exception as exc:
            logger.error(
                "Failed to release interrupted document indexing claims for {}: {}",
                self.project_id,
                exc,
            )

    def _observe_background_task(
        self,
        task: asyncio.Task,
        *,
        document_id: str,
    ) -> None:
        self._background_tasks.discard(task)
        if self._document_tasks.get(document_id) is task:
            del self._document_tasks[document_id]
        if task.cancelled():
            return
        try:
            task.result()
        except BackgroundWorkRejected as exc:
            logger.warning("Document indexing remains queued: {}", exc.message)
        except Exception as exc:
            logger.error("Background document indexing failed: {}", exc)
        self._request_drain()

    def _request_drain(self) -> None:
        """Wake the project-local durable admission loop when runtime work changes."""
        if not self._started or self._stopping:
            return
        self._drain_wakeup.set()
        if self._drain_task is None or self._drain_task.done():
            self._drain_task = asyncio.create_task(
                self._drain_durable_work(),
                name=f"document-index-drain:{self.project_id}",
            )

    def wake_pending_indexes(self) -> None:
        """Request bounded durable admission after an external catalog update."""
        self._request_drain()

    async def _drain_durable_work(self) -> None:
        """Admit queued durable work in bounded batches until the project is clear."""
        try:
            while not self._stopping:
                await self._drain_wakeup.wait()
                self._drain_wakeup.clear()

                while not self._stopping:
                    await self._admit_durable_work(_DRAIN_BATCH_SIZE)
                    if await self.pending_index_count() == 0:
                        break
                    try:
                        await asyncio.wait_for(
                            self._drain_wakeup.wait(),
                            timeout=_DRAIN_RETRY_SECONDS,
                        )
                    except TimeoutError:
                        pass
                    self._drain_wakeup.clear()
        except asyncio.CancelledError:
            raise
        except Exception as exc:
            logger.error("Document index admission loop failed: {}", exc)

    async def _admit_durable_work(self, limit: int) -> int:
        """Submit one bounded durable slice, looking past local in-flight work."""
        pending = await self._reader.list_documents_for_index_recovery(
            limit + len(self._document_tasks)
        )
        submitted = 0
        for document in pending:
            document_id = str(document["document_id"])
            if document_id in self._document_tasks:
                continue
            await self.schedule_document_index(
                document_id=document_id,
            )
            submitted += 1
            if submitted >= limit:
                break

        return submitted

    async def start(self) -> None:
        """Recover durable index work when this project runtime becomes active."""
        if self._started:
            return
        self._stopping = False
        self._started = True
        await self._reconcile_project_files()
        await self.recover_pending_indexes()
        if self._reconciliation_callback is not None:
            self._reconciliation_task = asyncio.create_task(
                self._reconcile_periodically(),
                name=f"document-reconcile:{self.project_id}",
            )
        self._request_drain()

    async def _reconcile_periodically(self) -> None:
        assert self._reconciliation_interval_seconds is not None
        try:
            while not self._stopping:
                await asyncio.sleep(self._reconciliation_interval_seconds)
                if self._stopping:
                    return
                await self._reconcile_project_files()
        except asyncio.CancelledError:
            raise

    async def _reconcile_project_files(self) -> None:
        callback = self._reconciliation_callback
        if callback is None:
            return
        try:
            result = await callback()
        except Exception:
            logger.exception("Document filesystem reconciliation failed for {}", self.project_id)
            return
        if any(result.get(key, 0) for key in ("created", "changed", "deleted")):
            logger.info(
                "Document filesystem reconciliation for {}: created={}, changed={}, deleted={}",
                self.project_id,
                result.get("created", 0),
                result.get("changed", 0),
                result.get("deleted", 0),
            )
            self._request_drain()

    async def shutdown(self) -> None:
        """Cancel local submitters; cancellation requeues active durable claims."""
        self._stopping = True
        drain_task = self._drain_task
        self._drain_task = None
        reconciliation_task = self._reconciliation_task
        self._reconciliation_task = None
        self._drain_wakeup.set()
        tasks = [task for task in self._background_tasks if not task.done()]
        if drain_task is not None and not drain_task.done():
            tasks.append(drain_task)
        if reconciliation_task is not None and not reconciliation_task.done():
            tasks.append(reconciliation_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._document_tasks.clear()
        self._started = False

    async def recover_pending_indexes(self, limit: int = 16) -> int:
        """Resume durable queued work and repair claims left by a stopped runtime."""
        self._last_recovery_requeued = await self._writer.requeue_interrupted_indexes(
            updated_at=get_now_iso()
        )
        recovered = await self._admit_durable_work(limit)
        self._recovered_count += recovered
        if recovered or self._last_recovery_requeued:
            logger.info(
                "Document indexing recovery for {}: requeued={}, submitted={}",
                self.project_id,
                self._last_recovery_requeued,
                recovered,
            )
        return recovered

    async def pending_index_count(self) -> int:
        return await self._reader.count_documents_for_index_recovery()

    def indexing_snapshot(self) -> Dict:
        return {
            "inline_index_max_bytes": self._policy.inline_index_max_bytes,
            "embedding_chunk_batch_size": self._policy.embedding_chunk_batch_size,
            "local_submission_tasks": len(
                [task for task in self._background_tasks if not task.done()]
            ),
            "admission_loop_active": bool(
                self._drain_task is not None and not self._drain_task.done()
            ),
            "recovered_count": self._recovered_count,
            "last_recovery_requeued": self._last_recovery_requeued,
        }

    def indexing_snapshot_for_health(self) -> dict[str, object]:
        return sanitize_health_details(self.indexing_snapshot())
