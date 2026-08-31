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
        self._workspace_source_tasks: dict[str, asyncio.Task] = {}
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

    async def index_document(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> Dict:
        """Claim, derive, and atomically publish one document's index."""

        policy = policy or self._policy
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
        )
        metadata = rows[0] if rows else None
        if metadata is None:
            raise FileNotFoundError("Document not found")
        if metadata["status"] == "indexed":
            return metadata

        claimed = await self._writer.transition_index_status(
            document_id=document_id,
            session_id=session_id,
            status="indexing",
            allowed_statuses=("queued", "failed"),
            updated_at=get_now_iso(),
        )
        if claimed is None:
            refreshed = await self._reader.fetch_documents_by_reference(
                document_id=document_id,
                relative_path=None,
                session_id=session_id,
            )
            if not refreshed:
                raise FileNotFoundError("Document not found")
            return refreshed[0]

        try:
            async with self._index_claim(
                document_id=document_id,
                session_id=session_id,
            ):
                raw_bytes = await self._source_bytes(claimed, session_id=session_id)
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
                    session_id=session_id,
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
                        session_id=session_id,
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
        *,
        session_id: Optional[str],
    ) -> bytes | None:
        """Read current manual-upload bytes from the local project tree.

        Folder and managed-workspace sources are still migrated in later
        commits, so they retain their existing durable source route for now.
        """
        if (
            self._filesystem is not None
            and document.get("source_kind") == "manual_upload"
            and document.get("visibility_scope") == "project"
        ):
            return await self._run_blocking(
                self._filesystem.read_bytes,
                document["relative_path"],
            )
        return await self._reader.fetch_document_content(
            document_id=str(document["document_id"]),
            session_id=session_id,
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
        session_id: Optional[str],
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
                session_id=session_id,
                error_message=detail[:MAX_ERROR_MESSAGE_LENGTH],
                updated_at=get_now_iso(),
            )
            raise

    async def schedule_document_index(
        self,
        *,
        document_id: str,
        session_id: Optional[str] = None,
    ) -> Dict:
        """Admit one durable document into inline or bounded background work."""
        if self._stopping:
            raise RuntimeError("Document indexer is stopping")
        rows = await self._reader.fetch_documents_by_reference(
            document_id=document_id,
            relative_path=None,
            session_id=session_id,
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
                session_id=session_id,
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
                session_id=session_id,
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
                    session_id=session_id,
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

    def queue_workspace_source_indexing(
        self,
        *,
        source_id: str,
        session_id: Optional[str] = None,
    ) -> None:
        """Queue one coalesced, bounded batch for a workspace source."""
        if self._stopping:
            return
        self._submit_workspace_source_batch(
            source_id=source_id,
            session_id=session_id,
        )

    def _submit_workspace_source_batch(
        self,
        *,
        source_id: str,
        session_id: Optional[str],
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> None:
        if self._background_work is None:
            return
        existing = self._workspace_source_tasks.get(source_id)
        if existing is not None and not existing.done():
            return
        policy = policy or self._policy
        task = asyncio.create_task(
            self._background_work.submit(
                self.project_id,
                lambda: self._index_workspace_source_batch(
                    source_id=source_id,
                    session_id=session_id,
                    policy=policy,
                ),
                name="workspace-source-index",
                coalesce_key=f"workspace-source-index:{source_id}",
            ),
            name=f"workspace-source-index:{self.project_id}:{source_id}",
        )
        self._background_tasks.add(task)
        self._workspace_source_tasks[source_id] = task
        task.add_done_callback(
            lambda completed: self._observe_workspace_source_batch(
                completed,
                source_id=source_id,
                session_id=session_id,
                policy=policy,
            )
        )

    @asynccontextmanager
    async def _workspace_index_claim(
        self,
        *,
        claimed: List[Dict],
        session_id: Optional[str],
    ) -> AsyncIterator[None]:
        document_ids = [str(document["document_id"]) for document in claimed]
        try:
            yield
        except asyncio.CancelledError:
            await self._release_index_claims(document_ids)
            raise
        except Exception as exc:
            await asyncio.gather(
                *(
                    self._record_workspace_index_failure(
                        document_id=document_id,
                        session_id=session_id,
                        error=exc,
                    )
                    for document_id in document_ids
                )
            )
            raise

    async def _index_workspace_source_batch(
        self,
        *,
        source_id: str,
        session_id: Optional[str],
        policy: Optional[DocumentIndexPolicy] = None,
    ) -> bool:
        """Index one fair, cross-file batch of queued workspace documents."""
        policy = policy or self._policy
        claimed = await self._writer.claim_workspace_documents(
            source_id=source_id,
            limit=policy.workspace_document_batch_size,
            updated_at=get_now_iso(),
        )
        if not claimed:
            return False

        semaphore = asyncio.Semaphore(policy.workspace_prepare_concurrency)

        async def prepare_document(document: Dict):
            document_id = str(document["document_id"])
            try:
                async with semaphore:
                    raw_bytes = await self._reader.fetch_document_content(
                        document_id=document_id,
                        session_id=document["session_id"],
                    )
                    if raw_bytes is None:
                        raise FileNotFoundError("Document content is missing")
                    extraction = await self._run_blocking(
                        extract_and_split_document,
                        raw_bytes,
                        document["extension"],
                    )
            except Exception as exc:
                return document, None, None, exc
            return document, extraction.text, extraction.chunks, None

        async with self._workspace_index_claim(
            claimed=claimed,
            session_id=session_id,
        ):
            preparation_results = await asyncio.gather(
                *(prepare_document(document) for document in claimed)
            )
            prepared: list[tuple[Dict, str, List[DocumentChunk]]] = []
            for document, text, chunks, error in preparation_results:
                if error is not None:
                    await self._record_workspace_index_failure(
                        document_id=str(document["document_id"]),
                        session_id=session_id,
                        error=error,
                    )
                else:
                    prepared.append((document, text, chunks))

            try:
                all_chunks = [chunk for _, _, chunks in prepared for chunk in chunks]
                all_embedding_texts = [
                    embedding_text(chunk, document["relative_path"])
                    for document, _, chunks in prepared
                    for chunk in chunks
                ]
                embeddings = await self._encode_embeddings(
                    all_embedding_texts,
                    policy=policy,
                )
                self._validate_embeddings(embeddings, all_chunks)
            except Exception as exc:
                for document, _, _ in prepared:
                    await self._record_workspace_index_failure(
                        document_id=document["document_id"],
                        session_id=session_id,
                        error=exc,
                    )
            else:
                offset = 0
                indexed_documents = []
                for document, text, chunks in prepared:
                    next_offset = offset + len(chunks)
                    indexed_documents.append(
                        {
                            "document_id": str(document["document_id"]),
                            "relative_path": document["relative_path"],
                            "content_hash": document["content_hash"],
                            "extracted_text": text,
                            "chunks": chunks,
                            "embeddings": embeddings[offset:next_offset],
                        }
                    )
                    offset = next_offset
                try:
                    await self._writer.persist_workspace_indexed_documents(
                        documents=indexed_documents,
                        indexed_at=get_now_iso(),
                    )
                except Exception as exc:
                    for document in indexed_documents:
                        await self._record_workspace_index_failure(
                            document_id=document["document_id"],
                            session_id=session_id,
                            error=exc,
                        )
            return bool(await self._reader.count_queued_workspace_documents(source_id))

    def _observe_workspace_source_batch(
        self,
        task: asyncio.Task,
        *,
        source_id: str,
        session_id: Optional[str],
        policy: DocumentIndexPolicy,
    ) -> None:
        self._background_tasks.discard(task)
        if self._workspace_source_tasks.get(source_id) is task:
            del self._workspace_source_tasks[source_id]
        if task.cancelled():
            return
        try:
            has_more = task.result()
        except BackgroundWorkRejected as exc:
            logger.warning("Workspace indexing remains queued: {}", exc.message)
        except Exception as exc:
            logger.error("Workspace indexing batch failed: {}", exc)
        else:
            if has_more and not self._stopping:
                self._submit_workspace_source_batch(
                    source_id=source_id,
                    session_id=session_id,
                    policy=policy,
                )
        self._request_drain()

    async def _record_workspace_index_failure(
        self,
        *,
        document_id: str,
        session_id: Optional[str],
        error: Exception,
    ) -> None:
        error_message = str(error).strip() or type(error).__name__
        try:
            await self._writer.record_index_failure(
                document_id=document_id,
                session_id=session_id,
                error_message=error_message[:MAX_ERROR_MESSAGE_LENGTH],
                updated_at=get_now_iso(),
            )
        except Exception as failure_error:
            logger.error(
                "Failed to record workspace document indexing failure for {}: {}",
                document_id,
                failure_error,
            )

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
                session_id=document.get("session_id"),
            )
            submitted += 1
            if submitted >= limit:
                break

        workspace_sources = await self._reader.list_workspace_sources_for_index_recovery(
            limit + len(self._workspace_source_tasks)
        )
        for source in workspace_sources:
            source_id = str(source["source_id"])
            if source_id in self._workspace_source_tasks:
                continue
            self.queue_workspace_source_indexing(
                source_id=source_id,
                session_id=source.get("session_id"),
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
        await self.recover_pending_indexes()
        self._started = True
        self._request_drain()

    async def shutdown(self) -> None:
        """Cancel local submitters; cancellation requeues active durable claims."""
        self._stopping = True
        drain_task = self._drain_task
        self._drain_task = None
        self._drain_wakeup.set()
        tasks = [task for task in self._background_tasks if not task.done()]
        if drain_task is not None and not drain_task.done():
            tasks.append(drain_task)
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        self._background_tasks.clear()
        self._document_tasks.clear()
        self._workspace_source_tasks.clear()
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
        legacy = await self._reader.count_documents_for_index_recovery()
        workspace = await self._reader.count_workspace_documents_for_index_recovery()
        return legacy + workspace

    def indexing_snapshot(self) -> Dict:
        return {
            "inline_index_max_bytes": self._policy.inline_index_max_bytes,
            "embedding_chunk_batch_size": self._policy.embedding_chunk_batch_size,
            "workspace_document_batch_size": self._policy.workspace_document_batch_size,
            "workspace_prepare_concurrency": self._policy.workspace_prepare_concurrency,
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
