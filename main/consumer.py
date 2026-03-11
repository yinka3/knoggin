import asyncio
import json
from typing import Awaitable, Callable, Dict, List, Optional
from loguru import logger
from db.store import MemGraphStore
import redis.asyncio as aioredis
from main.processor import BatchProcessor, BatchResult
from shared.utils.events import emit, emit_sync
from shared.infra.redis import RedisKeys


class BatchConsumer:

    def __init__(self, user_name: str, session_id: str, store: MemGraphStore, processor: BatchProcessor, redis: aioredis.Redis,
                get_session_context: Callable[[int, Optional[int]], Awaitable[List[Dict]]],
                run_session_jobs: Callable[[], Awaitable[None]],
                write_to_graph: Callable[[BatchResult], Awaitable[None]],
                batch_size: int = 8, batch_timeout: float =  360.0, 
                checkpoint_interval: int = 24, session_window: int = 18):
        
        self.user_name = user_name
        self.session_id = session_id
        self.store = store
        self.processor = processor
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.checkpoint_interval = checkpoint_interval
        self.session_window = session_window
        self.redis = redis

        # callbacks
        self.get_session_ctx = get_session_context
        self.run_session_jobs = run_session_jobs
        self.write_to_graph = write_to_graph

        self._wake_event = asyncio.Event()
        self._shutdown_requested = False
        self._task: Optional[asyncio.Task] = None
        self._flush_future = None
    

    @property
    def _buffer_key(self) -> str:
        return RedisKeys.buffer(self.user_name, self.session_id)

    @property
    def _checkpoint_key(self) -> str:
        return RedisKeys.checkpoint(self.user_name, self.session_id)
    

    def start(self):
        if self._task is not None:
            logger.warning("BatchConsumer already running")
            return
        
        self._shutdown_requested = False
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_task_done)

        emit_sync(self.session_id, "pipeline", "consumer_started", {
            "batch_size": self.batch_size,
            "checkpoint_interval": self.checkpoint_interval
        })

    async def stop(self):
        if self._task is None:
            logger.warning("BatchConsumer not running")
            return
        
        logger.info("Stopping BatchConsumer...")
        self._shutdown_requested = True
        self._wake_event.set()  # wake if waiting
        
        try:
            await self._task
        except asyncio.CancelledError:
            pass
        
        self._task = None
        await emit(self.session_id, "pipeline", "consumer_stopped", {})

    def signal(self):
        self._wake_event.set()

    def _on_task_done(self, task: asyncio.Task):
        if task.cancelled():
            logger.info("BatchConsumer task cancelled")
            return
        
        if exc := task.exception():
            logger.error(f"BatchConsumer task failed: {exc}")
    
    async def flush(self):
        """Force a partial drain of the buffer. Blocks until complete."""
        future = asyncio.get_running_loop().create_future()
        self._flush_future = future
        self._wake_event.set()
        await future
    
    def _format_session_text(self, conversation: List[Dict]) -> str:
        lines = []
        for turn in conversation:
            content = turn["content"]
            lines.append(f"[{turn['role_label']}]: {content}")
        return "\n".join(lines)
    
    def update_ingestion_settings(
        self,
        batch_size: Optional[int] = None,
        batch_timeout: Optional[float] = None,
        checkpoint_interval: Optional[int] = None,
        session_window: Optional[int] = None
    ):
        """Update settings dynamically while running."""
        if batch_size is not None:
            self.batch_size = batch_size
        if batch_timeout is not None:
            self.batch_timeout = batch_timeout
        if checkpoint_interval is not None:
            self.checkpoint_interval = checkpoint_interval
        if session_window is not None:
            self.session_window = session_window
        
        logger.info(f"Consumer ingestion settings updated: batch={self.batch_size}, timeout={self.batch_timeout}")

    async def _run(self):
        logger.info(f"BatchConsumer started for {self.user_name}")

        while not self._shutdown_requested:
            timed_out = False
            try:
                await asyncio.wait_for(
                    self._wake_event.wait(), 
                    timeout=self.batch_timeout
                )
            except asyncio.TimeoutError:
                timed_out = True
            
            self._wake_event.clear()
            try:
                await self._drain_buffer(flush_partial=timed_out or self._flush_future is not None)
            except Exception as e:
                logger.error(f"BatchConsumer: Unexpected error during _drain_buffer: {e}")
                await asyncio.sleep(5)
            finally:
                if self._flush_future and not self._flush_future.done():
                    self._flush_future.set_result(None)
                self._flush_future = None

        logger.info("BatchConsumer shutting down, final drain...")
        await self._drain_buffer(flush_partial=True)
        await self.run_session_jobs()
        logger.info("BatchConsumer shutdown complete")

    

    async def _drain_buffer(self, flush_partial: bool):
        batches_count = 0
        total_processed = 0
        all_msg_ids = []
        dlq_count = 0

        while True:
            buffer_len = await self.redis.llen(self._buffer_key)
            if buffer_len < self.batch_size and not flush_partial:
                break

            raw = await self.redis.lrange(self._buffer_key, 0, self.batch_size - 1)
            if not raw:
                await emit(self.session_id, "pipeline", "buffer_empty", {})
                break

            await emit(self.session_id, "pipeline", "buffer_draining", {
                "queued": len(raw)
            })

            messages = [json.loads(m) for m in raw]
            
            conversation = await self.get_session_ctx(self.session_window, messages[0]['id'])
            session_text = self._format_session_text(conversation)

            result = await self.processor.run(messages, session_text)

            if not result.success:
                dlq_success = await self.processor.move_to_dead_letter(
                    messages, 
                    result.error,
                    stage="processing",
                    session_text=session_text
                )
                if not dlq_success:
                    logger.critical(f"DLQ write failed. Leaving {len(messages)} messages in buffer for retry.")
                    await emit(self.session_id, "pipeline", "dlq_write_failed", {
                        "msg_count": len(messages)
                    })
                    break
                dlq_count += len(messages)
            else:
                error_msg = None
                graph_success = True
                loop = asyncio.get_running_loop()
                batch = [
                    {
                        "id": msg['id'],
                        "content": msg['message'],
                        "role": msg.get('role', 'user'),
                        "timestamp": msg.get('timestamp', ''),
                        "embedding": result.message_embeddings.get(msg['id'], [])
                    }
                    for msg in messages
                ]
                try:
                    await asyncio.wait_for(
                        loop.run_in_executor(None, self.store.save_message_logs, batch),
                        timeout=30.0
                    )
                except Exception as e:
                    logger.error(f"Failed to save message logs: {e}")
                    dlq_success = await self.processor.move_to_dead_letter(
                        messages,
                        f"MESSAGE_LOG_SAVE_FAILED: {e}",
                        stage="message_log",
                        session_text=None
                    )
                    if not dlq_success:
                        logger.critical(f"DLQ write failed after message log failure. Leaving messages in buffer.")
                        break
                    dlq_count += len(messages)
                    batches_count += 1
                    total_processed += len(messages)
                    all_msg_ids.extend([m["id"] for m in messages])
                    await self.redis.ltrim(self._buffer_key, len(messages), -1)
                    continue

                if result.extraction_result:
                    try:
                        success, error_msg = await asyncio.wait_for(
                            self.write_to_graph(result),
                            timeout=self.batch_timeout
                        )
                    except asyncio.TimeoutError:
                        success, error_msg = False, "GRAPH_WRITE_TIMEOUT"
                    except Exception as e:
                        success, error_msg = False, str(e)
                        
                    if success:
                        # If graph write is successful, we continue processing the batch,
                        # not break out of the drain loop.
                        pass 
                
                    if not success:
                        logger.error(f"Graph write failed. Error: {error_msg}")
                        await emit(self.session_id, "pipeline", "graph_write_failed", {
                            "error": error_msg
                        })
                        graph_success = False

                if not graph_success:
                    dlq_success = await self.processor.move_to_dead_letter(
                        messages, 
                        error_msg or "GRAPH_WRITE_FAILED [unknown]",
                        stage="graph_write",
                        batch_result=result
                    )
                    if not dlq_success:
                        logger.critical(f"DLQ write failed after graph failure. Leaving {len(messages)} messages in buffer for retry.")
                        await emit(self.session_id, "pipeline", "dlq_write_failed", {
                            "msg_count": len(messages)
                        })
                        break
                    dlq_count += len(messages)
                else:
                    count = await self.redis.incrby(self._checkpoint_key, len(messages))
                    if count >= self.checkpoint_interval:
                        await emit(self.session_id, "pipeline", "checkpoint_reached", {
                            "message_count": count
                        })
                        await self.run_session_jobs()
                        await self.redis.set(self._checkpoint_key, 0)
                    
                    if messages:
                        last_id = max(m["id"] for m in messages)
                        await self.redis.set(RedisKeys.last_processed(self.user_name, self.session_id), last_id)
                    
            batches_count += 1
            total_processed += len(messages)
            all_msg_ids.extend([m["id"] for m in messages])
            
            await self.redis.ltrim(self._buffer_key, len(messages), -1)

        await emit(self.session_id, "pipeline", "drain_complete", {
            "batches_processed": batches_count,
            "total_messages": total_processed,
            "msg_ids": all_msg_ids,
            "dlq_count": dlq_count,
            "partial_flush": flush_partial
        })
           
    

