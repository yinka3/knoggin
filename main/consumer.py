import asyncio
import json
from typing import Awaitable, Callable, Dict, List, Optional
from loguru import logger
from main.redisclient import AsyncRedisClient
from main.processor import BatchProcessor, BatchResult


class BatchConsumer:

    def __init__(self, user_name: str, processor: BatchProcessor, 
                 get_session_context: Callable[[int], Awaitable[List[Dict]]],
                 run_session_jobs: Callable[[], Awaitable[None]],
                 write_to_graph: Callable[[BatchResult], Awaitable[None]],
                 batch_size: int = 10, batch_timeout: float =  15.0, 
                 checkpoint_interval: int = 30, session_window: int = 60):
        
        self.user_name = user_name
        self.processor = processor
        self.batch_size = batch_size
        self.batch_timeout = batch_timeout
        self.checkpoint_interval = checkpoint_interval
        self.session_window = session_window
        self.redis = AsyncRedisClient().get_client()

        # callbacks
        self.get_session_ctx = get_session_context
        self.run_session_jobs = run_session_jobs
        self.write_to_graph = write_to_graph

        self._wake_event = asyncio.Event()
        self._shutdown_requested = False
        self._task: Optional[asyncio.Task] = None
    

    @property
    def _buffer_key(self) -> str:
        return f"buffer:{self.user_name}"

    @property
    def _checkpoint_key(self) -> str:
        return f"checkpoint_count:{self.user_name}"
    

    def start(self):
        if self._task is not None:
            logger.warning("BatchConsumer already running")
            return
        
        self._shutdown_requested = False
        self._task = asyncio.create_task(self._run())
        self._task.add_done_callback(self._on_task_done)

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

    def signal(self):
        self._wake_event.set()

    def _on_task_done(self, task: asyncio.Task):
        if task.cancelled():
            logger.info("BatchConsumer task cancelled")
            return
        
        if exc := task.exception():
            logger.error(f"BatchConsumer task failed: {exc}")

    async def _run(self):
        logger.info(f"BatchConsumer started for {self.user_name}")

        while not self._shutdown_requested:
            try:
                await asyncio.wait_for(
                    self._wake_event.wait(), 
                    timeout=self.batch_timeout
                )
            except asyncio.TimeoutError:
                pass
            
            self._wake_event.clear()

            await self._drain_buffer()

        logger.info("BatchConsumer shutting down, final drain...")
        await self._drain_buffer()
        await self.run_session_jobs()
        logger.info("BatchConsumer shutdown complete")
    

    async def _drain_buffer(self):
        while True:
            buffer_len = await self.redis.llen(self._buffer_key)
            if buffer_len == 0:
                break

            raw = await self.redis.lrange(self._buffer_key, 0, self.batch_size - 1)
            if not raw:
                break

            messages = [json.loads(m) for m in raw]
            
            conversation = await self.get_session_ctx(self.session_window)
            session_text = self._format_session_text(conversation)

            result = await self.processor.run(messages, session_text)

            if not result.success:
                await self.processor.move_to_dead_letter(messages, result.error)
            else:
                if result.emotions:
                    await self.redis.rpush(f"emotions:{self.user_name}", *result.emotions)
                
                if result.extraction_result:
                   await self.write_to_graph(result)

            await self.redis.ltrim(self._buffer_key, len(messages), -1)

            # Checkpoint check
            count = await self.redis.incrby(self._checkpoint_key, len(messages))
            if count >= self.checkpoint_interval:
                await self.redis.set(self._checkpoint_key, 0)
                await self.run_session_jobs()
    
    def _format_session_text(self, conversation: List[Dict]) -> str:
        lines = []
        for turn in conversation:
            content = turn["content"]
            if turn["role"] == "assistant" and len(content) > 200:
                content = content[:200] + "..."
            lines.append(f"[{turn['role_label']}]: {content}")
        return "\n".join(lines)
