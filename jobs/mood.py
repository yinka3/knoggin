from typing import Counter

from loguru import logger
from db.memgraph import MemGraphStore
from jobs.base import BaseJob, JobContext, JobResult


class MoodCheckpointJob(BaseJob):

    VOLUME_THRESHOLD = 5

    def __init__(self, user_name: str, store: MemGraphStore):
        self.user_name = user_name
        self.store = store
    
    @property
    def name(self) -> str:
        return "mood_checkpoint"
    
    async def should_run(self, ctx: JobContext) -> bool:
        return await ctx.redis.llen(f"emotions:{ctx.user_name}") >= self.VOLUME_THRESHOLD
    

    async def execute(self, ctx: JobContext) -> JobResult:
        emotions_key = f"emotions:{ctx.user_name}"
        
        raw_emotions = await ctx.redis.lpop(emotions_key, self.VOLUME_THRESHOLD)
        if not raw_emotions:
            return JobResult(success=True, summary="No emotions to log")
        
        emotions = [e if isinstance(e, bytes) else e for e in raw_emotions]
        
        self._write_checkpoint(emotions)
        
        return JobResult(success=True, summary=f"Logged checkpoint: {len(emotions)} emotions")

    async def flush(self, ctx: JobContext) -> JobResult:
        """Flush remaining emotions regardless of threshold. Called on shutdown."""
        emotions_key = f"emotions:{ctx.user_name}"
        
        remaining = await ctx.redis.lrange(emotions_key, 0, -1)
        if not remaining:
            return JobResult(success=True, summary="Nothing to flush")
        
        await ctx.redis.delete(emotions_key)
        
        emotions = [e if isinstance(e, bytes) else e for e in remaining]
        
        self._write_checkpoint(emotions)
        
        return JobResult(success=True, summary=f"Flushed {len(emotions)} emotions")

    def _write_checkpoint(self, emotions: list):
        counts = Counter(emotions)
        top_two = counts.most_common(2)
        
        primary, primary_count = top_two[0]
        secondary, secondary_count = top_two[1] if len(top_two) > 1 else ("neutral", 0)
        
        self.store.log_mood_checkpoint(
            user_name=self.user_name,
            primary=primary,
            primary_count=primary_count,
            secondary=secondary,
            secondary_count=secondary_count,
            message_count=len(emotions)
        )
    
    async def on_shutdown(self, ctx: JobContext) -> None:
        """Flush any remaining emotions on shutdown."""
        result = await self.flush(ctx)
        if result.summary != "Nothing to flush":
            logger.info(f"Mood shutdown: {result.summary}")