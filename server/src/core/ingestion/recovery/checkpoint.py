"""Idempotent durable checkpoint commits for ingestion batches."""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass

import redis.asyncio as aioredis

from core.ingestion.batch import IngestionBatch
from infrastructure.redis_client import RedisKeys

CHECKPOINT_COMMIT_TTL_SECONDS = 7 * 24 * 60 * 60

_COMMIT_CHECKPOINT_SCRIPT = """
local previous = redis.call('GET', KEYS[4])
if previous then
    local delimiter = string.find(previous, ':')
    return {
        tonumber(string.sub(previous, 1, delimiter - 1)),
        tonumber(string.sub(previous, delimiter + 1))
    }
end

local count = redis.call('INCRBY', KEYS[1], ARGV[1])
local reached = 0
if count >= tonumber(ARGV[2]) then
    redis.call('SET', KEYS[1], 0)
    reached = 1
end
local session_last_id = redis.call('GET', KEYS[2])
if not session_last_id or tonumber(ARGV[3]) > tonumber(session_last_id) then
    redis.call('SET', KEYS[2], ARGV[3])
end
local project_last_id = redis.call('GET', KEYS[3])
if not project_last_id or tonumber(ARGV[3]) > tonumber(project_last_id) then
    redis.call('SET', KEYS[3], ARGV[3])
end
redis.call('SET', KEYS[4], tostring(count) .. ':' .. tostring(reached), 'EX', ARGV[4])
return {count, reached}
"""


def _checkpoint_commit_token(batch: IngestionBatch) -> str:
    """Return a retry-stable identity for one message batch."""

    identity = {
        "user_name": batch.scope.user_name,
        "project_id": batch.scope.project_id,
        "session_id": batch.scope.session_id,
        "message_ids": sorted(int(message["id"]) for message in batch.messages),
    }
    return hashlib.sha256(
        json.dumps(identity, sort_keys=True, separators=(",", ":")).encode("utf-8")
    ).hexdigest()[:24]


@dataclass(frozen=True, slots=True)
class CheckpointCommit:
    """One durable checkpoint result returned by Redis."""

    count_before_reset: int
    threshold_reached: bool

    @property
    def current_count(self) -> int:
        return 0 if self.threshold_reached else self.count_before_reset


async def commit_ingestion_checkpoint(
    redis: aioredis.Redis,
    batch: IngestionBatch,
) -> CheckpointCommit:
    """Atomically commit one batch's cursor and checkpoint exactly once."""

    if not batch.messages:
        raise ValueError("Checkpoint commit requires batch messages")
    checkpoint_interval = batch.policy.checkpoint_interval

    last_id = max(int(message["id"]) for message in batch.messages)
    scope = batch.scope
    response = await redis.eval(
        _COMMIT_CHECKPOINT_SCRIPT,
        4,
        RedisKeys.checkpoint(scope.user_name, scope.session_id),
        RedisKeys.last_processed(scope.user_name, scope.session_id),
        RedisKeys.project_last_processed(scope.user_name, scope.project_id),
        RedisKeys.checkpoint_commit(
            scope.user_name,
            scope.session_id,
            _checkpoint_commit_token(batch),
        ),
        len(batch.messages),
        checkpoint_interval,
        last_id,
        CHECKPOINT_COMMIT_TTL_SECONDS,
    )
    if not isinstance(response, (list, tuple)) or len(response) != 2:
        raise RuntimeError("Checkpoint commit returned an invalid Redis response")

    count = int(response[0])
    reached = bool(int(response[1]))
    batch.record_checkpoint_progress(
        current_count=0 if reached else count,
    )
    return CheckpointCommit(count_before_reset=count, threshold_reached=reached)
