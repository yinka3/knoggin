import asyncio
from typing import Optional
from urllib.parse import urlsplit, urlunsplit

import redis.asyncio as aioredis
from loguru import logger

from common.exceptions import DependencyError
from common.schema.settings import RedisConnectionSettings

SESSION_RUNTIME_TTL_SECONDS = 72 * 3600
SHORT_LIVED_DEDUP_TTL_SECONDS = 5 * 60
PROJECT_ACTIVITY_TTL_SECONDS = 30 * 24 * 3600


def _endpoint_label(url: str) -> str:
    """Return a credential-free endpoint for logs and errors."""
    try:
        parsed = urlsplit(url)
        host = parsed.hostname or "unknown"
        port = parsed.port or (6380 if parsed.scheme == "rediss" else 6379)
        path = parsed.path or "/0"
        return urlunsplit((parsed.scheme, f"{host}:{port}", path, "", ""))
    except ValueError:
        return "configured Redis endpoint"


def _safe_error_type(exc: BaseException) -> str:
    """Avoid copying a credential-bearing URL from an exception into logs."""
    return type(exc).__name__


class AsyncRedisClient:
    """Resource-owned async Redis connection lifecycle."""

    def __init__(self, settings: RedisConnectionSettings):
        self.settings = settings
        self._client: Optional[aioredis.Redis] = None
        self._lock = asyncio.Lock()

    @property
    def client(self) -> aioredis.Redis:
        if self._client is None:
            raise RuntimeError("Redis client is not connected")
        return self._client

    def _build_client(self) -> aioredis.Redis:
        return aioredis.Redis.from_url(
            self.settings.url,
            decode_responses=True,
            max_connections=self.settings.max_connections,
            health_check_interval=self.settings.health_check_interval,
            socket_connect_timeout=self.settings.connect_timeout,
        )

    async def _close_client(self, client: aioredis.Redis) -> bool:
        try:
            await client.aclose(close_connection_pool=True)
            return True
        except Exception as exc:
            logger.warning(
                f"Failed to close Redis connection pool: {_safe_error_type(exc)}"
            )
            return False

    async def connect(self) -> aioredis.Redis:
        """Connect once and return the stable raw redis-py client."""
        async with self._lock:
            if self._client is not None:
                return self._client

            try:
                candidate = self._build_client()
            except Exception as exc:
                endpoint = _endpoint_label(self.settings.url)
                raise DependencyError(
                    f"Failed to configure Redis at {endpoint}",
                    details={
                        "error_type": _safe_error_type(exc),
                        "endpoint": endpoint,
                        "attempts": 0,
                    },
                ) from exc

            last_error: Optional[Exception] = None
            try:
                for attempt in range(1, self.settings.startup_attempts + 1):
                    try:
                        await asyncio.wait_for(
                            candidate.ping(),
                            timeout=self.settings.connect_timeout,
                        )
                        self._client = candidate
                        logger.info(
                            f"Redis connected: {_endpoint_label(self.settings.url)}"
                        )
                        return candidate
                    except Exception as exc:
                        last_error = exc
                        if attempt < self.settings.startup_attempts:
                            delay = self.settings.startup_backoff_seconds * (
                                2 ** (attempt - 1)
                            )
                            logger.warning(
                                f"Redis startup check {attempt}/"
                                f"{self.settings.startup_attempts} failed: "
                                f"{_safe_error_type(exc)}. "
                                f"Retrying in {delay}s..."
                            )
                            await asyncio.sleep(delay)
            except asyncio.CancelledError:
                await self._close_client(candidate)
                raise

            await self._close_client(candidate)
            endpoint = _endpoint_label(self.settings.url)
            raise DependencyError(
                f"Failed to connect to Redis at {endpoint}",
                details={
                    "error_type": _safe_error_type(last_error),
                    "endpoint": endpoint,
                    "attempts": self.settings.startup_attempts,
                },
            ) from last_error

    async def close(self) -> None:
        """Detach the client and close its complete connection pool."""
        async with self._lock:
            client = self._client
            self._client = None
            if client is not None:
                closed = await self._close_client(client)
                if closed:
                    logger.info("Redis connection closed")


class RedisKeys:
    """
    Centralized Redis key patterns for explicit project and session scopes.

    Ownership rule: Redis keys are cache/coordination only. Durable projects,
    sessions, agents, topic configuration, documents, messages, and merge
    history are Postgres-owned. Legacy helpers remain only so cleanup and old
    tests can name those key families; they are not authoritative write paths.
    """

    REBUILDABLE_FROM_POSTGRES = frozenset(
        {
            "conversation",
            "recent_conversation",
            "message_content",
            "last_processed",
            "project_last_processed",
            "project_last_activity",
        }
    )
    EPHEMERAL_ONLY = frozenset(
        {
            "buffer",
            "checkpoint",
            "message_dedup",
            "heartbeat_counter",
            "project_heartbeat_counter",
            "merge_queue",
            "merge_proposals",
            "merge_intent",
            "merge_intents_index",
            "job_last_run",
            "job_lease",
            "maintenance_attempts",
            "maintenance_cooldown",
            "dlq",
            "dlq_processing",
            "dlq_state",
            "dlq_claims",
            "dlq_parked",
            "dlq_completed",
            "community_discussion_active",
            "community_pubsub_channel",
            "dirty_entities",
        }
    )
    LEGACY_NON_AUTHORITATIVE = frozenset(
        {
            "projects",
            "project_topic_config",
            "sessions",
            "project_sessions",
            "agents",
            "agents_default",
            "agent_directives",
            "session_memory",
            "community_agent_memory",
        }
    )

    @staticmethod
    def merge_queue(user_name: str, project_id: str) -> str:
        return f"merge_queue:{user_name}:{project_id}"

    @staticmethod
    def merge_proposals(user: str, project_id: str) -> str:
        return f"merge_proposals:{user}:{project_id}"

    @staticmethod
    def dlq(user: str, project_id: str) -> str:
        return f"dlq:{user}:{project_id}"

    @staticmethod
    def dlq_processing(user: str, project_id: str) -> str:
        return f"dlq:processing:{user}:{project_id}"

    @staticmethod
    def dlq_state(user: str, project_id: str) -> str:
        return f"dlq:state:{user}:{project_id}"

    @staticmethod
    def dlq_claims(user: str, project_id: str) -> str:
        return f"dlq:claims:{user}:{project_id}"

    @staticmethod
    def dlq_parked(user: str, project_id: str) -> str:
        return f"dlq:parked:{user}:{project_id}"

    @staticmethod
    def dlq_completed(user: str, project_id: str) -> str:
        return f"dlq:completed:{user}:{project_id}"

    @staticmethod
    def project_last_processed(user: str, project_id: str) -> str:
        return f"project_last_processed_msg:{user}:{project_id}"

    @staticmethod
    def project_last_activity(user: str, project_id: str) -> str:
        return f"project_last_activity:{user}:{project_id}"

    @staticmethod
    def project_heartbeat_counter(user: str, project_id: str) -> str:
        return f"project_heartbeat_counter:{user}:{project_id}"

    @staticmethod
    def dirty_entities(user: str, project_id: str) -> str:
        return f"dirty_entities:{user}:{project_id}"

    @staticmethod
    def project_profile_complete(user: str, project_id: str) -> str:
        return f"profile_complete:{user}:{project_id}"

    @staticmethod
    def last_profile_update(user: str, project_id: str, entity_id: int | str) -> str:
        return f"last_profile_update:{user}:{project_id}:{entity_id}"

    @staticmethod
    def project_cleanup_keys(user: str, project_id: str) -> list[str]:
        """Return fixed Redis keys wholly owned by one project."""
        return [
            RedisKeys.merge_queue(user, project_id),
            RedisKeys.merge_proposals(user, project_id),
            RedisKeys.merge_intents_index(user, project_id),
            RedisKeys.dlq(user, project_id),
            RedisKeys.dlq_processing(user, project_id),
            RedisKeys.dlq_state(user, project_id),
            RedisKeys.dlq_claims(user, project_id),
            RedisKeys.dlq_parked(user, project_id),
            RedisKeys.dlq_completed(user, project_id),
            RedisKeys.project_last_processed(user, project_id),
            RedisKeys.project_last_activity(user, project_id),
            RedisKeys.project_heartbeat_counter(user, project_id),
            RedisKeys.dirty_entities(user, project_id),
            RedisKeys.project_sessions(user, project_id),
            RedisKeys.community_discussion_active(user, project_id),
        ]

    @staticmethod
    def project_cleanup_patterns(user: str, project_id: str) -> list[str]:
        """Return variable-suffix Redis key patterns owned by one project."""
        return [
            f"merge_intent:{user}:{project_id}:*",
            f"last_run:*:{user}:{project_id}",
            f"job_lease:{user}:{project_id}:*",
            f"maintenance_attempts:{user}:{project_id}:*",
            f"maintenance_cooldown:{user}:{project_id}:*",
            f"last_profile_update:{user}:{project_id}:*",
        ]

    @staticmethod
    def session_keys(user: str, session: str) -> list[str]:
        """Returns all Redis keys that are scoped to a specific session."""
        return [
            RedisKeys.buffer(user, session),
            RedisKeys.checkpoint(user, session),
            RedisKeys.conversation(user, session),
            RedisKeys.recent_conversation(user, session),
            RedisKeys.message_content(user, session),
            RedisKeys.last_processed(user, session),
            RedisKeys.heartbeat_counter(user, session),
        ]

    @staticmethod
    def conversation(user: str, session: str) -> str:
        return f"conversation:{user}:{session}"

    @staticmethod
    def recent_conversation(user: str, session: str) -> str:
        return f"recent_conversation:{user}:{session}"

    @staticmethod
    def message_dedup(user: str, session: str, digest: str) -> str:
        return f"msg_dedup:{user}:{session}:{digest}"

    @staticmethod
    def message_dedup_pattern(user: str, session: str) -> str:
        return f"msg_dedup:{user}:{session}:*"

    @staticmethod
    def buffer(user: str, session: str) -> str:
        return f"buffer:{user}:{session}"

    @staticmethod
    def checkpoint(user: str, session: str) -> str:
        return f"checkpoint_count:{user}:{session}"

    @staticmethod
    def message_content(user: str, session: str) -> str:
        return f"message_content:{user}:{session}"

    @staticmethod
    def last_processed(user: str, session: str) -> str:
        return f"last_processed_msg:{user}:{session}"

    @staticmethod
    def merge_intent(
        user: str, project_id: str, primary_id: int, secondary_id: int
    ) -> str:
        return f"merge_intent:{user}:{project_id}:{primary_id}:{secondary_id}"

    @staticmethod
    def merge_intents_index(user: str, project_id: str) -> str:
        return f"merge_intents_index:{user}:{project_id}"

    @staticmethod
    def job_last_run(job_name: str, user: str, project_id: str) -> str:
        return f"last_run:{job_name}:{user}:{project_id}"

    @staticmethod
    def job_lease(user: str, project_id: str, job_name: str) -> str:
        return f"job_lease:{user}:{project_id}:{job_name}"

    @staticmethod
    def maintenance_attempts(user: str, project_id: str, candidate_id: str) -> str:
        return f"maintenance_attempts:{user}:{project_id}:{candidate_id}"

    @staticmethod
    def maintenance_cooldown(user: str, project_id: str, candidate_id: str) -> str:
        return f"maintenance_cooldown:{user}:{project_id}:{candidate_id}"

    @staticmethod
    def projects(user: str) -> str:
        return f"projects:{user}"

    @staticmethod
    def project_topic_config(user: str) -> str:
        return f"project_topic_config:{user}"

    @staticmethod
    def sessions(user: str) -> str:
        return f"sessions:{user}"

    @staticmethod
    def project_sessions(user: str, project_id: str) -> str:
        return f"project_sessions:{user}:{project_id}"

    @staticmethod
    def agents(user: str) -> str:
        return f"agents:{user}"

    @staticmethod
    def agents_default(user: str) -> str:
        return f"agents:default:{user}"

    @staticmethod
    def session_memory(user: str, session: str, topic: str) -> str:
        return f"memory:{user}:{session}:{topic}"

    @staticmethod
    def session_memory_pattern(user: str, session: str) -> str:
        return f"memory:{user}:{session}:*"

    @staticmethod
    def heartbeat_counter(user: str, session: str) -> str:
        return f"heartbeat_counter:{user}:{session}"

    @staticmethod
    def agent_directives(user: str, agent_id: str) -> str:
        return f"agent_directives:{user}:{agent_id}"

    @staticmethod
    def community_discussion_active(user: str, project_id: str) -> str:
        return f"community:discussion:active:{user}:{project_id}"

    @staticmethod
    def community_agent_memory(user_name: str, agent_id: str) -> str:
        return f"community:{user_name}:agent_memory:{agent_id}"

    @staticmethod
    def community_pubsub_channel() -> str:
        return "community:events"
