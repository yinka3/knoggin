import fnmatch
import json
import time
from collections import defaultdict
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from common.schema.settings import RedisConnectionSettings
from common.utils.time_utils import get_now_iso
from infrastructure.redis_client import AsyncRedisClient


class FakePipeline:
    def __init__(self):
        self.refresh_count = 0

    def refresh_topic_mappings(self):
        self.refresh_count += 1


class FakePipelineWriter:
    def __init__(self, redis):
        self.redis = redis
        self.commands = []

    def hset(self, key, field, value):
        self.commands.append(("hset", key, field, value))
        return self

    def zadd(self, key, mapping):
        self.commands.append(("zadd", key, mapping))
        return self

    def zremrangebyrank(self, key, start, end):
        self.commands.append(("zremrangebyrank", key, start, end))
        return self

    def zrem(self, key, *members):
        self.commands.append(("zrem", key, members))
        return self

    def expire(self, key, ttl):
        self.commands.append(("expire", key, ttl))
        return self

    def hdel(self, key, *fields):
        self.commands.append(("hdel", key, fields))
        return self

    def hget(self, key, field):
        self.commands.append(("hget", key, field))
        return self

    async def execute(self):
        results = []
        for command in self.commands:
            name = command[0]
            if name == "hset":
                _, key, field, value = command
                results.append(await self.redis.hset(key, field, value))
            elif name == "zadd":
                _, key, mapping = command
                results.append(await self.redis.zadd(key, mapping))
            elif name == "zremrangebyrank":
                _, key, start, end = command
                results.append(await self.redis.zremrangebyrank(key, start, end))
            elif name == "zrem":
                _, key, members = command
                results.append(await self.redis.zrem(key, *members))
            elif name == "expire":
                _, key, ttl = command
                results.append(await self.redis.expire(key, ttl))
            elif name == "hdel":
                _, key, fields = command
                results.append(await self.redis.hdel(key, *fields))
            elif name == "hget":
                _, key, field = command
                results.append(await self.redis.hget(key, field))
        return results


class FakeRedis:
    def __init__(self):
        self.strings: dict[str, str] = {}
        self.hashes: dict[str, dict[str, str]] = defaultdict(dict)
        self.sets: dict[str, set[str]] = defaultdict(set)
        self.lists: dict[str, list[str]] = defaultdict(list)
        self.zsets: dict[str, dict[str, float]] = defaultdict(dict)
        self.expirations: list[tuple[str, int]] = []
        self.deleted_keys: list[str] = []
        self.evals: list[tuple[str, tuple[Any, ...]]] = []
        self.key_expirations: dict[str, float] = {}
        self.published: list[tuple[str, str]] = []
        self.closed = False
        self.pipeline_calls = 0

    async def ping(self):
        return True

    async def aclose(self, close_connection_pool=True):
        self.closed = True

    async def publish(self, channel, message):
        self.published.append((channel, message))
        return 1

    def pipeline(self):
        self.pipeline_calls += 1
        return FakePipelineWriter(self)

    async def hset(self, key, field, value):
        self._purge_expired(key)
        self.hashes[key][str(field)] = value
        return 1

    async def hget(self, key, field):
        self._purge_expired(key)
        return self.hashes.get(key, {}).get(str(field))

    async def hgetall(self, key):
        self._purge_expired(key)
        return dict(self.hashes.get(key, {}))

    async def hlen(self, key):
        self._purge_expired(key)
        return len(self.hashes.get(key, {}))

    async def hmget(self, key, *fields):
        self._purge_expired(key)
        return [self.hashes.get(key, {}).get(str(f)) for f in fields]

    async def hdel(self, key, *fields):
        self._purge_expired(key)
        removed = 0
        for f in fields:
            removed += int(self.hashes.get(key, {}).pop(str(f), None) is not None)
        return removed

    async def sadd(self, key, *values):
        self._purge_expired(key)
        before = len(self.sets[key])
        self.sets[key].update(str(value) for value in values)
        return len(self.sets[key]) - before

    async def srem(self, key, *values):
        self._purge_expired(key)
        removed = 0
        for value in values:
            if str(value) in self.sets.get(key, set()):
                self.sets[key].remove(str(value))
                removed += 1
        return removed

    async def smembers(self, key):
        self._purge_expired(key)
        return set(self.sets.get(key, set()))

    async def scard(self, key):
        self._purge_expired(key)
        return len(self.sets.get(key, set()))

    async def srandmember(self, key, number=None):
        self._purge_expired(key)
        values = sorted(self.sets.get(key, set()))
        if number is None:
            return values[0] if values else None
        return values[: int(number)]

    async def delete(self, *keys):
        deleted = 0
        for key in keys:
            self._purge_expired(key)
            existed = False
            for store in (self.strings, self.hashes, self.sets, self.lists, self.zsets):
                if key in store:
                    existed = True
                    del store[key]
            self.key_expirations.pop(key, None)
            if existed:
                self.deleted_keys.append(key)
                deleted += 1
        return deleted

    async def scan(self, cursor=0, match=None, count=100):
        self._purge_all_expired()
        keys = set()
        for store in (self.strings, self.hashes, self.sets, self.lists, self.zsets):
            keys.update(store.keys())
        if match:
            keys = {key for key in keys if fnmatch.fnmatch(key, match)}
        return 0, sorted(keys)

    async def incr(self, key):
        return await self.incrby(key, 1)

    async def incrby(self, key, amount):
        self._purge_expired(key)
        current = int(self.strings.get(key, "0")) + int(amount)
        self.strings[key] = str(current)
        return current

    async def get(self, key):
        self._purge_expired(key)
        return self.strings.get(key)

    async def set(self, key, value, ex=None, nx=False):
        self._purge_expired(key)
        if nx and key in self.strings:
            return False
        self.strings[key] = str(value)
        if ex is not None:
            self.key_expirations[key] = time.monotonic() + float(ex)
            self.expirations.append((key, ex))
        else:
            self.key_expirations.pop(key, None)
        return True

    async def setex(self, key, ttl, value):
        self._purge_expired(key)
        self.strings[key] = str(value)
        self.key_expirations[key] = time.monotonic() + float(ttl)
        self.expirations.append((key, ttl))
        return True

    async def rpush(self, key, value):
        self._purge_expired(key)
        self.lists[key].append(value)
        return len(self.lists[key])

    async def lpop(self, key):
        self._purge_expired(key)
        if not self.lists.get(key):
            return None
        return self.lists[key].pop(0)

    async def llen(self, key):
        self._purge_expired(key)
        return len(self.lists.get(key, []))

    async def lrange(self, key, start, end):
        self._purge_expired(key)
        items = self.lists.get(key, [])
        stop = len(items) if end == -1 else end + 1
        return items[start:stop]

    async def ltrim(self, key, start, end):
        self._purge_expired(key)
        items = self.lists.get(key, [])
        stop = len(items) if end == -1 else end + 1
        self.lists[key] = items[start:stop]
        return True

    async def eval(self, script, numkeys, *args):
        self.evals.append((script, args))
        if numkeys == 1 and len(args) >= 2:
            key, expected_value = args[0], str(args[1])
            self._purge_expired(key)
            if self.strings.get(key) == expected_value:
                await self.delete(key)
                return 1
            return 0
        return None

    async def expire(self, key, ttl):
        self._purge_expired(key)
        if not self._key_exists(key):
            return False
        self.expirations.append((key, ttl))
        if float(ttl) <= 0:
            await self.delete(key)
            return True
        self.key_expirations[key] = time.monotonic() + float(ttl)
        return True

    def _purge_expired(self, key):
        expires_at = self.key_expirations.get(key)
        if expires_at is not None and expires_at <= time.monotonic():
            for store in (self.strings, self.hashes, self.sets, self.lists, self.zsets):
                store.pop(key, None)
            self.key_expirations.pop(key, None)

    def _purge_all_expired(self):
        for key in list(self.key_expirations):
            self._purge_expired(key)

    def _key_exists(self, key):
        return any(
            key in store
            for store in (self.strings, self.hashes, self.sets, self.lists, self.zsets)
        )

    async def zadd(self, key, mapping):
        self._purge_expired(key)
        for member, score in mapping.items():
            self.zsets[key][str(member)] = float(score)
        return len(mapping)

    async def zrange(self, key, start, end, desc=False, **kwargs):
        self._purge_expired(key)
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        if desc:
            items = list(reversed(items))
        if kwargs.get("byscore"):
            def parse_bound(value):
                exclusive = isinstance(value, str) and value.startswith("(")
                raw = value[1:] if exclusive else value
                if raw == "-inf":
                    return float("-inf"), exclusive
                if raw == "+inf":
                    return float("inf"), exclusive
                return float(raw), exclusive

            start_score, start_exclusive = parse_bound(start)
            end_score, end_exclusive = parse_bound(end)

            def in_range(score):
                if desc:
                    below_start = (
                        score < start_score
                        if start_exclusive
                        else score <= start_score
                    )
                    above_end = (
                        score > end_score if end_exclusive else score >= end_score
                    )
                    return below_start and above_end
                above_start = (
                    score > start_score if start_exclusive else score >= start_score
                )
                below_end = score < end_score if end_exclusive else score <= end_score
                return above_start and below_end

            items = [item for item in items if in_range(item[1])]
            offset = int(kwargs.get("offset") or 0)
            num = kwargs.get("num")
            if num is None:
                return [member for member, _ in items[offset:]]
            return [member for member, _ in items[offset : offset + int(num)]]
        if end == -1:
            sliced = items[start:]
        else:
            sliced = items[start : end + 1]
        return [member for member, _ in sliced]

    async def zscore(self, key, member):
        self._purge_expired(key)
        return self.zsets.get(key, {}).get(str(member))

    async def zrank(self, key, member):
        self._purge_expired(key)
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        target = str(member)
        for index, (item, _) in enumerate(items):
            if item == target:
                return index
        return None

    async def zremrangebyrank(self, key, start, end):
        self._purge_expired(key)
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        if not items:
            return 0
        stop = len(items) if end == -1 else end + 1
        removed = items[start:stop]
        for member, _ in removed:
            self.zsets[key].pop(member, None)
        return len(removed)

    async def zrem(self, key, *members):
        self._purge_expired(key)
        removed = 0
        for member in members:
            removed += int(
                self.zsets.get(key, {}).pop(str(member), None) is not None
            )
        return removed


class FakeGraphClient:
    def __init__(self):
        self.saved_message_logs = []
        self.recent_project_messages = []
        self.identity_calls = []
        self.next_entity_id = 2
        self.next_message_id = 1
        self.search_rebuild_calls = []

    async def allocate_entity_id(self):
        entity_id = self.next_entity_id
        self.next_entity_id += 1
        return entity_id

    async def allocate_message_id(self):
        message_id = self.next_message_id
        self.next_message_id += 1
        return message_id

    async def rebuild_project_search_indexes(
        self,
        project_id,
        user_name,
        identity_project_ids,
    ):
        call = {
            "project_id": project_id,
            "user_name": user_name,
            "identity_project_ids": list(identity_project_ids),
        }
        self.search_rebuild_calls.append(call)
        return {"messages": 0, "entities": 0, "facts": 0, "identity": 1}

    async def get_max_entity_id(self):
        return 0

    async def ensure_identity_entity(self, user_name, aliases=None):
        self.identity_calls.append((user_name, list(aliases or [])))
        return {
            "id": 1,
            "user_name": user_name,
            "canonical_name": user_name,
            "aliases": list(aliases or []),
            "project_id": "__identity__",
        }

    async def save_message_logs(self, messages):
        self.saved_message_logs.append(messages)
        return True

    async def get_recent_project_messages(
        self, user_name, project_id, limit, before_message_id=None
    ):
        messages = [
            message
            for message in self.recent_project_messages
            if message.get("user_name", user_name) == user_name
            and message.get("project_id", project_id) == project_id
            and (
                before_message_id is None
                or int(message.get("id", 0)) <= int(before_message_id)
            )
        ]
        return messages[-limit:]


class FakeEmbeddingService:
    async def encode(self, values):
        return [[0.1, 0.2, 0.3] for _ in values]

    def cleanup(self):
        pass


class FakeLLMService:
    async def close(self):
        pass


class FakeRedisManager(AsyncRedisClient):
    def __init__(self, client: FakeRedis):
        super().__init__(RedisConnectionSettings())
        self._client = client

    async def connect(self):
        return self.client


class FakePostgresClient:
    def __init__(self):
        self.read_results = []
        self.write_count = 1
        self.calls = []

    async def execute_read(self, query, params=None):
        self.calls.append(("execute_read", query, params))
        if not self.read_results:
            return []
        return self.read_results.pop(0)

    async def execute_write(self, query, params=None):
        self.calls.append(("execute_write", query, params))
        return self.write_count


@dataclass
class FakeResources:
    redis: FakeRedis = field(default_factory=FakeRedis)
    redis_manager: Any = None
    graph: FakeGraphClient = field(default_factory=FakeGraphClient)
    postgres: FakePostgresClient = field(default_factory=FakePostgresClient)
    document_storage_root: Path = field(
        default_factory=lambda: Path("data/documents")
    )
    embedding: FakeEmbeddingService = field(default_factory=FakeEmbeddingService)
    llm_service: FakeLLMService = field(default_factory=FakeLLMService)
    executor: Any = None
    gliner: Any = None
    spacy: Any = None

    def __post_init__(self):
        if self.redis_manager is None:
            self.redis_manager = FakeRedisManager(self.redis)


class FakeScheduler:
    def __init__(self):
        self.running = False
        self.started = 0
        self.stopped = 0
        self.activity_count = 0

    async def start(self):
        self.running = True
        self.started += 1

    async def stop(self):
        self.running = False
        self.stopped += 1

    async def record_activity(self):
        self.activity_count += 1


class FakeConsumer:
    def __init__(self):
        self.signaled = 0
        self.stopped = 0

    def signal(self):
        self.signaled += 1

    async def stop(self):
        self.stopped += 1


class FakeContext:
    def __init__(self, session_id="session-1", project_id="project-1"):
        self.session_id = session_id
        self.project_id = project_id
        self.shutdown_count = 0
        self.document_service = None

    async def shutdown(self):
        self.shutdown_count += 1


class FakeProjectManager:
    def __init__(self, project_state=None):
        self.project_state = project_state
        self.acquire_calls: list[tuple[str, str]] = []
        self.acquire_topic_configs = []
        self.release_calls: list[str] = []
        self.add_session_calls: list[tuple[str, str]] = []
        self.remove_session_calls: list[tuple[str, str]] = []

    async def acquire_project_for_session(
        self, project_id, session_id, topics_config=None
    ):
        self.acquire_calls.append((project_id, session_id))
        self.acquire_topic_configs.append(topics_config)
        if self.project_state is not None:
            return self.project_state
        return object()

    async def release_project(self, project_id):
        self.release_calls.append(project_id)

    async def add_session(self, project_id, session_id):
        self.add_session_calls.append((project_id, session_id))

    async def remove_session(self, project_id, session_id):
        self.remove_session_calls.append((project_id, session_id))


class FakeConfigValue:
    def __init__(self, conversation_context_turns=100):
        self.developer_settings = type(
            "DeveloperSettings",
            (),
            {
                "limits": type(
                    "Limits",
                    (),
                    {"conversation_context_turns": conversation_context_turns},
                )()
            },
        )()


def make_turn_payload(role="user", content="hello", timestamp=None, user_msg_id=1):
    timestamp = timestamp or get_now_iso()
    return json.dumps(
        {
            "role": role,
            "content": content,
            "timestamp": timestamp,
            "user_msg_id": user_msg_id,
            "metadata": None,
        }
    )


class RecordingCursor:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        self.client.cursor_enters += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.client.cursor_exits += 1
        return False

    async def execute(self, query, params=None):
        self.client.calls.append(("execute", query, params))
        self.client._raise_next("execute_exceptions")

    async def fetchone(self):
        self.client._raise_next("fetchone_exceptions")
        if not self.client.fetchone_results:
            return None
        return self.client.fetchone_results.pop(0)

    async def fetchall(self):
        self.client._raise_next("fetchall_exceptions")
        if not self.client.fetchall_results:
            return []
        return self.client.fetchall_results.pop(0)


class RecordingTransaction:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        self.client.transaction_enters += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.client.transaction_exits += 1
        return False


class RecordingConnection:
    def __init__(self, client):
        self.client = client

    async def __aenter__(self):
        self.client.connection_enters += 1
        return self

    async def __aexit__(self, exc_type, exc, tb):
        self.client.connection_exits += 1
        return False

    def transaction(self):
        return RecordingTransaction(self.client)

    def cursor(self):
        return RecordingCursor(self.client)


class RecordingAsyncPool:
    def __init__(self, client):
        self.client = client

    def connection(self):
        return RecordingConnection(self.client)


class RecordingPostgresClient:
    def __init__(
        self,
        fetchone_results=None,
        fetchall_results=None,
        execute_read_results=None,
        execute_write_results=None,
        execute_exceptions=None,
        fetchone_exceptions=None,
        fetchall_exceptions=None,
        execute_read_exceptions=None,
        execute_write_exceptions=None,
    ):
        self.calls = []
        self.fetchone_results = list(fetchone_results or [])
        self.fetchall_results = list(fetchall_results or [])
        self.execute_read_results = list(execute_read_results or [])
        self.execute_write_results = list(execute_write_results or [])
        self.execute_exceptions = list(execute_exceptions or [])
        self.fetchone_exceptions = list(fetchone_exceptions or [])
        self.fetchall_exceptions = list(fetchall_exceptions or [])
        self.execute_read_exceptions = list(execute_read_exceptions or [])
        self.execute_write_exceptions = list(execute_write_exceptions or [])
        self.connection_enters = 0
        self.connection_exits = 0
        self.transaction_enters = 0
        self.transaction_exits = 0
        self.cursor_enters = 0
        self.cursor_exits = 0
        self.async_pool = RecordingAsyncPool(self)

    def _raise_next(self, attr):
        exceptions = getattr(self, attr)
        if not exceptions:
            return
        exc = exceptions.pop(0)
        if exc is not None:
            raise exc

    def build_cypher(
        self,
        cypher_query,
        return_types="result agtype",
        graph_name="knoggin_graph",
    ):
        return f"cypher<{graph_name}|{return_types}>:{cypher_query}"

    async def execute_read(self, query, params=None):
        self.calls.append(("execute_read", query, params))
        self._raise_next("execute_read_exceptions")
        if not self.execute_read_results:
            return []
        return self.execute_read_results.pop(0)

    async def execute_write(self, query, params=None):
        self.calls.append(("execute_write", query, params))
        self._raise_next("execute_write_exceptions")
        if not self.execute_write_results:
            return 1
        return self.execute_write_results.pop(0)
