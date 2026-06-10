import fnmatch
import json
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any

from common.utils.time_utils import get_now_iso


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

    async def ping(self):
        return True

    def pipeline(self):
        return FakePipelineWriter(self)

    async def hset(self, key, field, value):
        self.hashes[key][str(field)] = value
        return 1

    async def hget(self, key, field):
        return self.hashes.get(key, {}).get(str(field))

    async def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    async def hlen(self, key):
        return len(self.hashes.get(key, {}))

    async def hmget(self, key, *fields):
        return [self.hashes.get(key, {}).get(str(f)) for f in fields]

    async def hdel(self, key, *fields):
        removed = 0
        for f in fields:
            removed += int(self.hashes.get(key, {}).pop(str(f), None) is not None)
        return removed

    async def sadd(self, key, *values):
        before = len(self.sets[key])
        self.sets[key].update(str(value) for value in values)
        return len(self.sets[key]) - before

    async def srem(self, key, *values):
        removed = 0
        for value in values:
            if str(value) in self.sets.get(key, set()):
                self.sets[key].remove(str(value))
                removed += 1
        return removed

    async def smembers(self, key):
        return set(self.sets.get(key, set()))

    async def scard(self, key):
        return len(self.sets.get(key, set()))

    async def srandmember(self, key, number=None):
        values = sorted(self.sets.get(key, set()))
        if number is None:
            return values[0] if values else None
        return values[: int(number)]

    async def delete(self, *keys):
        deleted = 0
        for key in keys:
            existed = False
            for store in (self.strings, self.hashes, self.sets, self.lists, self.zsets):
                if key in store:
                    existed = True
                    del store[key]
            if existed:
                self.deleted_keys.append(key)
                deleted += 1
        return deleted

    async def scan(self, cursor=0, match=None, count=100):
        keys = set()
        for store in (self.strings, self.hashes, self.sets, self.lists, self.zsets):
            keys.update(store.keys())
        if match:
            keys = {key for key in keys if fnmatch.fnmatch(key, match)}
        return 0, sorted(keys)

    async def incr(self, key):
        return await self.incrby(key, 1)

    async def incrby(self, key, amount):
        current = int(self.strings.get(key, "0")) + int(amount)
        self.strings[key] = str(current)
        return current

    async def get(self, key):
        return self.strings.get(key)

    async def set(self, key, value, ex=None, nx=False):
        if nx and key in self.strings:
            return False
        self.strings[key] = str(value)
        return True

    async def setex(self, key, ttl, value):
        self.strings[key] = str(value)
        self.expirations.append((key, ttl))
        return True

    async def rpush(self, key, value):
        self.lists[key].append(value)
        return len(self.lists[key])

    async def lpop(self, key):
        if not self.lists.get(key):
            return None
        return self.lists[key].pop(0)

    async def llen(self, key):
        return len(self.lists.get(key, []))

    async def lrange(self, key, start, end):
        items = self.lists.get(key, [])
        stop = len(items) if end == -1 else end + 1
        return items[start:stop]

    async def ltrim(self, key, start, end):
        items = self.lists.get(key, [])
        stop = len(items) if end == -1 else end + 1
        self.lists[key] = items[start:stop]
        return True

    async def eval(self, script, numkeys, *args):
        self.evals.append((script, args))
        return None

    async def expire(self, key, ttl):
        self.expirations.append((key, ttl))
        return True

    async def zadd(self, key, mapping):
        for member, score in mapping.items():
            self.zsets[key][str(member)] = float(score)
        return len(mapping)

    async def zrange(self, key, start, end, desc=False, **kwargs):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        if desc:
            items = list(reversed(items))
        if end == -1:
            sliced = items[start:]
        else:
            sliced = items[start : end + 1]
        return [member for member, _ in sliced]

    async def zscore(self, key, member):
        return self.zsets.get(key, {}).get(str(member))

    async def zrank(self, key, member):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        target = str(member)
        for index, (item, _) in enumerate(items):
            if item == target:
                return index
        return None

    async def zremrangebyrank(self, key, start, end):
        items = sorted(self.zsets.get(key, {}).items(), key=lambda item: item[1])
        if not items:
            return 0
        stop = len(items) if end == -1 else end + 1
        removed = items[start:stop]
        for member, _ in removed:
            self.zsets[key].pop(member, None)
        return len(removed)


class FakeGraphClient:
    def __init__(self):
        self.saved_message_logs = []
        self.recent_project_messages = []

    async def get_max_entity_id(self):
        return 0

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


@dataclass
class FakeResources:
    redis: FakeRedis = field(default_factory=FakeRedis)
    graph_client: FakeGraphClient = field(default_factory=FakeGraphClient)
    embedding: FakeEmbeddingService = field(default_factory=FakeEmbeddingService)
    llm_service: FakeLLMService = field(default_factory=FakeLLMService)
    executor: Any = None
    gliner: Any = None
    spacy: Any = None


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


class FakeFileRAG:
    def __init__(self):
        self.cleanup_count = 0

    def cleanup_session(self):
        self.cleanup_count += 1


class FakeContext:
    def __init__(self, session_id="session-1", project_id="global"):
        self.session_id = session_id
        self.project_id = project_id
        self.shutdown_count = 0
        self.file_rag = FakeFileRAG()

    async def shutdown(self):
        self.shutdown_count += 1


class FakeProjectManager:
    def __init__(self, project_state=None):
        self.project_state = project_state
        self.acquire_calls: list[tuple[str, str]] = []
        self.release_calls: list[str] = []
        self.add_session_calls: list[tuple[str, str]] = []
        self.remove_session_calls: list[tuple[str, str]] = []

    async def acquire_project_for_session(
        self, project_id, session_id, topics_config=None
    ):
        self.acquire_calls.append((project_id, session_id))
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
