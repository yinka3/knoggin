import fnmatch
import json
import time
from collections import defaultdict
from contextlib import asynccontextmanager
from dataclasses import dataclass, field
from datetime import datetime, timezone
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

    async def mget(self, *keys):
        return [await self.get(key) for key in keys]

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

    async def lmove(self, source, destination, src="LEFT", dest="RIGHT"):
        self._purge_expired(source)
        self._purge_expired(destination)
        if not self.lists.get(source):
            return None
        src = str(src).upper()
        dest = str(dest).upper()
        value = self.lists[source].pop(0) if src == "LEFT" else self.lists[source].pop()
        if dest == "LEFT":
            self.lists[destination].insert(0, value)
        else:
            self.lists[destination].append(value)
        return value

    async def lrem(self, key, count, value):
        self._purge_expired(key)
        items = self.lists.get(key, [])
        if count == 0:
            removed = items.count(value)
            self.lists[key] = [item for item in items if item != value]
            return removed

        remaining = []
        removed = 0
        limit = abs(int(count))
        iterable = items if count > 0 else list(reversed(items))
        for item in iterable:
            if item == value and removed < limit:
                removed += 1
                continue
            remaining.append(item)
        self.lists[key] = remaining if count > 0 else list(reversed(remaining))
        return removed

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
        if numkeys == 1 and len(args) >= 3:
            key, expected_value, ttl = args[0], str(args[1]), args[2]
            self._purge_expired(key)
            if self.strings.get(key) == expected_value:
                return await self.expire(key, ttl)
            return 0
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
                        score < start_score if start_exclusive else score <= start_score
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
            removed += int(self.zsets.get(key, {}).pop(str(member), None) is not None)
        return removed


class FakeKnowledgeStore:
    def __init__(self):
        self.saved_message_logs = []
        self.saved_candidate_suggestions = []
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
        return {"messages": 0, "entities": 0, "episodes": 0, "identity": 1}

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

    async def save_assistant_message_with_source_refs(self, message, candidates):
        self.saved_message_logs.append([message])
        return list(candidates)

    async def save_candidate_suggestions(self, scope, suggestions):
        self.saved_candidate_suggestions.append((scope, list(suggestions)))
        return len(suggestions)

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
        self.agents: dict[str, dict[str, Any]] = {}
        self.projects: dict[str, dict[str, Any]] = {}
        self.project_read_scopes: dict[tuple[str, str], set[str]] = defaultdict(set)
        self.sessions: dict[str, dict[str, Any]] = {}
        self.messages: list[dict[str, Any]] = []

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        if not self.read_results:
            inserted = self._fetch_write_against_stores(query, params or {})
            if inserted is not None:
                return inserted
            return self._fetch_from_stores(query, params or {})
        return self.read_results.pop(0)

    async def fetch_one(self, query, params=None):
        self.calls.append(("fetch_one", query, params))
        if not self.read_results:
            return None
        result = self.read_results.pop(0)
        if isinstance(result, list):
            return result[0] if result else None
        return result

    async def execute(self, query, params=None):
        self.calls.append(("execute", query, params))
        self._execute_against_stores(query, params or {})
        return self.write_count

    @staticmethod
    def build_cypher(
        cypher_query,
        return_types="result agtype",
        graph_name="knoggin_graph",
    ):
        return f"cypher<{graph_name}|{return_types}>:{cypher_query}"

    @asynccontextmanager
    async def transaction(self):
        yield _FakePostgresCursor(self)

    @staticmethod
    def _now():
        return datetime.now(timezone.utc)

    def upsert_agent(self, agent):
        data = agent.to_dict() if hasattr(agent, "to_dict") else dict(agent)
        self.agents[data["id"]] = {
            "agent_id": data["id"],
            "user_name": data.get("user_name", "ada"),
            "name": data["name"],
            "persona": data.get("persona_markdown") or data.get("persona"),
            "brain": data.get("brain"),
            "model": data.get("model"),
            "temperature": data.get("temperature", 0.7),
            "enabled_tools": data.get("enabled_tools"),
            "is_default": data.get("is_default", False),
            "is_spawned": data.get("is_spawned", False),
            "spawned_by": data.get("spawned_by"),
            "brain_revision": data.get("brain_revision", 1),
            "created_at": data.get("created_at", get_now_iso()),
        }

    def upsert_project(self, project_id, status="active", user_name="ada"):
        self.projects[project_id] = {
            "project_id": project_id,
            "user_name": user_name,
            "name": project_id,
            "description": None,
            "access_mode": "open",
            "status": status,
            "topic_config": {},
            "created_at": self._now(),
            "updated_at": self._now(),
            "archived_at": None,
            "deleted_at": None,
            "last_activity_at": None,
        }

    def _project_row(self, row):
        result = dict(row)
        key = (row.get("user_name"), row.get("project_id"))
        result["session_count"] = sum(
            1
            for session in self.sessions.values()
            if session.get("project_id") == row.get("project_id")
        )
        result["allowed_projects"] = sorted(self.project_read_scopes.get(key, set()))
        return result

    def _fetch_write_against_stores(self, query, params):
        normalized = " ".join(query.lower().split())
        if "insert into public.projects" in normalized:
            project_id = params.get("project_id")
            if not project_id:
                return []
            now = self._now()
            self.projects[project_id] = {
                "project_id": project_id,
                "user_name": params.get("user_name"),
                "name": params.get("name"),
                "description": params.get("description"),
                "access_mode": params.get("access_mode", "open"),
                "status": params.get("status", "active"),
                "topic_config": json.loads(params["topic_config"])
                if isinstance(params.get("topic_config"), str)
                else params.get("topic_config", {}),
                "created_at": now,
                "updated_at": now,
                "archived_at": None,
                "deleted_at": None,
                "last_activity_at": None,
            }
            return [{"created_at": now, "updated_at": now}]
        return None

    def _fetch_from_stores(self, query, params):
        normalized = " ".join(query.lower().split())
        if "from public.projects" in normalized:
            rows = [
                self._project_row(row)
                for row in sorted(
                    self.projects.values(),
                    key=lambda item: item["project_id"],
                )
                if row.get("user_name") == params.get("user_name")
            ]
            if "and p.project_id = %(project_id)s" in normalized:
                rows = [
                    row
                    for row in rows
                    if row.get("project_id") == params.get("project_id")
                ]
            elif "and project_id = any(%(requested)s)" in normalized:
                requested = set(params.get("requested") or [])
                rows = [row for row in rows if row.get("project_id") in requested]
            elif "and project_id = any(%(allowed)s)" in normalized:
                allowed = set(params.get("allowed") or [])
                rows = [
                    {"project_id": row["project_id"]}
                    for row in rows
                    if row.get("project_id") in allowed
                    and row.get("status") in {"active", "archived"}
                ]
            elif "status in ('active', 'archived')" in normalized:
                rows = [
                    {"project_id": row["project_id"]}
                    for row in rows
                    if row.get("status") in {"active", "archived"}
                ]
            return rows[:1] if "limit 1" in normalized else rows

        if "from public.sessions" in normalized:
            rows = [
                dict(row)
                for row in self.sessions.values()
                if row.get("user_name") == params.get("user_name")
            ]
            if "session_id = %(session_id)s" in normalized:
                rows = [
                    row
                    for row in rows
                    if row.get("session_id") == params.get("session_id")
                ]
            return rows[:1] if "limit 1" in normalized else rows

        if "from public.messages" in normalized:
            rows = [
                {
                    "message_id": row["message_id"],
                    "role": row["role"],
                    "content": row["content"],
                    "timestamp": row["timestamp_ms"],
                }
                for row in self.messages
                if row.get("user_name") == params.get("user_name")
                and row.get("session_id") == params.get("session_id")
            ]
            rows.sort(key=lambda row: row["message_id"])
            limit = params.get("limit")
            return rows[: int(limit)] if limit else rows

        if "from public.agents" not in normalized:
            return []

        rows = [
            dict(row)
            for row in self.agents.values()
            if row.get("user_name") == params.get("user_name")
        ]
        if "and agent_id = %(agent_id)s" in normalized:
            rows = [row for row in rows if row["agent_id"] == params.get("agent_id")]
        elif "lower(name) = lower(%(name)s)" in normalized:
            wanted = str(params.get("name", "")).lower()
            rows = [row for row in rows if row["name"].lower() == wanted]
        elif "is_default = true" in normalized:
            rows = [row for row in rows if row.get("is_default")]
        elif "agent_id = any(%(agent_ids)s)" in normalized:
            ids = set(params.get("agent_ids") or [])
            rows = [row for row in rows if row["agent_id"] in ids]
            if "count(*)" in normalized:
                return [{"count": sum(1 for row in rows if row.get("is_spawned"))}]
        return rows[:1] if "limit 1" in normalized else rows

    def _execute_against_stores(self, query, params):
        normalized = " ".join(query.lower().split())
        if "insert into public.projects" in normalized:
            project_id = params.get("project_id")
            if not project_id:
                return
            now = self._now()
            self.projects[project_id] = {
                "project_id": project_id,
                "user_name": params.get("user_name"),
                "name": params.get("name"),
                "description": params.get("description"),
                "access_mode": params.get("access_mode", "open"),
                "status": params.get("status", "active"),
                "topic_config": json.loads(params["topic_config"])
                if isinstance(params.get("topic_config"), str)
                else params.get("topic_config", {}),
                "created_at": now,
                "updated_at": now,
                "archived_at": None,
                "deleted_at": None,
                "last_activity_at": None,
            }
            return

        if "insert into public.project_read_scopes" in normalized:
            key = (params.get("user_name"), params.get("project_id"))
            self.project_read_scopes[key].add(params.get("readable"))
            return

        if "delete from public.project_read_scopes" in normalized:
            key = (params.get("user_name"), params.get("project_id"))
            self.project_read_scopes.pop(key, None)
            return

        if "insert into public.sessions" in normalized:
            session_id = params.get("session_id")
            if not session_id:
                return
            now = self._now()
            self.sessions[session_id] = {
                "session_id": session_id,
                "user_name": params.get("user_name"),
                "project_id": params.get("project_id"),
                "model": params.get("model"),
                "agent_id": params.get("agent_id"),
                "enabled_tools": json.loads(params["enabled_tools"])
                if isinstance(params.get("enabled_tools"), str)
                else params.get("enabled_tools"),
                "document_focus": None,
                "status": "open",
                "created_at": now,
                "last_active_at": now,
                "deleted_at": None,
            }
            return

        if "update public.sessions" in normalized:
            row = self.sessions.get(params.get("session_id"))
            if not row:
                return
            if "document_focus = null" in normalized:
                row["document_focus"] = None
            elif "document_focus = %(focus)s" in normalized:
                focus = params.get("focus")
                row["document_focus"] = (
                    json.loads(focus) if isinstance(focus, str) else focus
                )
            if "last_active_at = now()" in normalized:
                row["last_active_at"] = self._now()
            for field in ("model", "agent_id", "enabled_tools"):
                if field in params:
                    value = params[field]
                    if field == "enabled_tools" and isinstance(value, str):
                        value = json.loads(value)
                    row[field] = value
            return

        if "delete from public.messages" in normalized:
            self.messages = [
                row
                for row in self.messages
                if not (
                    row.get("user_name") == params.get("user_name")
                    and row.get("session_id") == params.get("session_id")
                    and (
                        "message_id = %(message_id)s" not in normalized
                        or row.get("message_id") == params.get("message_id")
                    )
                )
            ]
            return

        if "delete from public.sessions" in normalized:
            self.sessions.pop(params.get("session_id"), None)
            return

        if "delete from public.projects" in normalized:
            project_id = params.get("project_id")
            self.projects.pop(project_id, None)
            self.sessions = {
                session_id: row
                for session_id, row in self.sessions.items()
                if row.get("project_id") != project_id
            }
            self.messages = [
                row for row in self.messages if row.get("project_id") != project_id
            ]
            return

        if "update public.projects" in normalized:
            row = self.projects.get(params.get("project_id"))
            if not row:
                return
            for field in ("name", "description", "status", "topic_config"):
                if field in params:
                    value = params[field]
                    if field == "topic_config" and isinstance(value, str):
                        value = json.loads(value)
                    row[field] = value
            if "archived_at = now()" in normalized:
                row["archived_at"] = self._now()
            if "archived_at = null" in normalized:
                row["archived_at"] = None
            if "deleted_at = now()" in normalized:
                row["deleted_at"] = self._now()
            row["updated_at"] = self._now()
            return

        if "insert into public.agents" in normalized:
            agent_id = params.get("agent_id")
            if not agent_id:
                return
            self.agents[agent_id] = {
                "agent_id": agent_id,
                "user_name": params.get("user_name"),
                "project_id": params.get("project_id"),
                "name": params.get("name"),
                "persona": params.get("persona"),
                "brain": params.get("brain"),
                "model": params.get("model"),
                "temperature": params.get("temperature", 0.7),
                "enabled_tools": json.loads(params["enabled_tools"])
                if isinstance(params.get("enabled_tools"), str)
                else params.get("enabled_tools"),
                "is_default": "%(enabled_tools)s, true" in normalized,
                "is_spawned": "true, %(spawned_by)s" in normalized,
                "spawned_by": params.get("spawned_by"),
                "brain_revision": 1,
                "created_at": get_now_iso(),
            }
            return

        if "update public.agents" in normalized:
            if "set is_default = (agent_id = %(agent_id)s)" in normalized:
                for candidate_id, candidate in self.agents.items():
                    if candidate.get("user_name") == params.get("user_name"):
                        candidate["is_default"] = candidate_id == params.get(
                            "agent_id"
                        )
                return
            if "set is_default = false" in normalized:
                for row in self.agents.values():
                    if row.get("user_name") == params.get("user_name"):
                        row["is_default"] = False
                return
            agent_id = params.get("agent_id")
            row = self.agents.get(agent_id)
            if not row:
                return
            for field in ("name", "brain", "model", "temperature", "enabled_tools"):
                if field in params:
                    value = params[field]
                    if field == "enabled_tools" and isinstance(value, str):
                        value = json.loads(value)
                    row[field] = value
            if "brain_revision = brain_revision + 1" in normalized:
                row["brain_revision"] += 1
            if "set is_default = true" in normalized:
                row["is_default"] = True
            return

        if "delete from public.agents" in normalized:
            self.agents.pop(params.get("agent_id"), None)


class _FakePostgresCursor:
    def __init__(self, client):
        self.client = client

    async def execute(self, query, params=None):
        self.client.calls.append(("execute", query, params))
        self.client._execute_against_stores(query, params or {})
        return self.client.write_count


@dataclass
class FakeResources:
    redis: FakeRedis = field(default_factory=FakeRedis)
    redis_manager: Any = None
    knowledge_store: FakeKnowledgeStore = field(default_factory=FakeKnowledgeStore)
    postgres: FakePostgresClient = field(default_factory=FakePostgresClient)
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


class FakeSession:
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

    async def execute(self, query, params=None):
        self.client.calls.append(("execute", query, params))
        self.client._raise_next("cursor_execute_exceptions")

    async def fetchone(self):
        self.client._raise_next("fetch_one_exceptions")
        if not self.client.fetch_one_results:
            return None
        return self.client.fetch_one_results.pop(0)

    async def fetchall(self):
        self.client._raise_next("fetch_all_exceptions")
        if not self.client.fetch_all_results:
            return []
        return self.client.fetch_all_results.pop(0)


class RecordingPostgresClient:
    def __init__(
        self,
        fetch_one_results=None,
        fetch_all_results=None,
        transaction_exceptions=None,
        cursor_execute_exceptions=None,
        execute_exceptions=None,
        fetch_one_exceptions=None,
        fetch_all_exceptions=None,
    ):
        self.calls = []
        self.fetch_one_results = list(fetch_one_results or [])
        self.fetch_all_results = list(fetch_all_results or [])
        self.transaction_exceptions = list(transaction_exceptions or [])
        self.cursor_execute_exceptions = list(cursor_execute_exceptions or [])
        self.execute_exceptions = list(execute_exceptions or [])
        self.fetch_one_exceptions = list(fetch_one_exceptions or [])
        self.fetch_all_exceptions = list(fetch_all_exceptions or [])
        self.transaction_enters = 0
        self.transaction_exits = 0
        self.cursor_enters = 0
        self.cursor_exits = 0

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

    @asynccontextmanager
    async def transaction(self):
        self.transaction_enters += 1
        cursor_entered = False
        try:
            self._raise_next("transaction_exceptions")
            self.cursor_enters += 1
            cursor_entered = True
            yield RecordingCursor(self)
        finally:
            if cursor_entered:
                self.cursor_exits += 1
            self.transaction_exits += 1

    async def fetch_one(self, query, params=None):
        self.calls.append(("fetch_one", query, params))
        self._raise_next("fetch_one_exceptions")
        if not self.fetch_one_results:
            return None
        return self.fetch_one_results.pop(0)

    async def fetch_all(self, query, params=None):
        self.calls.append(("fetch_all", query, params))
        self._raise_next("fetch_all_exceptions")
        if not self.fetch_all_results:
            return []
        return self.fetch_all_results.pop(0)

    async def execute(self, query, params=None):
        self.calls.append(("execute_command", query, params))
        self._raise_next("execute_exceptions")
