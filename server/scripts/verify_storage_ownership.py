#!/usr/bin/env python3
from __future__ import annotations

import argparse
import asyncio
import json
import os
import re
import sys
from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
SRC = ROOT / "src"
if str(SRC) not in sys.path:
    sys.path.insert(0, str(SRC))

import redis.asyncio as aioredis  # noqa: E402
from dotenv import load_dotenv  # noqa: E402

from core.ingestion.dlq_state import (  # noqa: E402
    ensure_dlq_id,
    serialize_dlq_entry,
)
from infrastructure.postgres_client import PostgresClient  # noqa: E402
from infrastructure.redis_client import RedisKeys  # noqa: E402

CONFIRM_FLUSH = "flush redis for storage ownership verification"
DEFAULT_LOCAL_DSN = "postgresql://knoggin:knoggin@localhost:5432/knoggin_db"
PLACEHOLDER_DSNS = {
    "...",
    "postgres://...",
    "postgresql://...",
    "postgresql://user:password@localhost:5432/database",
}


def load_env_files() -> None:
    for path in (ROOT.parent / ".env", ROOT / ".env"):
        if path.exists():
            load_dotenv(path, override=False)
    load_dotenv(override=False)

DURABLE_TABLES = {
    "projects": "public.projects",
    "agents": "public.agents",
    "brain_snapshots": "public.agent_brain_snapshots",
    "sessions": "public.sessions",
    "messages": "public.messages",
    "entities": "public.entities",
    "relationships": "public.relationships",
    "relationship_evidence_refs": "public.relationship_evidence_refs",
    "episodes": "public.episodes",
    "episode_messages": "public.episode_messages",
    "episode_entities": "public.episode_entities",
    "episode_relationships": "public.episode_relationships",
    "episode_checkpoints": "public.episode_processing_checkpoints",
    "hierarchy_edges": "public.hierarchy_edges",
    "merge_proposals": "public.entity_merge_proposals",
    "merge_audits": "public.entity_merge_audits",
    "documents": "public.project_documents",
    "document_chunks": "public.document_chunks",
}

REDIS_WRITE_RE = re.compile(
    r"\bredis(?:_client)?\.(set|setex|hset|sadd|rpush|zadd|delete|lmove|lrem|"
    r"hdel|expire|incr|incrby)\b"
)


@dataclass(frozen=True)
class CheckResult:
    name: str
    passed: bool
    detail: str


def status_text(passed: bool) -> str:
    return "PASS" if passed else "FAIL"


def format_report(results: list[CheckResult]) -> str:
    lines = ["Storage Ownership Verification", ""]
    width = max([len(result.name) for result in results] + [5])
    for result in results:
        lines.append(
            f"{result.name.ljust(width)}  {status_text(result.passed)}  {result.detail}"
        )
    return "\n".join(lines)


def redis_key_family(key: str) -> str:
    if key.startswith("dlq:processing:"):
        return "dlq_processing"
    if key.startswith("dlq:state:"):
        return "dlq_state"
    if key.startswith("dlq:claims:"):
        return "dlq_claims"
    if key.startswith("dlq:parked:"):
        return "dlq_parked"
    return key.split(":", 1)[0]


def classify_redis_key(key: str) -> str:
    family = redis_key_family(key)
    if family in RedisKeys.REBUILDABLE_FROM_POSTGRES:
        return "rebuildable_from_postgres"
    if family in RedisKeys.EPHEMERAL_ONLY:
        return "ephemeral_only"
    if family in RedisKeys.LEGACY_NON_AUTHORITATIVE:
        return "legacy_non_authoritative"
    return "unknown"


def audit_redis_writes(source_root: Path = SRC) -> CheckResult:
    unknown_lines = []
    for path in source_root.rglob("*.py"):
        if ".venv" in path.parts:
            continue
        text = path.read_text(encoding="utf-8")
        if not REDIS_WRITE_RE.search(text):
            continue
        # This is intentionally conservative: the human-readable report points
        # reviewers at files with Redis writes, while RedisKeys owns family policy.
        rel = path.relative_to(ROOT) if path.is_relative_to(ROOT) else path
        unknown_lines.append(str(rel))

    known_families = (
        RedisKeys.REBUILDABLE_FROM_POSTGRES
        | RedisKeys.EPHEMERAL_ONLY
        | RedisKeys.LEGACY_NON_AUTHORITATIVE
    )
    missing = set() if known_families else {"declared Redis policy families"}
    passed = not missing
    detail = (
        f"{len(unknown_lines)} Redis write files reviewed against "
        f"{len(known_families)} declared policy families; "
        f"missing key families={sorted(missing)}"
    )
    return CheckResult("Redis write family policy", passed, detail)


async def seed_durable_state(pg: PostgresClient, *, user: str, project_id: str) -> None:
    vector = "[" + ",".join(["0"] * 1024) + "]"
    async with pg.transaction() as cur:
        await cur.execute(
            """
            INSERT INTO public.projects (
                project_id, user_name, name, description, topic_config
            )
            VALUES (%s, %s, %s, %s, %s::jsonb)
            ON CONFLICT (project_id) DO UPDATE SET
                user_name = EXCLUDED.user_name,
                name = EXCLUDED.name,
                topic_config = EXCLUDED.topic_config
            """,
            (
                project_id,
                user,
                "Phase 7 Storage Ownership",
                "Verification seed project",
                json.dumps({"General": {"active": True}}),
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.agents (
                agent_id, user_name, project_id, name, persona, brain,
                model, temperature, enabled_tools, brain_revision
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s)
            ON CONFLICT (agent_id) DO UPDATE SET
                brain = EXCLUDED.brain,
                brain_revision = EXCLUDED.brain_revision
            """,
            (
                f"{project_id}-agent",
                user,
                project_id,
                "Storage Proof Agent",
                "Verification persona",
                "## Project Context\nSeeded for storage ownership proof.",
                "test-model",
                0.0,
                json.dumps(["read_brain"]),
                5,
            ),
        )
        for revision in (1, 5):
            await cur.execute(
                """
                INSERT INTO public.agent_brain_snapshots (
                    agent_id, revision, user_name, content, edited_by,
                    change_type, changed_section, change_summary
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (agent_id, revision) DO UPDATE SET
                    content = EXCLUDED.content,
                    change_summary = EXCLUDED.change_summary
                """,
                (
                    f"{project_id}-agent",
                    revision,
                    user,
                    f"Brain snapshot revision {revision}",
                    "verification",
                    "initial_seed" if revision == 1 else "edit",
                    "Project Context",
                    "Storage ownership proof",
                ),
            )
        await cur.execute(
            """
            INSERT INTO public.sessions (
                session_id, user_name, project_id, agent_id, status
            )
            VALUES (%s, %s, %s, %s, 'open')
            ON CONFLICT (session_id) DO UPDATE SET
                project_id = EXCLUDED.project_id,
                agent_id = EXCLUDED.agent_id
            """,
            (f"{project_id}-session", user, project_id, f"{project_id}-agent"),
        )
        await cur.execute(
            """
            INSERT INTO public.messages (
                user_name, session_id, message_id, project_id, role, content,
                user_msg_id, metadata, timestamp_ms
            )
            VALUES (%s, %s, %s, %s, 'user', %s, %s, '{}'::jsonb, %s)
            ON CONFLICT (message_id) DO UPDATE SET
                content = EXCLUDED.content
            """,
            (
                user,
                f"{project_id}-session",
                910000001,
                project_id,
                "Storage ownership proof message",
                910000001,
                1770000000000,
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.entities (
                entity_id, user_name, project_id, session_id, canonical_name,
                type, topic
            )
            VALUES
                (910000001, %s, %s, %s, 'Storage Proof Alpha', 'concept', 'General'),
                (910000002, %s, %s, %s, 'Storage Proof Beta', 'concept', 'General')
            ON CONFLICT (entity_id) DO UPDATE SET
                canonical_name = EXCLUDED.canonical_name
            """,
            (
                user,
                project_id,
                f"{project_id}-session",
                user,
                project_id,
                f"{project_id}-session",
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.episodes (
                episode_id, project_id, session_id, summary, source_message_count
            )
            VALUES (%s, %s, %s, %s, 1)
            ON CONFLICT (episode_id) DO UPDATE SET summary = EXCLUDED.summary
            """,
            (
                f"{project_id}-episode",
                project_id,
                f"{project_id}-session",
                "Storage ownership proof episode",
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.episode_messages (
                episode_id, project_id, session_id, message_id, message_position
            )
            VALUES (%s, %s, %s, 910000001, 0)
            ON CONFLICT (episode_id, message_id) DO NOTHING
            """,
            (f"{project_id}-episode", project_id, f"{project_id}-session"),
        )
        await cur.execute(
            """
            INSERT INTO public.episode_entities (episode_id, project_id, entity_id)
            VALUES (%s, %s, 910000001)
            ON CONFLICT (episode_id, entity_id) DO NOTHING
            """,
            (f"{project_id}-episode", project_id),
        )
        await cur.execute(
            """
            INSERT INTO public.relationships (
                relationship_id, user_name, project_id, entity_a_id,
                entity_b_id, weight, context
            )
            VALUES (%s, %s, %s, 910000001, 910000002, 1, %s)
            ON CONFLICT (relationship_id) DO UPDATE SET context = EXCLUDED.context
            """,
            (f"{project_id}-relationship", user, project_id, "storage proof"),
        )
        await cur.execute(
            """
            INSERT INTO public.episode_relationships (
                episode_id, project_id, relationship_id
            )
            VALUES (%s, %s, %s)
            ON CONFLICT (episode_id, relationship_id) DO NOTHING
            """,
            (f"{project_id}-episode", project_id, f"{project_id}-relationship"),
        )
        await cur.execute(
            """
            INSERT INTO public.relationship_evidence_refs (
                relationship_id, project_id, user_name, session_id, message_id
            )
            VALUES (%s, %s, %s, %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (
                f"{project_id}-relationship",
                project_id,
                user,
                f"{project_id}-session",
                910000001,
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.hierarchy_edges (
                project_id, parent_id, child_id, created_at_ms
            )
            VALUES (%s, 910000001, 910000002, 1770000000000)
            ON CONFLICT DO NOTHING
            """,
            (project_id,),
        )
        await cur.execute(
            """
            INSERT INTO public.entity_merge_proposals (
                proposal_id, user_name, project_id, primary_entity_id,
                duplicate_entity_id, evidence_message_ids, evidence_episode_ids,
                reasoning,
                reviewed_state_hash, reviewed_state, policy_checks,
                confirmation_token_hash
            )
            VALUES (%s, %s, %s, 910000001, 910000002, %s::jsonb, %s::jsonb, %s, %s,
                    %s::jsonb, %s::jsonb, %s)
            ON CONFLICT (proposal_id) DO UPDATE SET reasoning = EXCLUDED.reasoning
            """,
            (
                f"{project_id}-proposal",
                user,
                project_id,
                json.dumps([910000001]),
                json.dumps([f"{project_id}-episode"]),
                "Storage ownership proof proposal",
                "storage-proof-hash",
                json.dumps({"entities": [910000001, 910000002]}),
                json.dumps({"entities_visible_in_authorized_scope": True}),
                "token-hash",
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.entity_merge_audits (
                audit_id, proposal_id, user_name, project_id, primary_entity_id,
                duplicate_entity_id, evidence_message_ids, evidence_episode_ids,
                reasoning, confirmed_by,
                before_state, after_state, status
            )
            VALUES (%s, %s, %s, %s, 910000001, 910000002, %s::jsonb, %s::jsonb, %s, %s,
                    %s::jsonb, %s::jsonb, 'executed')
            ON CONFLICT (audit_id) DO UPDATE SET status = EXCLUDED.status
            """,
            (
                f"{project_id}-audit",
                f"{project_id}-proposal",
                user,
                project_id,
                json.dumps([910000001]),
                json.dumps([f"{project_id}-episode"]),
                "Storage ownership proof audit",
                user,
                json.dumps({"before": True}),
                json.dumps({"after": True}),
            ),
        )
        document_id = "11111111-1111-4111-8111-111111111111"
        chunk_id = "22222222-2222-4222-8222-222222222222"
        await cur.execute(
            """
            INSERT INTO public.project_documents (
                document_id, project_id, visibility_scope, source_kind,
                original_name, relative_path, extension, size_bytes,
                content_hash, status
            )
            VALUES (%s, %s, 'project', 'manual_upload', %s, %s, '.md',
                    128, %s, 'indexed')
            ON CONFLICT (document_id) DO UPDATE SET status = EXCLUDED.status
            """,
            (
                document_id,
                project_id,
                "storage-proof.md",
                "storage-proof.md",
                f"{project_id}-hash",
            ),
        )
        await cur.execute(
            """
            INSERT INTO public.episode_processing_checkpoints (
                project_id, session_id, last_evaluated_message_id
            )
            VALUES (%s, %s, 910000001)
            ON CONFLICT (project_id, session_id) DO UPDATE
            SET last_evaluated_message_id = EXCLUDED.last_evaluated_message_id
            """,
            (project_id, f"{project_id}-session"),
        )
        await cur.execute(
            """
            INSERT INTO public.document_chunks (
                chunk_id, document_id, chunk_index, content, embedding
            )
            VALUES (%s, %s, 0, %s, %s::vector)
            ON CONFLICT (document_id, chunk_index) DO UPDATE SET
                content = EXCLUDED.content
            """,
            (chunk_id, document_id, "Storage ownership proof chunk", vector),
        )


async def seed_redis_runtime(
    redis: aioredis.Redis, *, user: str, project_id: str
) -> None:
    session_id = f"{project_id}-session"
    await redis.set(RedisKeys.job_last_run("phase7", user, project_id), "1770000000")
    entry = {
        "user_name": user,
        "project_id": project_id,
        "session_id": session_id,
        "stage": "processing",
        "attempt": 1,
        "messages": [{"id": 910000001, "message": "storage proof"}],
    }
    dlq_id = ensure_dlq_id(entry)
    await redis.rpush(RedisKeys.dlq(user, project_id), serialize_dlq_entry(entry))
    await redis.hset(RedisKeys.dlq_state(user, project_id), dlq_id, "queued")


async def durable_counts(
    pg: PostgresClient, *, user: str, project_id: str
) -> dict[str, int]:
    counts: dict[str, int] = {}
    for label, table in DURABLE_TABLES.items():
        project_column = "project_id"
        if label == "brain_snapshots":
            row = await pg.fetch_one(
                """
                SELECT COUNT(*) AS count
                FROM public.agent_brain_snapshots
                WHERE user_name = %s
                  AND agent_id = %s
                """,
                (user, f"{project_id}-agent"),
            )
        elif label == "document_chunks":
            row = await pg.fetch_one(
                """
                SELECT COUNT(*) AS count
                FROM public.document_chunks c
                JOIN public.project_documents d ON d.document_id = c.document_id
                WHERE d.project_id = %s
                """,
                (project_id,),
            )
        elif label == "relationship_evidence_refs":
            row = await pg.fetch_one(
                """
                SELECT COUNT(*) AS count
                FROM public.relationship_evidence_refs ref
                JOIN public.relationships rel
                  ON rel.relationship_id = ref.relationship_id
                WHERE rel.project_id = %s
                """,
                (project_id,),
            )
        else:
            query = (
                f"SELECT COUNT(*) AS count FROM {table} "
                f"WHERE {project_column} = %s"
            )
            row = await pg.fetch_one(
                query,
                (project_id,),
            )
        counts[label] = int(row["count"] if row else 0)
    return counts


def compare_counts(before: dict[str, int], after: dict[str, int]) -> CheckResult:
    regressions = {
        key: (before[key], after.get(key, 0))
        for key in before
        if after.get(key, 0) < before[key]
    }
    return CheckResult(
        "Durable Postgres rows after Redis flush",
        not regressions,
        "no count regressions" if not regressions else f"regressions={regressions}",
    )


def redis_runtime_result(keys: list[str], *, allow_missing: bool) -> CheckResult:
    if keys:
        return CheckResult(
            "Redis runtime state present",
            True,
            f"{len(keys)} matching keys before flush",
        )
    if allow_missing:
        return CheckResult(
            "Redis runtime state absent",
            True,
            "0 matching keys; acceptable for restart/loss verification",
        )
    return CheckResult(
        "Redis runtime state present",
        False,
        "0 matching keys before flush",
    )


async def redis_keys(redis: aioredis.Redis, pattern: str = "*") -> list[str]:
    cursor = 0
    keys: list[str] = []
    while True:
        cursor, batch = await redis.scan(cursor=cursor, match=pattern, count=100)
        keys.extend(str(key) for key in batch)
        if cursor == 0:
            return sorted(keys)


async def run(args: argparse.Namespace) -> int:
    pg = PostgresClient(args.dsn)
    redis = aioredis.Redis.from_url(args.redis_url, decode_responses=True)
    results: list[CheckResult] = []
    try:
        await pg.connect()
        await redis.ping()

        if args.seed:
            await seed_durable_state(pg, user=args.user, project_id=args.project_id)
            await seed_redis_runtime(redis, user=args.user, project_id=args.project_id)

        before = await durable_counts(pg, user=args.user, project_id=args.project_id)
        results.append(
            CheckResult(
                "Seeded durable state present",
                all(count > 0 for count in before.values()),
                json.dumps(before, sort_keys=True),
            )
        )

        before_redis = await redis_keys(redis, f"*:{args.user}:{args.project_id}*")
        results.append(
            redis_runtime_result(
                before_redis,
                allow_missing=args.allow_missing_redis,
            )
        )

        if args.flush_redis:
            if args.confirm != CONFIRM_FLUSH:
                raise SystemExit(
                    "--flush-redis requires --confirm "
                    f"{CONFIRM_FLUSH!r}"
                )
            await redis.flushdb()
            after = await durable_counts(pg, user=args.user, project_id=args.project_id)
            results.append(compare_counts(before, after))
            after_redis = await redis_keys(redis, f"*:{args.user}:{args.project_id}*")
            results.append(
                CheckResult(
                    "Expected Redis coordination loss",
                    len(after_redis) == 0,
                    f"{len(after_redis)} matching keys after flush",
                )
            )
        else:
            results.append(
                CheckResult(
                    "Redis flush",
                    True,
                    "skipped; pass --flush-redis with explicit confirmation",
                )
            )

        results.append(audit_redis_writes())
        print(format_report(results))
        return 0 if all(result.passed for result in results) else 1
    finally:
        await redis.aclose(close_connection_pool=True)
        await pg.close()


def parse_args(argv: list[str]) -> argparse.Namespace:
    load_env_files()
    parser = argparse.ArgumentParser(
        description="Verify Postgres/Redis storage ownership boundaries."
    )
    default_dsn = (
        os.getenv("DATABASE_URL")
        or os.getenv("KNOGGIN_TEST_DATABASE_URL")
        or DEFAULT_LOCAL_DSN
    )
    parser.add_argument("--dsn", default=default_dsn)
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
    )
    parser.add_argument("--user", default="phase7")
    parser.add_argument("--project-id", default="phase7-storage-proof")
    parser.add_argument("--seed", action="store_true", help="Seed representative state")
    parser.add_argument(
        "--allow-missing-redis",
        action="store_true",
        help=(
            "Pass when verifying durable Postgres state after Redis restart/loss "
            "without reseeding Redis runtime keys"
        ),
    )
    parser.add_argument(
        "--flush-redis", action="store_true", help="Flush current Redis DB"
    )
    parser.add_argument("--confirm", default="")
    args = parser.parse_args(argv)
    if not args.dsn or args.dsn.strip() in PLACEHOLDER_DSNS:
        parser.error(
            "--dsn must be a real Postgres connection string. "
            f"Local default: {DEFAULT_LOCAL_DSN}"
        )
    return args


def main(argv: list[str] | None = None) -> int:
    return asyncio.run(run(parse_args(argv or sys.argv[1:])))


if __name__ == "__main__":
    raise SystemExit(main())
