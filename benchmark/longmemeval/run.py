"""Replay LongMemEval questions through Knoggin and record its answers.

For each question: import every haystack session into a fresh project at its
original dates, wait for memory to finish after each one, then ask the
question in a new session at the question's date. Runs the engine in-process
so the test clock is shared with it.
"""

import argparse
import asyncio
import hashlib
import json
import logging
import os
import subprocess
import sys
import time
import traceback
from datetime import datetime, timedelta, timezone
from pathlib import Path

import yaml

from common.conf.domain_config import DomainConfig
from common.exceptions import StorageWriteError
from common.schema.agent.tool_names import tool_groups
from common.schema.primitives import Message
from common.utils.time_utils import reset_clock, set_test_clock
from core.agent import prompt_context
from core.knowledge.context.render import render_context_markdown
from infrastructure.schema_migrations import migrate
from runtime.application import ApplicationRuntime

from dataset import Question, load_questions

logger = logging.getLogger("longmemeval")

# A general personal-life domain; not tuned to the benchmark's questions.
DEFAULT_DOMAIN = Path(__file__).with_name("domain.json")


class _ReplayClock:
    """A session's date, running at real speed from when it is set.

    Importing takes seconds, far under the idle flush, so memory still cannot
    build half a session early; retries after a rate limit still come due.
    """

    def __init__(self, start: datetime) -> None:
        self._start = start
        self._started = time.monotonic()

    def now(self) -> datetime:
        return self._start + timedelta(seconds=time.monotonic() - self._started)


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, required=True,
                        help="longmemeval_oracle.json or longmemeval_s_cleaned.json")
    parser.add_argument("--out", type=Path, required=True, help="run directory")
    parser.add_argument("--types", help="comma-separated question types to keep")
    parser.add_argument("--question-ids", help="comma-separated question IDs to keep")
    parser.add_argument("--limit", type=int, help="stop after this many questions")
    parser.add_argument("--resume", action="store_true",
                        help="skip questions already answered in --out")
    parser.add_argument("--agent-models", default="anthropic/claude-haiku-5.5",
                        help="comma-separated; each answers every question from "
                             "the same memory, built once")
    parser.add_argument("--extraction-model", default="google/gemini-2.5-flash")
    parser.add_argument("--merge-model", default="google/gemini-2.5-pro")
    parser.add_argument("--maintenance-model",
                        help="Context maintainer model; defaults to extraction model")
    parser.add_argument("--domain", type=Path, default=DEFAULT_DOMAIN,
                        help="project domain: topics, entity and relationship types")
    parser.add_argument("--budget-usd", type=float, default=5.0,
                        help="LLM spending ceiling for the whole run")
    parser.add_argument("--no-message-search", action="store_true",
                        help="answer without free-text search over raw messages; an "
                             "Episode's own source messages stay readable")
    parser.add_argument("--maintain-every-episodes", type=int,
                        help="run the Context maintainer this often (default: the setting)")
    parser.add_argument("--notebook-episodes", type=int, default=16,
                        help="Episodes the agent's notebook holds (benchmark only)")
    parser.add_argument("--context-max-tokens", type=int,
                        help="size Context is kept under (default: the setting)")
    parser.add_argument("--settle-timeout", type=float, default=900.0,
                        help="seconds to wait for one session's memory")
    parser.add_argument("--sessions-per-settle", type=int, default=1,
                        help="import this many history sessions before waiting for memory")
    parser.add_argument("--episode-source-tokens", type=int,
                        help="maximum source tokens in one generated episode")
    parser.add_argument("--episode-source-messages", type=int,
                        help="maximum source messages in one generated episode")
    parser.add_argument("--idle-flush-seconds", type=int,
                        help="replayed idle time before an automatic semantic-window flush")
    parser.add_argument("--min-episode-coverage", type=float, default=0.8,
                        help="stop the run if less than this share of a question's "
                             "messages reach an Episode; 0 turns the check off")
    return parser.parse_args()


def _select(questions: list[Question], args) -> list[Question]:
    if args.types:
        types = set(args.types.split(","))
        questions = [q for q in questions if q.question_type in types]
    if args.question_ids:
        ids = set(args.question_ids.split(","))
        questions = [q for q in questions if q.question_id in ids]
    return questions[: args.limit] if args.limit else questions


def _write_config(config_dir: Path, args) -> dict:
    """Pin the models and turn off background work that is not under test."""

    config = {
        "llm": {
            "agent_model": args.agent_models.split(",")[0],
            "extraction_model": args.extraction_model,
            "merge_model": args.merge_model,
            "spending_budget": {"limit_usd": args.budget_usd},
        },
        "developer_settings": {
            "jobs": {
                "conflict_discovery": {"enabled": False},
                "duplicate_review": {"enabled": False},
            },
            "community": {"enabled": False},
            "chat_compaction": {"enabled": False},
        },
    }
    context_overrides = {}
    if args.maintain_every_episodes is not None:
        context_overrides["maintain_every_episodes"] = args.maintain_every_episodes
    if args.maintenance_model is not None:
        context_overrides["maintenance_model"] = args.maintenance_model
    if args.context_max_tokens is not None:
        context_overrides["max_tokens"] = args.context_max_tokens
    ingestion_overrides = {}
    if context_overrides:
        ingestion_overrides["context"] = context_overrides
    if args.idle_flush_seconds is not None:
        ingestion_overrides["idle_flush_seconds"] = args.idle_flush_seconds
    if ingestion_overrides:
        config["developer_settings"]["ingestion"] = ingestion_overrides
    episode_overrides = {}
    if args.episode_source_tokens is not None:
        episode_overrides["max_episode_source_tokens"] = args.episode_source_tokens
    if args.episode_source_messages is not None:
        episode_overrides["max_episode_source_messages"] = args.episode_source_messages
    if episode_overrides:
        config["developer_settings"]["jobs"]["episode"] = episode_overrides
    config["developer_settings"]["limits"] = {
        "max_accumulated_episodes": args.notebook_episodes,
    }
    config_dir.mkdir(parents=True, exist_ok=True)
    (config_dir / "knoggin.yml").write_text(yaml.safe_dump(config))
    return config


def _append(path: Path, row: dict) -> None:
    with path.open("a") as file:
        file.write(json.dumps(row) + "\n")


def _answered(path: Path) -> set[str]:
    if not path.exists():
        return set()
    return {json.loads(line)["question_id"] for line in path.read_text().splitlines()}


async def _snapshot(runtime, project, revision) -> str:
    if revision is None:
        return ""
    snapshot = await runtime.resources.knowledge_store.get_project_context_snapshot(
        str(revision.revision_id),
        user_name=runtime.sessions.user_name,
        project_id=project.project_id,
    )
    return render_context_markdown(snapshot, project.compiled_domain)


class EpisodeCoverageTooLow(Exception):
    """Most of the history has no Episode, so the run would not measure memory."""


IMPORT_ATTEMPTS = 3

# Checked once this many batches have built, so one odd window cannot stop a run.
COVERAGE_CHECK_AFTER_BATCHES = 3


async def _episode_coverage(runtime, project_id: str) -> float:
    """Share of the messages in finished windows that reached an Episode."""

    row = await runtime.resources.postgres.fetch_one(
        "SELECT count(*) AS total, count(attached.message_id) AS attached "
        "FROM public.project_semantic_window_messages AS windowed "
        "JOIN public.project_semantic_windows AS window_row USING (window_id) "
        "LEFT JOIN (SELECT DISTINCT message_id FROM public.episode_messages "
        "           WHERE project_id = %s) AS attached USING (message_id) "
        "WHERE windowed.project_id = %s AND window_row.stage = 'completed'",
        (project_id, project_id),
    )
    return row["attached"] / row["total"] if row["total"] else 1.0


async def _import_with_retry(history, exchanges) -> None:
    """Retry a lost database deadlock; imports skip what is already stored."""

    for attempt in range(IMPORT_ATTEMPTS):
        try:
            await history.import_exchanges(exchanges)
            return
        except StorageWriteError:
            if attempt == IMPORT_ATTEMPTS - 1:
                raise
            logger.warning("Import write failed; retrying (attempt %d)", attempt + 1)
            await asyncio.sleep(1)


def _agent_dir(out: Path, model: str) -> Path:
    return out / "agents" / model.replace("/", "_")


# Without free-text message search, an answer must come from Context, the entity
# graph, or an Episode found by search, opened to read its source messages.
MESSAGE_SEARCH_TOOLS = frozenset({"search_knowledge_messages"})
TRACED_EVENTS = frozenset({"tool_start", "tool_end", "tool_error"})


def _record_episode_results(retrieval, seen: list[dict]) -> None:
    """Note which Episodes each search hands the agent, for scoring afterwards."""

    def wrap(name: str, episodes_of):
        original = getattr(retrieval, name)

        async def recording(*args, **kwargs):
            result = await original(*args, **kwargs)
            seen.append({
                "call": name,
                "resolution": result.get("resolution") if isinstance(result, dict) else None,
                "episode_ids": episodes_of(result),
            })
            return result

        setattr(retrieval, name, recording)

    def from_groups(result: dict) -> list[str]:
        return [
            episode["episode_id"]
            for group in result.get("results", [])
            for episode in group.get("episodes", [])
        ]

    wrap("episode_check", from_groups)
    wrap("read_recent_episodes", from_groups)


def _remembering_render(pages: list[str]):
    """Keep what the agent read: the notebook grows, so the last page shows it all."""

    original = prompt_context.render_notebook

    def render(*args, **kwargs):
        page = original(*args, **kwargs)
        pages.append(page)
        return page

    return render


async def _ask(
    runtime, project_id: str, question: Question, model: str, no_message_search: bool,
    trace_path: Path | None = None,
) -> tuple[str, dict | None]:
    """Ask in a new session, so no history turns sit in the prompt."""

    enabled_tools = (
        sorted(tool_groups()["core"] - MESSAGE_SEARCH_TOOLS) if no_message_search else None
    )
    notebook_pages: list[str] = []
    render_notebook = prompt_context.render_notebook
    session = await runtime.sessions.create_session(
        project_id, model=model, enabled_tools=enabled_tools
    )
    try:
        retrieval = session.project.knowledge_retrieval
        episode_results: list[dict] = []
        _record_episode_results(retrieval, episode_results)
        prompt_context.render_notebook = _remembering_render(notebook_pages)
        stream = await session.open_agent_run_stream(Message(content=question.question))
        answer, usage = None, None
        tool_events = []
        async for event in stream:
            data = event.get("data") or {}
            if event.get("event") in TRACED_EVENTS:
                tool_events.append({"event": event["event"], **data})
            if event.get("event") == "response":
                answer, usage = data.get("content", ""), data.get("usage")
            elif event.get("event") == "clarification":
                answer, usage = data.get("question", ""), data.get("usage")
            elif event.get("event") == "error":
                raise RuntimeError(f"Agent run failed: {data}")
        if answer is None:
            raise RuntimeError("Agent run ended without an answer")
        return answer, usage
    finally:
        # Written even for a failed run, which is when the trace matters most.
        if trace_path is not None:
            _append(trace_path, {
                "question_id": question.question_id,
                "tools": tool_events,
                "episode_results": episode_results,
                "notebook": notebook_pages[-1] if notebook_pages else "",
            })
        prompt_context.render_notebook = render_notebook
        for name in ("episode_check", "read_recent_episodes"):
            vars(retrieval).pop(name, None)
        # Deleted so a later model's search cannot find this model's answer.
        await runtime.sessions.delete_session(session.session_id)


async def _replay(
    runtime,
    question: Question,
    out: Path,
    settle_timeout: float,
    sessions_per_settle: int,
    domain: DomainConfig,
    models: list[str],
    no_message_search: bool = False,
    min_episode_coverage: float = 0.0,
) -> dict:
    name = f"lme-{question.question_id}"
    # A question cut off earlier starts over in a clean project.
    for row in await runtime.projects.list_projects():
        if row["name"] == name and row["status"] != "deleted":
            await runtime.projects.delete_project(row["id"])
    project_id = (await runtime.projects.create_project(name, domain))["id"]

    started = time.monotonic()
    history = await runtime.sessions.create_session(project_id)
    exchanges = 0
    source_messages = 0
    source_tokens_est = 0
    sessions_since_settle = 0
    batches = 0
    batch_started = time.monotonic()
    try:
        nonempty_sessions = [
            (index, session) for index, session in enumerate(question.sessions)
            if session.exchanges
        ]
        for position, (index, session) in enumerate(nonempty_sessions, 1):
            set_test_clock(_ReplayClock(session.started_at))
            await _import_with_retry(history, session.exchanges)
            exchanges += len(session.exchanges)
            for exchange in session.exchanges:
                for content in (
                    getattr(exchange, "user_content", None),
                    getattr(exchange, "assistant_content", None),
                ):
                    if content:
                        source_messages += 1
                        source_tokens_est += max(1, (len(content) + 2) // 3)
            sessions_since_settle += 1
            if sessions_since_settle < sessions_per_settle and position != len(nonempty_sessions):
                continue
            revision = await history.project.wait_for_semantic_idle(
                timeout_s=settle_timeout
            )
            window_progress = await runtime.resources.postgres.fetch_one(
                "SELECT count(*) FILTER (WHERE stage = 'completed') AS completed, "
                "COALESCE(sum(source_token_count) FILTER "
                "(WHERE stage = 'completed'), 0) AS completed_tokens "
                "FROM public.project_semantic_windows WHERE project_id = %s",
                (project_id,),
            )
            coverage = await _episode_coverage(runtime, project_id)
            logger.info(
                "%s memory progress: sessions=%d/%d messages=%d "
                "source_tokens_est=%d windows_completed=%d "
                "window_tokens=%d episode_coverage=%.3f batch_seconds=%.1f",
                question.question_id, position, len(nonempty_sessions),
                source_messages, source_tokens_est,
                window_progress["completed"], window_progress["completed_tokens"],
                coverage, time.monotonic() - batch_started,
            )
            batches += 1
            if batches >= COVERAGE_CHECK_AFTER_BATCHES and coverage < min_episode_coverage:
                raise EpisodeCoverageTooLow(
                    f"{question.question_id}: only {coverage:.1%} of messages have an "
                    f"Episode after {batches} batches (minimum {min_episode_coverage:.0%})"
                )
            batch_started = time.monotonic()
            sessions_since_settle = 0
            snapshot_dir = out / "snapshots" / question.question_id
            snapshot_dir.mkdir(parents=True, exist_ok=True)
            number = revision.revision_number if revision else 0
            (snapshot_dir / f"{index:03d}.md").write_text(
                f"<!-- session {session.session_id} at {session.started_at.isoformat()}"
                f" | Context revision {number} -->\n\n"
                + await _snapshot(runtime, history.project, revision)
            )
    finally:
        await runtime.sessions.deactivate_runtime_session(history.session_id)

    memory_seconds = round(time.monotonic() - started, 1)
    # Every agent reads the same memory; one failing does not stop the rest.
    answers: dict[str, tuple[str, dict | None] | Exception] = {}
    for model in models:
        set_test_clock(question.question_date)
        try:
            answers[model] = await _ask(
                runtime, project_id, question, model, no_message_search,
                _agent_dir(out, model) / "traces.jsonl",
            )
        except Exception as exc:
            answers[model] = exc
    windows = await runtime.resources.postgres.fetch_one(
        "SELECT count(*) AS count FROM public.project_semantic_windows "
        "WHERE project_id = %s",
        (project_id,),
    )
    spent = await runtime.resources.postgres.fetch_one(
        "SELECT COALESCE(sum(spent_usd), 0) AS spent FROM public.llm_budget_windows"
    )
    llm_totals = await runtime.resources.llm_service.spending_snapshot()
    return {
        "answers": answers,
        "metrics": {
            "question_id": question.question_id,
            "question_type": question.question_type,
            "sessions": len(question.sessions),
            "exchanges": exchanges,
            "windows": windows["count"],
            "episode_coverage": round(await _episode_coverage(runtime, project_id), 4),
            "memory_seconds": memory_seconds,
            # Both are running totals for this run; subtract rows for per-question.
            "run_spent_usd": round(float(spent["spent"]), 4),
            "run_prompt_cache": llm_totals["prompt_cache"],
            "answer_usage": {
                model: answer[1] for model, answer in answers.items()
                if not isinstance(answer, Exception)
            },
        },
    }


def _manifest(args, config: dict, domain: dict) -> dict:
    repo = Path(__file__).resolve().parents[2]
    sha = subprocess.run(
        ["git", "rev-parse", "HEAD"], cwd=repo, capture_output=True, text=True
    ).stdout.strip()
    return {
        "git_sha": sha,
        "dataset": str(args.data),
        "dataset_sha256": hashlib.sha256(args.data.read_bytes()).hexdigest(),
        "config": config,
        "domain": domain,
        "selection": {"types": args.types, "question_ids": args.question_ids,
                      "limit": args.limit},
        "replay": {"sessions_per_settle": args.sessions_per_settle},
        "no_message_search": args.no_message_search,
        "started_at": datetime.now(timezone.utc).isoformat(),
    }


async def main() -> int:
    args = _args()
    if args.sessions_per_settle < 1:
        sys.exit("--sessions-per-settle must be at least 1")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        sys.exit("Set DATABASE_URL to a database used only for this benchmark")
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=True)
    models = args.agent_models.split(",")
    if (out / "agents").exists() and not args.resume:
        sys.exit(f"{out} has answers; pass --resume or pick a new --out")
    for model in models:
        _agent_dir(out, model).mkdir(parents=True, exist_ok=True)

    questions = _select(load_questions(args.data), args)
    # A question is done once every agent has answered it.
    done = set.intersection(
        *(_answered(_agent_dir(out, model) / "hypotheses.jsonl") for model in models)
    )
    pending = [q for q in questions if q.question_id not in done]
    logger.info("%d questions selected, %d left to run", len(questions), len(pending))

    domain_payload = json.loads(args.domain.read_text())
    domain = DomainConfig.from_mapping(domain_payload)
    config = _write_config(out / "config", args)
    manifest = _manifest(args, config, domain_payload)
    migrate(database_url)
    runtime = await ApplicationRuntime.start(
        user_name=f"lme_{out.name}", config_dir=out / "config"
    )
    failures = 0
    try:
        for number, question in enumerate(pending, 1):
            logger.info("[%d/%d] %s (%s, %d sessions)", number, len(pending),
                        question.question_id, question.question_type,
                        len(question.sessions))
            try:
                result = await _replay(
                    runtime, question, out, args.settle_timeout,
                    args.sessions_per_settle, domain, models,
                    args.no_message_search, args.min_episode_coverage,
                )
            except EpisodeCoverageTooLow as exc:
                # Every later question would waste its budget the same way.
                failures += 1
                logger.error("Stopping the run: %s", exc)
                _append(out / "errors.jsonl", {
                    "question_id": question.question_id, "error": repr(exc),
                })
                break
            except Exception as exc:
                failures += 1
                logger.error("%s failed: %s", question.question_id, exc)
                _append(out / "errors.jsonl", {
                    "question_id": question.question_id,
                    "error": repr(exc),
                    "traceback": traceback.format_exc(),
                })
                continue
            finally:
                reset_clock()
            for model, answer in result["answers"].items():
                if isinstance(answer, Exception):
                    failures += 1
                    logger.error("%s failed for %s: %s",
                                 question.question_id, model, answer)
                    _append(_agent_dir(out, model) / "errors.jsonl", {
                        "question_id": question.question_id,
                        "error": repr(answer),
                    })
                    continue
                _append(_agent_dir(out, model) / "hypotheses.jsonl",
                        {"question_id": question.question_id, "hypothesis": answer[0]})
            _append(out / "metrics.jsonl", result["metrics"])
    finally:
        manifest["finished_at"] = datetime.now(timezone.utc).isoformat()
        (out / "manifest.json").write_text(json.dumps(manifest, indent=2))
        await runtime.shutdown()
    logger.info("Done: %d questions, %d failures", len(pending), failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(asyncio.run(main()))
