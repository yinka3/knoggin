"""Measure Episode recall on a built run, with no LLM calls.

For each question: does its gold evidence have an Episode, and does
search_episodes return that Episode for the question's own wording? Gold IDs
are used here only to score. Point DATABASE_URL at a copy of the run's database.
"""

import argparse
import asyncio
import json
import os
import sys
from pathlib import Path

import psycopg

from common.schema.settings import SearchSettings
from core.knowledge.retrieval import KnowledgeRetrieval
from runtime.application import ApplicationRuntime

THRESHOLDS = (0.35, 0.4, 0.5, 0.6)


def _gold_turns(row: dict) -> list[str]:
    """The turns marked as evidence, or whole gold sessions when none are."""

    turns = []
    for session_id, session in zip(row["haystack_session_ids"], row["haystack_sessions"]):
        if session_id in row["answer_session_ids"]:
            marked = [turn for turn in session if turn.get("has_answer")]
            turns.extend(turn["content"] for turn in marked or session)
    return turns


async def _gold_episodes(postgres, project_id: str, turns: list[str]) -> tuple[int, set[str]]:
    rows = await postgres.fetch_all(
        "SELECT m.message_id, e.episode_id FROM public.messages m "
        "LEFT JOIN public.episode_messages e ON e.message_id = m.message_id "
        "AND e.project_id = m.project_id "
        "WHERE m.project_id = %s AND m.content = ANY(%s)",
        (project_id, turns),
    )
    return len(rows), {row["episode_id"] for row in rows if row["episode_id"]}


async def _search(runtime, user_name, project_id, question, threshold) -> dict:
    retrieval = KnowledgeRetrieval(
        project_id=project_id,
        readable_project_ids=[project_id],
        user_name=user_name,
        entities=None,
        embedding_service=runtime.resources.embedding,
        knowledge_store=runtime.resources.knowledge_store,
        search_settings=SearchSettings(episode_similarity_threshold=threshold),
    )
    return await retrieval.episode_check(question, session_id="recall", raw_fallback=False)


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--config", type=Path, required=True, help="the run's config directory")
    parser.add_argument("--out", type=Path, required=True, help="JSONL of per-question results")
    args = parser.parse_args()
    if not os.environ.get("DATABASE_URL"):
        sys.exit("Set DATABASE_URL to a copy of the run's database")

    rows = {row["question_id"]: row for row in json.loads(args.data.read_text())}
    bootstrap = await _user_name()
    runtime = await ApplicationRuntime.start(user_name=bootstrap, config_dir=args.config)
    results = []
    try:
        postgres = runtime.resources.postgres
        projects = await postgres.fetch_all(
            "SELECT name, project_id FROM public.projects WHERE status <> 'deleted'"
        )
        for project in projects:
            question_id = project["name"].removeprefix("lme-")
            row = rows.get(question_id)
            if row is None:
                continue
            found, gold = await _gold_episodes(postgres, project["project_id"], _gold_turns(row))
            result = {
                "question_id": question_id,
                "question_type": row["question_type"],
                "gold_turns_found": found,
                "gold_has_episode": bool(gold),
                "gold_episodes": len(gold),
                "rank": {},
                "gold_found": {},
                "resolution": {},
            }
            for threshold in THRESHOLDS:
                search = await _search(
                    runtime, bootstrap, project["project_id"], row["question"], threshold
                )
                episodes = [
                    episode["episode_id"]
                    for group in search["results"]
                    for episode in group.get("episodes", [])
                ]
                ranks = [i for i, episode in enumerate(episodes, 1) if episode in gold]
                result["rank"][str(threshold)] = ranks[0] if ranks else None
                result["gold_found"][str(threshold)] = len(gold.intersection(episodes))
                result["resolution"][str(threshold)] = search["resolution"]
            results.append(result)
            print(json.dumps(result), flush=True)
    finally:
        await runtime.shutdown()
    args.out.write_text("".join(json.dumps(result) + "\n" for result in results))

    covered = [result for result in results if result["gold_has_episode"]]
    print(f"\n{len(covered)}/{len(results)} questions have gold evidence in an Episode")
    total_gold = sum(result["gold_episodes"] for result in results)
    best = sum(max(result["gold_found"]["0.4"], result["gold_found"]["0.5"]) for result in results)
    print(f"gold Episodes found, best of 0.4 and 0.5 per question: {best}/{total_gold}")
    for threshold in THRESHOLDS:
        key = str(threshold)
        hits = sum(result["rank"][key] is not None for result in covered)
        empty = sum(result["resolution"][key] == "no_episodes" for result in results)
        print(f"threshold {threshold}: gold Episode in top 5 for {hits}/{len(covered)} "
              f"covered questions; no Episodes at all on {empty}/{len(results)}")


async def _user_name() -> str:
    with psycopg.connect(os.environ["DATABASE_URL"]) as connection:
        return connection.execute("SELECT user_name FROM public.projects LIMIT 1").fetchone()[0]


if __name__ == "__main__":
    asyncio.run(main())
