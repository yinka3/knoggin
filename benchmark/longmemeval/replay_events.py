"""Replay built memory windows through the Episode step and show the events.

Read-only: it loads a window's frozen messages from a saved benchmark
database, runs the current Episode prompt on them, and prints what it would
store. Nothing is written, so the saved memory stays as it was built.

    uv run --env-file ../../.env --package server python replay_events.py \
        --db knoggin_lme_m_six2 --project lme-gpt4_7f6b06db --contains "Muir Woods"
"""

import argparse
import asyncio
import json
import os

import psycopg
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from common.api_keys import llm_api_key
from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.generator import EpisodeGenerator
from core.knowledge.episodes.policy import EpisodeGenerationPolicy
from infrastructure.llm_client import LLMService


class _NoEmbedding:
    """Episode generation embeds results; replay only needs the text."""

    def split_for_embedding(self, text):
        return [text]

    async def encode(self, texts):
        return [[0.0] * 768 for _ in texts]


def _window_messages(conn, project: str, contains: str) -> tuple[str, list[dict]]:
    """The frozen messages of the window holding the first matching message."""

    row = conn.execute(
        """
        SELECT wm.window_id, p.project_id
        FROM messages m
        JOIN projects p ON p.project_id = m.project_id
        JOIN project_semantic_window_messages wm ON wm.message_id = m.message_id
        WHERE p.name = %s AND m.content ILIKE %s
        ORDER BY m.role <> 'user', m.timestamp_ms LIMIT 1
        """,
        (project, f"%{contains}%"),
    ).fetchone()
    if row is None:
        raise SystemExit(f"No window in {project} has a message containing {contains!r}")
    messages = conn.execute(
        """
        SELECT m.message_id, m.session_id, m.role, m.content, m.timestamp_ms,
               m.user_msg_id, m.exchange_outcome
        FROM project_semantic_window_messages wm
        JOIN messages m ON m.message_id = wm.message_id
        WHERE wm.window_id = %s
        ORDER BY wm.ordinal
        """,
        (row["window_id"],),
    ).fetchall()
    return row["project_id"], [dict(message) for message in messages]


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--db", required=True)
    parser.add_argument("--project", required=True, help="project name, such as lme-<question id>")
    parser.add_argument("--contains", required=True, help="text in one message of the window")
    parser.add_argument("--model", default="anthropic/claude-haiku-5.5")
    parser.add_argument("--source-tokens", type=int, default=20000)
    parser.add_argument("--source-messages", type=int, default=120)
    args = parser.parse_args()

    base = conninfo_to_dict(os.environ["DATABASE_URL"])
    with psycopg.connect(make_conninfo(**{**base, "dbname": args.db}), row_factory=dict_row) as conn:
        project_id, messages = _window_messages(conn, args.project, args.contains)

    llm = LLMService(api_key=llm_api_key(), extraction_model=args.model)
    policy = EpisodeGenerationPolicy.capture(
        settings=EpisodeSettings(
            max_episode_source_tokens=args.source_tokens,
            max_episode_source_messages=args.source_messages,
        )
    )
    build = await EpisodeGenerator(llm=llm, embedding_service=_NoEmbedding()).generate(
        user_name="replay", project_id=project_id, messages=messages, policy=policy,
    )
    match = [m for m in messages if args.contains.lower() in str(m["content"]).lower()]
    print(json.dumps({
        "window_messages": len(messages),
        "match": [{"role": m["role"], "content": str(m["content"])[:300]} for m in match],
        "rejected": build.rejected_proposals,
        "episodes": [
            {
                "summary": episode.summary[:200],
                "events": [
                    {
                        "what": event.what,
                        "kind": event.kind,
                        "dates": [str(event.start_date), str(event.end_date)],
                        "precision": event.precision,
                        "date_text": event.date_text,
                    }
                    for event in episode.events
                ],
            }
            for episode in build.final_episodes
        ],
    }, indent=1))


if __name__ == "__main__":
    asyncio.run(main())
