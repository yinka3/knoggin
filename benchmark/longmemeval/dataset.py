"""Turn LongMemEval questions into replayable sessions of timed exchanges."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.schema.primitives import ImportedExchange

logger = logging.getLogger("longmemeval")

# Turns in one session are spaced out so each keeps its own time and order.
TURN_SPACING = timedelta(minutes=2)
REPLY_DELAY = timedelta(seconds=30)


@dataclass(frozen=True)
class ReplaySession:
    session_id: str
    started_at: datetime
    exchanges: list[ImportedExchange]


@dataclass(frozen=True)
class Question:
    question_id: str
    question_type: str
    question: str
    answer: str
    question_date: datetime
    sessions: list[ReplaySession]

    @property
    def is_abstention(self) -> bool:
        return self.question_id.endswith("_abs")


def parse_date(value: str) -> datetime:
    """Read LongMemEval's '2023/05/20 (Sat) 02:21' as UTC."""

    day, _weekday, clock = value.split(" ")
    return datetime.strptime(f"{day} {clock}", "%Y/%m/%d %H:%M").replace(
        tzinfo=timezone.utc
    )


def pair_turns(
    turns: list[dict], started_at: datetime, *, label: str
) -> list[ImportedExchange]:
    """Pair each user turn with the assistant turn after it.

    A user turn with no reply stays user-only. An assistant turn with no user
    turn before it joins the previous reply, or is dropped at the start.
    """

    pairs: list[list[str | None]] = []
    for turn in turns:
        role, content = turn["role"], turn["content"]
        if role == "user":
            pairs.append([content, None])
        elif role == "assistant":
            if not pairs:
                logger.warning("%s: dropped a leading assistant turn", label)
            elif pairs[-1][1] is None:
                pairs[-1][1] = content
            else:
                logger.warning("%s: joined an extra assistant turn", label)
                pairs[-1][1] = f"{pairs[-1][1]}\n\n{content}"
        else:
            logger.warning("%s: dropped a turn with role %r", label, role)

    exchanges = []
    for index, (user, assistant) in enumerate(pairs):
        if not user or not user.strip():
            logger.warning("%s: dropped a blank user turn", label)
            continue
        if assistant is not None and not assistant.strip():
            assistant = None
        user_at = started_at + index * TURN_SPACING
        exchanges.append(
            ImportedExchange(
                user_content=user,
                user_timestamp=user_at,
                assistant_content=assistant,
                assistant_timestamp=user_at + REPLY_DELAY if assistant else None,
            )
        )
    return exchanges


def load_questions(path: Path) -> list[Question]:
    questions = []
    for row in json.loads(path.read_text()):
        sessions = [
            ReplaySession(
                session_id=session_id,
                started_at=parse_date(date),
                exchanges=pair_turns(
                    turns,
                    parse_date(date),
                    label=f"{row['question_id']}/{session_id}",
                ),
            )
            for session_id, date, turns in zip(
                row["haystack_session_ids"],
                row["haystack_dates"],
                row["haystack_sessions"],
                strict=True,
            )
        ]
        ordered = sorted(sessions, key=lambda session: session.started_at)
        if ordered != sessions:
            logger.info("%s: sorted sessions by date", row["question_id"])
        questions.append(
            Question(
                question_id=row["question_id"],
                question_type=row["question_type"],
                question=row["question"],
                answer=str(row["answer"]),
                question_date=parse_date(row["question_date"]),
                sessions=ordered,
            )
        )
    return questions
