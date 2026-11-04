"""Turn LongMemEval questions into replayable sessions of timed exchanges."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.schema.primitives import ImportedAssistantTurn, ImportedExchange

logger = logging.getLogger("longmemeval")

# Turns in one session are spaced out so each keeps its own time and order, but
# tightly: M has sessions minutes apart, and overlapping times would braid two
# conversations together once a window sorts its messages by time.
TURN_SPACING = timedelta(seconds=2)
REPLY_DELAY = timedelta(seconds=1)
SESSION_GAP = timedelta(seconds=1)


@dataclass(frozen=True)
class ReplaySession:
    session_id: str
    started_at: datetime
    exchanges: list[ImportedExchange | ImportedAssistantTurn]


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
) -> list[ImportedExchange | ImportedAssistantTurn]:
    """Preserve ordered messages, pairing only adjacent user/assistant turns."""

    segments: list[list[str | None]] = []
    for turn in turns:
        role, content = turn["role"], turn["content"]
        if not content or not content.strip():
            logger.warning("%s: dropped a blank %s turn", label, role)
            continue
        if role == "user":
            segments.append(["user", content, None])
        elif role == "assistant":
            if segments and segments[-1][0] == "user" and segments[-1][2] is None:
                segments[-1][2] = content
            else:
                segments.append(["assistant", content, None])
        else:
            logger.warning("%s: dropped a turn with role %r", label, role)

    result: list[ImportedExchange | ImportedAssistantTurn] = []
    for index, (role, content, reply) in enumerate(segments):
        at = started_at + index * TURN_SPACING
        if role == "assistant":
            result.append(ImportedAssistantTurn(
                assistant_content=content, assistant_timestamp=at,
            ))
        else:
            result.append(ImportedExchange(
                user_content=content, user_timestamp=at,
                assistant_content=reply,
                assistant_timestamp=at + REPLY_DELAY if reply else None,
            ))
    return result


def _last_turn_at(turns: list[ImportedExchange | ImportedAssistantTurn]) -> datetime | None:
    if not turns:
        return None
    last = turns[-1]
    if isinstance(last, ImportedAssistantTurn):
        return last.assistant_timestamp
    return last.assistant_timestamp or last.user_timestamp


def _without_overlaps(rows: list[tuple[str, datetime, list[dict]]], label: str) -> list[ReplaySession]:
    """Replay date-ordered sessions, starting each after the previous one ends.

    The dataset gives each session a start minute only, and some share one.
    """

    sessions: list[ReplaySession] = []
    previous_end: datetime | None = None
    for session_id, date, turns in sorted(rows, key=lambda row: row[1]):
        started_at = max(date, previous_end + SESSION_GAP) if previous_end else date
        exchanges = pair_turns(turns, started_at, label=f"{label}/{session_id}")
        sessions.append(ReplaySession(session_id, started_at, exchanges))
        previous_end = _last_turn_at(exchanges) or previous_end
    return sessions


def load_questions(path: Path) -> list[Question]:
    questions = []
    for row in json.loads(path.read_text()):
        sessions = _without_overlaps(
            [
                (session_id, parse_date(date), turns)
                for session_id, date, turns in zip(
                    row["haystack_session_ids"],
                    row["haystack_dates"],
                    row["haystack_sessions"],
                    strict=True,
                )
            ],
            row["question_id"],
        )
        questions.append(
            Question(
                question_id=row["question_id"],
                question_type=row["question_type"],
                question=row["question"],
                answer=str(row["answer"]),
                question_date=parse_date(row["question_date"]),
                sessions=sessions,
            )
        )
    return questions
