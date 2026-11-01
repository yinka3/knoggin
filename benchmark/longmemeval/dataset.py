"""Turn LongMemEval questions into replayable sessions of timed exchanges."""

import json
import logging
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from pathlib import Path

from common.schema.primitives import ImportedAssistantTurn, ImportedExchange

logger = logging.getLogger("longmemeval")

# Turns in one session are spaced out so each keeps its own time and order.
TURN_SPACING = timedelta(minutes=2)
REPLY_DELAY = timedelta(seconds=30)


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
