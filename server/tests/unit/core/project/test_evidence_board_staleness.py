from contextlib import asynccontextmanager
from datetime import datetime, timezone
from uuid import uuid4

import pytest

from common.schema.evidence_board import (
    BoardClaim,
    BoardJudgment,
    BoardQuestion,
    BoardState,
)
from core.project.notebook_store import ProjectNotebookStore


class Client:
    @asynccontextmanager
    async def transaction(self):
        yield None


def _judgment(**fields):
    return BoardJudgment(
        verdict="support", reason="ok", confidence=0.9,
        evaluated_at=datetime(2026, 11, 1, tzinfo=timezone.utc), fingerprint="f" * 64,
        **fields,
    )


def _board(child_first: bool):
    parent_ref, child_ref = uuid4(), uuid4()
    parent = BoardQuestion(
        question_id=uuid4(), text="Is the API stable?",
        claims=[BoardClaim(
            claim_id=uuid4(), text="It is", source_ref_ids=[parent_ref],
            source_versions={str(parent_ref): "v1"}, judgment=_judgment(),
        )],
    )
    child = BoardQuestion(
        question_id=uuid4(), text="Can we ship?", depends_on=parent.question_id,
        claims=[BoardClaim(
            claim_id=uuid4(), text="Yes", source_ref_ids=[child_ref],
            source_versions={str(child_ref): "v1"},
        )],
    )
    stored = BoardState(questions=[parent, child])
    # The child was judged against the parent as stored.
    child.claims[0].judgment = _judgment(
        dependency_fingerprint=ProjectNotebookStore._dependency_fingerprint(stored, child),
    )
    questions = [child, parent] if child_first else [parent, child]
    return BoardState(questions=questions), parent_ref, child


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize("child_first", [True, False])
async def test_a_child_claim_is_judged_the_same_whatever_the_question_order(child_first):
    board, parent_ref, child = _board(child_first)
    store = ProjectNotebookStore(Client(), "ada")

    async def get(project_id, notebook_id):
        return {"notebook_id": notebook_id, "view": "evidence_board",
                "board_state": board.model_dump(mode="json")}

    async def source(_cursor, _project_id, _kind, ref_id):
        # The parent's citation changed since it was judged; the child's did not.
        return {"version": "v2" if ref_id == str(parent_ref) else "v1"}

    store.get = get
    store._source = source

    visible = await store.evidence_board("p1", "n1")

    questions = {question["question_id"]: question for question in visible["board_state"]["questions"]}
    parent_claim = next(q for q in questions.values() if q["depends_on"] is None)["claims"][0]
    child_claim = questions[str(child.question_id)]["claims"][0]
    assert parent_claim["judgment"]["stale"] is True
    assert child_claim["judgment"]["stale"] is False
