from types import SimpleNamespace

import pytest

from common.schema.jev import (
    ChoiceAnswer,
    JevResponse,
    JevResult,
    JevSettings,
    JevUsage,
)
from core.project.evidence_board_service import (
    BoardEvaluationUnavailable,
    EvidenceBoardService,
)


@pytest.mark.no_network
async def test_board_evaluation_uses_only_user_selected_claim_and_sources():
    calls = []

    class Notebooks:
        async def prepare_board_evaluation(self, *args, **kwargs):
            assert args == ("project-1", "board-1")
            assert kwargs["claim_id"] == "claim-1"
            return {
                "question": "What caused the failure?",
                "claim": "The parser caused it",
                "sources": [{"source_ref_id": "ref-1", "excerpt": "Parser was healthy"}],
                "fingerprint": "a" * 64,
            }

        async def record_board_judgment(self, *args, **kwargs):
            calls.append((args, kwargs))
            return {"revision": 2}

    class JEV:
        async def evaluate(self, **kwargs):
            calls.append(kwargs)
            return JevResult(
                outcome="available",
                response=JevResponse(
                    model="jev-1.13.0",
                    answers={"claim": ChoiceAnswer(
                        type="choice", choice="conflict", confidence=0.91,
                        probabilities={
                            "support": 0.04, "conflict": 0.91,
                            "insufficient_evidence": 0.05,
                        },
                    )},
                    usage=JevUsage(input_tokens=100, output_tokens=8),
                ),
            )

    service = EvidenceBoardService(
        Notebooks(), JEV(),
        SimpleNamespace(config=SimpleNamespace(jev=JevSettings())),
    )
    result = await service.evaluate(
        "project-1", "board-1", expected_revision=1,
        question_id="question-1", claim_id="claim-1",
    )

    assert result == {"revision": 2}
    assert calls[0]["capability"] == "board"
    assert calls[0]["state"]["cited_sources"] == [
        {"source_ref_id": "ref-1", "excerpt": "Parser was healthy"}
    ]
    assert calls[1][1]["verdict"] == "conflict"
    assert calls[1][1]["fingerprint"] == "a" * 64


@pytest.mark.no_network
async def test_board_evaluation_needs_jev():
    class Notebooks:
        async def prepare_board_evaluation(self, *args, **kwargs):
            return {"question": "Q", "claim": "C", "sources": []}

    service = EvidenceBoardService(Notebooks(), None, None)
    with pytest.raises(BoardEvaluationUnavailable):
        await service.evaluate(
            "project-1", "board-1", expected_revision=1,
            question_id="question-1", claim_id="claim-1",
        )
