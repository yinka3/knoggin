"""Explicit, bounded JEV evaluation for user-owned evidence boards."""

from __future__ import annotations

from common.schema.jev import ChoiceAnswer, ChoiceQuestion
from infrastructure.jev_client import JevWorkBudget


class BoardEvaluationUnavailable(RuntimeError):
    pass


class EvidenceBoardService:
    def __init__(self, notebooks, jev_client, config_manager):
        self.notebooks = notebooks
        self.jev_client = jev_client
        self.config_manager = config_manager

    async def evaluate(
        self, project_id: str, notebook_id: str, *, expected_revision: int,
        question_id: str, claim_id: str,
    ):
        snapshot = await self.notebooks.prepare_board_evaluation(
            project_id, notebook_id, expected_revision=expected_revision,
            question_id=question_id, claim_id=claim_id,
        )
        if self.jev_client is None or self.config_manager is None:
            raise BoardEvaluationUnavailable("JEV is unavailable")
        policy = self.config_manager.config.jev.capture_policy()
        question = ChoiceQuestion(
            instructions=(
                "Evaluate only the supplied claim and cited evidence. Choose whether "
                "the evidence supports the claim, conflicts with it, or is insufficient."
            ),
            criteria={
                "support": "Cited evidence supports the claim",
                "conflict": "Cited evidence conflicts with the claim",
                "insufficient_evidence": "Cited evidence cannot establish either outcome",
            },
        )
        result = await self.jev_client.evaluate(
            state={
                "question": snapshot["question"],
                "claim": snapshot["claim"],
                "cited_sources": snapshot["sources"],
            },
            questions={"claim": question},
            capability="board",
            policy=policy,
            work_budget=JevWorkBudget(
                max_calls=1,
                max_elapsed_seconds=min(
                    policy.total_timeout_seconds, policy.max_elapsed_seconds_per_window
                ),
            ),
        )
        if result.outcome != "available" or result.response is None:
            raise BoardEvaluationUnavailable("JEV could not evaluate this claim")
        answer = result.response.answers.get("claim")
        if not isinstance(answer, ChoiceAnswer):
            raise BoardEvaluationUnavailable("JEV returned an invalid evaluation")
        verdict = answer.choice
        if verdict not in {"support", "conflict", "insufficient_evidence"}:
            raise BoardEvaluationUnavailable("JEV returned an invalid evaluation")
        reason = (
            f"JEV ranked {verdict.replace('_', ' ')} highest from "
            f"{len(snapshot['sources'])} cited source(s) "
            f"(probability {answer.probabilities[verdict]:.2f}, "
            f"confidence {answer.confidence:.2f}). Review the cited sources before confirming."
        )
        return await self.notebooks.record_board_judgment(
            project_id, notebook_id, expected_revision=expected_revision,
            question_id=question_id, claim_id=claim_id,
            fingerprint=snapshot["fingerprint"], verdict=verdict,
            reason=reason, confidence=answer.confidence,
        )
