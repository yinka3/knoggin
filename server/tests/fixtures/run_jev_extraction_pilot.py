"""Run reviewed bounded-extraction cases against the real JEV endpoint."""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from dotenv import load_dotenv

from common.conf.domain_config import DomainConfig
from common.schema.jev import JevSettings
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.jev_extraction import (
    ExtractionCandidate,
    accepted_extraction_type,
    prepare_extraction_request,
)
from core.ingestion.policy import IngestionPolicy
from infrastructure.external_model_budget import ExternalModelSpendingLedger
from infrastructure.jev_client import JevClient, JevWorkBudget
from tests.fixtures.jev_measurements import summarize_results
from tests.fixtures.jev_review_validation import index_unique, validate_case_ids

REPOSITORY_ENV = Path(__file__).resolve().parents[3] / ".env"
OPENROUTER_ENDPOINT = "https://openrouter.ai/api/alpha/decisions"
OPENROUTER_MODEL = "typesafe/jev-1.13"


def load_reviewed_cases(path: Path) -> list[dict]:
    packet = json.loads(path.read_text(encoding="utf-8"))
    if packet.get("review_status") != "reviewed":
        raise ValueError("Extraction cases must be human reviewed before evaluation")
    cases = packet.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("Extraction review packet has no cases")
    if any(not case.get("reason") or "expected_type" not in case for case in cases):
        raise ValueError("Every extraction case needs an expected type and reason")
    validate_case_ids(cases)
    return cases


def _policy(settings: JevSettings) -> IngestionPolicy:
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {}},
            "entity_types": {
                name: {
                    "topic": "Work",
                    "labels": [name.lower()],
                    "description": f"A {name.lower()} entity.",
                }
                for name in ("Company", "Person", "Project", "Database")
            },
        }
    ).compile()
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=settings.capture_policy(),
    )


def evaluate(cases: list[dict], observations: list[dict]) -> dict:
    validate_case_ids(cases)
    keyed = index_unique(
        observations, lambda item: item["case_id"], "extraction observation"
    )
    evidence_errors = []
    counts = {
        "reviewed": len(cases),
        "observed": len(observations),
        "raw_correct": 0,
        "raw_wrong": 0,
        "unavailable": 0,
        "expected_positive": 0,
        "accepted": 0,
        "correct_accepts": 0,
        "wrong_accepts": 0,
        "known_type_evidence_scored": 0,
        "known_type_evidence_missing": 0,
    }
    for case in cases:
        item = keyed.get(case["id"])
        expected_type = case["expected_type"]
        if expected_type is not None:
            counts["expected_positive"] += 1
        if item is None or item["result"]["outcome"] != "available":
            counts["unavailable"] += 1
            continue
        if case.get("proposed_type") is not None:
            # This request has only an evidence Noul, never a type Choice.
            response = item["result"].get("response") or {}
            evidence = (
                response.get("answers", {}).get("entity_evidence", {}).get("noul")
            )
            if evidence is None:
                counts["known_type_evidence_missing"] += 1
            else:
                counts["known_type_evidence_scored"] += 1
                evidence_errors.append(
                    (evidence - float(expected_type is not None)) ** 2
                )
        else:
            actual = item["raw_choice"]
            expected = expected_type or case.get("expected_choice")
            counts["raw_correct" if actual == expected else "raw_wrong"] += 1
        accepted = item["accepted_type"]
        if accepted is not None:
            counts["accepted"] += 1
            if accepted == expected_type:
                counts["correct_accepts"] += 1
            else:
                counts["wrong_accepts"] += 1
    scored = counts["raw_correct"] + counts["raw_wrong"]
    counts["raw_choice_scored"] = scored
    counts["raw_accuracy"] = counts["raw_correct"] / scored if scored else None
    counts["known_type_evidence_brier_score"] = (
        sum(evidence_errors) / len(evidence_errors) if evidence_errors else None
    )
    counts["raw_accuracy_scope"] = "Unknown-type Choice answers only"
    counts["known_type_evidence_scope"] = (
        "Mean squared error of raw entity-evidence Noul against entity/non-entity labels; "
        "lower is better, independent of acceptance thresholds; does not measure type correctness"
    )
    counts["acceptance_precision"] = (
        counts["correct_accepts"] / counts["accepted"] if counts["accepted"] else 0.0
    )
    counts["positive_recall"] = (
        counts["correct_accepts"] / counts["expected_positive"]
        if counts["expected_positive"]
        else 0.0
    )
    return counts


async def run(cases: list[dict], api_key: str):
    validate_case_ids(cases)
    settings = JevSettings(
        api_key=api_key,
        endpoint=OPENROUTER_ENDPOINT,
        model=OPENROUTER_MODEL,
        extraction_mode="observe",
        max_retries=0,
    )
    policy = _policy(settings)
    ledger = ExternalModelSpendingLedger()
    client = JevClient(settings, spending_ledger=ledger)
    observations = []
    try:
        for case in cases:
            candidate = ExtractionCandidate(
                block_id=uuid5(NAMESPACE_URL, f"knoggin:jev:extraction:{case['id']}"),
                name=case["candidate"],
                evidence_origin="review_packet",
                support_text=case["context"],
                proposed_type=case["proposed_type"],
                source_start=case["context"].find(case["candidate"]),
                source_end=None,
            )
            state, questions, mapping, truncated = prepare_extraction_request(
                candidate, policy
            )
            result = await client.evaluate(
                state=state,
                questions=questions,
                capability="extraction",
                policy=policy.jev,
                work_budget=JevWorkBudget(1, policy.jev.max_elapsed_seconds_per_window),
            )
            accepted = accepted_extraction_type(
                result,
                candidate,
                mapping,
                policy.jev,
                candidate_set_truncated=truncated,
            )
            raw_choice = None
            raw_entity_noul = None
            if result.response is not None:
                raw_entity_noul = result.response.answers["entity_evidence"].noul
                if candidate.proposed_type is None:
                    choice = result.response.answers["type_choice"].choice
                    raw_choice = mapping.get(choice, choice)
            observations.append(
                {
                    "case_id": case["id"],
                    "result": result.model_dump(mode="json"),
                    "option_mapping": mapping,
                    "raw_choice": raw_choice,
                    "raw_entity_noul": raw_entity_noul,
                    "accepted_type": accepted,
                    "candidate_set_truncated": truncated,
                }
            )
    finally:
        await client.close()
    return observations, await ledger.snapshot()


async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--review-cases",
        type=Path,
        default=Path(__file__).with_name("jev_extraction_review_cases.json"),
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    cases = load_reviewed_cases(args.review_cases)
    load_dotenv(REPOSITORY_ENV)
    api_key = os.environ.get("OPENROUTER_API_KEY", "").strip()
    if not api_key:
        raise SystemExit("Set OPENROUTER_API_KEY before running the live pilot")
    observations, spending = await run(cases, api_key)
    report = evaluate(cases, observations)
    costs = [item["result"].get("cost_usd") for item in observations]
    report["provider_reported_cost_usd"] = round(
        sum(float(cost) for cost in costs if cost is not None), 8
    )
    report["spending"] = spending
    report["measurements"] = summarize_results([item["result"] for item in observations])
    report["active_ready"] = False
    report["readiness_reason"] = "Diagnostic packet only; held-out quality and fallback savings are unverified"
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name, value in (
        ("jev_extraction_observations.json", observations),
        ("jev_extraction_report.json", report),
    ):
        (args.output_dir / name).write_text(
            json.dumps(value, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(main())
