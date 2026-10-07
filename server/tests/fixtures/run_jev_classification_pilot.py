"""Run reviewed topic cases against the real JEV endpoint.

Run from server after reviewing ``jev_classification_review_cases.json``::

    # Put OPENROUTER_API_KEY in the repository root .env first.
    python -m tests.fixtures.run_jev_classification_pilot --output-dir <private-directory>
"""

from __future__ import annotations

import argparse
import asyncio
import json
import os
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

from dotenv import load_dotenv

from common.conf.domain_config import DomainConfig
from common.schema.ingestion.contracts import ContextBlockMention
from common.schema.jev import JevSettings
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.policy import IngestionPolicy
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.external_model_budget import ExternalModelSpendingLedger
from infrastructure.jev_client import JevClient
from tests.fixtures.evaluate_jev_classification import (
    MIN_AMBIGUOUS_OCCURRENCES,
    MIN_NON_DEFAULT_OCCURRENCES,
    MIN_REPEATED_ENTITY_AGGREGATES,
    MIN_REVIEWED_OCCURRENCES,
    evaluate_classification_observations,
)
from tests.fixtures.jev_measurements import summarize_results

API_KEY_ENV = "OPENROUTER_API_KEY"
REPOSITORY_ENV = Path(__file__).resolve().parents[3] / ".env"
OPENROUTER_DECISIONS_ENDPOINT = "https://openrouter.ai/api/alpha/decisions"
OPENROUTER_JEV_MODEL = "typesafe/jev-1.13"


class EmptyKnowledgeStore:
    async def get_visible_entities_for_resolution(self, *, visible_project_ids):
        return []


def review_packet_coverage(cases: list[dict]) -> dict[str, int | bool]:
    occurrences = sum(len(case.get("occurrences", ())) for case in cases)
    non_default = sum(
        occurrence.get("proposed_choice")
        not in {case.get("default_topic"), "insufficient_evidence"}
        for case in cases
        for occurrence in case.get("occurrences", ())
    )
    repeated = sum(len(case.get("occurrences", ())) > 1 for case in cases)
    ambiguous = sum(
        occurrence.get("proposed_choice") == "insufficient_evidence"
        for case in cases
        for occurrence in case.get("occurrences", ())
    )
    return {
        "occurrences": occurrences,
        "non_default_occurrences": non_default,
        "ambiguous_occurrences": ambiguous,
        "repeated_entity_aggregates": repeated,
        "meets_sample_gate": (
            occurrences >= MIN_REVIEWED_OCCURRENCES
            and non_default >= MIN_NON_DEFAULT_OCCURRENCES
            and ambiguous >= MIN_AMBIGUOUS_OCCURRENCES
            and repeated >= MIN_REPEATED_ENTITY_AGGREGATES
        ),
    }


def validate_review_cases(cases: list[dict]) -> None:
    if not isinstance(cases, list) or not cases:
        raise ValueError("Classification review packet has no cases")
    case_ids = [case.get("id") for case in cases]
    if any(not isinstance(case_id, str) or not case_id.strip() for case_id in case_ids):
        raise ValueError("Every classification case needs a nonblank ID")
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("Classification review case IDs must be unique")
    for case in cases:
        occurrences = case.get("occurrences")
        allowed_topics = case.get("allowed_topics")
        if not occurrences or not case.get("reason"):
            raise ValueError("Every classification case needs occurrences and a reason")
        if (
            not isinstance(allowed_topics, list)
            or len(allowed_topics) < 2
            or len(allowed_topics) != len(set(allowed_topics))
            or case.get("default_topic") not in allowed_topics
        ):
            raise ValueError("Classification case topics are invalid")
        if any("proposed_choice" not in item for item in occurrences):
            raise ValueError("Every classification occurrence needs a reviewed choice")
        choices = {item["proposed_choice"] for item in occurrences}
        if not choices <= {*allowed_topics, "insufficient_evidence"}:
            raise ValueError("A reviewed classification choice is outside allowed topics")
        proposed = [
            topic
            for topic in allowed_topics
            if topic in choices and topic != case["default_topic"]
        ]
        expected_status = (
            "conflicting_proposals"
            if len(proposed) > 1
            else "consistent_proposal"
            if proposed
            else "no_proposal"
        )
        if (
            case.get("proposed_topics") != proposed
            or case.get("proposed_aggregate_status") != expected_status
        ):
            raise ValueError("Classification aggregate label is inconsistent")
        if len(occurrences) > 1 and len(
            {item.get("context") for item in occurrences}
        ) != 1:
            raise ValueError(
                "Repeated-entity pilot occurrences must use identical evidence"
            )


def load_reviewed_cases(path: Path) -> list[dict]:
    packet = json.loads(path.read_text(encoding="utf-8"))
    if packet.get("review_status") != "reviewed":
        raise ValueError(
            "Classification review cases must be human reviewed before live evaluation"
        )
    cases = packet.get("cases")
    validate_review_cases(cases)
    if not review_packet_coverage(cases)["meets_sample_gate"]:
        raise ValueError("Classification review packet does not meet sample gates")
    return cases


def _ids(case_id: str, occurrence_index: int):
    window_id = uuid5(NAMESPACE_URL, f"knoggin:jev:classification:window:{case_id}")
    block_id = uuid5(
        NAMESPACE_URL,
        f"knoggin:jev:classification:block:{case_id}:{occurrence_index}",
    )
    return window_id, block_id


def build_review_labels(cases: list[dict]) -> dict[str, list[dict]]:
    occurrence_labels = []
    aggregate_labels = []
    for case in cases:
        occurrence_keys = []
        for index, occurrence in enumerate(case["occurrences"]):
            window_id, block_id = _ids(case["id"], index)
            occurrence_key = f"{index}:{block_id}:topic"
            occurrence_keys.append(occurrence_key)
            proposed = occurrence["proposed_choice"]
            occurrence_labels.append(
                {
                    "case_id": case["id"],
                    "window_id": str(window_id),
                    "pass_number": 1,
                    "occurrence_key": occurrence_key,
                    "review_status": "reviewed",
                    "default_topic": case["default_topic"],
                    "expected_topic": (
                        None if proposed == "insufficient_evidence" else proposed
                    ),
                    "expected_choice": (
                        "insufficient_evidence"
                        if proposed == "insufficient_evidence"
                        else "topic"
                    ),
                }
            )
        aggregate_labels.append(
            {
                "case_id": case["id"],
                "window_id": occurrence_labels[-1]["window_id"],
                "pass_number": 1,
                "anchor_occurrence_key": occurrence_keys[0],
                "review_status": "reviewed",
                "expected_status": case["proposed_aggregate_status"],
                "expected_proposed_topics": case["proposed_topics"],
                "observation_count": len(case["occurrences"]),
            }
        )
    return {"occurrences": occurrence_labels, "aggregates": aggregate_labels}


def provider_usage_summary(observations: list[dict]) -> dict[str, int | float]:
    """Summarize exact provider usage separately from configured budget pricing."""

    results = [item["record"]["result"] for item in observations]
    reported_costs = [
        float(result["cost_usd"])
        for result in results
        if result.get("cost_usd") is not None
    ]
    return {
        "requests": len(results),
        "cost_reported_requests": len(reported_costs),
        "input_tokens": sum(int(result.get("input_tokens", 0)) for result in results),
        "output_tokens": sum(
            int(result.get("output_tokens", 0)) for result in results
        ),
        "reported_cost_usd": round(sum(reported_costs), 8),
    }


def _policy(case: dict, settings: JevSettings) -> IngestionPolicy:
    topics = {
        topic: {"description": f"Information about {topic}."}
        for topic in case["allowed_topics"]
    }
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": topics,
            "entity_types": {
                case["entity_type"]: {
                    "topic": case["default_topic"],
                    "allowed_topics": case["allowed_topics"],
                    "labels": [case["entity_type"].lower()],
                }
            },
        }
    ).compile()
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=settings.capture_policy(),
    )


async def run_pilot(cases: list[dict], api_key: str):
    settings = JevSettings(
        api_key=api_key,
        endpoint=OPENROUTER_DECISIONS_ENDPOINT,
        model=OPENROUTER_JEV_MODEL,
        classification_mode="observe",
        max_calls_per_window=12,
        max_retries=0,
    )
    ledger = ExternalModelSpendingLedger()
    client = JevClient(settings, spending_ledger=ledger)
    observations = []
    aggregates = []
    resolver_timings = []
    try:
        for case_index, case in enumerate(cases):
            window_id, _ = _ids(case["id"], 0)
            block_ids = [_ids(case["id"], index)[1] for index in range(len(case["occurrences"]))]
            next_id = iter(range(90_000 + case_index * 100, 90_100 + case_index * 100))

            async def allocate_entity_id():
                return next(next_id)

            timing = {}
            result = await EntityResolver(
                EmptyKnowledgeStore(),
                "project-1",
                ["project-1"],
                jev_client=client,
            ).resolve_context_block_mentions(
                [
                    ContextBlockMention(
                        block_ids=(block_id,),
                        name=case["entity_name"],
                        entity_type=case["entity_type"],
                        topic=case["default_topic"],
                        origin="vp01",
                    )
                    for block_id in block_ids
                ],
                block_text_by_id={
                    block_id: occurrence["context"]
                    for block_id, occurrence in zip(
                        block_ids, case["occurrences"], strict=True
                    )
                },
                policy=_policy(case, settings),
                allocate_entity_id=allocate_entity_id,
                window_id=window_id,
                pass_number=1,
                timing=timing,
            )
            resolver_timings.append({"case_id": case["id"], **timing})
            observations.extend(result.classification_decisions)
            aggregates.extend(
                {"window_id": str(window_id), **aggregate}
                for aggregate in result.classification_aggregates
            )
    finally:
        await client.close()
    spending = await ledger.snapshot()
    spending["resolver_timings"] = resolver_timings
    return observations, aggregates, spending


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--review-cases",
        type=Path,
        default=Path(__file__).with_name("jev_classification_review_cases.json"),
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()
    cases = load_reviewed_cases(args.review_cases)
    load_dotenv(REPOSITORY_ENV)
    api_key = os.environ.get(API_KEY_ENV, "").strip()
    if not api_key:
        raise SystemExit(f"Set {API_KEY_ENV} before running the live pilot")

    labels = build_review_labels(cases)
    observations, aggregates, spending = await run_pilot(cases, api_key)
    report = evaluate_classification_observations(labels, observations, aggregates)
    report["resolver_timings"] = spending.pop("resolver_timings")
    report["spending"] = spending
    report["provider_usage"] = provider_usage_summary(observations)
    report["measurements"] = summarize_results(
        [item["record"]["result"] for item in observations]
    )
    args.output_dir.mkdir(parents=True, exist_ok=True)
    for name, value in (
        ("jev_classification_labels.json", labels),
        ("jev_classification_observations.json", observations),
        ("jev_classification_aggregates.json", aggregates),
        ("jev_classification_report.json", report),
    ):
        (args.output_dir / name).write_text(
            json.dumps(value, indent=2) + "\n", encoding="utf-8"
        )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
