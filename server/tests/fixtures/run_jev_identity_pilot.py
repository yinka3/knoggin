"""Run the reviewed identity packet against the real JEV endpoint.

Run from ``server`` after reviewing ``jev_identity_review_cases.json``::

    # Put OPENROUTER_API_KEY in the repository root .env first.
    python -m tests.fixtures.run_jev_identity_pilot --output-dir <private-directory>

The output contains bounded decision records, not API credentials. Keep it in a
private evaluation directory because the evidence can contain project text.
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
from tests.fixtures.evaluate_jev_identity import (
    evaluate_identity_acceptance,
    evaluate_identity_observations,
)

API_KEY_ENV = "OPENROUTER_API_KEY"
REPOSITORY_ENV = Path(__file__).resolve().parents[3] / ".env"
OPENROUTER_DECISIONS_ENDPOINT = "https://openrouter.ai/api/alpha/decisions"
OPENROUTER_JEV_MODEL = "typesafe/jev-1.13"


class ReviewKnowledgeStore:
    """Small read-only store containing only a review case's supplied entities."""

    def __init__(self, entities: list[dict]):
        self._entities = entities

    async def get_visible_entities_for_resolution(self, *, visible_project_ids):
        return [
            entity
            for entity in self._entities
            if any(
                context["project_id"] in visible_project_ids
                for context in entity["contexts"]
            )
        ]


def load_reviewed_cases(path: Path) -> list[dict]:
    packet = json.loads(path.read_text(encoding="utf-8"))
    if packet.get("review_status") != "reviewed":
        raise ValueError(
            "Identity review cases must be human reviewed before live evaluation"
        )
    cases = packet.get("cases")
    if not isinstance(cases, list) or not cases:
        raise ValueError("Identity review packet has no cases")
    if any("proposed_choice" not in case or not case.get("reason") for case in cases):
        raise ValueError("Every identity review case needs a choice and reason")
    return cases


def _case_ids(case_id: str):
    block_id = uuid5(NAMESPACE_URL, f"knoggin:jev:identity:block:{case_id}")
    window_id = uuid5(NAMESPACE_URL, f"knoggin:jev:identity:window:{case_id}")
    return block_id, window_id


def _gold_label(case: dict) -> dict:
    block_id, window_id = _case_ids(case["id"])
    proposed = case["proposed_choice"]
    if isinstance(proposed, int) and not isinstance(proposed, bool):
        correct_entity_id = proposed
        expected_choice = "candidate"
    elif proposed == "candidate_recall_failure":
        correct_entity_id = int(case["stored_identity"]["entity_id"])
        expected_choice = "candidate"
    elif proposed in {"none_of_these", "insufficient_evidence"}:
        correct_entity_id = None
        expected_choice = proposed
    else:
        raise ValueError(f"Unsupported reviewed choice for case {case['id']}")
    return {
        "case_id": case["id"],
        "window_id": str(window_id),
        "pass_number": 1,
        "occurrence_key": f"0:{block_id}",
        "review_status": "reviewed",
        "correct_entity_id": correct_entity_id,
        "expected_choice": expected_choice,
    }


def build_review_labels(cases: list[dict]) -> list[dict]:
    return [_gold_label(case) for case in cases]


def _entity_rows(case: dict) -> list[dict]:
    supplied = [*case.get("candidates", [])]
    if case.get("stored_identity") is not None:
        supplied.append(case["stored_identity"])
    return [
        {
            "id": int(candidate["entity_id"]),
            "canonical_name": candidate["canonical_name"],
            "aliases": candidate.get("aliases", []),
            "contexts": [
                {
                    "project_id": (
                        "project-2" if "foreign_type" in candidate else "project-1"
                    ),
                    "entity_type": candidate.get(
                        "type", candidate.get("foreign_type", "Person")
                    ),
                    "topic": "Work",
                }
            ],
        }
        for candidate in supplied
    ]


def _mention_type(case: dict) -> str:
    supplied = [*case.get("candidates", [])]
    if supplied:
        return "Person" if "foreign_type" in supplied[0] else supplied[0]["type"]
    return case["stored_identity"]["type"]


def _pilot_policy(settings: JevSettings) -> IngestionPolicy:
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                entity_type: {"topic": "Work", "labels": [entity_type.lower()]}
                for entity_type in ("Person", "Company", "Project", "Database")
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
        identity_mode="observe",
        identity_match_sample_rate=1,
        max_calls_per_window=1,
        max_retries=0,
    )
    ledger = ExternalModelSpendingLedger()
    client = JevClient(settings, spending_ledger=ledger)
    policy = _pilot_policy(settings)
    observations = []
    try:
        for index, case in enumerate(cases):
            block_id, window_id = _case_ids(case["id"])
            resolver = EntityResolver(
                ReviewKnowledgeStore(_entity_rows(case)),
                "project-1",
                ["project-1", "project-2"],
                jev_client=client,
            )

            async def allocate_entity_id(value=90_000 + index):
                return value

            result = await resolver.resolve_context_block_mentions(
                [
                    ContextBlockMention(
                        block_ids=(block_id,),
                        name=case["mention"],
                        entity_type=_mention_type(case),
                        topic="Work",
                        origin="vp01",
                    )
                ],
                block_text_by_id={block_id: case["context"]},
                policy=policy,
                allocate_entity_id=allocate_entity_id,
                window_id=window_id,
                pass_number=1,
            )
            observation = next(
                (
                    decision["jev"]
                    for decision in result.identity_decisions
                    if "jev" in decision
                ),
                None,
            )
            if observation is not None:
                observations.append(observation)
    finally:
        await client.close()
    return observations, await ledger.snapshot()


async def _main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--review-cases",
        type=Path,
        default=Path(__file__).with_name("jev_identity_review_cases.json"),
    )
    parser.add_argument("--output-dir", required=True, type=Path)
    args = parser.parse_args()

    cases = load_reviewed_cases(args.review_cases)
    load_dotenv(REPOSITORY_ENV)
    api_key = os.environ.get(API_KEY_ENV, "").strip()
    if not api_key:
        raise SystemExit(f"Set {API_KEY_ENV} before running the live pilot")

    labels = build_review_labels(cases)
    observations, spending = await run_pilot(cases, api_key)
    report = {
        "observation_quality": evaluate_identity_observations(labels, observations),
        "active_policy": evaluate_identity_acceptance(labels, observations),
        "spending": spending,
    }
    args.output_dir.mkdir(parents=True, exist_ok=True)
    (args.output_dir / "jev_identity_labels.json").write_text(
        json.dumps(labels, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "jev_identity_observations.json").write_text(
        json.dumps(observations, indent=2) + "\n", encoding="utf-8"
    )
    (args.output_dir / "jev_identity_report.json").write_text(
        json.dumps(report, indent=2) + "\n", encoding="utf-8"
    )
    print(json.dumps(report, indent=2))


if __name__ == "__main__":
    asyncio.run(_main())
