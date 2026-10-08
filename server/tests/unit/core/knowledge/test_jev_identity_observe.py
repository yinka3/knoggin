"""Identity observation uses scoped candidates without changing stored decisions."""

import asyncio
import json
from dataclasses import replace
from types import SimpleNamespace
from uuid import uuid4

import httpx
import pytest

from common.conf.domain_config import DomainConfig
from common.schema.ingestion.contracts import ContextBlockMention
from common.schema.jev import JevPolicy, JevResponse, JevResult, JevSettings
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.policy import IngestionPolicy
from core.knowledge.entity.resolver import EntityResolver
from infrastructure.external_model_budget import ExternalModelSpendingLedger
from infrastructure.jev_client import JevClient, JevWorkBudget


class Store:
    def __init__(self, entities):
        self.entities = entities

    async def get_visible_entities_for_resolution(self, *, visible_project_ids):
        return [
            entity
            for entity in self.entities
            if entity.get("status", "active") == "active"
            and any(
                context["project_id"] in visible_project_ids
                for context in entity["contexts"]
            )
        ]


class FakeJev:
    def __init__(
        self,
        *,
        choice="candidate_1",
        unavailable=False,
        confidence=0.7,
        evidence_noul=0.6,
        selected_probability=1.0,
        runner_up_probability=0.0,
    ):
        self.calls = []
        self.choice = choice
        self.unavailable = unavailable
        self.confidence = confidence
        self.evidence_noul = evidence_noul
        self.selected_probability = selected_probability
        self.runner_up_probability = runner_up_probability

    async def evaluate(self, **kwargs):
        self.calls.append(kwargs)
        if self.unavailable:
            return JevResult(outcome="unavailable", reason="transport_error")
        handles = kwargs["questions"]["identity_choice"].criteria
        return JevResult(
            outcome="available",
            response=JevResponse.model_validate(
                {
                    "model": "jev-1.13.0",
                    "answers": {
                        "identity_choice": {
                            "type": "choice",
                            "choice": self.choice,
                            "probabilities": self._probabilities(handles),
                            "confidence": self.confidence,
                        },
                        "identity_evidence": {
                            "type": "noul",
                            "noul": self.evidence_noul,
                        },
                    },
                    "usage": {"input_tokens": 100, "output_tokens": 0},
                }
            ),
        )

    def _probabilities(self, handles):
        probabilities = {handle: 0.0 for handle in handles}
        if self.choice in probabilities:
            probabilities[self.choice] = self.selected_probability
        runner_up = next(
            (handle for handle in handles if handle != self.choice), None
        )
        if runner_up is not None:
            probabilities[runner_up] = self.runner_up_probability
        return probabilities


def entity(entity_id, canonical_name, entity_type, *, project_id="project-1"):
    return {
        "id": entity_id,
        "canonical_name": canonical_name,
        "aliases": ["Bob"],
        "contexts": [
            {
                "project_id": project_id,
                "entity_type": entity_type,
                "topic": "Work",
            }
        ],
    }


def identity_policy(
    mode="observe",
    *,
    max_candidates=8,
    max_calls_per_window=12,
    identity_match_sample_rate=0,
    acceptance_policy_version="observe-v1",
):
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                "Person": {"topic": "Work", "labels": ["person"]},
                "Company": {"topic": "Work", "labels": ["company"]},
            },
        }
    ).compile()
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=JevPolicy(
            identity_mode=mode,
            max_candidates=max_candidates,
            max_calls_per_window=max_calls_per_window,
            identity_match_sample_rate=identity_match_sample_rate,
            acceptance_policy_version=acceptance_policy_version,
        ),
    )


async def resolve(resolver, policy, supports, *, name="Bob", work_budget=None, timing=None):
    block_ids = [uuid4() for _ in supports]
    next_id = iter(range(900, 900 + len(supports)))

    async def allocate():
        return next(next_id)

    return await resolver.resolve_context_block_mentions(
        [
            ContextBlockMention(
                block_ids=(block_id,),
                name=name,
                entity_type="Person",
                topic="Work",
                origin="vp01",
            )
            for block_id in block_ids
        ],
        block_text_by_id=dict(zip(block_ids, supports, strict=True)),
        policy=policy,
        allocate_entity_id=allocate,
        window_id=uuid4(),
        work_budget=work_budget,
        timing=timing,
    )


@pytest.mark.no_network
async def test_resolution_lock_measures_wait_and_hold_even_on_failure(monkeypatch):
    resolver = EntityResolver(Store([]), "project-1", ["project-1"])
    clock = iter((1.0, 1.25, 2.0))
    monkeypatch.setattr(
        "core.knowledge.entity.resolver.time",
        SimpleNamespace(monotonic=lambda: next(clock)),
    )
    timing = {}
    with pytest.raises(RuntimeError, match="test failure"):
        async with resolver._measured_resolution_lock(timing):
            assert resolver.resolution_lock.locked()
            raise RuntimeError("test failure")
    assert timing == {"wait_seconds": 0.25, "held_seconds": 0.75}
    assert not resolver.resolution_lock.locked()


@pytest.mark.no_network
@pytest.mark.parametrize("mode", ["disabled", "active"])
async def test_non_observe_modes_keep_baseline_without_judging(mode):
    jev = FakeJev()
    store = Store(
        [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
    )
    resolver = EntityResolver(store, "project-1", ["project-1"], jev_client=jev)

    result = await resolve(resolver, identity_policy(mode), ["Bob joined the meeting."])

    assert result.entity_ids == (900,)
    assert result.identity_decisions[0]["outcome"] == "abstained"
    assert "jev" not in result.identity_decisions[0]
    assert jev.calls == []


@pytest.mark.no_network
async def test_active_identity_positive_gate_reuses_supplied_eligible_candidate():
    jev = FakeJev(confidence=0.95, evidence_noul=0.95)
    store = Store(
        [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
    )
    resolver = EntityResolver(store, "project-1", ["project-1"], jev_client=jev)

    result = await resolve(
        resolver,
        identity_policy(
            "active", acceptance_policy_version="identity-positive-v1"
        ),
        ["Bob joined the meeting."],
    )

    assert result.entity_ids == (701,)
    assert result.new_entity_ids == frozenset()
    decision = result.identity_decisions[0]
    assert decision["selection_source"] == "jev_active"
    assert decision["jev"]["accepted_entity_id"] == 701
    assert decision["jev"]["baseline_entity_id"] is None
    assert decision["jev"]["record"]["acceptance_status"] == "accepted"


@pytest.mark.no_network
async def test_active_identity_below_gate_uses_pending_new_id_baseline():
    jev = FakeJev(confidence=0.95, evidence_noul=0.2)
    store = Store(
        [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
    )
    resolver = EntityResolver(store, "project-1", ["project-1"], jev_client=jev)

    result = await resolve(
        resolver,
        identity_policy(
            "active", acceptance_policy_version="identity-positive-v1"
        ),
        ["Bob joined the meeting."],
    )

    assert result.entity_ids == (900,)
    assert result.new_entity_ids == frozenset({900})
    observation = result.identity_decisions[0]["jev"]
    assert observation["accepted_entity_id"] is None
    assert observation["baseline_entity_id"] == 900
    assert observation["record"]["acceptance_status"] == "rejected"


@pytest.mark.no_network
async def test_active_identity_never_accepts_a_truncated_candidate_set():
    jev = FakeJev(confidence=0.95, evidence_noul=0.95)
    store = Store(
        [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
    )
    resolver = EntityResolver(store, "project-1", ["project-1"], jev_client=jev)

    result = await resolve(
        resolver,
        identity_policy(
            "active",
            max_candidates=1,
            acceptance_policy_version="identity-positive-v1",
        ),
        ["Bob joined the meeting."],
    )

    assert result.entity_ids == (900,)
    observation = result.identity_decisions[0]["jev"]
    assert observation["candidate_set_truncated"] is True
    assert observation["accepted_entity_id"] is None
    assert observation["record"]["acceptance_status"] == "indeterminate"


@pytest.mark.no_network
@pytest.mark.parametrize(
    "jev",
    [
        FakeJev(confidence=0.5, evidence_noul=0.95),
        FakeJev(
            confidence=0.95,
            evidence_noul=0.95,
            selected_probability=0.7,
        ),
        FakeJev(
            confidence=0.95,
            evidence_noul=0.95,
            selected_probability=0.85,
            runner_up_probability=0.75,
        ),
    ],
)
async def test_active_identity_requires_every_probability_gate(jev):
    resolver = EntityResolver(
        Store(
            [
                entity(701, "Robert Chen", "Person"),
                entity(702, "Bob Smith", "Person"),
            ]
        ),
        "project-1",
        ["project-1"],
        jev_client=jev,
    )

    result = await resolve(
        resolver,
        identity_policy(
            "active", acceptance_policy_version="identity-positive-v1"
        ),
        ["Bob joined the meeting."],
    )

    assert result.entity_ids == (900,)
    assert result.identity_decisions[0]["jev"]["accepted_entity_id"] is None


@pytest.mark.no_network
async def test_clear_deterministic_match_does_not_call_jev():
    jev = FakeJev()
    resolver = EntityResolver(
        Store([entity(701, "Robert Chen", "Person")]),
        "project-1",
        ["project-1"],
        jev_client=jev,
    )

    result = await resolve(
        resolver,
        identity_policy(),
        ["Robert Chen joined the meeting."],
        name="Robert Chen",
    )

    assert result.entity_ids == (701,)
    assert jev.calls == []


@pytest.mark.no_network
async def test_sampled_deterministic_reuse_is_observed_without_changing_identity():
    jev = FakeJev()
    resolver = EntityResolver(
        Store([entity(701, "Robert Chen", "Person")]),
        "project-1",
        ["project-1"],
        jev_client=jev,
    )

    result = await resolve(
        resolver,
        identity_policy(identity_match_sample_rate=1),
        ["Robert Chen joined the meeting."],
        name="Robert Chen",
    )

    assert result.entity_ids == (701,)
    assert len(jev.calls) == 1
    assert result.identity_decisions[0]["jev"]["baseline_entity_id"] == 701
    assert result.identity_decisions[0]["jev"]["record"]["baseline_outcome"] == (
        "deterministic_reuse"
    )


async def test_observe_positive_policy_never_accepts_or_changes_baseline():
    jev = FakeJev(confidence=0.95, evidence_noul=0.95)
    resolver = EntityResolver(Store([
        entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person"),
    ]), "project-1", ["project-1"], jev_client=jev)
    result = await resolve(resolver, identity_policy(
        "observe", acceptance_policy_version="identity-positive-v1",
    ), ["Bob joined the meeting."])
    observation = result.identity_decisions[0]["jev"]
    assert result.entity_ids == (900,)
    assert observation["accepted_entity_id"] is None
    assert observation["record"]["acceptance_status"] == "proposed"


async def test_many_aliases_cannot_hide_competing_identity_from_jev():
    first = entity(701, "Robert A 000", "Person")
    first["aliases"] = [f"Robert A {i:03d}" for i in range(61)]
    second = entity(702, "Robert Z 999", "Person")
    second["aliases"] = []
    resolver = EntityResolver(Store([first, second]), "project-1", ["project-1"],
        jev_client=FakeJev(confidence=0.95, evidence_noul=0.95))
    result = await resolve(resolver, identity_policy(
        "active", max_candidates=1, acceptance_policy_version="identity-positive-v1",
    ), ["Robert Z 999."], name="Robert")
    observation = result.identity_decisions[0]["jev"]
    assert set(observation["eligible_candidate_ids"]) == {701, 702}
    assert observation["candidate_set_truncated"]
    assert observation["accepted_entity_id"] is None
    assert result.entity_ids == (900,)


@pytest.mark.parametrize("changed", [None, "evidence", "policy"])
async def test_accepted_decision_is_carried_only_for_identical_frozen_inputs(changed):
    jev = FakeJev(confidence=0.95, evidence_noul=0.95)
    resolver = EntityResolver(Store([
        entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person"),
    ]), "project-1", ["project-1"], jev_client=jev)
    frozen = identity_policy("active", acceptance_policy_version="identity-positive-v1")
    block_id, window_id = uuid4(), uuid4()
    mention = ContextBlockMention(block_ids=(block_id,), name="Bob", entity_type="Person", topic="Work", origin="vp01")

    async def allocate():
        return 900

    first = await resolver.resolve_context_block_mentions([mention],
        block_text_by_id={block_id: "Bob joined the meeting."}, policy=frozen,
        allocate_entity_id=allocate, window_id=window_id)
    assert first.entity_ids == (701,)
    unavailable = FakeJev(unavailable=True)
    resolver._jev_client = unavailable
    second = await resolver.resolve_context_block_mentions([mention],
        block_text_by_id={block_id: "Bob reviewed an invoice." if changed == "evidence" else "Bob joined the meeting."},
        policy=replace(frozen, candidate_fuzzy_threshold=84) if changed == "policy" else frozen,
        allocate_entity_id=allocate, window_id=window_id, pass_number=2,
        previous_identity_decisions=first.identity_decisions)
    if changed is None:
        assert second.entity_ids == (701,)
        assert unavailable.calls == []
    else:
        assert second.entity_ids == (900,)
        assert len(unavailable.calls) == 1


@pytest.mark.no_network
async def test_observe_keeps_baseline_and_limits_handles_to_eligible_candidates():
    jev = FakeJev()
    store = Store(
        [
            entity(701, "Robert Chen", "Person"),
            entity(702, "Bob Smith", "Person"),
            entity(703, "Bob Incorporated", "Company"),
            entity(704, "Bob Foreign", "Person", project_id="project-2"),
            entity(705, "Bob Hidden", "Person", project_id="private"),
        ]
    )
    resolver = EntityResolver(
        store, "project-1", ["project-1", "project-2"], jev_client=jev
    )

    result = await resolve(
        resolver,
        identity_policy(max_candidates=2),
        ["Bob joined one meeting.", "Bob joined a different meeting."],
    )

    assert result.entity_ids == (900, 901)
    assert len(jev.calls) == 2
    assert [call["state"]["support_text"] for call in jev.calls] == [
        "Bob joined one meeting.",
        "Bob joined a different meeting.",
    ]
    for decision in result.identity_decisions:
        shadow = decision["jev"]
        assert shadow["eligible_candidate_count"] == 3
        assert set(shadow["eligible_candidate_ids"]) == {701, 702, 704}
        assert shadow["candidate_catalog_truncated"] is False
        assert shadow["offered_candidate_count"] == 2
        assert shadow["candidate_set_truncated"] is True
        assert shadow["suggested_entity_id"] is None
        assert shadow["baseline_entity_id"] in {900, 901}
        record = shadow["record"]
        assert record["mode"] == "observe"
        assert record["acceptance_status"] == "indeterminate"
        assert record["baseline_outcome"] == "deterministic_abstention"
        assert set(record["option_mapping"].values()).issubset({"701", "702", "704"})
        assert "703" not in record["option_mapping"].values()
        assert "705" not in record["option_mapping"].values()
        assert set(jev.calls[0]["questions"]) == {
            "identity_choice",
            "identity_evidence",
        }
    assert (
        result.identity_decisions[0]["jev"]["record"]["occurrence_key"]
        != (result.identity_decisions[1]["jev"]["record"]["occurrence_key"])
    )
    assert resolver.has_cached_entity(701) is False


@pytest.mark.no_network
@pytest.mark.parametrize(
    "choice,unavailable", [("invented_handle", False), ("candidate_1", True)]
)
async def test_invalid_or_unavailable_suggestion_cannot_change_resolution(
    choice, unavailable
):
    jev = FakeJev(choice=choice, unavailable=unavailable)
    store = Store(
        [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
    )
    resolver = EntityResolver(store, "project-1", ["project-1"], jev_client=jev)

    result = await resolve(resolver, identity_policy(), ["Bob joined the meeting."])

    assert result.entity_ids == (900,)
    assert result.identity_decisions[0]["jev"]["suggested_entity_id"] is None
    assert result.identity_decisions[0]["jev"]["record"]["acceptance_status"] == (
        "unavailable" if unavailable else "proposed"
    )


@pytest.mark.no_network
async def test_only_hard_incompatible_candidate_never_reaches_jev():
    jev = FakeJev()
    resolver = EntityResolver(
        Store([entity(703, "Bob Incorporated", "Company")]),
        "project-1",
        ["project-1"],
        jev_client=jev,
    )

    result = await resolve(resolver, identity_policy(), ["Bob joined the meeting."])

    assert result.entity_ids == (900,)
    assert jev.calls == []


@pytest.mark.no_network
async def test_foreign_project_classification_is_neutral_in_request():
    jev = FakeJev()
    resolver = EntityResolver(
        Store([entity(701, "Bob Foreign", "Person", project_id="project-2")]),
        "project-1",
        ["project-1", "project-2"],
        jev_client=jev,
    )

    result = await resolve(resolver, identity_policy(), ["Bob joined the meeting."])

    assert result.entity_ids == (900,)
    assert jev.calls[0]["state"]["candidates"][0]["classification"] == (
        "foreign_project_classification_not_applicable"
    )


@pytest.mark.no_network
async def test_provider_exception_falls_back_to_baseline():
    class FailingJev:
        async def evaluate(self, **_kwargs):
            raise RuntimeError("provider failed")

    resolver = EntityResolver(
        Store(
            [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
        ),
        "project-1",
        ["project-1"],
        jev_client=FailingJev(),
    )

    result = await resolve(resolver, identity_policy(), ["Bob joined the meeting."])

    assert result.entity_ids == (900,)
    assert result.identity_decisions[0]["jev"]["record"]["result"]["reason"] == (
        "provider_error"
    )


@pytest.mark.no_network
async def test_shared_work_budget_limits_occurrences_in_one_build():
    sent = []

    def answer(request):
        sent.append(request)
        payload = json.loads(request.content)
        handles = payload["questions"]["identity_choice"]["criteria"]
        return httpx.Response(
            200,
            json={
                "model": "jev-1.13.0",
                "answers": {
                    "identity_choice": {
                        "type": "choice",
                        "choice": "insufficient_evidence",
                        "probabilities": {
                            handle: float(handle == "insufficient_evidence")
                            for handle in handles
                        },
                        "confidence": 0.8,
                    },
                    "identity_evidence": {"type": "noul", "noul": 0.1},
                },
                "usage": {"input_tokens": 100, "output_tokens": 0},
            },
        )

    settings = JevSettings(
        api_key="fake", identity_mode="observe", max_calls_per_window=1
    )
    client = JevClient(
        settings,
        spending_ledger=ExternalModelSpendingLedger(),
        transport=httpx.MockTransport(answer),
    )
    resolver = EntityResolver(
        Store(
            [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
        ),
        "project-1",
        ["project-1"],
        jev_client=client,
    )
    try:
        result = await resolve(
            resolver,
            identity_policy(max_candidates=2, max_calls_per_window=1),
            ["Bob joined one meeting.", "Bob joined another meeting."],
        )
    finally:
        await client.close()

    assert len(sent) == 1
    assert result.entity_ids == (900, 901)
    assert result.identity_decisions[0]["jev"]["record"]["result"]["outcome"] == (
        "available"
    )
    assert result.identity_decisions[1]["jev"]["record"]["result"]["reason"] == (
        "work_budget_exhausted"
    )


@pytest.mark.no_network
async def test_observation_count_is_bounded_when_provider_is_unavailable():
    jev = FakeJev(unavailable=True)
    resolver = EntityResolver(
        Store(
            [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
        ),
        "project-1",
        ["project-1"],
        jev_client=jev,
    )

    result = await resolve(
        resolver,
        identity_policy(),
        ["Bob joined one meeting.", "Bob joined another meeting."],
        work_budget=JevWorkBudget(12, max_observations=1),
    )

    assert result.entity_ids == (900, 901)
    assert len(jev.calls) == 1
    assert "jev" in result.identity_decisions[0]
    assert "jev" not in result.identity_decisions[1]


@pytest.mark.no_network
async def test_cancellation_propagates_without_allocating_pending_identity():
    class CancellingJev:
        async def evaluate(self, **_kwargs):
            raise asyncio.CancelledError

    resolver = EntityResolver(
        Store(
            [entity(701, "Robert Chen", "Person"), entity(702, "Bob Smith", "Person")]
        ),
        "project-1",
        ["project-1"],
        jev_client=CancellingJev(),
    )

    with pytest.raises(asyncio.CancelledError):
        await resolve(resolver, identity_policy(), ["Bob joined the meeting."])
    assert resolver.has_cached_entity(701) is False
