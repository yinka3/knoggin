"""Observe-only JEV topic classification contracts."""

import asyncio
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.schema.ingestion.contracts import ContextBlockMention
from common.schema.jev import JevPolicy, JevResponse, JevResult
from common.schema.settings import EntityResolutionSettings, TextProcessorSettings
from core.ingestion.policy import IngestionPolicy
from core.knowledge.entity.resolver import EntityResolver


class Store:
    def __init__(self, entities=()):
        self.entities = list(entities)

    async def get_visible_entities_for_resolution(self, *, visible_project_ids):
        return [
            entity
            for entity in self.entities
            if any(
                context["project_id"] in visible_project_ids
                for context in entity["contexts"]
            )
        ]


class TopicJev:
    def __init__(self, choice="topic_2"):
        self.choice = choice
        self.calls = []

    async def evaluate(self, **kwargs):
        self.calls.append(kwargs)
        choice = (
            self.choice[len(self.calls) - 1]
            if isinstance(self.choice, list)
            else self.choice
        )
        criteria = kwargs["questions"]["topic_choice"].criteria
        return JevResult(
            outcome="available",
            response=JevResponse.model_validate(
                {
                    "model": kwargs["policy"].model,
                    "answers": {
                        "topic_choice": {
                            "type": "choice",
                            "choice": choice,
                            "probabilities": {
                                handle: float(handle == choice)
                                for handle in criteria
                            },
                            "confidence": 0.95,
                        },
                        "topic_evidence": {"type": "noul", "noul": 0.9},
                    },
                    "usage": {"input_tokens": 20, "output_tokens": 0},
                }
            ),
        )


def policy(
    mode="observe",
    *,
    topics=("Work", "Finance"),
    max_options=64,
    acceptance_policy="disabled",
):
    domain = DomainConfig.from_mapping(
        {
            "version": 4,
            "topics": {
                topic: {"description": f"Information about {topic}."}
                for topic in topics
            },
            "entity_types": {
                "Company": {
                    "topic": "Work",
                    "allowed_topics": list(topics),
                    "labels": ["company"],
                }
            },
        }
    ).compile()
    return IngestionPolicy.capture(
        text_processor=TextProcessorSettings(),
        entity_resolution=EntityResolutionSettings(),
        compiled_domain=domain,
        jev=JevPolicy(
            classification_mode=mode,
            classification_acceptance_policy_version=acceptance_policy,
            max_options_per_choice=max_options,
        ),
    )


def entity(*, project_id, topic):
    return {
        "id": 701,
        "canonical_name": "Acme",
        "aliases": [],
        "contexts": [
            {
                "project_id": project_id,
                "entity_type": "Company",
                "topic": topic,
            }
        ],
    }


async def resolve(store, jev, frozen_policy):
    block_id = uuid4()

    async def allocate():
        return 900

    return await EntityResolver(
        store,
        "project-1",
        ["project-1", "project-2"],
        jev_client=jev,
    ).resolve_context_block_mentions(
        [
            ContextBlockMention(
                block_ids=(block_id,),
                name="Acme",
                entity_type="Company",
                topic="Work",
                origin="vp01",
            )
        ],
        block_text_by_id={block_id: "Acme manages the project budget."},
        policy=frozen_policy,
        allocate_entity_id=allocate,
        window_id=uuid4(),
    )


async def resolve_repeated(jev, frozen_policy):
    block_ids = (uuid4(), uuid4())
    text = "Acme manages the project budget."
    next_id = iter((900, 901))

    async def allocate():
        return next(next_id)

    return await EntityResolver(
        Store(),
        "project-1",
        ["project-1"],
        jev_client=jev,
    ).resolve_context_block_mentions(
        [
            ContextBlockMention(
                block_ids=(block_id,),
                name="Acme",
                entity_type="Company",
                topic="Work",
                origin="vp01",
            )
            for block_id in block_ids
        ],
        block_text_by_id={block_id: text for block_id in block_ids},
        policy=frozen_policy,
        allocate_entity_id=allocate,
        window_id=uuid4(),
    )


@pytest.mark.no_network
async def test_observe_records_topic_suggestion_but_keeps_default_classification():
    jev = TopicJev()

    result = await resolve(Store(), jev, policy())

    assert result.entity_ids == (900,)
    assert result.project_classifications[900].topic == "Work"
    assert result.pending_entity_writes[900].topic == "Work"
    assert len(jev.calls) == 1
    assert jev.calls[0]["capability"] == "classification"
    assert set(jev.calls[0]["questions"]) == {"topic_choice", "topic_evidence"}
    assert "default_topic" not in jev.calls[0]["state"]["mention"]
    assert "directly connects" in jev.calls[0]["questions"][
        "topic_choice"
    ].instructions
    assert jev.calls[0]["state"]["allowed_topics"] == [
        {
            "handle": "topic_1",
            "name": "Work",
            "description": "Information about Work.",
        },
        {
            "handle": "topic_2",
            "name": "Finance",
            "description": "Information about Finance.",
        },
    ]
    observation = result.classification_decisions[0]
    assert observation["baseline_topic"] == "Work"
    assert observation["suggested_topic"] == "Finance"
    assert observation["record"]["acceptance_status"] == "proposed"
    assert observation["record"]["mode"] == "observe"
    assert result.classification_aggregates == (
        {
            "entity_id": 900,
            "entity_type": "Company",
            "membership": "missing",
            "status": "consistent_proposal",
            "proposed_topics": ["Finance"],
            "operational_topic": "Work",
            "observation_count": 1,
        },
    )
    assert observation["aggregation"] == result.classification_aggregates[0]


@pytest.mark.no_network
async def test_single_topic_type_skips_classification_call():
    jev = TopicJev(choice="topic_1")

    result = await resolve(Store(), jev, policy(topics=("Work",)))

    assert result.project_classifications[900].topic == "Work"
    assert result.classification_decisions == ()
    assert jev.calls == []


@pytest.mark.no_network
async def test_existing_project_classification_is_authoritative_and_not_observed():
    jev = TopicJev()

    result = await resolve(
        Store([entity(project_id="project-1", topic="Finance")]),
        jev,
        policy(),
    )

    assert result.entity_ids == (701,)
    assert result.project_classifications[701].topic == "Finance"
    assert result.project_classifications[701].membership == "existing"
    assert result.classification_decisions == ()
    assert jev.calls == []


@pytest.mark.no_network
async def test_foreign_classification_does_not_supply_current_project_topic():
    jev = TopicJev()

    result = await resolve(
        Store([entity(project_id="project-2", topic="Finance")]),
        jev,
        policy(),
    )

    assert result.entity_ids == (900,)
    assert result.project_classifications[900].topic == "Work"
    assert result.project_classifications[900].membership == "missing"
    assert result.classification_decisions[0]["suggested_topic"] == "Finance"


@pytest.mark.no_network
async def test_truncated_topic_options_are_indeterminate():
    jev = TopicJev(choice="topic_1")

    result = await resolve(
        Store(),
        jev,
        policy(topics=("Work", "Finance", "Legal"), max_options=2),
    )

    observation = result.classification_decisions[0]
    assert observation["option_set_truncated"] is True
    assert observation["suggested_topic"] is None
    assert observation["record"]["acceptance_status"] == "indeterminate"
    assert result.project_classifications[900].topic == "Work"


@pytest.mark.no_network
async def test_default_topic_answer_is_recorded_without_an_override_proposal():
    result = await resolve(Store(), TopicJev(choice="topic_1"), policy())

    observation = result.classification_decisions[0]
    assert observation["baseline_topic"] == "Work"
    assert observation["suggested_topic"] is None
    assert observation["record"]["result"]["response"]["answers"][
        "topic_choice"
    ]["choice"] == "topic_1"
    assert result.classification_aggregates[0]["status"] == "no_proposal"
    assert result.classification_aggregates[0]["proposed_topics"] == []


@pytest.mark.no_network
async def test_active_classification_mode_keeps_default_until_a_policy_is_defined():
    jev = TopicJev()

    result = await resolve(Store(), jev, policy("active"))

    assert result.project_classifications[900].topic == "Work"
    assert result.classification_decisions == ()
    assert jev.calls == []


@pytest.mark.no_network
async def test_active_override_policy_accepts_one_supported_non_default_topic():
    result = await resolve(
        Store(),
        TopicJev(choice="topic_2"),
        policy("active", acceptance_policy="override-positive-v1"),
    )

    observation = result.classification_decisions[0]
    assert result.project_classifications[900].topic == "Finance"
    assert result.pending_entity_writes[900].topic == "Finance"
    assert observation["baseline_topic"] == "Work"
    assert observation["suggested_topic"] == "Finance"
    assert observation["accepted_topic"] == "Finance"
    assert observation["record"]["acceptance_status"] == "accepted"
    assert result.classification_aggregates[0]["operational_topic"] == "Finance"


@pytest.mark.no_network
async def test_active_override_policy_keeps_default_for_default_answer():
    result = await resolve(
        Store(),
        TopicJev(choice="topic_1"),
        policy("active", acceptance_policy="override-positive-v1"),
    )

    observation = result.classification_decisions[0]
    assert result.project_classifications[900].topic == "Work"
    assert result.pending_entity_writes[900].topic == "Work"
    assert observation["suggested_topic"] is None
    assert observation["accepted_topic"] is None
    assert observation["record"]["acceptance_status"] == "rejected"


@pytest.mark.no_network
async def test_classification_provider_failure_keeps_default_and_records_unavailable():
    class FailingJev:
        async def evaluate(self, **_kwargs):
            raise RuntimeError("provider failed")

    result = await resolve(Store(), FailingJev(), policy())

    assert result.project_classifications[900].topic == "Work"
    observation = result.classification_decisions[0]
    assert observation["suggested_topic"] is None
    assert observation["record"]["acceptance_status"] == "unavailable"
    assert observation["record"]["result"]["reason"] == "provider_error"


@pytest.mark.no_network
async def test_classification_cancellation_propagates():
    class CancellingJev:
        async def evaluate(self, **_kwargs):
            raise asyncio.CancelledError

    with pytest.raises(asyncio.CancelledError):
        await resolve(Store(), CancellingJev(), policy())


@pytest.mark.no_network
async def test_repeated_entity_proposals_are_aggregated_after_identity_resolution():
    result = await resolve_repeated(TopicJev(), policy())

    assert result.entity_ids == (900,)
    assert len(result.classification_decisions) == 2
    assert result.classification_aggregates == (
        {
            "entity_id": 900,
            "entity_type": "Company",
            "membership": "missing",
            "status": "consistent_proposal",
            "proposed_topics": ["Finance"],
            "operational_topic": "Work",
            "observation_count": 2,
        },
    )
    assert result.project_classifications[900].topic == "Work"
    assert result.pending_entity_writes[900].topic == "Work"


@pytest.mark.no_network
async def test_conflicting_non_default_proposals_keep_default_and_record_conflict():
    result = await resolve_repeated(
        TopicJev(choice=["topic_2", "topic_3"]),
        policy(topics=("Work", "Finance", "Legal")),
    )

    aggregate = result.classification_aggregates[0]
    assert aggregate["status"] == "conflicting_proposals"
    assert aggregate["proposed_topics"] == ["Finance", "Legal"]
    assert aggregate["operational_topic"] == "Work"
    assert result.project_classifications[900].topic == "Work"
    assert result.pending_entity_writes[900].topic == "Work"
    assert {
        item["record"]["acceptance_status"]
        for item in result.classification_decisions
    } == {"conflicting"}


@pytest.mark.no_network
async def test_active_conflicting_non_default_proposals_keep_default():
    result = await resolve_repeated(
        TopicJev(choice=["topic_2", "topic_3"]),
        policy(
            "active",
            topics=("Work", "Finance", "Legal"),
            acceptance_policy="override-positive-v1",
        ),
    )

    assert result.project_classifications[900].topic == "Work"
    assert result.pending_entity_writes[900].topic == "Work"
    assert result.classification_aggregates[0]["status"] == "conflicting_proposals"
    assert {
        item["record"]["acceptance_status"]
        for item in result.classification_decisions
    } == {"conflicting"}
