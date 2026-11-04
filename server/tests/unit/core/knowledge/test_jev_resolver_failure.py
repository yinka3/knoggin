"""Optional topic provider failure retains deterministic classification."""


import pytest

from common.schema.ingestion.contracts import EntityMention
from core.knowledge.entity.resolver import EntityResolver
from tests.unit.core.knowledge.test_jev_topic_observe import Store, policy, resolve


class BrokenJev:
    async def evaluate(self, **kwargs):
        raise RuntimeError("offline provider")


async def test_topic_provider_exception_retains_default_and_unavailable_provenance():
    result = await resolve(
        Store(), BrokenJev(), policy("active", acceptance_policy="override-positive-v1")
    )
    assert result.project_classifications[900].topic == "Work"
    record = result.classification_decisions[0]["record"]
    assert record["result"]["reason"] == "provider_error"
    assert record["acceptance_status"] == "unavailable"


async def test_topic_observation_requires_window_scope_before_provider_work():
    message_id = 1

    async def allocate():
        return 900

    with pytest.raises(TypeError, match="window ID"):
        await EntityResolver(
            Store(), "project-1", ["project-1"], jev_client=BrokenJev()
        ).resolve_mentions(
            [
                EntityMention(
                    message_ids=(message_id,),
                    name="Acme",
                    entity_type="Company",
                    topic="Work",
                    origin="vp01",
                )
            ],
            message_text_by_id={message_id: "Acme manages the budget."},
            policy=policy(),
            allocate_entity_id=allocate,
        )
