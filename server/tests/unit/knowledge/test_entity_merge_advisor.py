"""The merge model receives bounded evidence but cannot create a review itself."""

import json

import pytest

from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.entity.merge_advisor import EntityMergeAdvice
from core.knowledge.maintenance.maintenance_reviews import EntityMergePlan
from tests.fixtures.fakes import RecordingPostgresClient


def _preview(*, with_evidence=True):
    return {
        "ready": True,
        "state_hash": "a" * 64,
        "context_conflicts": [],
        "plan": EntityMergePlan(survivor_entity_id=2, retired_entity_id=3),
        "snapshot": {
            "entities": [
                {"entity_id": 2, "canonical_name": "Ada Lovelace"},
                {"entity_id": 3, "canonical_name": "Augusta Ada King"},
            ],
            "aliases": [
                {"entity_id": 2, "alias": "Ada"},
                {"entity_id": 3, "alias": "Augusta"},
            ],
            "contexts": [
                {
                    "entity_id": 2,
                    "project_id": "project-1",
                    "entity_type": "Person",
                    "topic": "History",
                },
                {
                    "entity_id": 3,
                    "project_id": "project-1",
                    "entity_type": "Person",
                    "topic": "History",
                },
            ],
            "message_refs": (
                [
                    {"message_id": 101, "entity_id": 2},
                    {"message_id": 102, "entity_id": 3},
                ]
                if with_evidence
                else []
            ),
            "episode_entities": (
                [{"episode_id": "episode-1", "project_id": "project-1", "entity_id": 3}]
                if with_evidence
                else []
            ),
        },
    }


class FakeMergeLLM:
    merge_model = "merge-model-test"

    def __init__(self, response=None, error=None):
        self.response = response
        self.error = error
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        if self.error is not None:
            raise self.error
        return self.response


def _service(client, llm, *, with_evidence=True):
    service = EntityMaintenanceService(client, "ada", llm=llm)
    preview_calls = []

    async def preview_merge(**kwargs):
        preview_calls.append(kwargs)
        return _preview(with_evidence=with_evidence)

    service.preview_merge = preview_merge

    class Reviews:
        def __init__(self):
            self.opened = []

        async def open(self, **kwargs):
            self.opened.append(kwargs)

    service.review_writer = Reviews()
    return service, preview_calls


@pytest.mark.unit
@pytest.mark.no_network
async def test_merge_advisor_uses_merge_model_with_scoped_bounded_evidence():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 101,
                    "project_id": "project-1",
                    "role": "user",
                    "content": "A" * 900,
                },
                {
                    "message_id": 102,
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Augusta is Ada.",
                },
            ],
            [
                {
                    "episode_id": "episode-1",
                    "project_id": "project-1",
                    "summary": "Same person.",
                }
            ],
        ]
    )
    llm = FakeMergeLLM(
        EntityMergeAdvice(
            verdict="merge",
            preferred_survivor_id=2,
            reasoning="The cited records identify the same person.",
            evidence_message_ids=[101, 102],
            evidence_episode_ids=["episode-1"],
        )
    )
    service, preview_calls = _service(client, llm)

    result = await service.advise_merge(survivor_entity_id=2, retired_entity_id=3)

    assert result["verdict"] == "merge"
    assert result["state_hash"] == "a" * 64
    assert result["model_called"] is True
    assert result["evidence_message_ids"] == [101, 102]
    assert service.review_writer.opened == []
    assert preview_calls[0]["user_name"] == "ada"
    assert len(llm.calls) == 1
    call = llm.calls[0]
    assert call["model"] == llm.merge_model
    assert call["response_model"] is EntityMergeAdvice
    payload = json.loads(call["user"])
    assert [entity["entity_id"] for entity in payload["entities"]] == [2, 3]
    assert [message["message_id"] for message in payload["messages"]] == [101, 102]
    assert len(payload["messages"][0]["text"]) == 800
    assert payload["episodes"][0]["summary"] == "Same person."
    assert "user_name = %s" in client.calls[0][1]
    assert client.calls[0][2][0] == "ada"
    assert "project.user_name = %s" in client.calls[1][1]


@pytest.mark.unit
@pytest.mark.no_network
async def test_merge_advisor_skips_model_when_candidate_has_no_cited_evidence():
    client = RecordingPostgresClient()
    llm = FakeMergeLLM()
    service, _ = _service(client, llm, with_evidence=False)

    result = await service.advise_merge(survivor_entity_id=2, retired_entity_id=3)

    assert result["verdict"] == "uncertain"
    assert result["model_called"] is False
    assert llm.calls == []
    assert client.calls == []
    assert service.review_writer.opened == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_merge_advisor_sends_at_most_two_recent_messages_per_entity():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": message_id,
                    "project_id": "project-1",
                    "role": "user",
                    "content": str(message_id),
                }
                for message_id in (6, 5, 3, 2)
            ]
        ]
    )
    llm = FakeMergeLLM(
        EntityMergeAdvice(verdict="uncertain", reasoning="The excerpts are ambiguous.")
    )
    service, _ = _service(client, llm)
    preview = _preview()
    preview["snapshot"]["message_refs"] = [
        {"message_id": message_id, "entity_id": entity_id}
        for entity_id, message_ids in ((2, (1, 2, 3)), (3, (4, 5, 6)))
        for message_id in message_ids
    ]
    preview["snapshot"]["episode_entities"] = []

    async def preview_merge(**_kwargs):
        return preview

    service.preview_merge = preview_merge

    await service.advise_merge(survivor_entity_id=2, retired_entity_id=3)

    assert client.calls[0][2] == ("ada", [2, 3, 5, 6])
    payload = json.loads(llm.calls[0]["user"])
    assert {row["message_id"] for row in payload["messages"]} == {2, 3, 5, 6}
    assert len(payload["messages"]) == 4


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.parametrize(
    "response, error",
    [
        (
            EntityMergeAdvice(
                verdict="merge",
                preferred_survivor_id=2,
                reasoning="Unseen message supports this.",
                evidence_message_ids=[999],
            ),
            "outside the supplied excerpts",
        ),
        (
            EntityMergeAdvice(
                verdict="merge",
                preferred_survivor_id=2,
                reasoning="These look alike.",
            ),
            "requires cited evidence",
        ),
        (
            EntityMergeAdvice(
                verdict="merge",
                preferred_survivor_id=4,
                reasoning="The cited message identifies both.",
                evidence_message_ids=[101],
            ),
            "outside the candidate pair",
        ),
        (
            EntityMergeAdvice(
                verdict="merge",
                preferred_survivor_id=2,
                reasoning="Only one entity is cited.",
                evidence_message_ids=[101],
            ),
            "both candidate entities",
        ),
    ],
)
async def test_merge_advisor_rejects_unsupported_model_output(response, error):
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 101,
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Ada.",
                },
                {
                    "message_id": 102,
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Augusta.",
                },
            ],
            [
                {
                    "episode_id": "episode-1",
                    "project_id": "project-1",
                    "summary": "History.",
                }
            ],
        ]
    )
    service, _ = _service(client, FakeMergeLLM(response))

    with pytest.raises(ValueError, match=error):
        await service.advise_merge(survivor_entity_id=2, retired_entity_id=3)

    assert service.review_writer.opened == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_merge_advisor_model_failure_creates_no_review():
    client = RecordingPostgresClient(
        fetch_all_results=[
            [
                {
                    "message_id": 101,
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Ada.",
                },
                {
                    "message_id": 102,
                    "project_id": "project-1",
                    "role": "user",
                    "content": "Augusta.",
                },
            ],
            [],
        ]
    )
    service, _ = _service(
        client, FakeMergeLLM(error=RuntimeError("provider unavailable"))
    )

    with pytest.raises(RuntimeError, match="provider unavailable"):
        await service.advise_merge(survivor_entity_id=2, retired_entity_id=3)

    assert service.review_writer.opened == []
