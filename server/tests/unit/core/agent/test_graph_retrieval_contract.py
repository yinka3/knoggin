from types import SimpleNamespace

import pytest

from common.schema.evidence import EvidenceBundle
from core.knowledge.retrieval import KnowledgeRetrieval


def _retrieval(store, *, readable_project_ids=None):
    class Entities:
        async def get_profile(self, entity_id):
            if entity_id in {2, 3}:
                return SimpleNamespace(canonical_name={2: "Ade", 3: "Acme"}[entity_id])
            return None

    return KnowledgeRetrieval(
        project_id="project-1",
        readable_project_ids=readable_project_ids or ["project-1"],
        user_name="ada",
        entities=Entities(),
        embedding_service=SimpleNamespace(),
        knowledge_store=store,
        search_config={"default_activity_hours": 72},
    )


@pytest.mark.no_network
async def test_recent_activity_uses_stable_id_and_message_evidence():
    class Store:
        def __init__(self):
            self.calls = []

        async def get_recent_activity(self, entity_id, **kwargs):
            self.calls.append((entity_id, kwargs))
            return [
                {
                    "entity_id": 2,
                    "entity": "Ade",
                    "project_id": "project-1",
                    "evidence_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 7,
                        }
                    ],
                    "time": 123,
                }
            ]

        async def get_messages_by_ids(self, message_ids, **kwargs):
            assert message_ids == [7]
            assert kwargs["visible_project_ids"] == ["project-1"]
            return [
                {
                    "id": 7,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "content": "Ade discussed Acme",
                    "timestamp": 1_700_000_000_000,
                }
            ]

    store = Store()
    result = await _retrieval(store).get_recent_activity(2, session_id="session-1")

    assert store.calls == [
        (
            2,
            {
                "hours": 72,
                "visible_project_ids": ["project-1"],
            },
        )
    ]
    assert result[0]["evidence"][0]["id"] == "msg_7"


@pytest.mark.no_network
async def test_connections_keep_stored_direction_when_selected_from_target():
    class Store:
        async def get_related_entities(self, entity_ids, **kwargs):
            assert entity_ids == [3]
            assert kwargs == {"limit": 40, "visible_project_ids": ["project-1"]}
            return [
                {
                    "project_id": "project-1",
                    "relationship_id": "project-1:2:3:works_at",
                    "source_entity_id": 2,
                    "target_entity_id": 3,
                    "source": "Ade",
                    "target": "Acme",
                    "relationship_type": "works_at",
                    "symmetric": False,
                    "evidence_refs": [],
                }
            ]

    result = await _retrieval(Store()).get_connections(3, session_id="session-1")

    assert result == [
        {
            "project_id": "project-1",
            "relationship_id": "project-1:2:3:works_at",
            "source_entity_id": 2,
            "target_entity_id": 3,
            "source": "Ade",
            "target": "Acme",
            "relationship_type": "works_at",
            "symmetric": False,
            "evidence": [],
        }
    ]


@pytest.mark.no_network
async def test_result_hydration_batches_messages_without_mutating_store_results():
    class Store:
        def __init__(self):
            self.message_calls = []
            self.results = [
                {
                    "relationship_id": "r1",
                    "evidence_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 7,
                        }
                    ],
                },
                {
                    "relationship_id": "r2",
                    "evidence_refs": [
                        {
                            "user_name": "ada",
                            "session_id": "session-1",
                            "message_id": 8,
                        }
                    ],
                },
            ]

        async def get_related_entities(self, _entity_ids, **_kwargs):
            return self.results

        async def get_messages_by_ids(self, message_ids, **kwargs):
            self.message_calls.append((message_ids, kwargs))
            return [
                {
                    "id": message_id,
                    "user_name": "ada",
                    "session_id": "session-1",
                    "content": f"evidence-{message_id}",
                }
                for message_id in message_ids
            ]

    store = Store()
    result = await _retrieval(store).get_connections(3, session_id="session-1")

    assert len(store.message_calls) == 1
    assert store.message_calls[0][0] == [7, 8]
    assert [item["evidence"][0]["id"] for item in result] == ["msg_7", "msg_8"]
    assert all("evidence_refs" in item for item in store.results)


@pytest.mark.no_network
async def test_failed_result_hydration_leaves_store_results_unchanged():
    original = {
        "relationship_id": "r1",
        "evidence_refs": [
            {
                "user_name": "ada",
                "session_id": "session-1",
                "message_id": 7,
            }
        ],
    }

    class Store:
        async def get_messages_by_ids(self, _message_ids, **_kwargs):
            raise RuntimeError("storage unavailable")

    with pytest.raises(RuntimeError, match="storage unavailable"):
        await _retrieval(Store())._hydrate_result_evidence(
            [original], session_id="session-1"
        )

    assert original["evidence_refs"][0]["message_id"] == 7
    assert "evidence" not in original


@pytest.mark.no_network
async def test_message_evidence_rejects_a_different_user_scope():
    class Store:
        async def get_messages_by_ids(self, _message_ids, **_kwargs):
            raise AssertionError("mismatched user refs must fail before storage")

    with pytest.raises(ValueError, match="outside retrieval user scope"):
        await _retrieval(Store())._hydrate_evidence(
            [
                {
                    "user_name": "grace",
                    "session_id": "session-1",
                    "message_id": 7,
                }
            ],
            session_id="session-1",
        )


@pytest.mark.no_network
async def test_path_returns_canonical_direction_and_project_attribution():
    class Store:
        def __init__(self):
            self.evidence_calls = []

        async def find_path(self, entity_a_id, entity_b_id, **kwargs):
            assert (entity_a_id, entity_b_id) == (3, 2)
            assert kwargs == {"max_depth": 4, "visible_project_ids": ["project-1"]}
            return [
                {
                    "step": 0,
                    "entity_a_id": 3,
                    "entity_b_id": 2,
                    "relationship_id": "project-1:2:3:works_at",
                    "project_id": "project-1",
                    "source_entity_id": 2,
                    "target_entity_id": 3,
                    "source": "Ade",
                    "target": "Acme",
                    "relationship_type": "works_at",
                    "symmetric": False,
                    "relationship_semantics": "observed_evidence",
                    "evidence_refs": [
                        {
                            "kind": "relationship_observation",
                            "project_id": "project-1",
                            "user_name": "ada",
                            "observation_id": 17,
                        },
                        {
                            "kind": "relationship_observation",
                            "project_id": "project-1",
                            "user_name": "ada",
                            "observation_id": 18,
                        },
                    ],
                }
            ]

        async def get_relationship_observations_evidence(
            self, observation_ids, **kwargs
        ):
            self.evidence_calls.append((observation_ids, kwargs))
            return (_observation_bundle(17), _missing_observation_bundle(18))

    store = Store()
    result = await _retrieval(store).find_relationship_path(
        3, 2, session_id="session-1"
    )

    assert result[0]["source"] == "Ade"
    assert result[0]["target"] == "Acme"
    assert result[0]["project_id"] == "project-1"
    assert store.evidence_calls == []
    evidence = result[0]["evidence"]
    assert evidence == [
        {
            "kind": "relationship_observation",
            "observation_id": 17,
            "project_id": "project-1",
        },
        {
            "kind": "relationship_observation",
            "observation_id": 18,
            "project_id": "project-1",
        },
    ]


@pytest.mark.no_network
async def test_observation_evidence_read_uses_a_bounded_readable_traversal():
    class Store:
        def __init__(self):
            self.calls = []

        async def get_visible_relationship_observation_evidence(
            self, observation_id, **kwargs
        ):
            self.calls.append((observation_id, kwargs))
            return _observation_bundle(observation_id)

    store = Store()

    result = await _retrieval(
        store,
        readable_project_ids=["project-1", "project-2"],
    ).read_observation_evidence(17)

    assert result["subject"] == {
        "kind": "relationship_observation",
        "identifier": "17",
    }
    assert len(store.calls) == 1
    observation_id, kwargs = store.calls[0]
    assert observation_id == 17
    assert kwargs["user_name"] == "ada"
    assert kwargs["visible_project_ids"] == ["project-1", "project-2"]
    limits = kwargs["limits"]
    assert (
        limits.max_observations,
        limits.max_context_blocks,
        limits.max_leaf_evidence,
        limits.max_edges,
    ) == (1, 4, 8, 16)


def _observation_bundle(observation_id: int) -> EvidenceBundle:
    block_id = "00000000-0000-0000-0000-000000000017"
    source_ref_id = "00000000-0000-0000-0000-000000000018"
    return EvidenceBundle.model_validate(
        {
            "subject": {
                "kind": "relationship_observation",
                "identifier": str(observation_id),
            },
            "nodes": [
                {
                    "pointer": {
                        "kind": "relationship_observation",
                        "identifier": str(observation_id),
                    },
                    "label": "works at",
                    "status": "active",
                },
                {
                    "pointer": {"kind": "context_block", "identifier": block_id},
                    "excerpt": "Ade joined Acme.",
                },
                {
                    "pointer": {"kind": "message", "identifier": "7"},
                    "role": "user",
                },
                {
                    "pointer": {
                        "kind": "source_reference",
                        "identifier": source_ref_id,
                    },
                    "source_kind": "text_document",
                    "content_hash": "a" * 64,
                    "locator": {"kind": "text_lines", "start_line": 1, "end_line": 1},
                    "excerpt": "Ade joined Acme.",
                },
            ],
            "edges": [
                {
                    "source": {"kind": "context_block", "identifier": block_id},
                    "target": {
                        "kind": "relationship_observation",
                        "identifier": str(observation_id),
                    },
                    "relation": "supports_relationship_observation",
                },
                {
                    "source": {"kind": "message", "identifier": "7"},
                    "target": {"kind": "context_block", "identifier": block_id},
                    "relation": "supports_context_block",
                },
                {
                    "source": {
                        "kind": "source_reference",
                        "identifier": source_ref_id,
                    },
                    "target": {"kind": "message", "identifier": "7"},
                    "relation": "source_owned_by_message",
                },
            ],
            "total_nodes": 4,
            "total_edges": 3,
            "nodes_truncated": False,
            "edges_truncated": False,
            "state_token": "a" * 64,
        }
    )


def _missing_observation_bundle(observation_id: int) -> EvidenceBundle:
    return EvidenceBundle.model_validate(
        {
            "subject": {
                "kind": "relationship_observation",
                "identifier": str(observation_id),
            },
            "nodes": [
                {
                    "pointer": {
                        "kind": "relationship_observation",
                        "identifier": str(observation_id),
                    },
                    "status": "missing",
                }
            ],
            "edges": [],
            "total_nodes": 1,
            "total_edges": 0,
            "nodes_truncated": False,
            "edges_truncated": False,
            "state_token": "b" * 64,
        }
    )
