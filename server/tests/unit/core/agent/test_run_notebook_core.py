from core.agent.notebook import NotebookCapacity, RunNotebook
from core.agent.run import AgentRunLimits


def test_notebook_deduplicates_entities_and_creates_reference_pages_and_hints():
    notebook = RunNotebook(limits=AgentRunLimits(max_accumulated_profiles=2))

    first = notebook.apply(
        "search_entity",
        {
            "data": [
                {"id": 24, "canonical_name": "Sarah Johnson"},
                {"id": 24, "canonical_name": "Sarah J."},
                {"id": 25, "canonical_name": "Grace Hopper"},
            ]
        },
    )

    assert first.changed is True
    assert first.references == ("entity:24", "entity:25")
    assert list(notebook.entities) == [
        {"id": 24, "canonical_name": "Sarah J."},
        {"id": 25, "canonical_name": "Grace Hopper"},
    ]
    assert notebook.entity_pages["entity:25"] == {
        "entity_ref": "entity:25",
        "relationship_refs": [],
        "episode_refs": [],
        "evidence_refs": [],
    }
    assert [hint["tool"] for hint in notebook.possible_next_steps] == [
        "get_connections",
        "episode_check",
        "get_connections",
        "episode_check",
    ]
    assert notebook.as_dict()["knowledge"]["entities"]["entity:25"]["id"] == 25


def test_notebook_shares_relationship_evidence_across_retrieval_surfaces():
    notebook = RunNotebook()
    message = {
        "id": "msg_7",
        "project_id": "project-a",
        "session_id": "session-a",
        "message": "Sarah joined Acme.",
        "role": "user",
    }

    notebook.apply("search_messages", {"data": [message]})
    notebook.apply(
        "get_connections",
        {
            "data": [
                {
                    "relationship_id": "relationship-1",
                    "source_entity_id": 24,
                    "target_entity_id": 25,
                    "relationship_type": "works_at",
                    "evidence": [message],
                }
            ]
        },
    )

    assert len(notebook.messages) == 1
    relationship = notebook.relationships[0]
    assert relationship["evidence_refs"] == ["message:project-a:session-a:msg_7"]
    assert notebook.entity_pages["entity:24"]["relationship_refs"] == [
        "relationship:relationship-1"
    ]
    assert notebook.entity_pages["entity:24"]["evidence_refs"] == [
        "message:project-a:session-a:msg_7"
    ]
    assert notebook.model_view()["graph"][0]["evidence"][0]["message"] == (
        "Sarah joined Acme."
    )


def test_notebook_accepts_episode_groups_fallback_messages_and_document_ranges():
    notebook = RunNotebook(
        limits=AgentRunLimits(
            max_accumulated_episodes=2,
            max_accumulated_messages=5,
            max_accumulated_sources=3,
        )
    )
    notebook.apply(
        "episode_check",
        {
            "data": {
                "resolution": "semantic",
                "results": [
                    {
                        "query": "career",
                        "episodes": [
                            {
                                "episode_id": "ep-1",
                                "summary": "Career changed",
                                "entities": [{"entity_id": 24}],
                            }
                        ],
                    }
                ],
            }
        },
    )
    notebook.apply(
        "episode_check",
        {"data": [{"id": "msg_8", "message": "No episode was stored."}]},
    )
    notebook.apply(
        "search_documents",
        {
            "data": [
                {"document_id": "doc-1", "chunk_index": 1, "content": "one"},
                {"document_id": "doc-1", "chunk_index": 1, "content": "duplicate"},
                {"document_id": "doc-1", "chunk_index": 2, "content": "two"},
            ]
        },
    )

    assert [item["episode_id"] for item in notebook.episodes] == ["ep-1"]
    assert notebook.model_view()["episodes"][0]["resolution"] == "semantic"
    assert [item["id"] for item in notebook.messages] == ["msg_8"]
    assert [item["chunk_index"] for item in notebook.documents] == [1, 2]
    assert notebook.entity_pages["entity:24"]["episode_refs"] == ["episode:ep-1"]


def test_notebook_capacity_rejects_an_oversized_result_atomically():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=1, max_render_tokens=1000)
    )

    first = notebook.apply(
        "search_messages",
        {"data": [{"id": "m1", "message": "first"}]},
    )
    before = notebook.as_dict()

    rejected = notebook.apply(
        "search_messages",
        {"data": [{"id": "m2", "message": "second"}]},
    )

    assert first.accepted is True
    assert rejected.accepted is False
    assert rejected.reason == "capacity"
    assert notebook.as_dict() == before
    assert all(
        item.get("id") != "m2"
        for item in notebook.as_dict()["evidence"]["messages"].values()
    )


def test_notebook_rollover_keeps_dependencies_and_resolvable_summary_refs():
    notebook = RunNotebook(
        capacity=NotebookCapacity(
            max_messages=4,
            max_entities=4,
            max_relationships=4,
            max_render_tokens=1000,
        )
    )
    notebook.apply(
        "get_connections",
        {
            "data": [
                {
                    "relationship_id": "r1",
                    "source_entity_id": 1,
                    "target_entity_id": 2,
                    "evidence": [{"id": "m1", "message": "linked"}],
                }
            ]
        },
    )

    result = notebook.rollover("Retained relationship context")
    snapshot = notebook.as_dict()

    assert result.generation == 2
    assert "relationship:r1" in result.retained_references
    assert "message:::m1" in result.retained_references
    assert snapshot["summary"]["references"]
    for reference in snapshot["summary"]["references"]:
        assert any(
            reference in section
            for section in (
                snapshot["knowledge"]["entities"],
                snapshot["knowledge"]["relationships"],
                snapshot["evidence"]["messages"],
            )
        )
    assert "entity:1" in snapshot["entity_pages"]
    assert "entity:2" in snapshot["entity_pages"]
    assert "relationship:r1" in snapshot["knowledge"]["relationships"]


def test_notebook_hard_token_rail_is_measured_with_injected_counter():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=10, max_render_tokens=5),
        token_counter=lambda rendered: len(rendered.split()),
    )
    before = notebook.as_dict()

    result = notebook.apply(
        "search_messages",
        {"data": [{"id": "m1", "message": "one two three four five"}]},
    )

    assert result.accepted is False
    assert result.reason == "capacity"
    assert notebook.as_dict() == before


def test_repeated_rollover_retains_episode_and_path_neighborhood():
    notebook = RunNotebook(
        capacity=NotebookCapacity(
            max_messages=8,
            max_entities=8,
            max_episodes=4,
            max_paths=4,
            max_render_tokens=1000,
        )
    )
    notebook.apply(
        "episode_check",
        {
            "data": {
                "results": [
                    {
                        "episodes": [
                            {
                                "episode_id": "ep-1",
                                "summary": "A decision",
                                "entities": [{"entity_id": 1}],
                                "evidence": [{"id": "m1", "message": "decision"}],
                            }
                        ]
                    }
                ]
            }
        },
    )
    notebook.apply(
        "find_path",
        {
            "data": [
                {
                    "path_id": "path-1",
                    "entity_a_id": 1,
                    "entity_b_id": 2,
                    "evidence": [{"id": "m2", "message": "path evidence"}],
                }
            ]
        },
    )

    first = notebook.rollover()
    second = notebook.rollover("Still relevant")
    snapshot = notebook.as_dict()

    assert first.generation == 2
    assert second.generation == 3
    assert "episode:ep-1" in second.retained_references
    assert "path:path-1" in second.retained_references
    assert "message:::m1" in second.retained_references
    assert "message:::m2" in second.retained_references
    assert snapshot["summary"]["references"]
    assert "episode:ep-1" in snapshot["knowledge"]["episodes"]
    assert "path:path-1" in snapshot["knowledge"]["paths"]


def test_rollover_discards_older_inactive_contributions():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=10, max_render_tokens=1000)
    )
    for index in range(4):
        assert notebook.apply(
            "search_messages",
            {"data": [{"id": f"m{index}", "message": f"message {index}"}]},
        ).accepted

    notebook.rollover()

    retained_ids = {item["id"] for item in notebook.messages}
    assert retained_ids == {"m1", "m2", "m3"}
