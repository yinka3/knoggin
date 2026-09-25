from core.agent.notebook import NotebookCapacity, RunNotebook
from core.agent.run import AgentRunLimits


def test_notebook_deduplicates_entities_and_creates_reference_pages_and_hints():
    notebook = RunNotebook(limits=AgentRunLimits(max_accumulated_profiles=2))

    first = notebook.apply(
        "search_knowledge_entities",
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
    assert notebook.section_items("entities") == (
        {"id": 24, "canonical_name": "Sarah J."},
        {"id": 25, "canonical_name": "Grace Hopper"},
    )
    assert notebook.entity_pages["entity:25"] == {
        "entity_ref": "entity:25",
        "relationship_refs": [],
        "episode_refs": [],
        "evidence_refs": [],
    }
    assert [hint["tool"] for hint in notebook.possible_next_steps] == [
        "get_entity_relationships",
        "search_episodes",
        "get_entity_relationships",
        "search_episodes",
    ]
    assert notebook.as_dict()["knowledge"]["entities"]["entity:25"]["id"] == 25


def test_notebook_public_views_cannot_mutate_canonical_state():
    notebook = RunNotebook()
    notebook.apply("search_knowledge_entities", {"data": [{"id": 25, "canonical_name": "Grace"}]})
    notebook.apply("edit_brain", {"data": {"success": True}})

    pages = notebook.entity_pages
    actions = notebook.actions
    pages["entity:25"]["relationship_refs"].append("relationship:fake")
    actions[0]["result"]["success"] = False

    assert notebook.entity_pages["entity:25"]["relationship_refs"] == []
    assert notebook.actions[0]["result"]["success"] is True


def test_notebook_shares_relationship_evidence_across_retrieval_surfaces():
    notebook = RunNotebook()
    message = {
        "id": "msg_7",
        "project_id": "project-a",
        "session_id": "session-a",
        "message": "Sarah joined Acme.",
        "role": "user",
    }

    notebook.apply("search_knowledge_messages", {"data": [message]})
    notebook.apply(
        "get_entity_relationships",
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

    assert len(notebook.section_items("messages")) == 1
    relationship = notebook.section_items("relationships")[0]
    assert relationship["evidence_refs"] == ["message:project-a:session-a:msg_7"]
    assert notebook.entity_pages["entity:24"]["relationship_refs"] == [
        "relationship:relationship-1"
    ]
    assert notebook.entity_pages["entity:24"]["evidence_refs"] == [
        "message:project-a:session-a:msg_7"
    ]
    assert notebook.section_items("messages")[0]["message"] == "Sarah joined Acme."


def test_notebook_retains_only_complete_activity_and_its_entity_dependency():
    notebook = RunNotebook()

    admission = notebook.apply(
        "get_entity_recent_activity",
        {
            "data": [
                {"error": "unavailable", "entity_id": 1, "time": 1},
                {"entity_id": 2},
                {"time": 3},
                {
                    "entity_id": 24,
                    "entity": "Sarah Johnson",
                    "time": "2026-09-21T12:00:00Z",
                    "content": "Sarah approved the release.",
                },
            ]
        },
    )

    assert len(admission.references) == 1
    assert admission.references[0].startswith("activity:")
    assert notebook.section_items("activities") == (
        {
            "entity_id": 24,
            "entity": "Sarah Johnson",
            "time": "2026-09-21T12:00:00Z",
            "content": "Sarah approved the release.",
        },
    )
    assert notebook.entity_pages["entity:24"]["entity_ref"] == "entity:24"

    notebook._retain_references(list(admission.references), recent_contributions=0)

    assert "entity:24" in notebook.entity_pages
    assert len(notebook.section_items("activities")) == 1


def test_notebook_evidence_text_requires_visible_content():
    notebook = RunNotebook()

    assert notebook._record_has_text({"context": [{"content": "supported"}]}) is True
    assert notebook._record_has_text({"context": [{"content": "  "}, None]}) is False
    assert notebook._observation_support_has_text(
        {"nodes": [{"excerpt": "source passage"}]}
    ) is True
    assert notebook._observation_support_has_text(
        {"nodes": [{"excerpt": "  "}, {"label": "placeholder"}]}
    ) is False


def test_notebook_accepts_episode_groups_fallback_messages_and_document_ranges():
    notebook = RunNotebook(
        limits=AgentRunLimits(
            max_accumulated_episodes=2,
            max_accumulated_messages=5,
            max_accumulated_documents=3,
        )
    )
    notebook.apply(
        "search_episodes",
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
        "search_episodes",
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

    assert [item["episode_id"] for item in notebook.section_items("episodes")] == [
        "ep-1"
    ]
    assert notebook.section_items("episodes")[0]["resolution"] == "semantic"
    assert [item["id"] for item in notebook.section_items("messages")] == ["msg_8"]
    assert [
        item["chunk_index"] for item in notebook.section_items("documents")
    ] == [1, 2]
    assert notebook.entity_pages["entity:24"]["episode_refs"] == ["episode:ep-1"]


def test_notebook_compiles_independent_evidence_and_render_capacities():
    capacity = NotebookCapacity.from_limits(
        AgentRunLimits(
            max_accumulated_paths=9,
            max_accumulated_messages=2,
            max_accumulated_documents=3,
            max_accumulated_web_discoveries=4,
            max_accumulated_web_reads=5,
            max_accumulated_actions=6,
            max_accumulated_next_steps=7,
            max_accumulated_summary_chars=800,
            max_notebook_render_tokens=900,
        )
    )

    assert capacity.max_messages == 2
    assert capacity.max_activities == 2
    assert capacity.max_paths == 9
    assert capacity.max_observation_supports == 9
    assert capacity.max_documents == 3
    assert capacity.max_web_discoveries == 4
    assert capacity.max_web_reads == 5
    assert capacity.max_actions == 6
    assert capacity.max_next_steps == 7
    assert capacity.max_summary_chars == 800
    assert capacity.max_render_tokens == 900


def test_notebook_source_application_is_atomic_when_capacity_is_exceeded():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_web_discoveries=1, max_render_tokens=1000)
    )
    notebook.apply(
        "web_search",
        {"data": [{"url": "https://example.com/one"}]},
    )
    before = notebook.as_dict()

    rejected = notebook.apply(
        "web_search",
        {
            "data": [
                {"url": "https://example.com/two"},
                {"url": "https://example.com/three"},
            ]
        },
    )

    assert rejected.accepted is False
    assert rejected.reason == "capacity"
    assert notebook.as_dict() == before


def test_notebook_capacity_rejects_an_oversized_result_atomically():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=1, max_render_tokens=1000)
    )

    first = notebook.apply(
        "search_knowledge_messages",
        {"data": [{"id": "m1", "message": "first"}]},
    )
    before = notebook.as_dict()

    rejected = notebook.apply(
        "search_knowledge_messages",
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
        "get_entity_relationships",
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
        "search_knowledge_messages",
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
        "search_episodes",
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
        "find_relationship_path",
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


def test_rollover_keeps_only_the_immediately_previous_page():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=10, max_render_tokens=1000)
    )
    notebook.apply(
        "search_knowledge_messages",
        {"data": [{"id": "m1", "message": "first page evidence"}]},
    )

    notebook.rollover()
    first_previous = notebook.previous_page

    assert first_previous is not None
    assert first_previous.generation == 1
    assert first_previous.previous_page is None
    assert notebook.show_previous_page is False

    notebook.apply(
        "search_knowledge_messages",
        {"data": [{"id": "m2", "message": "second page evidence"}]},
    )
    notebook.rollover()
    second_previous = notebook.previous_page

    assert second_previous is not None
    assert second_previous.generation == 2
    assert second_previous.previous_page is None


def test_token_overflow_automatically_retains_the_completed_page():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=10, max_render_tokens=4),
        token_counter=lambda rendered: rendered.count("[payload]"),
    )
    for index in range(4):
        assert notebook.apply(
            "search_knowledge_messages",
            {"data": [{"id": f"m{index}", "message": f"[payload] {index}"}]},
        ).accepted

    admission = notebook.apply(
        "search_knowledge_messages",
        {"data": [{"id": "m4", "message": "[payload] 4"}]},
    )
    previous = notebook.previous_page

    assert admission.accepted is True
    assert admission.reason == "rolled_over:2"
    assert notebook.generation == 2
    assert previous is not None
    assert previous.generation == 1
    assert len(previous.section_items("messages")) == 4
    assert notebook.show_previous_page is False


def test_rollover_discards_older_inactive_contributions():
    notebook = RunNotebook(
        capacity=NotebookCapacity(max_messages=10, max_render_tokens=1000)
    )
    for index in range(4):
        assert notebook.apply(
            "search_knowledge_messages",
            {"data": [{"id": f"m{index}", "message": f"message {index}"}]},
        ).accepted

    notebook.rollover()

    retained_ids = {item["id"] for item in notebook.section_items("messages")}
    assert retained_ids == {"m1", "m2", "m3"}
