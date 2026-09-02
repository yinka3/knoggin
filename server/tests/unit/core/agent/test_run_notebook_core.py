from core.agent.notebook import RunNotebook
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
    assert [item["id"] for item in notebook.messages] == ["msg_8"]
    assert [item["chunk_index"] for item in notebook.documents] == [1, 2]
    assert notebook.entity_pages["entity:24"]["episode_refs"] == ["episode:ep-1"]
