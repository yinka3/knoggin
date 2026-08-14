from datetime import datetime, timezone

from knoggin_app_api.projection import event_response, project_response
from knoggin_app_api.runs import RunEvent


def _event(event: str, data: dict) -> RunEvent:
    return RunEvent(
        run_id="run-1",
        session_id="session-1",
        event=event,
        sequence=3,
        timestamp=datetime(2026, 8, 14, tzinfo=timezone.utc),
        data=data,
    )


def test_ui_projection_hides_sdk_reasoning():
    assert event_response(_event("thinking", {"content": "local reasoning"})) is None


def test_project_projection_serializes_engine_timestamps_for_json():
    projected = project_response(
        {
            "id": "project-1",
            "name": "Research",
            "created_at": datetime(2026, 8, 14, tzinfo=timezone.utc),
            "updated_at": datetime(2026, 8, 14, 1, tzinfo=timezone.utc),
        }
    )

    assert projected["createdAt"] == "2026-08-14T00:00:00+00:00"
    assert projected["updatedAt"] == "2026-08-14T01:00:00+00:00"


def test_ui_projection_allowlists_tool_arguments():
    projected = event_response(
        _event(
            "tool_start",
            {
                "call_id": "call-1",
                "tool": "search",
                "args": {"query": "Knoggin", "content": "local SDK input"},
            },
        )
    )

    assert projected["type"] == "tool.started"
    assert projected["data"]["arguments"] == {"query": "Knoggin"}


def test_ui_projection_strips_source_metadata_from_completed_runs():
    projected = event_response(
        _event(
            "response",
            {
                "content": "Durable answer",
                "assistant_message_id": 42,
                "source_ref_ids": ["source-ref-1"],
                "sources_consulted": [
                    {
                        "source_kind": "text_document",
                        "excerpt": "Knoggin keeps durable context.",
                        "document_id": "document-1",
                        "locator": {"kind": "text_line", "start_line": 1},
                        "metadata": {
                            "document_name": "Project notes",
                            "local": "not public",
                        },
                    }
                ],
            },
        )
    )

    assert projected["data"]["citations"] == [
        {
            "id": "source-ref-1",
            "kind": "text_document",
            "label": "Project notes",
            "excerpt": "Knoggin keeps durable context.",
            "documentId": "document-1",
            "locator": {"kind": "text_line", "start_line": 1},
        }
    ]
