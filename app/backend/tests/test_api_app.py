from knoggin import DocumentFocusSubtree
from knoggin_app_api.contracts import MessageCreateRequest, document_focus_to_sdk
from knoggin_app_api.main import create_app


def test_fastapi_app_exposes_the_first_public_api_slice():
    app = create_app()
    paths = {route.path for route in app.routes}

    assert {
        "/api/v1/health",
        "/api/v1/projects",
        "/api/v1/projects/{project_id}/sessions",
        "/api/v1/sessions/{session_id}/messages",
        "/api/v1/runs/{run_id}",
        "/api/v1/runs/{run_id}/events",
    }.issubset(paths)


def test_message_request_accepts_a_structured_browser_document_focus():
    request = MessageCreateRequest.model_validate(
        {
            "content": "Compare the selected files",
            "documentFocus": {
                "targetType": "subtree",
                "folderRootId": "folder-1",
                "pathPrefix": "design/",
            },
        }
    )

    focus = document_focus_to_sdk(request.document_focus)
    assert focus == DocumentFocusSubtree(
        folder_root_id="folder-1",
        path_prefix="design/",
    )
