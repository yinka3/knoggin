import asyncio
import json
from types import SimpleNamespace

from routes.sessions import history


def test_history_exposes_only_public_message_metadata():
    class Sessions:
        async def list_sessions(self):
            return {"session-1": {"session_id": "session-1"}}

        async def get_session_history_readonly(self, session_id, limit):
            assert (session_id, limit) == ("session-1", 100)
            return [{
                "message_id": 4,
                "role": "user",
                "content": "Focus on recovery",
                "timestamp": 1234,
                "user_msg_id": 4,
                "metadata": {
                    "idempotency_key": "private-request-key",
                    "request_fingerprint": "private-fingerprint",
                    "internal_future_field": {"secret": True},
                    "response_annotations": [{"message_id": 2, "start": 0, "end": 5,
                                               "quote": "Earlier", "private": "hidden"}],
                    "run_steer": {
                        "steer_id": "steer-1",
                        "instruction": "Check the retry path",
                        "state": "applied",
                        "internal": "hidden",
                    },
                },
            }, {
                "message_id": 5,
                "role": "assistant",
                "content": "The retry path is safe.",
                "timestamp": 1235,
                "user_msg_id": 4,
                "metadata": {
                    "usage": {
                        "prompt_tokens": 10,
                        "completion_tokens": 4,
                        "total_tokens": 14,
                        "approximate": False,
                        "provider_payload": "hidden",
                    },
                    "research_mode": "research",
                    "fallback": True,
                },
            }]

    request = SimpleNamespace(
        path_params={"session_id": "session-1"},
        query_params={},
        app=SimpleNamespace(state=SimpleNamespace(
            runtime=SimpleNamespace(sessions=Sessions())
        )),
    )
    response = asyncio.run(history(request))
    payload = json.loads(response.body)

    assert payload[0] == {
        "message_id": 4,
        "role": "user",
        "content": "Focus on recovery",
        "timestamp": 1234,
        "user_msg_id": 4,
        "metadata": {
            "response_annotations": [{"message_id": 2, "start": 0, "end": 5,
                                      "quote": "Earlier"}],
            "run_steer": {
                "steer_id": "steer-1",
                "instruction": "Check the retry path",
                "state": "applied",
            },
        },
    }
    assert payload[1]["metadata"] == {
        "usage": {
            "prompt_tokens": 10,
            "completion_tokens": 4,
            "total_tokens": 14,
            "approximate": False,
        },
        "research_mode": "research",
        "fallback": True,
    }
    assert "private-request-key" not in response.body.decode()
    assert "private-fingerprint" not in response.body.decode()
