import asyncio
import json
from types import SimpleNamespace

from routes.agents import replace_persona
from routes.sessions import message_sources
from routes.settings import get_spending


def _request(*, runtime, path=None, body=None):
    class Request:
        app = SimpleNamespace(state=SimpleNamespace(runtime=runtime))
        path_params = path or {}

        async def json(self):
            return body

    return Request()


def test_message_sources_checks_session_and_assistant_scope():
    calls = []

    class Sessions:
        user_name = "ada"

        async def list_sessions(self):
            return {"s1": {"project_id": "p1"}}

    class Store:
        async def get_messages_by_ids(self, ids, **scope):
            calls.append((ids, scope))
            return [{"role": "user"}]

        async def get_message_source_refs(self, *_args, **_scope):
            raise AssertionError("User messages cannot expose assistant sources")

    runtime = SimpleNamespace(
        sessions=Sessions(), resources=SimpleNamespace(knowledge_store=Store())
    )
    response = asyncio.run(message_sources(_request(
        runtime=runtime, path={"session_id": "s1", "message_id": "42"}
    )))
    assert response.status_code == 404
    assert calls == [([42], {
        "user_name": "ada", "session_ids": ["s1"], "visible_project_ids": ["p1"],
    })]


def test_message_sources_reloads_the_live_stream_source_shape():
    source = {
        "source_kind": "user_pasted_text",
        "locator": {"kind": "character_span", "start_char": 0, "end_char": 4},
        "excerpt": "text", "source_message_id": 1,
        "source_status": "available", "contributing_message_id": 42,
    }

    class Sessions:
        user_name = "ada"

        async def list_sessions(self):
            return {"s1": {"project_id": "p1"}}

    class Store:
        async def get_messages_by_ids(self, *_args, **_scope):
            return [{"role": "assistant"}]

        async def get_message_source_refs(self, message_id, **scope):
            assert (message_id, scope) == (42, {
                "user_name": "ada", "project_id": "p1", "session_id": "s1",
            })
            return [source]

    runtime = SimpleNamespace(
        sessions=Sessions(), resources=SimpleNamespace(knowledge_store=Store())
    )
    response = asyncio.run(message_sources(_request(
        runtime=runtime, path={"session_id": "s1", "message_id": "42"}
    )))
    assert response.status_code == 200
    payload = json.loads(response.body)
    assert payload["message_id"] == 42
    assert all(payload["sources"][0][key] == value for key, value in source.items())


def test_spending_returns_bounded_ledger_summary():
    class LLM:
        async def spending_snapshot(self):
            return {
                "configured_limit_usd": 10.0, "spent_usd": 2.0,
                "reserved_usd": 1.0, "remaining_usd": 7.0,
                "request_count": 3, "enforced": True, "internal_token": "hidden",
            }

    runtime = SimpleNamespace(resources=SimpleNamespace(llm_service=LLM()))
    response = asyncio.run(get_spending(_request(runtime=runtime)))
    assert response.status_code == 200
    assert json.loads(response.body)["remaining_usd"] == 7.0
    assert "hidden" not in response.body.decode()


def test_persona_replacement_uses_agent_owner():
    received = []
    persona = {
        "attention_bias": "evidence", "reasoning_style": "careful",
        "social_temperament": "friendly", "communication_signature": "clear",
        "productive_flaw": "cautious",
    }

    class AgentManager:
        async def update_persona(self, agent_id, profile):
            received.append((agent_id, profile))
            return SimpleNamespace(
                id=agent_id, name="Ada", persona=SimpleNamespace(to_dict=lambda: profile),
                model=None, temperature=0.7, enabled_tools=[], aac_enabled=False,
                is_default=False, created_at=SimpleNamespace(isoformat=lambda: "now"),
                last_turn_at=None,
            )

    runtime = SimpleNamespace(agent_manager=AgentManager())
    response = asyncio.run(replace_persona(_request(
        runtime=runtime, path={"agent_id": "a1"}, body=persona
    )))
    assert response.status_code == 200
    assert received == [("a1", persona)]
    assert json.loads(response.body)["persona"] == persona
