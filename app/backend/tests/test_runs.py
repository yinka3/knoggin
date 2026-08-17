import pytest

from knoggin import Turn
from knoggin_app_api.runs import RunManager


class _FakeKnoggin:
    def __init__(self):
        self.calls = []

    async def open_turn_stream(self, *, session_id, turn, idempotency_key=None):
        if session_id != "session-1":
            raise LookupError("session_not_found")
        self.calls.append((session_id, turn, idempotency_key))
        return self._events()

    async def _events(self):
        yield {"event": "thinking", "data": {"content": "internal reasoning"}}
        yield {"event": "token", "data": {"content": "Hello"}}
        yield {
            "event": "response",
            "data": {
                "content": "Durable answer",
                "assistant_message_id": 42,
                "source_ref_ids": ["source-ref-1"],
                "sources_consulted": [
                    {
                        "source_kind": "text_document",
                        "excerpt": "Knoggin keeps durable context.",
                        "document_id": "document-1",
                        "metadata": {"local": "SDK source metadata"},
                    }
                ],
            },
        }


@pytest.mark.asyncio
async def test_run_manager_owns_ui_run_events_and_snapshots():
    manager = RunManager(_FakeKnoggin())

    submitted = await manager.submit_turn(
        session_id="session-1",
        turn=Turn(content="What is Knoggin?"),
        idempotency_key="request-1",
    )
    events = [event async for event in manager.subscribe_events(submitted.run_id)]

    assert [event.event for event in events] == [
        "run_queued",
        "run_started",
        "thinking",
        "token",
        "response",
    ]
    completed = manager.get_run(submitted.run_id)
    assert completed.status == "completed"
    assert completed.result["assistant_message_id"] == 42
    assert completed.sources[0].source_ref_id == "source-ref-1"
    assert completed.sources[0].metadata == {"local": "SDK source metadata"}

    await manager.close()


@pytest.mark.asyncio
async def test_run_manager_keeps_idempotency_with_the_ui_api():
    manager = RunManager(_FakeKnoggin())
    params = {
        "session_id": "session-1",
        "turn": Turn(content="What is Knoggin?"),
        "idempotency_key": "request-1",
    }

    first = await manager.submit_turn(**params)
    second = await manager.submit_turn(**params)

    assert second.run_id == first.run_id
    await manager.cancel(first.run_id)
    await manager.close()
