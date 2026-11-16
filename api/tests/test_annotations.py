from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from annotations import resolve_annotations, with_annotations
from contracts.sessions import ResponseAnnotation, StartRun


class Store:
    async def get_messages_by_ids(self, ids, **scope):
        assert ids == [12]
        assert scope == {"user_name": "ada", "session_ids": ["chat-1"],
                         "visible_project_ids": ["project-1"],
                         "discoverable_only": True}
        return [{"id": 12, "role": "assistant", "content": "One two three"}]


@pytest.mark.no_network
@pytest.mark.asyncio
async def test_response_annotation_checks_exact_scoped_excerpt():
    runtime = SimpleNamespace(
        sessions=SimpleNamespace(user_name="ada"),
        resources=SimpleNamespace(knowledge_store=Store()),
    )
    selected = ResponseAnnotation(message_id=12, start=4, end=7, quote="two")
    assert await resolve_annotations(runtime, "chat-1", "project-1", [selected]) == [
        selected.model_dump()
    ]
    assert "selected text: two" in with_annotations("Explain this", [selected.model_dump()])
    with pytest.raises(ValueError):
        await resolve_annotations(runtime, "chat-1", "project-1", [
            selected.model_copy(update={"quote": "One"})
        ])


def test_annotation_contract_is_bounded():
    with pytest.raises(ValidationError):
        StartRun(query="Explain", annotations=[
            ResponseAnnotation(message_id=12, start=0, end=3, quote="One")
        ] * 6)
