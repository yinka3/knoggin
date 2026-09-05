from types import SimpleNamespace
from uuid import uuid4

import pytest

from common.conf.domain_config import DomainConfig
from common.exceptions import LLMProviderError
from common.schema.agent.research import resolve_research_profile
from common.schema.artifacts import ArtifactDraft, MarkdownArtifactBlock
from common.schema.context import (
    AssertionKind,
    ContextBlockRecord,
    ContextRevisionOrigin,
    ContextSnapshot,
)
from core.agent.executor import AgentExecutor
from core.agent.run import AgentIdentity, AgentRun, AgentRunLimits


class StreamingLLM:
    agent_model = "architect-model"
    extraction_model = "librarian-model"

    def __init__(self, chunks=None, *, raises=None):
        self.chunks = chunks or []
        self.raises = raises
        self.calls = []

    async def stream_with_tools(self, **kwargs):
        self.calls.append(kwargs)
        if self.raises:
            raise self.raises
        for chunk in self.chunks:
            yield chunk


def make_executor(llm, *, research_profile=None):
    ctx = AgentRun.open(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
        user_query="What changed in profile behavior?",
        run_id="run-1",
        agent=AgentIdentity(
            config=SimpleNamespace(id="agent-1"),
            name="STELLA",
            persona="Careful memory assistant",
        ),
        limits=AgentRunLimits(),
        model="test-model",
        temperature=0.2,
        brain="Use citations",
        enabled_tools=["search_messages"],
        research_profile=research_profile,
    )
    tools = SimpleNamespace(document_service=None)
    return AgentExecutor(ctx, llm, tools)


@pytest.mark.no_network
async def test_executor_loads_missing_or_unreadable_project_brief_non_fatally():
    executor = make_executor(StreamingLLM())
    assert await executor._load_project_brief() == ""

    async def fail_reader():
        raise RuntimeError("workspace unavailable")

    executor.tools.document_service = SimpleNamespace(
        read_project_brief=fail_reader
    )
    assert await executor._load_project_brief() == ""


@pytest.mark.no_network
async def test_executor_loads_user_owned_project_brief_directly():
    executor = make_executor(StreamingLLM())

    async def read_context():
        return "# Project\nUse the repository conventions."

    executor.tools.document_service = SimpleNamespace(
        read_project_brief=read_context
    )
    assert await executor._load_project_brief() == (
        "# Project\nUse the repository conventions."
    )


@pytest.mark.no_network
async def test_executor_renders_current_context_from_the_canonical_reader_only():
    executor = make_executor(StreamingLLM())
    revision_id = uuid4()
    block = ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown="The semantic owner is project-scoped.",
        content_hash="a" * 64,
        assertion_kind=AssertionKind.USER_ASSERTED,
    )
    snapshot = ContextSnapshot(
        revision_id=revision_id,
        project_id="project-1",
        revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        content_hash="b" * 64,
        blocks=[block],
    )

    class Reader:
        async def get_current_revision(self, **kwargs):
            assert kwargs == {"user_name": "ada", "project_id": "project-1"}
            return SimpleNamespace(revision_id=revision_id)

        async def get_snapshot(self, value, **kwargs):
            assert value == revision_id
            assert kwargs == {"user_name": "ada", "project_id": "project-1"}
            return snapshot

    async def projection_is_not_an_authoritative_read():
        raise AssertionError("CONTEXT.md projection must not be read by the agent")

    executor.tools.project_context_reader = Reader()
    executor.tools.compiled_domain = DomainConfig.from_mapping({"version": 1}).compile()
    executor.tools.document_service = SimpleNamespace(
        read_project_brief=projection_is_not_an_authoritative_read
    )

    rendered = await executor._load_project_context()

    assert "# Project Context" in rendered
    assert "The semantic owner is project-scoped." in rendered


@pytest.mark.no_network
async def test_step_forwards_standard_stream_events(monkeypatch):
    chunks = [
        {"event": "token", "data": {"content": "I should search first. "}},
        {"event": "thinking", "data": {"content": "Need direct evidence"}},
        {
            "event": "tool_calls",
            "data": {
                "content": "Authoritative tool reasoning.",
                "calls": [
                    {
                        "name": "search_messages",
                        "arguments": '{"query": "profile behavior", "limit": 3}',
                        "id": "call-1",
                    },
                    {
                        "name": "get_recent_activity",
                        "arguments": '{"entity_name": "Knoggin", "hours": "24",}',
                        "id": "call-2",
                    },
                ],
            },
        },
        {
            "event": "step_completed",
            "data": {
                "content": "Authoritative tool reasoning.",
                "usage": {
                    "prompt_tokens": 10,
                    "completion_tokens": 5,
                    "total_tokens": 15,
                    "approximate": False,
                },
            },
        },
    ]
    llm = StreamingLLM(chunks)
    prompt_calls = []

    def fake_agent_prompt(*args, **kwargs):
        prompt_calls.append((args, kwargs))
        return "SYSTEM PROMPT"

    monkeypatch.setattr(
        "core.agent.executor.get_agent_prompt",
        fake_agent_prompt,
    )
    executor = make_executor(llm)

    events = [
        event
        async for event in executor._step(
            date="2026-02-03 04:05 UTC",
            model="test-model",
            reasoning="high",
            phase="PLAN",
            documents_context="- file.md",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert events[0] == {
        "event": "token",
        "data": {"content": "I should search first. "},
    }
    assert events[1] == {
        "event": "thinking",
        "data": {"content": "Need direct evidence"},
    }
    assert events[2] == chunks[2]
    assert events[3] == chunks[3]

    calls = executor._parse_tool_calls(
        events[2]["data"]["calls"],
        events[2]["data"]["content"],
    )
    assert [call.name for call in calls] == ["search_messages", "get_recent_activity"]
    assert calls[0].args == {"query": "profile behavior", "limit": 3}
    assert calls[0].thinking == "Authoritative tool reasoning."
    assert calls[0].call_id == "call-1"
    assert calls[1].args == {"entity_name": "Knoggin", "hours": "24"}

    llm_call = llm.calls[0]
    assert llm_call["system"] == "SYSTEM PROMPT"
    assert llm_call["model"] == "test-model"
    assert llm_call["temperature"] == 0.2
    assert llm_call["reasoning"] == "high"
    tool_names = [schema["function"]["name"] for schema in llm_call["tools"]]
    assert "search_messages" in tool_names
    assert "submit_answer" in tool_names
    assert "search_entity" not in tool_names

    _, prompt_kwargs = prompt_calls[0]
    assert prompt_kwargs["documents_context"] == "- file.md"
    assert prompt_kwargs["agent_brain"] == "Use citations"
    assert prompt_kwargs["phase"] == "PLAN"
    assert prompt_kwargs["research_profile"].mode == "normal"


@pytest.mark.no_network
async def test_step_forwards_selected_research_profile_to_prompt(monkeypatch):
    llm = StreamingLLM(
        [
            {
                "event": "tool_calls",
                "data": {
                    "content": "Research first.",
                    "calls": [
                        {
                            "name": "search_messages",
                            "arguments": '{"query": "profile"}',
                            "id": "research-call",
                        }
                    ],
                },
            },
            {
                "event": "step_completed",
                "data": {
                    "usage": {
                        "prompt_tokens": 1,
                        "completion_tokens": 1,
                        "total_tokens": 2,
                        "approximate": False,
                    }
                },
            },
        ]
    )
    prompt_kwargs = {}

    def fake_agent_prompt(**kwargs):
        prompt_kwargs.update(kwargs)
        return "SYSTEM PROMPT"

    monkeypatch.setattr(
        "core.agent.executor.get_agent_prompt",
        fake_agent_prompt,
    )
    executor = make_executor(
        llm,
        research_profile=resolve_research_profile("deep_research"),
    )

    _ = [
        event
        async for event in executor._step(
            date="2026-02-03 04:05 UTC",
            model="test-model",
            reasoning="high",
            phase="PLAN",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert prompt_kwargs["research_profile"].mode == "deep_research"


@pytest.mark.no_network
async def test_step_marks_invalid_arguments_for_later_tool_error():
    executor = make_executor(StreamingLLM())
    tool_call = executor._parse_tool_calls(
        [
            {
                "name": "search_messages",
                "arguments": "{not json",
                "id": "call-bad",
            }
        ],
        "",
    )[0]

    assert tool_call.args["_parse_error"] is True
    assert tool_call.args["_raw"] == "{not json"


@pytest.mark.no_network
async def test_step_completed_without_tool_calls_yields_formatting_step_error():
    llm = StreamingLLM(
        [
            {"event": "token", "data": {"content": "raw answer"}},
            {
                "event": "step_completed",
                "data": {
                    "content": "raw answer",
                    "usage": {
                        "prompt_tokens": 4,
                        "completion_tokens": 2,
                        "total_tokens": 6,
                        "approximate": True,
                    },
                },
            },
        ]
    )
    executor = make_executor(llm)

    events = [
        event
        async for event in executor._step(
            date="now",
            model="model",
            reasoning="high",
            phase="PLAN",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert events[-1]["event"] == "step_error"
    assert events[-1]["data"]["kind"] == "formatting"
    assert "must either call" in events[-1]["data"]["message"]
    assert events[-1]["data"]["usage"]["approximate"] is True


@pytest.mark.no_network
async def test_step_forwards_llm_error_chunk_and_exceptions():
    chunk_error = StreamingLLM(
        [
            {
                "event": "step_error",
                "data": {"kind": "provider", "message": "provider said no"},
            }
        ]
    )
    exception_error = StreamingLLM(raises=LLMProviderError("stream broke"))

    chunk_events = [
        event
        async for event in make_executor(chunk_error)._step(
            date="now",
            model="model",
            reasoning="high",
            phase="PLAN",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]
    exception_events = [
        event
        async for event in make_executor(exception_error)._step(
            date="now",
            model="model",
            reasoning="high",
            phase="PLAN",
            documents_context="",
            document_focus_context="",
            last_result=None,
        )
    ]

    assert chunk_events == [
        {
            "event": "step_error",
            "data": {"kind": "provider", "message": "provider said no"},
        }
    ]
    assert exception_events == [
        {
            "event": "step_error",
            "data": {
                "kind": "provider",
                "message": "LLM provider unavailable",
            },
        }
    ]


@pytest.mark.no_network
async def test_final_response_carries_validated_structured_artifact():
    executor = make_executor(StreamingLLM())
    artifact = ArtifactDraft(
        kind="research_brief",
        title="Brief",
        blocks=(MarkdownArtifactBlock(content="Finding"),),
    )

    event = executor._wrap_final_response(
        content="Answer",
        usage={
            "prompt_tokens": 1,
            "completion_tokens": 1,
            "total_tokens": 2,
            "approximate": False,
        },
        sources_consulted=[],
        artifact=artifact.model_dump(mode="json"),
    )

    assert event["data"]["artifact"]["kind"] == "research_brief"
    assert event["data"]["artifact"]["blocks"][0]["content"] == "Finding"
