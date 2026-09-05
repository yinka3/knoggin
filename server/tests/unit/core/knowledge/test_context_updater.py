from uuid import uuid4

import pytest
from pydantic import ValidationError

from common.conf.domain_config import DomainConfig
from common.schema.context import (
    AssertionKind,
    ContextAdd,
    ContextBlockRecord,
    ContextReplace,
    ContextRevisionOrigin,
    ContextSnapshot,
    LLMContextUpdate,
)
from common.schema.episode.models import Episode, MessageEpisode
from core.knowledge.context.updater import ContextUpdateBuild, ContextUpdater


def _domain():
    return DomainConfig(version=1, topics=(), entity_types=()).compile()


def _snapshot() -> ContextSnapshot:
    block = ContextBlockRecord(
        block_id=uuid4(),
        project_id="project-1",
        section_key="current_state",
        markdown="The prior state.",
        content_hash="a" * 64,
        assertion_kind=AssertionKind.AGENT_DERIVED,
    )
    return ContextSnapshot(
        revision_id=uuid4(),
        project_id="project-1",
        revision_number=1,
        origin=ContextRevisionOrigin.CONVERSATION,
        domain_version=1,
        content_hash="b" * 64,
        blocks=[block],
    )


def _messages():
    return [
        {
            "message_id": 11,
            "session_id": "session-1",
            "role": "user",
            "content": "I prefer a local-only project.",
            "timestamp_ms": 100,
        },
        {
            "message_id": 12,
            "session_id": "session-1",
            "role": "assistant",
            "content": "The document says the project is local-only.",
            "timestamp_ms": 120,
        },
    ]


def _source_refs(source_id):
    return [
        {
            "source_ref_id": str(source_id),
            "message_id": 12,
            "session_id": "session-1",
            "source_kind": "text_document",
            "locator": {"line": 4},
            "excerpt": "This project runs locally.",
        }
    ]


def _episode():
    return Episode(
        episode_id="episode-1",
        project_id="project-1",
        summary="The user confirmed the local-only constraint.",
        messages=[
            MessageEpisode(message_id=11, session_id="session-1", message_position=0),
            MessageEpisode(message_id=12, session_id="session-1", message_position=1),
        ],
    )


def _build(*, episodes=None):
    source_id = uuid4()
    return ContextUpdateBuild(
        project_id="project-1",
        domain=_domain(),
        snapshot=_snapshot(),
        messages=_messages(),
        assistant_source_refs=_source_refs(source_id),
        episodes=[] if episodes is None else episodes,
    ), source_id


@pytest.mark.unit
@pytest.mark.no_network
def test_context_updater_derives_user_block_time_and_persists_only_user_support():
    build, _ = _build()

    result = build.apply(
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="preferences",
                    markdown="Keep the project local-only.",
                    assertion_kind=AssertionKind.USER_ASSERTED,
                    evidence=[{"handle": "M1"}],
                )
            ],
            edit_summary="Recorded local-only preference",
        )
    )

    assert result.materialization is not None
    added = next(
        block
        for block in result.materialization.blocks
        if block.section_key == "preferences"
    )
    assert added.source_time_ms == 100
    assert [(support.message_id, support.support_kind.value) for support in result.materialization.supports] == [
        (11, "user_message")
    ]
    brief = build.evidence_brief()
    assert "[M1]" in brief and "[S1] owner=M2" in brief
    assert "C1" in brief


@pytest.mark.unit
@pytest.mark.no_network
def test_source_grounded_blocks_require_assistant_source_handles_and_keep_owner():
    build, source_id = _build()

    result = build.apply(
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="The source confirms local-only operation.",
                    assertion_kind=AssertionKind.SOURCE_GROUNDED,
                    evidence=[{"handle": "S1"}],
                )
            ]
        )
    )

    assert result.materialization is not None
    assert [(support.message_id, support.source_ref_id) for support in result.materialization.supports] == [
        (12, source_id)
    ]
    assert result.materialization.blocks[-1].source_time_ms == 120

    with pytest.raises(ValueError, match="assistant source"):
        build.apply(
            LLMContextUpdate(
                operations=[
                    ContextAdd(
                        section_key="current_state",
                        markdown="Unsupported assistant claim.",
                        assertion_kind=AssertionKind.SOURCE_GROUNDED,
                        evidence=[{"handle": "M2"}],
                    )
                ]
            )
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_episode_aid_expands_to_current_messages_and_cannot_be_terminal_evidence():
    build, _ = _build(episodes=[_episode()])

    result = build.apply(
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="active_work",
                    markdown="Work remains local-first.",
                    assertion_kind=AssertionKind.AGENT_DERIVED,
                    evidence=[{"handle": "E1"}],
                )
            ]
        )
    )

    assert result.materialization is not None
    assert {(support.message_id, support.support_kind.value) for support in result.materialization.supports} == {
        (11, "user_message"),
        (12, "assistant_message"),
    }

    stale_episode = _episode().model_copy(
        update={
            "messages": [
                MessageEpisode(message_id=99, session_id="other", message_position=0)
            ]
        }
    )
    stale_build, _ = _build(episodes=[stale_episode])
    with pytest.raises(ValueError, match="no current-window evidence"):
        stale_build.apply(
            LLMContextUpdate(
                operations=[
                    ContextAdd(
                        section_key="active_work",
                        markdown="Cannot use a stale episode alone.",
                        evidence=[{"handle": "E1"}],
                    )
                ]
            )
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_model_contract_fails_closed_for_missing_evidence_human_assertions_and_model_time():
    with pytest.raises(ValidationError, match="evidence"):
        LLMContextUpdate(
            operations=[ContextAdd(section_key="current_state", markdown="Missing evidence.")]
        )


@pytest.mark.unit
@pytest.mark.no_network
def test_late_evidence_cannot_replace_newer_context_and_oversized_output_fails_closed():
    snapshot = _snapshot()
    snapshot.blocks[0] = snapshot.blocks[0].model_copy(update={"source_time_ms": 200})
    source_id = uuid4()
    build = ContextUpdateBuild(
        project_id="project-1",
        domain=_domain(),
        snapshot=snapshot,
        messages=_messages(),
        assistant_source_refs=_source_refs(source_id),
        episodes=[],
    )
    with pytest.raises(ValueError, match="older or untimed"):
        build.apply(
            LLMContextUpdate(
                operations=[
                    ContextReplace(
                        section_key="current_state",
                        target={"handle": "C1"},
                        markdown="Late older evidence must not win.",
                        evidence=[{"handle": "M1"}],
                    )
                ]
            )
        )
    with pytest.raises(ValidationError, match="100000"):
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="x" * 50_000,
                    evidence=[{"handle": "M1"}],
                ),
                ContextAdd(
                    section_key="active_work",
                    markdown="x" * 50_000,
                    evidence=[{"handle": "M1"}],
                ),
                ContextAdd(
                    section_key="preferences",
                    markdown="x",
                    evidence=[{"handle": "M1"}],
                ),
            ]
        )
    with pytest.raises(ValidationError, match="human_asserted"):
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="Not a model capability.",
                    assertion_kind=AssertionKind.HUMAN_ASSERTED,
                    evidence=[{"handle": "M1"}],
                )
            ]
        )
    with pytest.raises(ValidationError, match="source_time"):
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="The server owns source time.",
                    evidence=[{"handle": "M1"}],
                    source_time_ms=1,
                )
            ]
        )


class _CapturingLLM:
    def __init__(self, response):
        self.response = response
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        return self.response


@pytest.mark.unit
@pytest.mark.no_network
async def test_context_updater_uses_the_structured_contract_and_server_prompt():
    response = LLMContextUpdate()
    llm = _CapturingLLM(response)
    updater = ContextUpdater(llm=llm)

    result = await updater.update(
        user_name="ada",
        project_id="project-1",
        domain=_domain(),
        snapshot=_snapshot(),
        messages=_messages(),
        assistant_source_refs=[],
        episodes=[],
    )

    assert result.materialization is None
    assert llm.calls[0]["response_model"] is LLMContextUpdate
    assert "EPISODES are interpretation aids" in llm.calls[0]["system"]
    assert llm.calls[0]["temperature"] == 0.0
