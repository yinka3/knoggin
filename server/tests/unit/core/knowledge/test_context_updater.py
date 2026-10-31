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
from common.schema.episode.models import EpisodeCard
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


def _build(*, earlier_episodes=(), budget_tokens=40_000):
    source_id = uuid4()
    return ContextUpdateBuild(
        project_id="project-1",
        domain=_domain(),
        snapshot=_snapshot(),
        messages=_messages(),
        assistant_source_refs=_source_refs(source_id),
        earlier_episodes=list(earlier_episodes),
        budget_tokens=budget_tokens,
    ), source_id


def _assert_rejected(result, reason):
    """An invalid model operation is dropped, never applied or fatal."""

    assert result.materialization is None
    assert len(result.rejected_operations) == 1
    assert reason in result.rejected_operations[0]


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
                    user_quote="I prefer a local-only project",
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
    assert added.assertion_kind is AssertionKind.USER_ASSERTED
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

    rejected = build.apply(
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
    _assert_rejected(rejected, "assistant source")


@pytest.mark.unit
@pytest.mark.no_network
def test_clarification_questions_cannot_establish_context_without_other_evidence():
    messages = _messages()
    messages[1]["exchange_outcome"] = "clarification"
    source_id = uuid4()
    build = ContextUpdateBuild(
        project_id="project-1",
        domain=_domain(),
        snapshot=_snapshot(),
        messages=messages,
        assistant_source_refs=_source_refs(source_id),
        budget_tokens=40_000,
    )

    result = build.apply(
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="The user has a local-only project.",
                    assertion_kind=AssertionKind.AGENT_DERIVED,
                    evidence=[{"handle": "M1"}, {"handle": "M2"}],
                )
            ]
        )
    )

    assert result.materialization is not None
    assert result.materialization.blocks[-1].source_time_ms == 100
    assert [
        (support.message_id, support.support_kind.value)
        for support in result.materialization.supports
    ] == [(11, "user_message")]
    assert "kind=clarification_question" in build.evidence_brief()

    clarification_only = ContextUpdateBuild(
        project_id="project-1",
        domain=_domain(),
        snapshot=_snapshot(),
        messages=[messages[1]],
        assistant_source_refs=[],
        budget_tokens=40_000,
    )
    rejected = clarification_only.apply(
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="The project is local-only.",
                    assertion_kind=AssertionKind.AGENT_DERIVED,
                    evidence=[{"handle": "M1"}],
                )
            ]
        )
    )
    _assert_rejected(rejected, "Clarification questions alone")


@pytest.mark.unit
@pytest.mark.no_network
def test_model_contract_drops_only_the_edits_that_break_its_rules():
    valid = {"operation": "add", "section_key": "current_state",
             "markdown": "Kept.", "evidence": [{"handle": "M1"}]}
    update = LLMContextUpdate.model_validate({"operations": [
        valid,
        {**valid, "markdown": "Missing evidence.", "evidence": []},
        {**valid, "markdown": "Not a model capability.",
         "assertion_kind": "human_asserted"},
        {**valid, "markdown": "Kept without its model-set source time.",
         "source_time_ms": 1},
        {**valid, "markdown": "Malformed handle.", "evidence": [{"handle": "msg1"}]},
        {**valid, "markdown": "Repeated handle.",
         "evidence": [{"handle": "M1"}, {"handle": "M1"}]},
        {**valid, "operation": "rename"},
    ]})

    assert [operation.markdown for operation in update.operations] == [
        "Kept.",
        "Kept without its model-set source time.",
    ]
    # The server computes source time; a model-set value is discarded.
    assert update.operations[1].source_time_ms is None


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
        budget_tokens=40_000,
    )
    rejected = build.apply(
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
    _assert_rejected(rejected, "older or untimed")
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
        earlier_episodes=[],
        budget_tokens=40_000,
    )

    assert result.materialization is None
    assert llm.calls[0]["response_model"] is LLMContextUpdate
    assert "EARLIER EPISODES are background" in llm.calls[0]["system"]
    assert llm.calls[0]["temperature"] == 0.0


@pytest.mark.unit
@pytest.mark.no_network
def test_one_invalid_operation_does_not_discard_valid_operations():
    build, _ = _build()

    result = build.apply(
        LLMContextUpdate(
            operations=[
                ContextAdd(
                    section_key="current_state",
                    markdown="Unknown evidence cannot ground this.",
                    evidence=[{"handle": "M99"}],
                ),
                ContextReplace(
                    section_key="current_state",
                    target={"handle": "C9"},
                    markdown="There is no ninth block.",
                    evidence=[{"handle": "M1"}],
                ),
                ContextAdd(
                    section_key="current_state",
                    markdown="The user prefers a local-only project.",
                    assertion_kind=AssertionKind.USER_ASSERTED,
                    user_quote="I prefer a local-only project.",
                    evidence=[{"handle": "M1"}],
                ),
            ]
        )
    )

    assert result.materialization is not None
    assert [block.markdown for block in result.materialization.blocks] == [
        "The prior state.",
        "The user prefers a local-only project.",
    ]
    assert [support.message_id for support in result.materialization.supports] == [11]
    assert len(result.rejected_operations) == 2
    assert "unknown evidence M99" in result.rejected_operations[0]
    assert "unknown block C9" in result.rejected_operations[1]


@pytest.mark.unit
@pytest.mark.no_network
def test_brief_names_the_section_keys_the_model_must_return():
    build = ContextUpdateBuild(
        project_id="project-1",
        domain=_domain(),
        snapshot=None,
        messages=_messages(),
        assistant_source_refs=[],
        budget_tokens=40_000,
    )

    brief = build.evidence_brief()

    for section in _domain().context_sections:
        assert f"- {section.key}: {section.title}" in brief



def _kinds(result):
    return [
        (block.markdown, block.assertion_kind) for block in result.materialization.blocks
        if block.markdown.startswith("Claim:")
    ]


@pytest.mark.unit
@pytest.mark.no_network
def test_a_user_claim_needs_the_users_own_words():
    build, _ = _build()

    result = build.apply(LLMContextUpdate(operations=[
        # The user's words, quoted with different spacing and case.
        ContextAdd(section_key="preferences", markdown="Claim: local only.",
                   assertion_kind=AssertionKind.USER_ASSERTED,
                   user_quote="i prefer a  LOCAL-ONLY project", evidence=[{"handle": "M1"}]),
        # The agent's words, cited beside the user's message.
        ContextAdd(section_key="preferences", markdown="Claim: the document agrees.",
                   assertion_kind=AssertionKind.USER_ASSERTED,
                   user_quote="The document says the project is local-only",
                   evidence=[{"handle": "M1"}, {"handle": "M2"}]),
        # No quote at all.
        ContextAdd(section_key="preferences", markdown="Claim: unquoted.",
                   assertion_kind=AssertionKind.USER_ASSERTED, evidence=[{"handle": "M1"}]),
    ]))

    assert _kinds(result) == [
        ("Claim: local only.", AssertionKind.USER_ASSERTED),
        ("Claim: the document agrees.", AssertionKind.AGENT_DERIVED),
        ("Claim: unquoted.", AssertionKind.AGENT_DERIVED),
    ]
    assert result.rejected_operations == ()



@pytest.mark.unit
@pytest.mark.no_network
def test_a_quote_may_join_pieces_of_the_users_words():
    build, _ = _build()

    result = build.apply(LLMContextUpdate(operations=[
        ContextAdd(section_key="preferences", markdown="Claim: joined pieces.",
                   assertion_kind=AssertionKind.USER_ASSERTED,
                   user_quote="I prefer\n... a local-only project", evidence=[{"handle": "M1"}]),
        ContextAdd(section_key="preferences", markdown="Claim: sentences from two messages.",
                   assertion_kind=AssertionKind.USER_ASSERTED,
                   user_quote="I prefer a local-only project. The document says the project is local-only.",
                   evidence=[{"handle": "M1"}, {"handle": "M2"}]),
        ContextAdd(section_key="preferences", markdown="Claim: one piece is not theirs.",
                   assertion_kind=AssertionKind.USER_ASSERTED,
                   user_quote="I prefer ... the document says so", evidence=[{"handle": "M1"}]),
    ]))

    assert _kinds(result) == [
        ("Claim: joined pieces.", AssertionKind.USER_ASSERTED),
        # The second sentence is the agent's (M2), so the claim is not the user's.
        ("Claim: sentences from two messages.", AssertionKind.AGENT_DERIVED),
        ("Claim: one piece is not theirs.", AssertionKind.AGENT_DERIVED),
    ]


def _earlier_episode(summary="Ada chose Postgres for the project last week."):
    return EpisodeCard(
        episode_id="episode-0", project_id="project-1", summary=summary
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_per_window_update_cannot_delete_so_one_window_cannot_empty_context():
    update = LLMContextUpdate.model_validate({"operations": [
        {"operation": "delete", "section_key": "current_state",
         "target": {"handle": "C1"}, "evidence": [{"handle": "M1"}]},
        {"operation": "replace", "section_key": "current_state",
         "target": {"handle": "C1"}, "markdown": "The plan is finished.",
         "evidence": [{"handle": "M1"}]},
    ]})

    assert [operation.operation.value for operation in update.operations] == ["replace"]


@pytest.mark.unit
@pytest.mark.no_network
def test_earlier_episodes_are_background_that_cannot_be_cited():
    build, _ = _build(earlier_episodes=[_earlier_episode()])
    brief = build.evidence_brief()

    background = brief.split("EARLIER EPISODES")[1]
    assert "Ada chose Postgres for the project last week." in background
    assert "[E1]" not in brief
    update = LLMContextUpdate.model_validate({"operations": [
        {"operation": "add", "section_key": "current_state",
         "markdown": "Uses Postgres.", "evidence": [{"handle": "E1"}]},
    ]})
    assert update.operations == []


@pytest.mark.unit
@pytest.mark.no_network
def test_brief_says_how_full_context_is():
    build, _ = _build(budget_tokens=40_000)

    assert "CONTEXT SIZE: about 4 of 40000 tokens" in build.evidence_brief()
