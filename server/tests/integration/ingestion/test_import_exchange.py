"""Importing recorded exchanges at their original times."""

from datetime import datetime, timedelta, timezone
from types import SimpleNamespace

import pytest
from pydantic import ValidationError

from common.conf.domain_config import DomainConfig
from common.schema.primitives import ImportedAssistantTurn, ImportedExchange, Message
from common.schema.settings import DeveloperSettings, IngestionSettings, RootConfig
from core.ingestion.semantic_window_admission import SemanticWindowAdmission
from core.knowledge.db.readers.semantic_window_reader import SemanticWindowReader
from core.knowledge.store import KnowledgeStore
from runtime.session_runtime import SessionRuntime

_USER_AT = datetime(2023, 5, 20, 2, 21, tzinfo=timezone.utc)
_ASSISTANT_AT = _USER_AT + timedelta(seconds=30)


def _ms(moment: datetime) -> int:
    return int(moment.timestamp() * 1000)


def _session(scope, monkeypatch):
    monkeypatch.setattr(
        SessionRuntime,
        "current_config",
        property(lambda self: RootConfig(developer_settings=DeveloperSettings())),
    )
    wakes = []
    project = SimpleNamespace(
        scheduler=object(),
        project_semantic_processor=object(),
        readable_project_ids=[scope["project_id"]],
        signal_semantic_work=lambda: wakes.append(True),
    )
    session = SessionRuntime(
        scope["user_name"],
        SimpleNamespace(
            postgres=scope["postgres"],
            knowledge_store=KnowledgeStore(scope["postgres"], object()),
            embedding=object(),
        ),
        session_id=scope["session_id"],
        project_id=scope["project_id"],
        project=project,
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    return session, wakes


async def _rows(scope):
    return await scope["postgres"].fetch_all(
        """
        SELECT message_id, role, timestamp_ms, metadata ->> 'origin' AS origin,
               lifecycle_state, exchange_state, exchange_outcome,
               exchange_closed_at_ms
        FROM public.messages
        WHERE session_id = %s
        ORDER BY message_id
        """,
        (scope["session_id"],),
    )


async def _proposed_message_ids(scope):
    admission = SemanticWindowAdmission(
        KnowledgeStore(scope["postgres"], object()),
        IngestionSettings(semantic_window_tokens=10_000),
        token_counter=lambda text: max(1, len(text.split())),
        now_ms=lambda: _ms(_USER_AT),
    )
    domain = DomainConfig.from_mapping(
        {
            "version": 1,
            "topics": {"Work": {"active": True}},
            "entity_types": {"Concept": {"topic": "Work", "labels": ["concept"]}},
        }
    ).compile()
    proposal = await admission.select(
        user_name=scope["user_name"],
        project_id=scope["project_id"],
        domain=domain,
        force_flush=True,
    )
    return [] if proposal is None else [m.message_id for m in proposal.messages]


_REPLIED = ImportedExchange(
    user_content="I moved to Lisbon.",
    user_timestamp=_USER_AT,
    assistant_content="Congrats on the move!",
    assistant_timestamp=_ASSISTANT_AT,
)
_USER_ONLY = ImportedExchange(
    user_content="Note: my sister is called Ana.",
    user_timestamp=_USER_AT + timedelta(minutes=2),
)


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_only_applied_standalone_steers_enter_semantic_memory(real_server_scope):
    scope = real_server_scope
    store = KnowledgeStore(scope["postgres"], object())
    first = {"steer_id": "steer-1", "instruction": "Use the first source",
             "state": "pending", "response_annotations": []}
    first_id = await store.allocate_message_id()
    await store.create_closed_standalone_message(
        user_name=scope["user_name"], project_id=scope["project_id"],
        session_id=scope["session_id"], message_id=first_id, role="user",
        content=first["instruction"], timestamp_ms=_ms(_USER_AT),
        acceptance_key="steer:one",
        metadata={"origin": "steer", "run_steer": first},
    )
    assert await _proposed_message_ids(scope) == []
    assert await store.record_run_steer(
        user_name=scope["user_name"], project_id=scope["project_id"],
        session_id=scope["session_id"], message_id=first_id,
        steer={**first, "state": "applied"}, expected_state="pending",
    )
    assert await _proposed_message_ids(scope) == [first_id]

    second = {"steer_id": "steer-2", "instruction": "Ignore that",
              "state": "pending", "response_annotations": []}
    second_id = await store.allocate_message_id()
    await store.create_closed_standalone_message(
        user_name=scope["user_name"], project_id=scope["project_id"],
        session_id=scope["session_id"], message_id=second_id, role="user",
        content=second["instruction"], timestamp_ms=_ms(_USER_AT + timedelta(minutes=2)),
        acceptance_key="steer:two",
        metadata={"origin": "steer", "run_steer": second},
    )
    assert await store.mark_orphaned_run_steers_undelivered(
        user_name=scope["user_name"],
    ) == 1
    assert await _proposed_message_ids(scope) == [first_id]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_assistant_only_import_is_durable_idempotent_and_admissible(
    real_server_scope, monkeypatch
):
    scope = real_server_scope
    session, wakes = _session(scope, monkeypatch)
    first = ImportedAssistantTurn(
        assistant_content="Earlier guidance: use a pilsner.",
        assistant_timestamp=_USER_AT,
    )
    later = ImportedExchange(
        user_content="I will use that recipe.",
        user_timestamp=_USER_AT + timedelta(minutes=2),
    )
    created = await session.import_exchanges([first, later])
    repeated = await session.import_exchanges([first, later])

    assert created[0]["user_message_id"] is None
    assert created[0]["created"] is True
    assert repeated == [{**item, "created": False} for item in created]
    rows = await _rows(scope)
    assert [row["role"] for row in rows] == ["assistant", "user"]
    assert rows[0]["exchange_outcome"] == "assistant_only"
    assert rows[0]["timestamp_ms"] == _ms(_USER_AT)
    assert rows[1]["exchange_outcome"] == "user_only"
    assert await _proposed_message_ids(scope) == [
        created[0]["assistant_message_id"], created[1]["user_message_id"]
    ]
    assert wakes == [True, True]

    store = KnowledgeStore(scope["postgres"], object())
    admission = SemanticWindowAdmission(
        store, IngestionSettings(semantic_window_tokens=10_000),
        token_counter=lambda text: max(1, len(text.split())),
        now_ms=lambda: _ms(_USER_AT),
    )
    domain = DomainConfig.from_mapping({
        "version": 1, "topics": {"Work": {"active": True}},
        "entity_types": {"Concept": {"topic": "Work", "labels": ["concept"]}},
    }).compile()
    proposal = await admission.select(
        user_name=scope["user_name"], project_id=scope["project_id"],
        domain=domain, force_flush=True,
    )
    assert proposal is not None
    await store.claim_project_semantic_window(proposal.window, list(proposal.messages))
    evidence = await SemanticWindowReader(scope["postgres"]).get_window_evidence_messages(
        proposal.window.window_id, user_name=scope["user_name"],
        project_id=scope["project_id"],
    )
    assert [(row["role"], row["content"]) for row in evidence] == [
        ("assistant", "Earlier guidance: use a pilsner."),
        ("user", "I will use that recipe."),
    ]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_imported_exchanges_keep_their_times_and_become_memory(
    real_server_scope, monkeypatch
):
    scope = real_server_scope
    session, wakes = _session(scope, monkeypatch)

    replied, user_only = await session.import_exchanges([_REPLIED, _USER_ONLY])

    user_id, assistant_id = replied["user_message_id"], replied["assistant_message_id"]
    note_id = user_only["user_message_id"]
    assert replied["created"] is True and user_only["created"] is True
    assert user_only["assistant_message_id"] is None
    assert await _rows(scope) == [
        {
            "message_id": user_id,
            "role": "user",
            "timestamp_ms": _ms(_USER_AT),
            "origin": "import",
            "lifecycle_state": "sealed",
            "exchange_state": "closed",
            "exchange_outcome": "assistant_final",
            "exchange_closed_at_ms": _ms(_ASSISTANT_AT),
        },
        {
            "message_id": assistant_id,
            "role": "assistant",
            "timestamp_ms": _ms(_ASSISTANT_AT),
            "origin": "import",
            "lifecycle_state": "sealed",
            # Only user rows carry the exchange; assistant rows keep the default.
            "exchange_state": "open",
            "exchange_outcome": None,
            "exchange_closed_at_ms": None,
        },
        {
            "message_id": note_id,
            "role": "user",
            "timestamp_ms": _ms(_USER_ONLY.user_timestamp),
            "origin": "import",
            "lifecycle_state": "sealed",
            "exchange_state": "closed",
            "exchange_outcome": "user_only",
            # Closes at its own time, not the clock's.
            "exchange_closed_at_ms": _ms(_USER_ONLY.user_timestamp),
        },
    ]
    assert wakes == [True, True]
    assert await _proposed_message_ids(scope) == [user_id, assistant_id, note_id]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_reimport_returns_the_stored_exchanges_without_writing(
    real_server_scope, monkeypatch
):
    scope = real_server_scope
    session, wakes = _session(scope, monkeypatch)

    first = await session.import_exchanges([_REPLIED, _USER_ONLY])
    rows = await _rows(scope)
    again = await session.import_exchanges([_REPLIED, _USER_ONLY])

    assert again == [{**result, "created": False} for result in first]
    assert await _rows(scope) == rows
    assert wakes == [True, True]


@pytest.mark.integration
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_reimport_finishes_an_exchange_a_stopped_import_left_open(
    real_server_scope, monkeypatch
):
    scope = real_server_scope
    session, _ = _session(scope, monkeypatch)
    # A stopped import saved the user turn but never reached the reply.
    accepted, _ = await session._accept_user_message(
        Message(
            content=_REPLIED.user_content,
            timestamp=_REPLIED.user_timestamp,
            metadata={"origin": "import"},
        )
    )

    [result] = await session.import_exchanges([_REPLIED])

    assert result["user_message_id"] == accepted.id
    assert result["created"] is True
    user_row, assistant_row = await _rows(scope)
    assert user_row["exchange_outcome"] == "assistant_final"
    assert assistant_row["message_id"] == result["assistant_message_id"]


@pytest.mark.unit
@pytest.mark.no_network
def test_imported_reply_must_not_precede_its_turn():
    with pytest.raises(ValidationError, match="must not precede"):
        ImportedExchange(
            user_content="Hi",
            user_timestamp=_ASSISTANT_AT,
            assistant_content="Hello",
            assistant_timestamp=_USER_AT,
        )
    with pytest.raises(ValidationError, match="given together"):
        ImportedExchange(
            user_content="Hi",
            user_timestamp=_USER_AT,
            assistant_content="Hello",
        )

