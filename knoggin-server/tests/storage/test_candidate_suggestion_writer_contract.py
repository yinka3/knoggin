import pytest

from common.schema.contracts import CandidateSuggestion, EngineScope
from knoggin_server.knowledge.db.writers.candidate_suggestion_writer import (
    CandidateSuggestionWriter,
)
from tests.fixtures.fakes import RecordingPostgresClient


def make_scope():
    return EngineScope(
        user_name="ada",
        project_id="project-1",
        session_id="session-1",
    )


def make_suggestion(*, created_entity_id=1001):
    return CandidateSuggestion(
        msg_id=7,
        mention="workspace notes tool",
        mention_type="tool",
        mention_topic="General",
        candidate_id=501,
        candidate_name="Notion",
        base_score=0.82,
        support_score=0.87,
        reasons=[
            "candidate_rejected",
            "below_resolution_threshold",
            "advisory_context_support",
        ],
        created_entity_id=created_entity_id,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_candidate_suggestion_writer_empty_input_is_noop():
    client = RecordingPostgresClient()
    writer = CandidateSuggestionWriter(client)

    saved = await writer.save_candidate_suggestions(make_scope(), [])

    assert saved == 0
    assert client.calls == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_candidate_suggestion_writer_saves_suggestion_with_scope_and_reasons():
    client = RecordingPostgresClient()
    writer = CandidateSuggestionWriter(client)
    suggestion = make_suggestion()

    saved = await writer.save_candidate_suggestions(make_scope(), [suggestion])

    assert saved == 1
    call = client.calls[0]
    assert call[0] == "execute_command"
    assert "INSERT INTO ingestion_candidate_suggestions" in call[1]
    assert "ON CONFLICT (suggestion_id) DO UPDATE" in call[1]
    assert call[2][1:4] == ("ada", "project-1", "session-1")
    assert call[2][4] == 7
    assert call[2][5] == "workspace notes tool"
    assert call[2][8] == 501
    assert call[2][12] == (
        '["candidate_rejected", "below_resolution_threshold", '
        '"advisory_context_support"]'
    )
    assert call[2][13] == 1001


@pytest.mark.storage
@pytest.mark.no_network
async def test_candidate_suggestion_writer_uses_deterministic_suggestion_id():
    client = RecordingPostgresClient()
    writer = CandidateSuggestionWriter(client)
    scope = make_scope()

    await writer.save_candidate_suggestions(scope, [make_suggestion()])
    await writer.save_candidate_suggestions(
        scope, [make_suggestion(created_entity_id=1002)]
    )

    first_id = client.calls[0][2][0]
    second_id = client.calls[1][2][0]
    assert first_id == second_id
    assert client.calls[1][2][13] == 1002
