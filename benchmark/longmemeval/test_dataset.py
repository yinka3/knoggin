import json
from datetime import datetime, timedelta, timezone

from dataset import load_questions, pair_turns, parse_date
from common.schema.primitives import ImportedAssistantTurn, ImportedExchange

START = datetime(2023, 5, 20, 2, 21, tzinfo=timezone.utc)


def test_parse_date_reads_the_dataset_format_as_utc():
    assert parse_date("2023/05/20 (Sat) 02:21") == START


def test_pairs_turns_and_preserves_assistant_only_history():
    turns = pair_turns(
        [
            {"role": "assistant", "content": "leading"},
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
            {"role": "assistant", "content": "extra"},
            {"role": "user", "content": "unanswered"},
            {"role": "user", "content": "last"},
            {"role": "assistant", "content": "  "},
        ],
        START, label="q",
    )

    assert [type(turn) for turn in turns] == [
        ImportedAssistantTurn, ImportedExchange, ImportedAssistantTurn,
        ImportedExchange, ImportedExchange,
    ]
    assert turns[0].assistant_content == "leading"
    assert turns[0].assistant_timestamp == START
    assert turns[1].user_content == "first"
    assert turns[1].assistant_content == "reply"
    assert turns[1].user_timestamp == START + timedelta(seconds=2)
    assert turns[1].assistant_timestamp == START + timedelta(seconds=3)
    assert turns[2].assistant_content == "extra"
    assert turns[2].assistant_timestamp == START + timedelta(seconds=4)
    assert [turn.user_content for turn in turns[3:]] == ["unanswered", "last"]
    assert turns[3].assistant_content is None


def test_load_sorts_sessions_by_date(tmp_path):
    path = tmp_path / "data.json"
    path.write_text(json.dumps([{
        "question_id": "q1_abs",
        "question_type": "knowledge-update",
        "question": "Where do I live?",
        "answer": "Lisbon",
        "question_date": "2023/06/01 (Thu) 10:00",
        "haystack_session_ids": ["later", "earlier"],
        "haystack_dates": ["2023/05/21 (Sun) 09:00", "2023/05/20 (Sat) 02:21"],
        "haystack_sessions": [
            [{"role": "user", "content": "I moved to Lisbon."}],
            [{"role": "user", "content": "I live in Porto."}],
        ],
    }]))

    [question] = load_questions(path)

    assert [s.session_id for s in question.sessions] == ["earlier", "later"]
    assert question.sessions[0].started_at == START
    assert question.is_abstention


def test_sessions_never_overlap_even_when_they_share_a_start_minute(tmp_path):
    turns = [{"role": "user", "content": "u"}, {"role": "assistant", "content": "a"}] * 3
    path = tmp_path / "data.json"
    path.write_text(json.dumps([{
        "question_id": "q", "question_type": "t", "question": "?", "answer": "!",
        "question_date": "2023/05/21 (Sun) 00:00",
        "haystack_session_ids": ["later", "first", "second"],
        "haystack_dates": ["2023/05/20 (Sat) 02:40", "2023/05/20 (Sat) 02:21",
                           "2023/05/20 (Sat) 02:21"],
        "haystack_sessions": [turns, turns, turns],
    }]))

    first, second, later = load_questions(path)[0].sessions

    assert [first.session_id, second.session_id, later.session_id] == ["first", "second", "later"]
    assert first.started_at == START
    assert second.started_at > first.exchanges[-1].assistant_timestamp
    # A session already clear of the one before it keeps its dataset date.
    assert later.started_at == parse_date("2023/05/20 (Sat) 02:40")
