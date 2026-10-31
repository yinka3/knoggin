import json
from datetime import datetime, timedelta, timezone

from dataset import load_questions, pair_turns, parse_date

START = datetime(2023, 5, 20, 2, 21, tzinfo=timezone.utc)


def test_parse_date_reads_the_dataset_format_as_utc():
    assert parse_date("2023/05/20 (Sat) 02:21") == START


def test_pairs_turns_and_spaces_them_in_order():
    exchanges = pair_turns(
        [
            {"role": "assistant", "content": "leading, dropped"},
            {"role": "user", "content": "first"},
            {"role": "assistant", "content": "reply"},
            {"role": "assistant", "content": "more reply"},
            {"role": "user", "content": "unanswered"},
            {"role": "user", "content": "last"},
            {"role": "assistant", "content": "  "},
        ],
        START,
        label="q",
    )

    assert [(e.user_content, e.assistant_content) for e in exchanges] == [
        ("first", "reply\n\nmore reply"),
        ("unanswered", None),
        ("last", None),
    ]
    assert [e.user_timestamp for e in exchanges] == [
        START, START + timedelta(minutes=2), START + timedelta(minutes=4)
    ]
    assert exchanges[0].assistant_timestamp == START + timedelta(seconds=30)
    assert exchanges[1].assistant_timestamp is None


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
