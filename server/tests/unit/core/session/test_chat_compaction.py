import pytest

from common.schema.settings import ChatCompactionSettings
from core.session.chat_compaction import ChatCompaction


class FakePostgres:
    def __init__(self):
        self.row = {"chat_summary": None, "chat_summary_through_message_id": 0}

    async def fetch_one(self, _query, _params):
        return dict(self.row)

    async def execute(self, _query, params):
        summary, through, _user, _session, expected = params
        if self.row["chat_summary_through_message_id"] == expected:
            self.row = {"chat_summary": summary, "chat_summary_through_message_id": through}


class FakeLLM:
    def __init__(self):
        self.prompts = []

    async def generate_text(self, system, user, **_kwargs):
        self.prompts.append(user)
        return f"- summary {len(self.prompts)}"


def _turns(count, words=100):
    return [
        {"message_id": index, "role": "user" if index % 2 else "assistant",
         "content": f"turn{index} " + "word " * words}
        for index in range(1, count + 1)
    ]


def _compaction(postgres, llm):
    # One token per word keeps the arithmetic readable.
    return ChatCompaction(
        postgres, llm, user_name="ada", session_id="s1",
        count_tokens=lambda text: len(text.split()),
    )


SETTINGS = ChatCompactionSettings(min_dropped_tokens=300, max_input_tokens=1_000)


@pytest.mark.unit
@pytest.mark.no_network
async def test_nothing_happens_until_enough_turns_fall_out_of_the_prompt():
    postgres, llm = FakePostgres(), FakeLLM()

    # 10 turns of 102 tokens; the prompt keeps 8 (816 of 900), so 2 drop: too few.
    changed = await _compaction(postgres, llm).extend(
        _turns(10), conversation_tokens=900, settings=SETTINGS
    )

    assert changed is False
    assert llm.prompts == []


@pytest.mark.unit
@pytest.mark.no_network
async def test_turns_outside_the_prompt_are_folded_into_the_summary_once():
    postgres, llm = FakePostgres(), FakeLLM()
    compaction = _compaction(postgres, llm)

    assert await compaction.extend(_turns(10), conversation_tokens=400, settings=SETTINGS)

    # The newest 3 turns (~306 tokens) stay in the prompt; turns 1-7 are summarized.
    assert "turn7 " in llm.prompts[0] and "turn8 " not in llm.prompts[0]
    assert await compaction.load() == ("- summary 1", 7)

    # Same history again: everything outside the prompt is already covered.
    assert not await compaction.extend(_turns(10), conversation_tokens=400, settings=SETTINGS)

    # Four more turns later, only the newly dropped ones are read, after the old summary.
    assert await compaction.extend(_turns(14), conversation_tokens=400, settings=SETTINGS)
    assert "Previous summary:\n- summary 1" in llm.prompts[1]
    assert "turn7 " not in llm.prompts[1] and "turn11 " in llm.prompts[1]
    assert await compaction.load() == ("- summary 2", 11)


@pytest.mark.unit
@pytest.mark.no_network
async def test_one_pass_reads_at_most_the_input_cap_oldest_first():
    postgres, llm = FakePostgres(), FakeLLM()

    await _compaction(postgres, llm).extend(
        _turns(30), conversation_tokens=400, settings=SETTINGS
    )

    # ~102 tokens a turn and a 1,000 token cap: turns 1-9 now, the rest later.
    assert "turn9 " in llm.prompts[0] and "turn10 " not in llm.prompts[0]
    assert postgres.row["chat_summary_through_message_id"] == 9
