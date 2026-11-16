import pytest

from common.schema.settings import ChatCompactionSettings
from core.session.chat_compaction import ChatCompaction


class OneLineLLM:
    async def generate_text(self, _system, _user, **_kwargs):
        return "- Ada chose SQLite for the prototype."


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_chat_summary_is_stored_on_the_session_and_cleared_on_delete(real_postgres_client):
    from core.knowledge.db.writers.session_deletion_writer import SessionDeletionWriter

    await real_postgres_client.execute(
        "INSERT INTO public.projects (project_id, user_name, name, domain_config) "
        "VALUES ('p1', 'ada', 'P1', '{}'::jsonb)"
    )
    await real_postgres_client.execute(
        "INSERT INTO public.sessions (session_id, user_name, project_id, status) "
        "VALUES ('s1', 'ada', 'p1', 'open')"
    )
    compaction = ChatCompaction(real_postgres_client, OneLineLLM(), user_name="ada", session_id="s1")
    turns = [
        {"message_id": index, "role": "user", "content": "x" * 400} for index in range(1, 6)
    ]

    assert await compaction.load() == ("", 0)
    assert await compaction.extend(
        turns, conversation_tokens=150,
        settings=ChatCompactionSettings(min_dropped_tokens=100),
    )
    assert await compaction.load() == ("- Ada chose SQLite for the prototype.", 4)

    await SessionDeletionWriter(real_postgres_client).delete_session(user_name="ada", session_id="s1")
    assert (await compaction.load())[0] == ""
