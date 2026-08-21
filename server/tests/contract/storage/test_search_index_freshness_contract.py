import pytest


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_message_fts_updates_from_canonical_content(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await real_postgres_client.execute(
        """
        INSERT INTO messages (
            user_name, session_id, message_id, project_id, role, content
        )
        VALUES ('ada', 'session-1', 7, 'project-1', 'user', 'stale content')
        """
    )
    await real_postgres_client.execute(
        "UPDATE messages SET content = %s WHERE message_id = %s",
        ("fresh canonical content", 7),
    )

    indexed = await real_postgres_client.fetch_one(
        """
        SELECT search_tsvector @@ plainto_tsquery('english', %s) AS matches
        FROM messages
        WHERE message_id = %s
        """,
        ("fresh", 7),
    )

    assert indexed == {"matches": True}
