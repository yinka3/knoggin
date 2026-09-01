import pytest


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.requires_pgvector
@pytest.mark.no_network
async def test_episode_lexical_search_vector_is_stored_and_gin_indexed(
    real_postgres_client,
):
    await real_postgres_client.execute(
        """
        INSERT INTO episodes (
            episode_id,
            project_id,
            summary,
            new_developments,
            updates,
            unresolved
        ) VALUES (
            'episode-lexical',
            'project-1',
            'The team selected episodic memory.',
            '["Episode retrieval supports lexical matching."]'::jsonb,
            '[]'::jsonb,
            '["Choose a ranking policy."]'::jsonb
        )
        """
    )

    match = await real_postgres_client.fetch_one(
        """
        SELECT search_tsvector @@ websearch_to_tsquery('simple', %s) AS matches
        FROM episodes
        WHERE episode_id = 'episode-lexical'
        """,
        ("episodic memory",),
    )
    index = await real_postgres_client.fetch_one(
        """
        SELECT indexdef
        FROM pg_indexes
        WHERE schemaname = 'public'
          AND indexname = 'episodes_search_tsvector_idx'
        """
    )

    assert match == {"matches": True}
    assert index is not None
    assert "USING gin (search_tsvector)" in index["indexdef"]

    async with real_postgres_client.transaction() as cursor:
        await cursor.execute("SET LOCAL enable_seqscan = off")
        await cursor.execute("SET LOCAL enable_indexscan = off")
        await cursor.execute(
            """
            EXPLAIN (COSTS OFF)
            SELECT episode_id
            FROM episodes
            WHERE search_tsvector @@ websearch_to_tsquery('simple', %s)
            """,
            ("episodic memory",),
        )
        plan = "\n".join(row["QUERY PLAN"] for row in await cursor.fetchall())

    assert "Bitmap Index Scan on episodes_search_tsvector_idx" in plan
