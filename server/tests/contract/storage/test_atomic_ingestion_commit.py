import pytest

from common.schema.ingestion.contracts import (
    AliasUpdate,
    EntityWrite,
    ExecutionScope,
    IngestionCommit,
    MessageEntityRef,
    RelationshipWrite,
)
from core.knowledge.db.writers.graph_writer import GraphWriter


async def _seed_claimed_message(client, *, batch_id: str = "claim-1") -> None:
    await client.execute(
        """
        INSERT INTO public.sessions (session_id, user_name, project_id)
        VALUES ('session-1', 'ada', 'project-1')
        """
    )
    await client.execute(
        """
        INSERT INTO public.messages (
            user_name, session_id, message_id, project_id, role, content,
            lifecycle_state, ingestion_state, ingestion_not_before_ms,
            ingestion_claim_id, ingestion_attempt_count, ingestion_last_failure_code
        ) VALUES (
            'ada', 'session-1', 101, 'project-1', 'user', 'Ada met Grace.',
            'sealed', 'claimed', 0, %s, 2, 'llm_provider_error'
        )
        """,
        (batch_id,),
    )


def _commit(
    *,
    batch_id: str = "claim-1",
    refs=(),
    entities=(),
    aliases=(),
    relationships=(),
) -> IngestionCommit:
    return IngestionCommit(
        scope=ExecutionScope(
            user_name="ada", project_id="project-1", session_id="session-1"
        ),
        batch_id=batch_id,
        message_ids=(101,),
        entity_writes=tuple(entities),
        alias_updates=tuple(aliases),
        message_entity_refs=tuple(refs),
        relationship_writes=tuple(relationships),
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_atomic_ingestion_commit_marks_exact_claim_processed(
    real_postgres_client,
):
    await _seed_claimed_message(real_postgres_client)

    summary = await GraphWriter(real_postgres_client).commit_ingestion(
        _commit(
            entities=(
                EntityWrite(
                    entity_id=201,
                    is_new=True,
                    canonical_name="Ada",
                    entity_type="person",
                    topic="People",
                    embedding=None,
                ),
                EntityWrite(
                    entity_id=202,
                    is_new=True,
                    canonical_name="Grace",
                    entity_type="person",
                    topic="People",
                    embedding=None,
                ),
            ),
            aliases=(AliasUpdate(entity_id=201, aliases=("Ada Lovelace",)),),
            refs=(
                MessageEntityRef(message_id=101, entity_id=201),
                MessageEntityRef(message_id=101, entity_id=202),
            ),
            relationships=(
                RelationshipWrite(
                    entity_a_id=201,
                    entity_b_id=202,
                    relationship_type="met",
                    message_id=101,
                    confidence=1.0,
                    context="Ada met Grace.",
                ),
            ),
        )
    )

    assert summary.entities_written == 2
    assert summary.relationships_written == 1
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.project_entity_contexts WHERE project_id = 'project-1'"
    ) == {"count": 2}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.relationship_observations WHERE message_id = 101"
    ) == {"count": 1}
    assert await real_postgres_client.fetch_all(
        "SELECT alias FROM public.entity_aliases WHERE entity_id = 201"
    ) == [{"alias": "Ada Lovelace"}]
    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_state, ingestion_claim_id, episode_eligible,
               ingestion_attempt_count, ingestion_last_failure_code
        FROM public.messages
        WHERE message_id = 101
        """
    ) == {
        "ingestion_state": "processed",
        "ingestion_claim_id": None,
        "episode_eligible": True,
        "ingestion_attempt_count": 2,
        "ingestion_last_failure_code": None,
    }


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_atomic_ingestion_commit_rejects_a_stale_claim_without_writing(
    real_postgres_client,
):
    await _seed_claimed_message(real_postgres_client)

    with pytest.raises(ValueError, match="exact claimed messages"):
        await GraphWriter(real_postgres_client).commit_ingestion(
            _commit(batch_id="different-claim")
        )

    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_state, ingestion_claim_id
        FROM public.messages
        WHERE message_id = 101
        """
    ) == {"ingestion_state": "claimed", "ingestion_claim_id": "claim-1"}


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_atomic_ingestion_commit_rolls_back_graph_work_and_claim_completion(
    real_postgres_client,
):
    await _seed_claimed_message(real_postgres_client)

    with pytest.raises(ValueError, match="outside scope"):
        await GraphWriter(real_postgres_client).commit_ingestion(
            _commit(refs=(MessageEntityRef(message_id=101, entity_id=999),))
        )

    assert await real_postgres_client.fetch_one(
        """
        SELECT ingestion_state, ingestion_claim_id
        FROM public.messages
        WHERE message_id = 101
        """
    ) == {"ingestion_state": "claimed", "ingestion_claim_id": "claim-1"}
    assert await real_postgres_client.fetch_one(
        "SELECT count(*) AS count FROM public.message_entity_refs"
    ) == {"count": 0}
