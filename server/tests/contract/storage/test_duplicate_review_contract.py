import pytest

from core.knowledge.entity.maintenance_service import EntityMaintenanceService
from core.knowledge.entity.merge_advisor import EntityMergeAdvice


class MergeLLM:
    merge_model = "merge-model-test"

    def __init__(self, advice):
        self.advice = advice
        self.calls = 0

    async def generate_structured(self, **_kwargs):
        self.calls += 1
        return self.advice


async def _seed_duplicates(pg):
    """Two stored identities for one person, linked by an alias and each
    mentioned in its own message."""

    await pg.execute(
        "INSERT INTO public.entities (entity_id, user_name, canonical_name) "
        "VALUES (2, 'ada', 'Ada Lovelace'), (3, 'ada', 'Augusta Ada King')"
    )
    await pg.execute(
        """INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 2, 'ada', 'Concept', 'General'),
                  ('project-1', 3, 'ada', 'Concept', 'General')"""
    )
    await pg.execute("INSERT INTO public.entity_aliases (entity_id, alias) VALUES (2, 'Augusta Ada King')")
    await pg.execute(
        "INSERT INTO public.sessions (session_id, user_name, project_id, status) "
        "VALUES ('chat', 'ada', 'project-1', 'open')"
    )
    for message_id, text in ((901, "Ada Lovelace wrote the first program."),
                             (902, "Augusta Ada King is Ada Lovelace.")):
        await _mention(pg, message_id, text, 2 if message_id == 901 else 3)


async def _mention(pg, message_id, text, entity_id):
    # An open exchange is not waiting on semantic memory, so merges are allowed.
    await pg.execute(
        """INSERT INTO public.messages
               (user_name, session_id, message_id, project_id, role, content, timestamp_ms)
           VALUES ('ada', 'chat', %s, 'project-1', 'user', %s, %s)""",
        (message_id, text, message_id),
    )
    await pg.execute(
        "INSERT INTO public.message_entity_refs (message_id, entity_id) VALUES (%s, %s)",
        (message_id, entity_id),
    )


async def _reviews(pg):
    return await pg.fetch_all(
        "SELECT dedupe_key, status FROM public.maintenance_reviews WHERE kind = 'entity_merge'"
    )


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_confirmed_duplicate_becomes_one_review_and_is_not_proposed_again(real_postgres_client):
    pg = real_postgres_client
    await _seed_duplicates(pg)
    llm = MergeLLM(EntityMergeAdvice(
        verdict="merge", preferred_survivor_id=2,
        reasoning="Message 902 says they are the same person.",
        evidence_message_ids=[901, 902],
    ))
    service = EntityMaintenanceService(pg, "ada", llm=llm)

    first = await service.review_duplicates(max_pairs=10)
    assert first == {"checked": 1, "proposed": 1, "blocked": 0, "failed": 0}
    assert await _reviews(pg) == [{"dedupe_key": "2:3", "status": "open"}]
    # Nothing was merged: both identities are still active.
    assert {row["status"] for row in await pg.fetch_all("SELECT status FROM public.entities WHERE entity_id IN (2, 3)")} == {"active"}

    # Open, then dismissed by the user: never sent to the model again.
    await service.review_duplicates(max_pairs=10)
    await pg.execute("UPDATE public.maintenance_reviews SET status = 'dismissed'")
    await _mention(pg, 903, "Ada again.", 3)
    await service.review_duplicates(max_pairs=10)
    assert llm.calls == 1 and len(await _reviews(pg)) == 1


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_pair_kept_separate_is_checked_again_only_after_it_changes(real_postgres_client):
    pg = real_postgres_client
    await _seed_duplicates(pg)
    llm = MergeLLM(EntityMergeAdvice(verdict="keep_separate", reasoning="Different people."))
    service = EntityMaintenanceService(pg, "ada", llm=llm)

    assert (await service.review_duplicates(max_pairs=10))["checked"] == 1
    assert (await service.review_duplicates(max_pairs=10))["checked"] == 0
    assert llm.calls == 1

    await _mention(pg, 903, "Augusta Ada King again.", 3)
    assert (await service.review_duplicates(max_pairs=10))["checked"] == 1
    assert llm.calls == 2 and await _reviews(pg) == []


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_review_checks_at_most_max_pairs(real_postgres_client):
    pg = real_postgres_client
    await _seed_duplicates(pg)
    await pg.execute(
        "INSERT INTO public.entities (entity_id, user_name, canonical_name) "
        "VALUES (4, 'ada', 'Rae'), (5, 'ada', 'Rae')"
    )
    await pg.execute(
        """INSERT INTO public.project_entity_contexts
               (project_id, entity_id, user_name, entity_type, topic)
           VALUES ('project-1', 4, 'ada', 'Concept', 'General'),
                  ('project-1', 5, 'ada', 'Concept', 'General')"""
    )
    await _mention(pg, 904, "Rae.", 4)
    await _mention(pg, 905, "Rae.", 5)
    llm = MergeLLM(EntityMergeAdvice(verdict="uncertain", reasoning="Thin evidence."))
    service = EntityMaintenanceService(pg, "ada", llm=llm)

    assert (await service.review_duplicates(max_pairs=1))["checked"] == 1
    assert (await service.review_duplicates(max_pairs=1))["checked"] == 1
    assert (await service.review_duplicates(max_pairs=1))["checked"] == 0
    assert llm.calls == 2


@pytest.mark.storage
@pytest.mark.requires_postgres
@pytest.mark.no_network
async def test_a_pair_waiting_on_memory_is_skipped_without_a_model_call(real_postgres_client):
    pg = real_postgres_client
    await _seed_duplicates(pg)
    # A finished exchange not yet built into memory blocks merges in its project.
    await pg.execute(
        """UPDATE public.messages SET exchange_state = 'closed', exchange_outcome = 'user_only',
                  exchange_closed_at_ms = 1 WHERE message_id = 902"""
    )
    llm = MergeLLM(EntityMergeAdvice(verdict="keep_separate", reasoning="Different people."))
    service = EntityMaintenanceService(pg, "ada", llm=llm)

    assert (await service.review_duplicates(max_pairs=10))["checked"] == 0
    assert llm.calls == 0
    assert await pg.fetch_all("SELECT 1 FROM public.entity_duplicate_checks") == []
