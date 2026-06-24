import pytest


async def seed_entity(
    entities,
    entity_id,
    canonical_name,
    *,
    aliases=None,
    entity_type="person",
    topic="Identity",
    project_id="project-1",
    embedding=None,
):
    entities._populate_cache(
        {
            "id": entity_id,
            "canonical_name": canonical_name,
            "aliases": list(aliases or []),
            "type": entity_type,
            "topic": topic,
            "project_id": project_id,
            "embedding": embedding,
        }
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_exact_alias_match_returns_score_one(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 101, "Robert Chen", aliases=["Bob"])

    candidates = await entities.get_candidate_ids("Bob")

    assert candidates == [(101, 1.0)]


@pytest.mark.storage
@pytest.mark.no_network
async def test_fuzzy_match_above_threshold_is_returned(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 202, "Knoggin", entity_type="project", topic="General")

    candidates = await entities.get_candidate_ids("Knogin")

    assert candidates
    assert candidates[0][0] == 202
    assert 0.85 <= candidates[0][1] < 1.0


@pytest.mark.storage
@pytest.mark.no_network
async def test_fuzzy_match_below_threshold_is_ignored(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 202, "Knoggin", entity_type="project", topic="General")

    candidates = await entities.get_candidate_ids("Calendar")

    assert candidates == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_short_names_avoid_accidental_high_confidence_reuse(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 301, "Alice")
    await seed_entity(entities, 302, "Bob")

    candidates = await entities.get_candidate_ids("AI")

    assert candidates == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_vector_candidate_is_included_when_similarity_finds_it(
    entity_manager_harness,
):
    entities, knowledge_store, embedding = entity_manager_harness
    await seed_entity(entities, 404, "Linear", entity_type="tool", topic="General")
    vector = embedding.vector_for("project planning app")
    knowledge_store.vector_results[tuple(vector)] = [(404, 0.91)]

    candidates = await entities.get_candidate_ids("project planning app")

    assert candidates == [(404, 0.91)]
    assert knowledge_store.vector_searches[-1]["vector"] == vector


@pytest.mark.storage
@pytest.mark.no_network
async def test_duplicate_exact_and_vector_candidates_keep_max_score(
    entity_manager_harness,
):
    entities, knowledge_store, embedding = entity_manager_harness
    await seed_entity(entities, 404, "Linear", aliases=["planning app"])
    vector = embedding.vector_for("planning app")
    knowledge_store.vector_results[tuple(vector)] = [(404, 0.88)]

    candidates = await entities.get_candidate_ids("planning app")

    assert candidates == [(404, 1.0)]


@pytest.mark.storage
@pytest.mark.no_network
async def test_unhydrated_vector_candidate_ids_are_dropped(entity_manager_harness):
    entities, knowledge_store, embedding = entity_manager_harness
    vector = embedding.vector_for("unknown but similar")
    knowledge_store.vector_results[tuple(vector)] = [(999, 0.95)]

    candidates = await entities.get_candidate_ids("unknown but similar")

    assert candidates == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_precomputed_mention_embedding_is_reused(entity_manager_harness):
    entities, knowledge_store, embedding = entity_manager_harness
    await seed_entity(entities, 505, "Notion", entity_type="tool", topic="General")
    precomputed = [9.0, 8.0, 7.0]
    knowledge_store.vector_results[tuple(precomputed)] = [(505, 0.93)]

    candidates = await entities.get_candidate_ids(
        "workspace notes tool",
        precomputed_embedding=precomputed,
    )

    assert candidates == [(505, 0.93)]
    assert embedding.single_calls == []
    assert knowledge_store.vector_searches[-1]["vector"] == precomputed


@pytest.mark.storage
@pytest.mark.no_network
async def test_embedding_failure_falls_back_to_cache_and_fuzzy_candidates(
    entity_manager_harness,
):
    entities, _, embedding = entity_manager_harness
    embedding.fail_single = True
    await seed_entity(entities, 202, "Knoggin", entity_type="project", topic="General")

    candidates = await entities.get_candidate_ids("Knogin")

    assert candidates
    assert candidates[0][0] == 202
    assert 0.85 <= candidates[0][1] < 1.0


@pytest.mark.storage
@pytest.mark.no_network
async def test_candidates_are_sorted_strongest_first(entity_manager_harness):
    entities, knowledge_store, embedding = entity_manager_harness
    await seed_entity(entities, 101, "Knoggin", aliases=["memory project"])
    await seed_entity(entities, 202, "Linear", entity_type="tool", topic="General")
    vector = embedding.vector_for("memory project")
    knowledge_store.vector_results[tuple(vector)] = [(202, 0.92)]

    candidates = await entities.get_candidate_ids("memory project")

    assert candidates == [(101, 1.0), (202, 0.92)]


@pytest.mark.storage
@pytest.mark.no_network
async def test_readable_project_ids_are_passed_to_vector_search(
    entity_manager_harness,
):
    entities, knowledge_store, embedding = entity_manager_harness
    vector = embedding.vector_for("project planning app")

    await entities.get_candidate_ids("project planning app")

    assert knowledge_store.vector_searches[-1] == {
        "vector": vector,
        "limit": 5,
        "score_threshold": 0.85,
        "visible_project_ids": ["project-1"],
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_candidate_fuzzy_threshold_changes_results(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 202, "Knoggin", entity_type="project", topic="General")
    entities.candidate_fuzzy_threshold = 95

    strict_candidates = await entities.get_candidate_ids("Knogin")
    entities.candidate_fuzzy_threshold = 80
    loose_candidates = await entities.get_candidate_ids("Knogin")

    assert strict_candidates == []
    assert loose_candidates
    assert loose_candidates[0][0] == 202
