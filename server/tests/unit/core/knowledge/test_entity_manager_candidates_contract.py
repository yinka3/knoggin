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
    record = entities.knowledge_store.add_entity(
        entity_id,
        canonical_name,
        aliases=aliases,
        entity_type=entity_type,
        topic=topic,
        project_id=project_id,
        embedding=embedding,
    )
    entities._populate_cache(record)


@pytest.mark.storage
@pytest.mark.no_network
async def test_exact_alias_match_returns_score_one(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 101, "Robert Chen", aliases=["Bob"])

    candidates = await entities.get_candidate_ids("Bob")

    assert candidates == [(101, 1.0)]
    assert candidates[0].signals == {"exact"}
    assert candidates[0].has_direct_name_evidence is True


@pytest.mark.storage
@pytest.mark.no_network
async def test_ambiguous_exact_alias_returns_candidates_without_direct_evidence(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 101, "Robert Chen", aliases=["Bob"])
    await seed_entity(entities, 202, "Bob Smith", aliases=["Bob"])

    candidates = await entities.get_candidate_ids(" Bob ")

    assert {candidate.entity_id for candidate in candidates} == {101, 202}
    for candidate in candidates:
        assert candidate.score == pytest.approx(1.0)
        assert candidate.signals == {"exact", "ambiguous_alias"}
        assert candidate.has_direct_name_evidence is False


@pytest.mark.storage
@pytest.mark.no_network
async def test_unique_exact_name_is_not_blocked_by_a_shared_fuzzy_alias(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 101, "Avery Stone", aliases=["Avery"])
    await seed_entity(entities, 202, "Avery Quinn", aliases=["Avery"])

    candidates = await entities.get_candidate_ids("Avery Stone")
    target = next(candidate for candidate in candidates if candidate.entity_id == 101)

    assert target.has_direct_name_evidence is True
    assert "ambiguous_alias" not in target.signals


@pytest.mark.storage
@pytest.mark.no_network
async def test_cold_resolver_hydrates_a_durable_exact_alias(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(101, "Robert Chen", aliases=["Bob"])

    candidates = await entities.get_candidate_ids("  BOB  ")

    assert candidates == [(101, 1.0)]
    assert candidates[0].signals == {"exact"}
    assert entities.has_cached_entity(101)
    assert knowledge_store.name_lookups == [
        {"names": ["bob"], "visible_project_ids": ["project-1"]}
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_durable_exact_lookup_preserves_hidden_warm_alias_owner_ambiguity(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    first = knowledge_store.add_entity(101, "Robert Chen", aliases=["Bob"])
    knowledge_store.add_entity(202, "Bob Smith", aliases=["Bob"])
    entities._populate_cache(first)

    candidates = await entities.get_candidate_ids("Bob")

    assert [candidate.entity_id for candidate in candidates] == [101, 202]
    for candidate in candidates:
        assert candidate.signals == {"exact", "ambiguous_alias"}
        assert candidate.has_direct_name_evidence is False


@pytest.mark.storage
@pytest.mark.no_network
async def test_strict_durable_exact_lookup_failure_is_not_treated_as_absence(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.fail_name_lookup = True

    with pytest.raises(RuntimeError, match="name lookup failed"):
        await entities.get_candidate_ids("Bob", strict=True)


@pytest.mark.storage
@pytest.mark.no_network
async def test_inactive_and_inaccessible_exact_owners_are_not_candidates(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(101, "Robert Chen", aliases=["Bob"])
    knowledge_store.add_entity(
        202,
        "Retired Bob",
        aliases=["Bob"],
        status="retired",
    )
    knowledge_store.add_entity(
        303,
        "Other Project Bob",
        aliases=["Bob"],
        project_id="project-2",
    )

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
    assert candidates[0].signals == {"fuzzy"}
    assert candidates[0].has_direct_name_evidence is False


@pytest.mark.storage
@pytest.mark.no_network
async def test_warm_retired_fuzzy_candidate_is_revalidated_before_return(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    await seed_entity(entities, 202, "Knoggin", entity_type="project", topic="General")
    knowledge_store.entities[202]["status"] = "retired"

    candidates = await entities.get_candidate_ids("Knogin")

    assert candidates == []


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
    assert candidates[0].signals == {"vector"}
    assert candidates[0].vector_score == pytest.approx(0.91)
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
    assert candidates[0].signals == {"exact", "vector"}
    assert candidates[0].exact_score == pytest.approx(1.0)
    assert candidates[0].vector_score == pytest.approx(0.88)


@pytest.mark.storage
@pytest.mark.no_network
async def test_missing_vector_candidate_ids_are_dropped(entity_manager_harness):
    entities, knowledge_store, embedding = entity_manager_harness
    vector = embedding.vector_for("unknown but similar")
    knowledge_store.vector_results[tuple(vector)] = [(999, 0.95)]

    candidates = await entities.get_candidate_ids("unknown but similar")

    assert candidates == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_vector_candidate_is_hydrated_from_durable_store(entity_manager_harness):
    entities, knowledge_store, embedding = entity_manager_harness
    knowledge_store.add_entity(999, "Persisted Similar", entity_type="concept")
    vector = embedding.vector_for("unseen embedding match")
    knowledge_store.vector_results[tuple(vector)] = [(999, 0.95)]

    candidates = await entities.get_candidate_ids("unseen embedding match")

    assert candidates == [(999, 0.95)]
    assert entities.has_cached_entity(999)


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
    assert candidates[0].signals == {"vector"}
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
