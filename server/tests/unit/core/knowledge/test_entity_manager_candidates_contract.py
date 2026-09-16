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
):
    record = entities.knowledge_store.add_entity(
        entity_id,
        canonical_name,
        aliases=aliases,
        entity_type=entity_type,
        topic=topic,
        project_id=project_id,
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
async def test_standalone_lookup_uses_one_private_durable_snapshot(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(101, "Robert Chen", aliases=["Bob"])
    alias_version = entities.get_alias_version()

    candidates = await entities.get_candidate_ids("  BOB  ")

    assert candidates == [(101, 1.0)]
    assert candidates[0].signals == {"exact"}
    assert entities.has_cached_entity(101) is False
    assert entities.get_alias_version() == alias_version
    assert knowledge_store.catalog_lookups == [{"visible_project_ids": ["project-1"]}]
    assert knowledge_store.name_lookups == []


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
async def test_candidate_catalog_failure_is_not_treated_as_absence(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.fail_catalog_lookup = True

    with pytest.raises(RuntimeError, match="candidate catalog lookup failed"):
        await entities.get_candidate_ids("Bob")


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
async def test_next_snapshot_uses_transferred_alias_not_a_stale_runtime_owner(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    former_owner = knowledge_store.add_entity(101, "Robert Chen", aliases=["Bob"])
    entities._populate_cache(former_owner)
    knowledge_store.entities[101]["aliases"] = []
    knowledge_store.add_entity(202, "Bob Smith", aliases=["Bob"])

    candidates = await entities.get_candidate_ids("Bob")

    assert candidates == [(202, 1.0)]
    assert entities.get_entity_ids_for_name("Bob") == {101}


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
async def test_candidates_are_sorted_strongest_first(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await seed_entity(entities, 101, "Knoggin", aliases=["memory project"])
    await seed_entity(
        entities, 202, "Memory projects", entity_type="tool", topic="General"
    )

    candidates = await entities.get_candidate_ids("memory project")

    assert candidates[0] == (101, 1.0)
    assert candidates[1].entity_id == 202


@pytest.mark.storage
@pytest.mark.no_network
async def test_cold_fuzzy_matching_loads_only_visible_durable_entities(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(202, "Knoggin", entity_type="project")
    knowledge_store.add_entity(303, "Knoggin Other", project_id="project-2")

    candidates = await entities.get_candidate_ids("Knogin")

    assert [candidate.entity_id for candidate in candidates] == [202]


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
