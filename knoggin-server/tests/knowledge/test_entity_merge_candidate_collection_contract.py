import pytest

from common.schema.primitives import FactRecord


def seed_entity(
    entities,
    knowledge_store,
    entity_id,
    canonical_name,
    *,
    aliases=None,
    entity_type="person",
    topic="Identity",
    project_id="project-1",
    embedding=None,
):
    entity = knowledge_store.add_entity(
        entity_id,
        canonical_name,
        aliases=aliases,
        entity_type=entity_type,
        topic=topic,
        project_id=project_id,
        embedding=embedding,
    )
    entities._populate_cache(entity)
    return entity


def make_fact(entity_id, content):
    return FactRecord(
        id=f"fact-{entity_id}",
        content=content,
        source_entity_id=entity_id,
        source_msg_id=1,
    )


@pytest.mark.storage
@pytest.mark.no_network
async def test_detect_merge_candidates_empty_cache_skips_graph_search(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness

    candidates = await entities.detect_merge_entity_candidates()

    assert candidates == []
    assert knowledge_store.similar_entity_searches == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_restricts_scan_to_dirty_ids(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "Robert Chen", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "Robert Chen Jr", embedding=[1.0, 0.1])
    seed_entity(entities, knowledge_store, 303, "Knoggin", embedding=[0.0, 1.0])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]
    knowledge_store.similar_entities_by_id[303] = [(202, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens=set())

    assert set(pairs) == {(101, 202)}
    assert knowledge_store.similar_entity_searches == [
        {
            "entity_id": 101,
            "limit": 50,
            "visible_project_ids": ["project-1"],
        }
    ]


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_skips_self_and_dedupes_reverse_pairs(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "Robert Chen", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "Robert Chen Jr", embedding=[1.0, 0.1])
    knowledge_store.similar_entities_by_id[101] = [(101, 1.0), (202, 0.95)]
    knowledge_store.similar_entities_by_id[202] = [(101, 0.95)]

    pairs = await entities._collect_candidate_pairs([101, 202], generic_tokens=set())

    assert set(pairs) == {(101, 202)}
    assert pairs[(101, 202)]["reasons"] == ["name_similarity"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_accepts_strong_fuzzy_name_similarity(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "Robert Chen", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "Robert Chen Jr", embedding=[1.0, 0.1])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens=set())

    assert pairs[(101, 202)]["fuzz_score"] >= entities.fuzzy_non_substring_threshold
    assert pairs[(101, 202)]["reasons"] == ["name_similarity"]


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_rejects_weak_fuzzy_and_low_cosine_match(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "Alice", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "Linear", embedding=[0.0, 1.0])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens=set())

    assert pairs == {}


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_accepts_high_cosine_vector_similarity(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "Alice", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "Linear", embedding=[0.95, 0.05])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens=set())

    assert pairs == {
        (101, 202): {
            "fuzz_score": 0,
            "reasons": ["vector_similarity"],
        }
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_rejects_short_substring_noise(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "AI", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "OpenAI", embedding=[0.0, 1.0])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens=set())

    assert pairs == {}


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_filters_generic_token_overlap(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "project", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "project alpha", embedding=[0.0, 1.0])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens={"project"})

    assert pairs == {}


@pytest.mark.storage
@pytest.mark.no_network
async def test_collect_candidate_pairs_skips_missing_neighbor_profiles(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    seed_entity(entities, knowledge_store, 101, "Robert Chen", embedding=[1.0, 0.0])
    knowledge_store.similar_entities_by_id[101] = [(999, 0.95)]

    pairs = await entities._collect_candidate_pairs([101], generic_tokens=set())

    assert pairs == {}
    assert knowledge_store.profile_lookups[-1] == {
        "entity_id": 999,
        "visible_project_ids": ["project-1"],
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_detect_merge_candidates_returns_facts_scores_and_reasons(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    fact_a = make_fact(101, "Robert Chen works on memory graphs.")
    fact_b = make_fact(202, "Robert Chen Jr works on memory graphs.")
    seed_entity(entities, knowledge_store, 101, "Robert Chen", embedding=[1.0, 0.0])
    seed_entity(entities, knowledge_store, 202, "Robert Chen Jr", embedding=[1.0, 0.1])
    knowledge_store.similar_entities_by_id[101] = [(202, 0.95)]
    knowledge_store.facts_by_entity[101] = [fact_a]
    knowledge_store.facts_by_entity[202] = [fact_b]

    candidates = await entities.detect_merge_entity_candidates(dirty_ids={101})

    assert len(candidates) == 1
    assert candidates[0]["primary_id"] == 101
    assert candidates[0]["secondary_id"] == 202
    assert candidates[0]["facts_a"] == [fact_a]
    assert candidates[0]["facts_b"] == [fact_b]
    assert candidates[0]["fuzz_score"] >= entities.fuzzy_non_substring_threshold
    assert candidates[0]["reasons"] == ["name_similarity"]
    assert knowledge_store.facts_for_entities_calls == [
        {"entity_ids": [101, 202], "active_only": True}
    ]
