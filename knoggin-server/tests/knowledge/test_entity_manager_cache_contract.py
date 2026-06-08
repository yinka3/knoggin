import pytest


@pytest.mark.storage
@pytest.mark.no_network
def test_populate_cache_loads_profiles_names_and_aliases(entity_manager_harness):
    entities, _, _ = entity_manager_harness

    profile = entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob", "Bobby"],
            "type": "person",
            "topic": "Identity",
            "project_id": "project-1",
            "embedding": [0.1, 0.2, 0.3],
        }
    )

    assert profile == {
        "canonical_name": "Robert Chen",
        "type": "person",
        "topic": "Identity",
        "project_id": "project-1",
        "embedding": [0.1, 0.2, 0.3],
    }
    assert entities.get_known_aliases()["robert chen"] == 101
    assert entities.get_known_aliases()["bob"] == 101
    assert entities.get_known_aliases()["bobby"] == 101
    assert set(entities.get_mentions_for_id(101)) == {
        "robert chen",
        "bob",
        "bobby",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_uses_cache_before_graph_lookup(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
            "type": "person",
            "topic": "Identity",
            "project_id": "project-1",
        }
    )

    entity_id = await entities.get_id("Bob")

    assert entity_id == 101
    assert graph.name_lookups == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_fetches_unknown_name_with_project_scope(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    graph.add_entity(202, "Knoggin", aliases=["memory project"], entity_type="project")

    entity_id = await entities.get_id("memory project")

    assert entity_id == 202
    assert graph.name_lookups == [
        {
            "names": ["memory project"],
            "visible_project_ids": ["project-1"],
        }
    ]
    assert entities.get_known_aliases()["knoggin"] == 202
    assert entities.get_known_aliases()["memory project"] == 202


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_respects_readable_project_scope(entity_manager_harness):
    entities, graph, _ = entity_manager_harness
    graph.add_entity(
        303,
        "Linear",
        entity_type="tool",
        topic="General",
        project_id="private-project",
    )

    entity_id = await entities.get_id("Linear")

    assert entity_id is None
    assert graph.name_lookups[-1]["visible_project_ids"] == ["project-1"]
    assert "linear" not in entities.get_known_aliases()


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_profile_uses_cache_then_fetches_missing_profiles(
    entity_manager_harness,
):
    entities, graph, _ = entity_manager_harness
    entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
            "type": "person",
            "topic": "Identity",
            "project_id": "project-1",
        }
    )
    graph.add_entity(202, "Knoggin", aliases=["memory project"], entity_type="project")

    cached = await entities.get_profile(101)
    fetched = await entities.get_profile(202)
    missing = await entities.get_profile(999)

    assert cached["canonical_name"] == "Robert Chen"
    assert fetched["canonical_name"] == "Knoggin"
    assert missing is None
    assert graph.profile_lookups == [
        {"entity_id": 202, "visible_project_ids": ["project-1"]},
        {"entity_id": 999, "visible_project_ids": ["project-1"]},
    ]
    assert entities.get_known_aliases()["memory project"] == 202


@pytest.mark.storage
@pytest.mark.no_network
def test_known_aliases_and_mentions_are_lowercase_cache_truth(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Ada Lovelace",
            "aliases": ["Countess Ada", "ADA"],
            "type": "person",
            "topic": "Identity",
            "project_id": "project-1",
        }
    )

    aliases = entities.get_known_aliases()

    assert aliases["ada lovelace"] == 101
    assert aliases["countess ada"] == 101
    assert aliases["ada"] == 101
    assert set(entities.get_mentions_for_id(101)) == {
        "ada lovelace",
        "countess ada",
        "ada",
    }


@pytest.mark.storage
@pytest.mark.no_network
async def test_register_entity_updates_profile_aliases_and_embedding(
    entity_manager_harness,
):
    entities, _, embedding = entity_manager_harness

    vector = await entities.register_entity(
        404,
        "Notion",
        ["Notion", "workspace notes"],
        "tool",
        "General",
        session_id="session-1",
        source_context="Used for workspace notes.",
    )

    assert vector == embedding.vector_for(
        "Notion (tool). Context: Used for workspace notes."
    )
    assert embedding.single_calls == [
        "Notion (tool). Context: Used for workspace notes."
    ]
    profile = await entities.get_profile(404)
    assert profile == {
        "canonical_name": "Notion",
        "type": "tool",
        "topic": "General",
        "project_id": "project-1",
        "embedding": vector,
    }
    assert entities.get_known_aliases()["notion"] == 404
    assert entities.get_known_aliases()["workspace notes"] == 404
    assert set(entities.get_mentions_for_id(404)) == {"notion", "workspace notes"}


@pytest.mark.storage
@pytest.mark.no_network
async def test_register_entity_skips_alias_collisions(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    await entities.register_entity(
        101,
        "Robert Chen",
        ["Robert Chen", "Bob"],
        "person",
        "Identity",
    )

    await entities.register_entity(
        202,
        "Bob Smith",
        ["Bob Smith", "Bob"],
        "person",
        "Identity",
    )

    aliases = entities.get_known_aliases()
    assert aliases["bob"] == 101
    assert aliases["bob smith"] == 202
    assert "bob" not in set(entities.get_mentions_for_id(202))


@pytest.mark.storage
@pytest.mark.no_network
async def test_compute_embedding_updates_known_profile_and_skips_unknown(
    entity_manager_harness,
):
    entities, _, embedding = entity_manager_harness
    entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Knoggin",
            "aliases": [],
            "type": "project",
            "topic": "General",
            "project_id": "project-1",
            "embedding": [0.0],
        }
    )

    vector = await entities.compute_embedding(
        101,
        "Knoggin (project). Context: Builds a memory graph.",
    )
    missing_vector = await entities.compute_embedding(999, "Unknown")

    assert vector == embedding.vector_for(
        "Knoggin (project). Context: Builds a memory graph."
    )
    assert missing_vector == []
    assert (await entities.get_profile(101))["embedding"] == vector
    assert embedding.single_calls == [
        "Knoggin (project). Context: Builds a memory graph."
    ]


@pytest.mark.storage
@pytest.mark.no_network
def test_remove_entities_clears_profiles_names_and_aliases_only_for_removed_ids(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
            "type": "person",
            "topic": "Identity",
            "project_id": "project-1",
        }
    )
    entities._populate_cache(
        {
            "id": 202,
            "canonical_name": "Knoggin",
            "aliases": ["memory project"],
            "type": "project",
            "topic": "General",
            "project_id": "project-1",
        }
    )

    removed = entities.remove_entities([101])

    assert removed == 1
    aliases = entities.get_known_aliases()
    assert "robert chen" not in aliases
    assert "bob" not in aliases
    assert aliases["knoggin"] == 202
    assert aliases["memory project"] == 202
    assert entities.get_mentions_for_id(101) == []
    assert set(entities.get_mentions_for_id(202)) == {"knoggin", "memory project"}
