import pytest

from core.knowledge.entity.index import EntityIndex
from core.knowledge.entity.profile import EntityProfile


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_populate_register_and_alias_views_are_coherent():
    index = EntityIndex()

    profile, changed = index.populate(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
            "type": "person",
            "topic": "Identity",
            "project_id": "project-1",
            "embedding": [0.1, 0.2],
        }
    )
    registered_changed = index.register(
        202,
        EntityProfile(
            canonical_name="Knoggin",
            entity_type="project",
            topic="General",
            project_id="project-1",
            embedding=[0.3, 0.4],
        ),
        "Knoggin",
        ["Memory Project"],
    )

    assert changed is True
    assert registered_changed is True
    assert index.get_profile(101) == profile
    assert index.has_entity(202) is True
    assert set(index.iter_profile_ids()) == {101, 202}
    assert index.get_entity_id_for_name(" Bob ") == 101
    assert index.get_entity_id_for_name("memory project") == 202
    assert set(index.get_mentions(101)) == {"robert chen", "bob"}
    assert index.get_aliases()["knoggin"] == 202


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_alias_collisions_and_idempotent_commits():
    index = EntityIndex()
    index.populate(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
        }
    )
    index.populate({"id": 202, "canonical_name": "Bob Smith", "aliases": []})

    changed = index.commit_aliases(202, ["Bob", "B. Smith"])
    second_changed = index.commit_aliases(202, ["B. Smith"])

    assert changed is True
    assert second_changed is False
    assert index.get_entity_id_for_name("bob") == 101
    assert index.get_entity_id_for_name("b. smith") == 202
    assert "bob" not in set(index.get_mentions(202))


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_populate_preserves_shared_alias_ambiguity():
    index = EntityIndex()
    index.populate(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
        }
    )

    _, changed = index.populate(
        {
            "id": 202,
            "canonical_name": "Bob Smith",
            "aliases": ["Bob"],
        }
    )

    assert changed is True
    assert index.get_entity_id_for_name("bob") is None
    assert index.get_entity_ids_for_name(" BOB ") == {101, 202}
    assert index.get_ambiguous_aliases()["bob"] == {101, 202}
    assert "bob" not in index.get_aliases()
    assert "bob" in set(index.get_mentions(101))
    assert "bob" in set(index.get_mentions(202))


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_normalizes_aliases_and_ignores_blanks():
    index = EntityIndex()

    _, changed = index.populate(
        {
            "id": 101,
            "canonical_name": " Bob ",
            "aliases": ["BOB", " ", ""],
        }
    )

    assert changed is True
    assert index.get_entity_id_for_name("bob") == 101
    assert index.get_entity_ids_for_name(" BOB ") == {101}
    assert set(index.iter_aliases()) == {"bob"}
    assert set(index.get_mentions(101)) == {"bob"}


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_remove_and_embedding_update_keep_views_coherent():
    index = EntityIndex()
    index.populate(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
            "embedding": [0.1],
        }
    )
    index.populate(
        {
            "id": 202,
            "canonical_name": "Rob Chen",
            "aliases": ["Robbie"],
            "embedding": [0.2],
        }
    )

    assert index.update_embedding(101, [0.9, 0.8]) is True
    assert index.get_profile(101).embedding == [0.9, 0.8]

    removed, aliases_changed = index.remove([101])

    assert removed == 1
    assert aliases_changed is True
    assert index.get_profile(101) is None
    assert index.get_entity_id_for_name("bob") is None
    assert index.get_mentions(101) == []
    assert index.get_profile(202) is not None
    assert index.get_entity_id_for_name("robbie") == 202


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_profile_eviction_removes_all_alias_views():
    index = EntityIndex(profile_maxsize=1, name_maxsize=10, names_by_id_maxsize=10)
    index.populate({"id": 101, "canonical_name": "Robert Chen", "aliases": ["Bob"]})
    index.populate({"id": 202, "canonical_name": "Grace Hopper", "aliases": []})

    assert index.get_profile(101) is None
    assert index.get_mentions(101) == []
    assert index.get_entity_id_for_name("robert chen") is None
    assert index.get_entity_id_for_name("bob") is None
    assert index.get_entity_id_for_name("grace hopper") == 202


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_alias_eviction_removes_the_inverse_entity_view():
    index = EntityIndex(profile_maxsize=10, name_maxsize=1, names_by_id_maxsize=10)
    index.populate({"id": 101, "canonical_name": "Robert Chen", "aliases": []})
    index.populate({"id": 202, "canonical_name": "Grace Hopper", "aliases": []})

    assert index.get_profile(101) is not None
    assert index.get_mentions(101) == []
    assert index.get_entity_id_for_name("robert chen") is None
    assert index.get_entity_id_for_name("grace hopper") == 202


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_index_entity_alias_eviction_removes_name_owners():
    index = EntityIndex(profile_maxsize=10, name_maxsize=10, names_by_id_maxsize=1)
    index.populate({"id": 101, "canonical_name": "Robert Chen", "aliases": []})
    index.populate({"id": 202, "canonical_name": "Grace Hopper", "aliases": []})

    assert index.get_profile(101) is not None
    assert index.get_mentions(101) == []
    assert index.get_entity_id_for_name("robert chen") is None
    assert index.get_entity_id_for_name("grace hopper") == 202


@pytest.mark.storage
@pytest.mark.no_network
def test_populate_cache_loads_profiles_names_and_aliases(entity_manager_harness):
    entities, _, _ = entity_manager_harness

    assert entities.get_alias_version() == 0
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

    assert profile == EntityProfile(
        canonical_name="Robert Chen",
        entity_type="person",
        topic="Identity",
        project_id="project-1",
        embedding=[0.1, 0.2, 0.3],
    )
    assert entities.get_known_aliases()["robert chen"] == 101
    assert entities.get_known_aliases()["bob"] == 101
    assert entities.get_known_aliases()["bobby"] == 101
    assert set(entities.get_mentions_for_id(101)) == {
        "robert chen",
        "bob",
        "bobby",
    }
    assert entities.get_alias_version() == 1


@pytest.mark.storage
@pytest.mark.no_network
def test_repeated_identical_populate_cache_does_not_bump_alias_version(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    record = {
        "id": 101,
        "canonical_name": "Robert Chen",
        "aliases": ["Bob", "Bobby"],
        "type": "person",
        "topic": "Identity",
        "project_id": "project-1",
        "embedding": [0.1, 0.2, 0.3],
    }

    entities._populate_cache(record)
    alias_version = entities.get_alias_version()
    entities._populate_cache(record)

    assert entities.get_alias_version() == alias_version


@pytest.mark.storage
@pytest.mark.no_network
def test_populate_cache_profile_only_update_does_not_bump_alias_version(
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
        }
    )
    alias_version = entities.get_alias_version()

    profile = entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
            "type": "person",
            "topic": "Work",
        }
    )

    assert profile.topic == "Work"
    assert entities.get_alias_version() == alias_version


@pytest.mark.storage
@pytest.mark.no_network
def test_populate_cache_shared_alias_bumps_alias_version(entity_manager_harness):
    entities, _, _ = entity_manager_harness
    entities._populate_cache(
        {
            "id": 101,
            "canonical_name": "Robert Chen",
            "aliases": ["Bob"],
        }
    )
    alias_version = entities.get_alias_version()

    entities._populate_cache(
        {
            "id": 202,
            "canonical_name": "Bob Smith",
            "aliases": ["Bob"],
        }
    )

    assert entities.get_alias_version() == alias_version + 1
    assert "bob" not in entities.get_known_aliases()
    assert entities.get_entity_ids_for_name("bob") == {101, 202}
    assert "bob" in set(entities.get_mentions_for_id(101))
    assert "bob" in set(entities.get_mentions_for_id(202))


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_uses_cache_before_graph_lookup(entity_manager_harness):
    entities, knowledge_store, _ = entity_manager_harness
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
    assert knowledge_store.name_lookups == []


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_fetches_unknown_name_with_project_scope(entity_manager_harness):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(
        202,
        "Knoggin",
        aliases=["memory project"],
        entity_type="project",
    )

    entity_id = await entities.get_id("memory project")

    assert entity_id == 202
    assert knowledge_store.name_lookups == [
        {
            "names": ["memory project"],
            "visible_project_ids": ["project-1"],
        }
    ]
    assert entities.get_known_aliases()["knoggin"] == 202
    assert entities.get_known_aliases()["memory project"] == 202


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_returns_none_when_storage_name_match_is_ambiguous(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(101, "Robert Chen", aliases=["Bob"])
    knowledge_store.add_entity(202, "Bob Smith", aliases=["Bob"])

    entity_id = await entities.get_id("Bob")

    assert entity_id is None
    assert entities.get_entity_ids_for_name("Bob") == {101, 202}
    assert "bob" not in entities.get_known_aliases()
    assert "bob" in set(entities.get_mentions_for_id(101))
    assert "bob" in set(entities.get_mentions_for_id(202))


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_id_respects_readable_project_scope(entity_manager_harness):
    entities, knowledge_store, _ = entity_manager_harness
    knowledge_store.add_entity(
        303,
        "Linear",
        entity_type="tool",
        topic="General",
        project_id="private-project",
    )

    entity_id = await entities.get_id("Linear")

    assert entity_id is None
    assert knowledge_store.name_lookups[-1]["visible_project_ids"] == ["project-1"]
    assert "linear" not in entities.get_known_aliases()


@pytest.mark.storage
@pytest.mark.no_network
async def test_get_profile_uses_cache_then_fetches_missing_profiles(
    entity_manager_harness,
):
    entities, knowledge_store, _ = entity_manager_harness
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
    knowledge_store.add_entity(
        202,
        "Knoggin",
        aliases=["memory project"],
        entity_type="project",
    )

    cached = await entities.get_profile(101)
    fetched = await entities.get_profile(202)
    missing = await entities.get_profile(999)

    assert cached.canonical_name == "Robert Chen"
    assert fetched.canonical_name == "Knoggin"
    assert missing is None
    assert knowledge_store.profile_lookups == [
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

    assert entities.get_alias_version() == 0
    vector = await entities.register_entity(
        404,
        "Notion",
        ["Notion", "workspace notes"],
        "tool",
        "General",
        session_id="session-1",
    )

    # Global identity vectors exclude project-context classification such as
    # type and topic, which can legitimately differ between projects.
    assert vector == embedding.vector_for("Notion (unknown)")
    assert embedding.single_calls == ["Notion (unknown)"]
    profile = await entities.get_profile(404)
    assert profile == EntityProfile(
        canonical_name="Notion",
        entity_type="tool",
        topic="General",
        project_id="project-1",
        embedding=vector,
    )
    assert entities.get_known_aliases()["notion"] == 404
    assert entities.get_known_aliases()["workspace notes"] == 404
    assert set(entities.get_mentions_for_id(404)) == {"notion", "workspace notes"}
    assert entities.get_alias_version() == 1


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
    alias_version = entities.get_alias_version()

    vector = await entities.compute_embedding(
        101,
        "Knoggin (project). Context: Builds a memory graph.",
    )
    missing_vector = await entities.compute_embedding(999, "Unknown")

    assert vector == embedding.vector_for(
        "Knoggin (project). Context: Builds a memory graph."
    )
    assert missing_vector == []
    assert (await entities.get_profile(101)).embedding == vector
    assert embedding.single_calls == [
        "Knoggin (project). Context: Builds a memory graph."
    ]
    assert entities.get_alias_version() == alias_version


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
    alias_version = entities.get_alias_version()

    removed = entities.remove_entities([101])

    assert removed == 1
    aliases = entities.get_known_aliases()
    assert "robert chen" not in aliases
    assert "bob" not in aliases
    assert aliases["knoggin"] == 202
    assert aliases["memory project"] == 202
    assert entities.get_mentions_for_id(101) == []
    assert set(entities.get_mentions_for_id(202)) == {"knoggin", "memory project"}
    assert entities.get_alias_version() == alias_version + 1
