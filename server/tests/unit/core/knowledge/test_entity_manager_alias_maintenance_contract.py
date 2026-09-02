import pytest


def seed_entity(
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
def test_validate_existing_confirms_existing_and_rejects_missing_or_removed(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    seed_entity(entities, 101, "Robert Chen", aliases=["Bob"])

    assert entities.validate_existing("Robert Chen", ["Robert Chen"]) == (
        101,
        False,
        [],
    )
    assert entities.validate_existing("Robert Chen", ["Bobby", "Bob"]) == (
        101,
        True,
        ["Bobby"],
    )
    assert entities.validate_existing("Missing Person", ["Alias"]) == (
        None,
        False,
        [],
    )

    entities.remove_entities([101])

    assert entities.validate_existing("Robert Chen", ["Bobby"]) == (
        None,
        False,
        [],
    )


@pytest.mark.storage
@pytest.mark.no_network
def test_commit_new_aliases_adds_lowercase_aliases_and_is_idempotent(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    seed_entity(entities, 101, "Robert Chen")
    alias_version = entities.get_alias_version()

    entities.commit_new_aliases(101, ["Bobby", "RC"])
    first_update_version = entities.get_alias_version()
    entities.commit_new_aliases(101, ["Bobby", "RC"])

    aliases = entities.get_known_aliases()
    assert aliases["bobby"] == 101
    assert aliases["rc"] == 101
    assert set(entities.get_mentions_for_id(101)) == {
        "robert chen",
        "bobby",
        "rc",
    }
    assert first_update_version == alias_version + 1
    assert entities.get_alias_version() == first_update_version


@pytest.mark.storage
@pytest.mark.no_network
def test_commit_new_aliases_skips_unknown_entities_and_alias_collisions(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    seed_entity(entities, 101, "Robert Chen", aliases=["Bob"])
    seed_entity(entities, 202, "Bob Smith")

    entities.commit_new_aliases(999, ["Ghost"])
    entities.commit_new_aliases(202, ["Bob", "B. Smith"])

    aliases = entities.get_known_aliases()
    assert "ghost" not in aliases
    assert aliases["bob"] == 101
    assert aliases["b. smith"] == 202
    assert "bob" not in set(entities.get_mentions_for_id(202))


@pytest.mark.storage
@pytest.mark.no_network
def test_remove_entities_cleans_aliases_added_by_alias_maintenance(
    entity_manager_harness,
):
    entities, _, _ = entity_manager_harness
    seed_entity(entities, 101, "Robert Chen")
    seed_entity(entities, 202, "Knoggin", entity_type="project", topic="General")
    entities.commit_new_aliases(101, ["Bobby", "RC"])

    removed = entities.remove_entities([101])

    aliases = entities.get_known_aliases()
    assert removed == 1
    assert "robert chen" not in aliases
    assert "bobby" not in aliases
    assert "rc" not in aliases
    assert aliases["knoggin"] == 202
    assert entities.get_mentions_for_id(101) == []
