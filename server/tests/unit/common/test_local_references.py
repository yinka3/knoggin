import pytest

from common.utils.local_references import (
    build_local_id_maps,
    extend_local_id_maps,
    register_short_uuid_references,
    resolve_local_id,
)


@pytest.mark.unit
@pytest.mark.no_network
def test_local_id_maps_are_deduplicated_and_ascending():
    actual_to_local, local_to_actual = build_local_id_maps([30, 2, 30, 11], "m")

    assert actual_to_local == {2: "m1", 11: "m2", 30: "m3"}
    assert local_to_actual == {"m1": 2, "m2": 11, "m3": 30}


@pytest.mark.unit
@pytest.mark.no_network
def test_local_id_maps_support_string_system_identifiers():
    actual_to_local, local_to_actual = build_local_id_maps(
        ["episode-z", "episode-a"], "ep"
    )

    assert actual_to_local == {"episode-a": "ep1", "episode-z": "ep2"}
    assert resolve_local_id("ep2", local_to_actual) == "episode-z"


@pytest.mark.unit
@pytest.mark.no_network
def test_resolve_local_id_accepts_prefixed_output_values():
    _, local_to_actual = build_local_id_maps([42], "e")

    assert resolve_local_id("e1", local_to_actual) == 42


@pytest.mark.unit
@pytest.mark.no_network
def test_unknown_local_id_is_rejected():
    _, local_to_actual = build_local_id_maps([42], "m")

    with pytest.raises(ValueError, match="Unknown local ID 'm2'"):
        resolve_local_id("m2", local_to_actual)


@pytest.mark.unit
@pytest.mark.no_network
def test_extend_local_id_maps_preserves_existing_references():
    actual_to_local, local_to_actual = build_local_id_maps([20], "ep")

    extend_local_id_maps(
        [30, 10, 20],
        "ep",
        actual_to_local,
        local_to_actual,
    )

    assert actual_to_local == {20: "ep1", 10: "ep2", 30: "ep3"}
    assert local_to_actual == {"ep1": 20, "ep2": 10, "ep3": 30}


@pytest.mark.unit
@pytest.mark.no_network
def test_short_uuid_references_use_one_flat_lookup_and_resolve_collisions():
    lookup = {}

    actual_to_short = register_short_uuid_references(
        [
            "a3f91c84-1111-4444-8888-111111111111",
            "a3f91c84-2222-4444-8888-222222222222",
        ],
        "ep",
        lookup,
    )

    assert actual_to_short == {
        "a3f91c84-1111-4444-8888-111111111111": "ep_a3f91c",
        "a3f91c84-2222-4444-8888-222222222222": "ep_a3f91c_2",
    }
    assert resolve_local_id("ep_a3f91c", lookup) == "a3f91c84-1111-4444-8888-111111111111"
    assert resolve_local_id("ep_a3f91c_2", lookup) == "a3f91c84-2222-4444-8888-222222222222"
