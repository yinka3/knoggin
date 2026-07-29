"""Small per-call ID maps for model-facing payloads."""

from collections.abc import Iterable
from typing import TypeAlias

SystemIdentifier: TypeAlias = int | str


def build_local_id_maps(
    identifiers: Iterable[SystemIdentifier],
    prefix: str,
) -> tuple[dict[SystemIdentifier, str], dict[str, SystemIdentifier]]:
    """Assign ascending local IDs to one complete set of real identifiers.

    The first dictionary translates real IDs to the short values placed in an
    LLM prompt. The second translates the LLM's returned value back to its real
    ID. `prefix` identifies the item kind in the prompt, for example `"m"` for
    messages or `"e"` for entities. Callers create separate maps for messages,
    entities, relationships, and any other item kind in the same prompt.
    """

    unique_identifiers = sorted(set(identifiers), key=_identifier_sort_key)
    actual_to_local = {
        identifier: f"{prefix}{position}"
        for position, identifier in enumerate(unique_identifiers, start=1)
    }
    local_to_actual = {
        local_id: identifier for identifier, local_id in actual_to_local.items()
    }
    return actual_to_local, local_to_actual


def resolve_local_id(
    local_id: int | str,
    local_to_actual: dict[str, SystemIdentifier],
) -> SystemIdentifier:
    """Resolve one LLM-returned local ID or raise a clear validation error."""

    try:
        return local_to_actual[str(local_id)]
    except KeyError as exc:
        raise ValueError(f"Unknown local ID '{local_id}' for this LLM call.") from exc


def extend_local_id_maps(
    identifiers: Iterable[SystemIdentifier],
    prefix: str,
    actual_to_local: dict[SystemIdentifier, str],
    local_to_actual: dict[str, SystemIdentifier],
) -> None:
    """Add a deterministic batch of IDs without changing existing references.

    Staged model contexts can receive new eligible items over several steps.
    Existing local values must remain stable, so this mutates the two small
    maps only for previously unseen IDs. Callers should pass the complete
    eligible batch for the current step.
    """

    next_position = len(actual_to_local) + 1
    for identifier in sorted(set(identifiers), key=_identifier_sort_key):
        if identifier in actual_to_local:
            continue

        local_id = f"{prefix}{next_position}"
        while local_id in local_to_actual:
            next_position += 1
            local_id = f"{prefix}{next_position}"

        actual_to_local[identifier] = local_id
        local_to_actual[local_id] = identifier
        next_position += 1


def register_short_uuid_references(
    identifiers: Iterable[str],
    prefix: str,
    local_to_actual: dict[str, str],
    *,
    length: int = 6,
) -> dict[str, str]:
    """Register compact, typed UUID handles in one active-run lookup.

    This is intended for a stateful tool loop, where a model may receive an
    opaque UUID in one result and pass it back in a later call.  Unlike the
    per-prompt ascending maps above, the handle is derived from the UUID so it
    remains readable without needing a second per-kind map.  ``local_to_actual``
    is still the authority: a UUID prefix is not reliably reversible.

    The returned map is useful for rendering the current result.  The supplied
    lookup is mutated in place and should be discarded when the execution ends.
    """

    if length < 1:
        raise ValueError("Short UUID reference length must be positive.")

    actual_to_local: dict[str, str] = {}
    existing_actual_to_local = {
        actual_id: local_id
        for local_id, actual_id in local_to_actual.items()
        if local_id.startswith(f"{prefix}_")
    }
    for actual_id in sorted({str(identifier) for identifier in identifiers}):
        existing_local_id = existing_actual_to_local.get(actual_id)
        if existing_local_id is not None:
            actual_to_local[actual_id] = existing_local_id
            continue

        uuid_prefix = actual_id.replace("-", "")[:length]
        local_id = f"{prefix}_{uuid_prefix}"
        suffix = 2
        while local_id in local_to_actual and local_to_actual[local_id] != actual_id:
            local_id = f"{prefix}_{uuid_prefix}_{suffix}"
            suffix += 1

        local_to_actual[local_id] = actual_id
        actual_to_local[actual_id] = local_id

    return actual_to_local


def _identifier_sort_key(identifier: SystemIdentifier) -> tuple[int, int | str]:
    """Sort integer and string system IDs deterministically without coercion."""

    if isinstance(identifier, int) and not isinstance(identifier, bool):
        return 0, identifier
    return 1, str(identifier)
