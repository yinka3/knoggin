from datetime import date, datetime
from typing import Any, Iterable


def _value(item: Any, name: str):
    if isinstance(item, dict):
        return item.get(name)
    return getattr(item, name, None)


def _sortable_time(value: Any) -> str:
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return str(value or "")


def build_entity_embedding_text(
    canonical_name: str,
    entity_type: str,
    facts: Iterable[Any] = (),
) -> str:
    name = str(canonical_name or "").strip()
    normalized_type = str(entity_type or "unknown").strip() or "unknown"
    base = f"{name} ({normalized_type})"

    active_facts = [
        fact
        for fact in facts
        if _value(fact, "invalid_at") is None
        and str(_value(fact, "content") or "").strip()
    ]
    active_facts.sort(
        key=lambda fact: (
            _sortable_time(_value(fact, "valid_at")),
            str(_value(fact, "id") or _value(fact, "fact_id") or ""),
        )
    )
    contents = [str(_value(fact, "content")).strip() for fact in active_facts]
    return f"{base}. {' '.join(contents)}" if contents else base
