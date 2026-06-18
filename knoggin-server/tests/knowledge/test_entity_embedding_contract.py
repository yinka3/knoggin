from datetime import datetime, timezone

import pytest

from knoggin_server.knowledge.services.entity_embedding import (
    build_entity_embedding_text,
)


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_embedding_text_uses_stable_active_fact_order():
    facts = [
        {
            "fact_id": "b",
            "content": "Second by ID.",
            "valid_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "invalid_at": None,
        },
        {
            "fact_id": "ignored",
            "content": "Invalid fact.",
            "valid_at": datetime(2025, 1, 1, tzinfo=timezone.utc),
            "invalid_at": datetime(2026, 1, 1, tzinfo=timezone.utc),
        },
        {
            "fact_id": "a",
            "content": "First by ID.",
            "valid_at": datetime(2026, 1, 2, tzinfo=timezone.utc),
            "invalid_at": None,
        },
    ]

    assert build_entity_embedding_text("Widget", "concept", facts) == (
        "Widget (concept). First by ID. Second by ID."
    )


@pytest.mark.storage
@pytest.mark.no_network
def test_entity_embedding_text_uses_name_and_type_without_facts():
    assert build_entity_embedding_text("Widget", None, []) == "Widget (unknown)"
