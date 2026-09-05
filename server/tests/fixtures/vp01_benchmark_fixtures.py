"""Frozen Context inputs for repeatable GLiNER2.5 VP-01 regression checks."""

from core.ingestion.vp01 import VP01EntitySpan
from core.ingestion.vp01_benchmark import VP01BenchmarkFixture

FROZEN_CONTEXT_FIXTURES = (
    VP01BenchmarkFixture(
        fixture_id="selected_provider",
        context_markdown="Acme Labs is the selected provider.",
        expected_entities=(
            VP01EntitySpan(text="Acme Labs", label="company", start=0, end=9),
        ),
    ),
    VP01BenchmarkFixture(
        fixture_id="lowercase_common_words",
        context_markdown="the current vendor is acme labs.",
        expected_entities=(
            VP01EntitySpan(text="acme labs", label="company", start=22, end=31),
        ),
    ),
    VP01BenchmarkFixture(
        fixture_id="no_entity",
        context_markdown="the plan remains active.",
        expected_entities=(),
    ),
)
