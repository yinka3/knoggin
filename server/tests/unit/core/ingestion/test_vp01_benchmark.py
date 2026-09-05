"""Regression contracts for the frozen GLiNER2.5 VP-01 benchmark."""

from dataclasses import replace

import pytest

from common.conf.domain_config import DomainConfig
from core.ingestion.vp01 import GLINER25_ENGLISH_MODEL, GLiNER25VP01Adapter
from core.ingestion.vp01_benchmark import (
    VP01BenchmarkGates,
    assert_vp01_regression_gates,
    run_vp01_benchmark,
)
from tests.fixtures.vp01_benchmark_fixtures import FROZEN_CONTEXT_FIXTURES


def _domain():
    return DomainConfig.from_mapping(
        {
            "version": 8,
            "topics": {"Work": {"active": True}},
            "entity_types": {
                "Company": {
                    "topic": "Work",
                    "labels": ["company"],
                    "description": "A company selected to work on this project.",
                }
            },
            "vp01_language": "en",
        }
    ).compile()


class RecordingGLiNER25Model:
    def __init__(self) -> None:
        self.calls = []

    def extract_entities(self, text, schema, **kwargs):
        self.calls.append((text, schema, kwargs))
        fixture = next(item for item in FROZEN_CONTEXT_FIXTURES if item.context_markdown == text)
        entities = {}
        for expected in fixture.expected_entities:
            entities.setdefault(expected.label, []).append(
                {
                    "text": expected.text,
                    "start": expected.start,
                    "end": expected.end,
                    "confidence": 0.99,
                }
            )
        return {"entities": entities}


@pytest.mark.unit
@pytest.mark.no_network
def test_gliner25_vp01_benchmark_uses_frozen_context_and_reports_regression_metrics():
    domain = _domain()
    raw_model = RecordingGLiNER25Model()
    adapter = GLiNER25VP01Adapter(raw_model, model_id=GLINER25_ENGLISH_MODEL)

    report = run_vp01_benchmark(
        adapter,
        domain,
        FROZEN_CONTEXT_FIXTURES,
        threshold=0.42,
    )

    assert report.model_id == GLINER25_ENGLISH_MODEL
    assert report.entity_schema == (
        ("company", "A company selected to work on this project."),
    )
    assert report.precision == 1.0
    assert report.recall == 1.0
    assert report.fallback_count == 0
    assert report.fallback_rate == 0.0
    assert report.vp02_path == "llm_required_outside_vp01_benchmark"
    assert report.peak_rss_kib is not None
    assert [result.fixture_id for result in report.cases] == [
        "selected_provider",
        "lowercase_common_words",
        "no_entity",
    ]
    assert [call[0] for call in raw_model.calls] == [
        fixture.context_markdown for fixture in FROZEN_CONTEXT_FIXTURES
    ]
    assert all(
        call[1] == {"company": "A company selected to work on this project."}
        and call[2]
        == {
            "threshold": 0.42,
            "include_spans": True,
            "include_confidence": True,
        }
        for call in raw_model.calls
    )
    assert_vp01_regression_gates(
        report,
        VP01BenchmarkGates(
            minimum_precision=1.0,
            minimum_recall=1.0,
            maximum_mean_latency_ms=1_000,
            maximum_peak_rss_delta_kib=(report.peak_rss_delta_kib or 0) + 1,
        ),
    )


@pytest.mark.unit
@pytest.mark.no_network
def test_gliner25_vp01_benchmark_gates_quality_and_resource_regressions():
    raw_model = RecordingGLiNER25Model()
    report = run_vp01_benchmark(
        GLiNER25VP01Adapter(raw_model, model_id=GLINER25_ENGLISH_MODEL),
        _domain(),
        FROZEN_CONTEXT_FIXTURES,
        threshold=0.42,
    )

    with pytest.raises(AssertionError, match="precision"):
        assert_vp01_regression_gates(
            replace(report, precision=0.5),
            VP01BenchmarkGates(minimum_precision=0.9),
        )
    with pytest.raises(AssertionError, match="mean latency"):
        assert_vp01_regression_gates(
            replace(report, mean_latency_ms=10.0),
            VP01BenchmarkGates(maximum_mean_latency_ms=1.0),
        )
    with pytest.raises(AssertionError, match="peak RSS delta"):
        assert_vp01_regression_gates(
            replace(report, peak_rss_delta_kib=32),
            VP01BenchmarkGates(maximum_peak_rss_delta_kib=1),
        )
