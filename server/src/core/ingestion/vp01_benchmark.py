"""Repeatable local regression measurement for the production VP-01 adapter."""

from __future__ import annotations

import sys
import time
from dataclasses import dataclass
from typing import Iterable

from common.conf.domain_config import CompiledDomain
from core.ingestion.vp01 import (
    GLiNER25VP01Adapter,
    VP01EntitySpan,
    entity_schema,
    model_id_for_language,
)


@dataclass(frozen=True, slots=True)
class VP01BenchmarkFixture:
    """Frozen Context text and expected VP-01 spans for one regression case."""

    fixture_id: str
    context_markdown: str
    expected_entities: tuple[VP01EntitySpan, ...]

    def __post_init__(self) -> None:
        if not self.fixture_id.strip():
            raise ValueError("VP-01 benchmark fixture_id must be non-blank")
        if not self.context_markdown.strip():
            raise ValueError("VP-01 benchmark Context text must be non-blank")


@dataclass(frozen=True, slots=True)
class VP01BenchmarkCaseResult:
    """Metrics for one immutable Context fixture."""

    fixture_id: str
    latency_ms: float
    expected_count: int
    extracted_count: int
    true_positives: int
    false_positives: int
    false_negatives: int


@dataclass(frozen=True, slots=True)
class VP01BenchmarkReport:
    """Aggregate quality and local resource measurements for one VP-01 run."""

    model_id: str
    entity_schema: tuple[tuple[str, str], ...]
    cases: tuple[VP01BenchmarkCaseResult, ...]
    mean_latency_ms: float
    max_latency_ms: float
    peak_rss_kib: int | None
    peak_rss_delta_kib: int | None
    precision: float
    recall: float
    fallback_count: int
    fallback_rate: float
    vp02_path: str = "llm_required_outside_vp01_benchmark"


@dataclass(frozen=True, slots=True)
class VP01BenchmarkGates:
    """Explicit quality and local-resource limits for a benchmark invocation."""

    minimum_precision: float = 0.90
    minimum_recall: float = 0.90
    maximum_mean_latency_ms: float | None = None
    maximum_peak_rss_delta_kib: int | None = None
    maximum_fallback_rate: float = 0.0


def run_vp01_benchmark(
    adapter: GLiNER25VP01Adapter,
    domain: CompiledDomain,
    fixtures: Iterable[VP01BenchmarkFixture],
    *,
    threshold: float,
) -> VP01BenchmarkReport:
    """Measure GLiNER2.5 against frozen Context fixtures.

    This intentionally exercises only VP-01.  Relationship extraction is not
    benchmarked here because VP-02 must remain on its separate LLM path.
    GLiNER2.5 has no legacy-model fallback, so every run has a zero fallback
    count unless this contract is changed deliberately.
    """

    if not isinstance(adapter, GLiNER25VP01Adapter):
        raise TypeError("VP-01 benchmark requires the GLiNER2.5 production adapter")
    if not isinstance(domain, CompiledDomain):
        raise TypeError("VP-01 benchmark requires a CompiledDomain")
    if adapter.model_id != model_id_for_language(domain.vp01_language):
        raise ValueError("VP-01 benchmark adapter does not match the domain language")
    if not isinstance(threshold, (int, float)) or isinstance(threshold, bool):
        raise TypeError("VP-01 benchmark threshold must be numeric")

    frozen_fixtures = tuple(fixtures)
    if not frozen_fixtures:
        raise ValueError("VP-01 benchmark requires at least one frozen Context fixture")
    if any(not isinstance(item, VP01BenchmarkFixture) for item in frozen_fixtures):
        raise TypeError("VP-01 benchmark fixtures must be typed")
    if len({item.fixture_id for item in frozen_fixtures}) != len(frozen_fixtures):
        raise ValueError("VP-01 benchmark fixture IDs must be unique")

    schema = entity_schema(domain)
    baseline_peak_rss_kib = _peak_rss_kib()
    peak_rss_kib = baseline_peak_rss_kib
    results: list[VP01BenchmarkCaseResult] = []
    true_positives = false_positives = false_negatives = 0

    for fixture in frozen_fixtures:
        started_ns = time.perf_counter_ns()
        extracted = adapter.extract_entities(
            fixture.context_markdown,
            domain,
            threshold=float(threshold),
        )
        latency_ms = (time.perf_counter_ns() - started_ns) / 1_000_000
        current_peak_rss_kib = _peak_rss_kib()
        if current_peak_rss_kib is not None:
            peak_rss_kib = max(peak_rss_kib or 0, current_peak_rss_kib)

        expected = {_span_key(item) for item in fixture.expected_entities}
        observed = {_span_key(item) for item in extracted}
        case_true_positives = len(expected & observed)
        case_false_positives = len(observed - expected)
        case_false_negatives = len(expected - observed)
        true_positives += case_true_positives
        false_positives += case_false_positives
        false_negatives += case_false_negatives
        results.append(
            VP01BenchmarkCaseResult(
                fixture_id=fixture.fixture_id,
                latency_ms=latency_ms,
                expected_count=len(expected),
                extracted_count=len(observed),
                true_positives=case_true_positives,
                false_positives=case_false_positives,
                false_negatives=case_false_negatives,
            )
        )

    precision_denominator = true_positives + false_positives
    recall_denominator = true_positives + false_negatives
    return VP01BenchmarkReport(
        model_id=adapter.model_id,
        entity_schema=tuple(sorted(schema.items())),
        cases=tuple(results),
        mean_latency_ms=sum(item.latency_ms for item in results) / len(results),
        max_latency_ms=max(item.latency_ms for item in results),
        peak_rss_kib=peak_rss_kib,
        peak_rss_delta_kib=(
            None
            if peak_rss_kib is None or baseline_peak_rss_kib is None
            else max(0, peak_rss_kib - baseline_peak_rss_kib)
        ),
        precision=(true_positives / precision_denominator if precision_denominator else 1.0),
        recall=(true_positives / recall_denominator if recall_denominator else 1.0),
        fallback_count=0,
        fallback_rate=0.0,
    )


def assert_vp01_regression_gates(
    report: VP01BenchmarkReport,
    gates: VP01BenchmarkGates,
) -> None:
    """Fail clearly when a frozen-fixture regression crosses a configured gate."""

    if not isinstance(report, VP01BenchmarkReport):
        raise TypeError("VP-01 regression gates require a benchmark report")
    if not isinstance(gates, VP01BenchmarkGates):
        raise TypeError("VP-01 regression gates require typed gate settings")

    failures = []
    if report.precision < gates.minimum_precision:
        failures.append(
            f"precision {report.precision:.3f} < {gates.minimum_precision:.3f}"
        )
    if report.recall < gates.minimum_recall:
        failures.append(f"recall {report.recall:.3f} < {gates.minimum_recall:.3f}")
    if (
        gates.maximum_mean_latency_ms is not None
        and report.mean_latency_ms > gates.maximum_mean_latency_ms
    ):
        failures.append(
            "mean latency "
            f"{report.mean_latency_ms:.3f}ms > {gates.maximum_mean_latency_ms:.3f}ms"
        )
    if (
        gates.maximum_peak_rss_delta_kib is not None
        and report.peak_rss_delta_kib is not None
        and report.peak_rss_delta_kib > gates.maximum_peak_rss_delta_kib
    ):
        failures.append(
            "peak RSS delta "
            f"{report.peak_rss_delta_kib}KiB > {gates.maximum_peak_rss_delta_kib}KiB"
        )
    if report.fallback_rate > gates.maximum_fallback_rate:
        failures.append(
            "fallback rate "
            f"{report.fallback_rate:.3f} > {gates.maximum_fallback_rate:.3f}"
        )
    if failures:
        raise AssertionError("VP-01 regression gate failed: " + "; ".join(failures))


def _span_key(span: VP01EntitySpan) -> tuple[str, str, int, int]:
    return (span.text.casefold(), span.label.casefold(), span.start, span.end)


def _peak_rss_kib() -> int | None:
    """Read the process high-water RSS without adding a runtime dependency."""

    try:
        import resource

        value = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
    except (ImportError, OSError):
        return None
    # macOS reports bytes while Linux, Knoggin's supported local target,
    # reports KiB.  Normalize the report to KiB for reproducible gates.
    return int(value // 1024) if sys.platform == "darwin" else int(value)
