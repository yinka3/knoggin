from tests.fixtures.jev_measurements import summarize_results


def test_missing_cost_is_unknown_and_latency_includes_failed_attempts():
    results = [
        {"attempts": 1, "elapsed_seconds": 0.1, "cost_usd": 0.001},
        {"attempts": 2, "elapsed_seconds": 0.9, "cost_usd": None},
        {"attempts": 0, "elapsed_seconds": 20, "cost_usd": 0},
    ]
    report = summarize_results(results)
    assert report["provider_attempts"] == 3
    assert report["known_cost_usd"] == 0.001
    assert report["total_cost_usd"] is None
    assert report["latency_seconds"] == {
        "mean": 0.5, "p50": 0.1, "p95": 0.9, "max": 0.9,
    }


def test_no_attempts_has_no_latency_measurement():
    assert summarize_results([])["latency_seconds"]["p95"] is None
