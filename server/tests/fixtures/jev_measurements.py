"""Comparable provider measurements for private evaluation reports."""

import math
from statistics import mean


def summarize_results(results: list[dict]) -> dict:
    attempted = [result for result in results if result.get("attempts", 0) > 0]
    durations = sorted(float(result["elapsed_seconds"]) for result in attempted)
    costs = [result.get("cost_usd") for result in attempted]
    known_costs = [float(cost) for cost in costs if cost is not None]

    def percentile(fraction):
        return durations[math.ceil(len(durations) * fraction) - 1] if durations else None

    return {
        "attempted_requests": len(attempted),
        "provider_attempts": sum(result["attempts"] for result in attempted),
        "latency_seconds": {
            "mean": mean(durations) if durations else None,
            "p50": percentile(0.5),
            "p95": percentile(0.95),
            "max": max(durations) if durations else None,
        },
        "cost_reported_requests": len(known_costs),
        "known_cost_usd": round(sum(known_costs), 8),
        "total_cost_usd": (
            round(sum(known_costs), 8) if len(known_costs) == len(attempted) else None
        ),
        "input_tokens": sum(result.get("input_tokens", 0) for result in attempted),
        "output_tokens": sum(result.get("output_tokens", 0) for result in attempted),
        "latency_scope": "client request including retries and usage settlement; excludes lock wait",
    }
