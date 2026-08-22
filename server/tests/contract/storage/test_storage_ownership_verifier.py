from pathlib import Path

from infrastructure.redis_client import RedisKeys
from scripts.verify_storage_ownership import (
    CheckResult,
    audit_redis_writes,
    classify_redis_key,
    compare_counts,
    format_report,
    redis_runtime_result,
)


def test_compare_counts_fails_only_on_regression():
    result = compare_counts({"agents": 1, "messages": 2}, {"agents": 1, "messages": 3})
    assert result.passed is True

    result = compare_counts({"agents": 1}, {"agents": 0})
    assert result.passed is False
    assert "agents" in result.detail


def test_format_report_is_human_readable():
    report = format_report([CheckResult("Redis flush", True, "skipped")])

    assert "Storage Ownership Verification" in report
    assert "Redis flush" in report
    assert "PASS" in report


def test_redis_key_classification_uses_declared_families():
    assert classify_redis_key(RedisKeys.dirty_entities("ada", "p1")) == "ephemeral_only"
    assert (
        classify_redis_key(RedisKeys.conversation("ada", "s1"))
        == "rebuildable_from_postgres"
    )
    assert classify_redis_key("unknown:ada:p1") == "unknown"


def test_redis_runtime_result_can_allow_missing_restart_state():
    result = redis_runtime_result([], allow_missing=True)
    assert result.passed is True
    assert result.name == "Redis runtime state absent"

    result = redis_runtime_result([], allow_missing=False)
    assert result.passed is False
    assert result.name == "Redis runtime state present"

    result = redis_runtime_result(["dirty_entities:ada:p1"], allow_missing=False)
    assert result.passed is True
    assert "1 matching keys" in result.detail


def test_static_redis_write_audit_checks_required_families(tmp_path):
    source = tmp_path / "src"
    source.mkdir()
    (source / "writer.py").write_text(
        "async def f(redis):\n    await redis.sadd('dirty_entities:ada:p1', '1')\n"
    )

    result = audit_redis_writes(Path(source))

    assert result.passed is True
    assert "Redis write files reviewed" in result.detail
