import tomllib
from pathlib import Path

import pytest

SERVER_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = SERVER_ROOT / "src"


@pytest.mark.smoke
@pytest.mark.no_network
def test_server_does_not_ship_the_fastapi_adapter():
    assert not (SRC_ROOT / "api").exists()
    assert not (SRC_ROOT / "application").exists()

    config = tomllib.loads((SERVER_ROOT / "pyproject.toml").read_text("utf-8"))
    dependencies = config["project"]["dependencies"]
    package_patterns = config["tool"]["setuptools"]["packages"]["find"]["include"]

    assert not any(dependency.startswith("fastapi") for dependency in dependencies)
    assert "api*" not in package_patterns
    assert "application*" not in package_patterns


@pytest.mark.smoke
@pytest.mark.no_network
def test_server_source_does_not_import_fastapi():
    violations = []
    for path in SRC_ROOT.rglob("*.py"):
        for line_number, line in enumerate(path.read_text("utf-8").splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith("from fastapi") or stripped.startswith("import fastapi"):
                violations.append(f"{path.relative_to(SRC_ROOT)}:{line_number}")

    assert violations == []
