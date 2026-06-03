from importlib import import_module
from pathlib import Path

import pytest


@pytest.mark.smoke
@pytest.mark.no_network
def test_server_package_imports():
    modules = [
        import_module("knoggin_server"),
        import_module("knoggin_server.session"),
        import_module("knoggin_server.agent.services.agent_manager"),
    ]

    assert all(module is not None for module in modules)


@pytest.mark.smoke
@pytest.mark.no_network
def test_server_source_has_no_stale_internal_knoggin_imports():
    src_root = Path(__file__).resolve().parents[2] / "src" / "knoggin_server"
    stale = []
    stale_package = "knoggin"

    for path in src_root.rglob("*.py"):
        text = path.read_text(encoding="utf-8")
        for line_number, line in enumerate(text.splitlines(), start=1):
            stripped = line.strip()
            if stripped.startswith(f"from {stale_package}.") or stripped.startswith(
                f"import {stale_package}."
            ):
                stale.append(f"{path.relative_to(src_root)}:{line_number}: {stripped}")

    assert stale == []
