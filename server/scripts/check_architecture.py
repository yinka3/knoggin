#!/usr/bin/env python3
"""Check the server's source import boundaries with the Python AST."""

from __future__ import annotations

import ast
import sys
from pathlib import Path
from typing import Iterable

FORBIDDEN_IMPORTS = {
    "common": {"core", "infrastructure", "runtime"},
    "infrastructure": {"core", "runtime"},
}

# ProjectManager, Session, and community runtime consumers still use concrete
# runtime objects during the structural transition. Replace this exception with
# an injected protocol/factory boundary before tightening the rule to core.
TRANSITIONAL_ALLOWED_IMPORTS = {("core", "runtime")}

LEGACY_MODULE_PATHS = (
    "infrastructure.resources",
    "infrastructure.knowledge_store",
    "core.session.boot",
    "core.project.state",
    "core.knowledge.db.write_graph_db",
    "core.agent.types",
    "core.agent.internals",
    "core.agent.source_adapters",
    "core.ingestion.services",
    "core.ingestion.episode_build",
    "core.ingestion.jobs.episode_job",
    "core.knowledge.episode_embedding",
)

PACKAGE_ROOT_IMPORTS = {
    "core.agent",
    "core.community",
    "core.ingestion",
    "core.knowledge",
    "core.project",
    "core.session",
}


def _module_name(source_root: Path, path: Path) -> str:
    relative = path.relative_to(source_root).with_suffix("")
    return ".".join(relative.parts)


def _imported_modules(node: ast.AST) -> Iterable[tuple[str, str]]:
    if isinstance(node, ast.Import):
        for alias in node.names:
            yield alias.name, "import"
    elif isinstance(node, ast.ImportFrom) and node.level == 0 and node.module:
        yield node.module, "from"


def _legacy_match(module: str) -> str | None:
    for legacy in LEGACY_MODULE_PATHS:
        if module == legacy or module.startswith(f"{legacy}."):
            return legacy
    return None


def check_source_tree(source_root: Path) -> list[str]:
    """Return human-readable violations found under ``source_root``."""
    violations: list[str] = []
    for path in sorted(source_root.rglob("*.py")):
        if "__pycache__" in path.parts:
            continue
        relative = path.relative_to(source_root)
        owner = relative.parts[0] if relative.parts else ""
        try:
            tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        except SyntaxError as exc:
            violations.append(f"{path}:{exc.lineno}: syntax error: {exc.msg}")
            continue

        for node in ast.walk(tree):
            if isinstance(node, ast.ImportFrom) and any(
                alias.name == "*" for alias in node.names
            ):
                violations.append(
                    f"{path}:{node.lineno}: wildcard imports are not allowed"
                )

            for module, import_kind in _imported_modules(node):
                legacy = _legacy_match(module)
                if legacy:
                    violations.append(
                        f"{path}:{node.lineno}: legacy import path {legacy}"
                    )

                imported_owner = module.split(".", 1)[0]
                if imported_owner in FORBIDDEN_IMPORTS.get(owner, set()):
                    if (owner, imported_owner) not in TRANSITIONAL_ALLOWED_IMPORTS:
                        violations.append(
                            f"{path}:{node.lineno}: {owner} may not import "
                            f"{imported_owner}"
                        )

                if (
                    import_kind == "from"
                    and module in PACKAGE_ROOT_IMPORTS
                    and isinstance(node, ast.ImportFrom)
                ):
                    violations.append(
                        f"{path}:{node.lineno}: import concrete modules instead "
                        f"of package root {module}"
                    )
    return violations


def main(argv: list[str] | None = None) -> int:
    args = list(argv or sys.argv[1:])
    source_root = Path(args[0]).resolve() if args else Path(__file__).parents[1] / "src"
    violations = check_source_tree(source_root)
    if violations:
        print("\n".join(violations))
        return 1
    print(f"Architecture import check passed: {source_root}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
