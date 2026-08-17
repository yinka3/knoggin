from pathlib import Path

from scripts.check_architecture import check_source_tree


def test_server_source_import_boundaries_are_clean():
    source_root = Path(__file__).parents[2] / "src"

    assert check_source_tree(source_root) == []
