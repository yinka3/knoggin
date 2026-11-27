import os
import time

import pytest

from common.utils.stale_files import remove_stale_entries


def _age(path, seconds):
    old = time.time() - seconds
    os.utime(path, (old, old))


@pytest.mark.unit
@pytest.mark.no_network
def test_only_old_matching_scratch_is_removed(tmp_path):
    old_dir = tmp_path / "knoggin-docling-abc"
    old_dir.mkdir()
    (old_dir / "source.bin").write_bytes(b"pdf")
    fresh_dir = tmp_path / "knoggin-docling-new"
    fresh_dir.mkdir()
    old_file = tmp_path / ".credentials.json.123.tmp"
    old_file.write_text("{}")
    unrelated = tmp_path / "keep-me.txt"
    unrelated.write_text("x")
    for path in (old_dir, old_file, unrelated):
        _age(path, 7 * 3600)

    removed = remove_stale_entries(
        tmp_path, ("knoggin-docling-*", ".credentials.json.*.tmp"), older_than_seconds=6 * 3600,
    )

    assert removed == 2
    assert not old_dir.exists() and not old_file.exists()
    assert fresh_dir.exists() and unrelated.exists()


@pytest.mark.unit
@pytest.mark.no_network
def test_a_missing_folder_is_not_an_error(tmp_path):
    assert remove_stale_entries(tmp_path / "absent", ("*",)) == 0


@pytest.mark.unit
@pytest.mark.no_network
@pytest.mark.skipif(os.name != "posix", reason="symlinks need privileges on Windows")
def test_a_link_is_removed_without_following_it(tmp_path):
    target = tmp_path / "elsewhere"
    target.mkdir()
    (target / "precious").write_text("keep")
    link = tmp_path / "knoggin-docling-link"
    link.symlink_to(target, target_is_directory=True)
    os.utime(link, (time.time() - 7 * 3600,) * 2, follow_symlinks=False)

    assert remove_stale_entries(tmp_path, ("knoggin-docling-*",)) == 1
    assert not link.exists() and (target / "precious").exists()
