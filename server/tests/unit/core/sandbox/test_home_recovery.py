import os

import pytest

from core.sandbox.manager import _recover_sandbox_homes


def _home(library_root, project="p1"):
    home = library_root / project / ".sandbox" / "home"
    home.mkdir(parents=True)
    return home


@pytest.mark.no_network
def test_a_swap_killed_between_renames_finishes_with_the_new_folder(tmp_path):
    home = _home(tmp_path)
    (home / ".python-old" / "oldpkg").mkdir(parents=True)
    (home / ".python-next" / "newpkg").mkdir(parents=True)  # complete, not yet renamed

    _recover_sandbox_homes(tmp_path)

    assert sorted(os.listdir(home)) == ["python"]
    assert (home / "python" / "newpkg").is_dir()


@pytest.mark.no_network
def test_a_copy_killed_before_the_swap_keeps_the_old_folder(tmp_path):
    home = _home(tmp_path)
    (home / "python" / "oldpkg").mkdir(parents=True)
    (home / ".python-next" / "half").mkdir(parents=True)  # partial copy

    _recover_sandbox_homes(tmp_path)

    assert sorted(os.listdir(home)) == ["python"]
    assert (home / "python" / "oldpkg").is_dir()
    assert not (home / "python" / "half").exists()


@pytest.mark.no_network
def test_partial_downloads_are_removed_and_finished_ones_kept(tmp_path):
    downloads = _home(tmp_path) / "downloads" / "huggingface" / "org" / "model"
    downloads.mkdir(parents=True)
    (downloads / "weights.bin.part").write_bytes(b"half")
    (downloads / "config.json").write_text("{}")

    _recover_sandbox_homes(tmp_path)

    assert sorted(os.listdir(downloads)) == ["config.json"]


@pytest.mark.no_network
@pytest.mark.skipif(os.name != "posix", reason="symlinks need privileges on Windows")
def test_links_in_a_home_are_removed_without_being_followed(tmp_path):
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "keep.part").write_text("not ours")
    home = _home(tmp_path)
    (home / "downloads").mkdir()
    (home / "downloads" / "escape").symlink_to(outside, target_is_directory=True)
    (home / ".python-next").symlink_to(outside, target_is_directory=True)

    _recover_sandbox_homes(tmp_path)

    assert (outside / "keep.part").exists()
    assert not (home / ".python-next").exists()
