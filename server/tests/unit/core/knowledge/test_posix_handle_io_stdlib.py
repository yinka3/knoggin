"""Dependency-free POSIX confinement checks, runnable in a plain Linux image."""

import importlib.util
import os
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


@unittest.skipUnless(os.name == "posix", "POSIX directory descriptor behavior")
class PosixHandleIOTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        module_path = Path(__file__).resolve().parents[4] / "src/core/knowledge/documents/io/posix_handle_io.py"
        spec = importlib.util.spec_from_file_location("posix_handle_io_under_test", module_path)
        cls.module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(cls.module)

    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name) / "projects" / "project-1"
        self.root.mkdir(parents=True)

    def test_file_marker_scan_and_cleanup(self):
        module = self.module
        module.write_project_file(self.root, "notes/a.md", b"alpha", overwrite=False, expected_hash=None)
        self.assertEqual(list(module.iter_project_paths(self.root))[0][:2], ("notes/a.md", 5))
        module.move_project_file(self.root, "notes/a.md", "moved.md", expected_hash=None)
        self.assertEqual(module.delete_project_file(self.root, "moved.md", expected_hash=None), b"alpha")
        descriptor = module.open_lock_descriptor(self.root.parent, "project.lock")
        os.close(descriptor)
        module.write_pending_marker(self.root.parent, "project.pending", "a" * 64)
        self.assertEqual(module.read_pending_marker(self.root.parent, "project.pending"), "a" * 64)
        self.assertTrue(module.delete_pending_marker(self.root.parent, "project.pending"))
        self.assertTrue(module.remove_project_tree(self.root.parent, self.root.name))
        self.assertFalse(self.root.exists())

    def test_symlink_parent_is_rejected(self):
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (self.root / "linked").symlink_to(outside, target_is_directory=True)
        with self.assertRaises((ValueError, OSError)):
            self.module.write_project_file(
                self.root, "linked/private.md", b"secret", overwrite=False,
                expected_hash=None,
            )
        self.assertFalse((outside / "private.md").exists())

    def test_parent_swap_aborts_before_publish(self):
        module = self.module
        module.create_project_folder(self.root, "notes")
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        moved = Path(self.temporary.name) / "moved-original"
        real_token_hex = module.secrets.token_hex

        def swap_and_name(length):
            (self.root / "notes").rename(moved)
            (self.root / "notes").symlink_to(outside, target_is_directory=True)
            return real_token_hex(length)

        with patch.object(module.secrets, "token_hex", side_effect=swap_and_name):
            with self.assertRaisesRegex(ValueError, "directory moved"):
                module.write_project_file(
                    self.root, "notes/private.md", b"secret", overwrite=False,
                    expected_hash=None,
                )
        self.assertFalse((outside / "private.md").exists())
        self.assertFalse((moved / "private.md").exists())

    def test_cleanup_detects_swapped_child(self):
        module = self.module
        module.write_project_file(self.root, "nested/owned.md", b"owned", overwrite=False, expected_hash=None)
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (outside / "keep.md").write_bytes(b"private")
        moved = Path(self.temporary.name) / "moved-original"
        real_open = os.open
        swapped = False

        def swap_on_open(path, flags, *args, **kwargs):
            nonlocal swapped
            if path == "nested" and kwargs.get("dir_fd") is not None and not swapped:
                swapped = True
                (self.root / "nested").rename(moved)
                (self.root / "nested").symlink_to(outside, target_is_directory=True)
            return real_open(path, flags, *args, **kwargs)

        with patch.object(module.os, "open", side_effect=swap_on_open):
            with self.assertRaisesRegex(RuntimeError, "changed during file cleanup"):
                module.remove_project_tree(self.root.parent, self.root.name)
        self.assertEqual((outside / "keep.md").read_bytes(), b"private")

    def test_project_filesystem_wrapper_uses_posix_handles(self):
        source_root = Path(__file__).resolve().parents[4] / "src"
        package = types.ModuleType("core.knowledge.documents")
        package.__path__ = [str(source_root / "core/knowledge/documents")]
        scanning = types.ModuleType("core.knowledge.documents.scanning")
        scanning.normalize_relative_path = lambda path, original: path or original
        module_path = source_root / "core/knowledge/documents/filesystem.py"
        spec = importlib.util.spec_from_file_location("filesystem_under_test", module_path)
        filesystem_module = importlib.util.module_from_spec(spec)
        with patch.object(sys, "path", [str(source_root), *sys.path]), patch.dict(sys.modules, {
            "core.knowledge.documents": package,
            "core.knowledge.documents.scanning": scanning,
            "core.knowledge.documents.io.posix_handle_io": self.module,
            "filesystem_under_test": filesystem_module,
        }):
            spec.loader.exec_module(filesystem_module)
            filesystem = filesystem_module.ProjectFilesystem(self.root)
            created = filesystem.write_bytes("notes/a.md", b"alpha")
            self.assertEqual(filesystem.read_bytes("notes/a.md"), b"alpha")
            self.assertEqual([item.relative_path for item in filesystem.iter_paths()], ["notes/a.md"])
            moved = filesystem.move_file(
                "notes/a.md", "moved.md", expected_content_hash=created.content_hash
            )
            self.assertEqual(filesystem.delete_file(
                "moved.md", expected_content_hash=moved.content_hash
            ).content_hash, created.content_hash)
            with filesystem._mutation_lock() as lease:
                filesystem.mark_pending_admission("orphan.md", "a" * 64, _lease=lease)
                self.assertEqual(filesystem.pending_admission_hash("orphan.md"), "a" * 64)
                filesystem.clear_pending_admission("orphan.md", _lease=lease)
            self.assertIsNone(filesystem.pending_admission_hash("orphan.md"))


if __name__ == "__main__":
    unittest.main()
