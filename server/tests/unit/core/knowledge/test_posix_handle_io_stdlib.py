"""Dependency-free POSIX confinement checks, runnable in a plain Linux image."""

import hashlib
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

    def test_mutations_reject_stale_hashes_and_unsafe_file_types(self):
        module = self.module
        module.write_project_file(self.root, "notes.md", b"owned", overwrite=False, expected_hash=None)
        owned_hash = hashlib.sha256(b"owned").hexdigest()

        with self.assertRaises(FileExistsError):
            module.write_project_file(self.root, "notes.md", b"other", overwrite=False, expected_hash=None)
        with self.assertRaises(module.PosixFileConflictError):
            module.write_project_file(self.root, "notes.md", b"other", overwrite=True, expected_hash="stale")
        with self.assertRaises(FileNotFoundError):
            module.write_project_file(self.root, "missing.md", b"other", overwrite=True, expected_hash=owned_hash)
        with self.assertRaises(module.PosixFileConflictError):
            module.move_project_file(self.root, "notes.md", "moved.md", expected_hash="stale")
        with self.assertRaises(module.PosixFileConflictError):
            module.delete_project_file(self.root, "notes.md", expected_hash="stale")
        self.assertEqual((self.root / "notes.md").read_bytes(), b"owned")

        (self.root / "folder").mkdir()
        (self.root / "linked.md").symlink_to(self.root / "notes.md")
        with self.assertRaisesRegex(ValueError, "regular file"):
            module.delete_project_file(self.root, "folder", expected_hash=None)
        with self.assertRaisesRegex(ValueError, "symlink"):
            module.delete_project_file(self.root, "linked.md", expected_hash=None)
        with module.open_directory_chain(self.root) as directory:
            with module.open_relative_parent(directory, "notes.md") as (parent, name):
                descriptor = module._open_file(parent, name)
                try:
                    with self.assertRaisesRegex(ValueError, "read limit"):
                        module._read(descriptor, max_bytes=2)
                finally:
                    os.close(descriptor)

    def test_directory_walk_rejects_links_missing_parts_and_relative_roots(self):
        module = self.module
        with self.assertRaisesRegex(ValueError, "absolute"):
            with module.open_directory_chain(Path("relative")):
                pass
        with module.open_directory_chain(self.root) as directory:
            with self.assertRaisesRegex(ValueError, "relative project path"):
                with module.open_relative_parent(directory, "."):
                    pass
            with self.assertRaises(FileNotFoundError):
                with module.open_relative_parent(directory, "missing/file.md"):
                    pass

        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (self.root / "linked").symlink_to(outside, target_is_directory=True)
        with self.assertRaisesRegex(ValueError, "symlink"):
            with module.open_directory_chain(self.root / "linked"):
                pass
        (self.root / "plain.md").write_bytes(b"content")
        with self.assertRaisesRegex(ValueError, "non-directory"):
            with module.open_directory_chain(self.root / "plain.md"):
                pass

    def test_removed_parent_and_failed_publish_leave_no_temporary_file(self):
        module = self.module
        child = self.root / "child"
        child.mkdir()
        with module.open_directory_chain(child) as descriptor:
            child.rmdir()
            with self.assertRaisesRegex(ValueError, "directory moved"):
                module._assert_parent_at_path(descriptor, child)

        with patch.object(module, "_write", side_effect=OSError("disk full")):
            with self.assertRaisesRegex(OSError, "disk full"):
                module.write_project_file(
                    self.root, "notes.md", b"new", overwrite=False, expected_hash=None
                )
        self.assertFalse(list(self.root.glob(".knoggin-*.tmp")))

        def remove_temporary_then_fail(_descriptor, _content):
            next(self.root.glob(".knoggin-*.tmp")).unlink()
            raise OSError("write failed after cleanup")

        with patch.object(module, "_write", side_effect=remove_temporary_then_fail):
            with self.assertRaisesRegex(OSError, "write failed after cleanup"):
                module.write_project_file(
                    self.root, "notes.md", b"new", overwrite=False, expected_hash=None
                )

    def test_failed_folder_verification_removes_new_directory(self):
        module = self.module
        with patch.object(module, "_assert_parent_at_path", side_effect=ValueError("moved")):
            with self.assertRaisesRegex(ValueError, "moved"):
                module.create_project_folder(self.root, "new-folder")
        self.assertFalse((self.root / "new-folder").exists())

        (self.root / "existing").mkdir()
        with patch.object(module, "_assert_parent_at_path", side_effect=ValueError("moved")):
            with self.assertRaisesRegex(ValueError, "moved"):
                module.create_project_folder(self.root, "existing")
        self.assertTrue((self.root / "existing").is_dir())

        real_open_directory = module._open_directory

        def reject_created_folder(parent, name, *, create=False):
            if name == "unreadable":
                raise OSError("cannot reopen folder")
            return real_open_directory(parent, name, create=create)

        with patch.object(module, "_open_directory", side_effect=reject_created_folder):
            with self.assertRaisesRegex(OSError, "cannot reopen folder"):
                module.create_project_folder(self.root, "unreadable")
        self.assertFalse((self.root / "unreadable").exists())

    def test_directory_creation_tolerates_another_creator_winning_the_race(self):
        module = self.module
        (self.root / "existing").mkdir()
        real_mkdir = os.mkdir

        def race_on_existing(name, *args, **kwargs):
            if name == "existing" and kwargs.get("dir_fd") is not None:
                raise FileExistsError(name)
            return real_mkdir(name, *args, **kwargs)

        with module.open_directory_chain(self.root) as directory:
            with patch.object(module.os, "mkdir", side_effect=race_on_existing):
                child = module._open_directory(directory, "existing", create=True)
            os.close(child)

    def test_create_and_move_reject_occupied_or_unsafe_destinations(self):
        module = self.module
        module.write_project_file(self.root, "source.md", b"owned", overwrite=False, expected_hash=None)
        module.write_project_file(self.root, "target.md", b"other", overwrite=False, expected_hash=None)
        with self.assertRaises(FileExistsError):
            module.move_project_file(self.root, "source.md", "target.md", expected_hash=None)
        self.assertEqual((self.root / "source.md").read_bytes(), b"owned")

        module.create_project_folder(self.root, "existing")
        module.create_project_folder(self.root, "existing")
        (self.root / "linked").symlink_to(self.root / "existing", target_is_directory=True)
        with self.assertRaises((ValueError, OSError)):
            module.create_project_folder(self.root, "linked")

        real_link = os.link

        def occupied_target(*args, **kwargs):
            if args[1] == "raced.md":
                raise FileExistsError("another writer published first")
            return real_link(*args, **kwargs)

        with patch.object(module.os, "link", side_effect=occupied_target):
            with self.assertRaises(FileExistsError):
                module.write_project_file(self.root, "raced.md", b"owned", overwrite=False, expected_hash=None)
        self.assertFalse(list(self.root.glob(".knoggin-*.tmp")))

    def test_cleanup_rejects_stale_identity_and_removes_link_without_following(self):
        module = self.module
        outside = Path(self.temporary.name) / "outside"
        outside.mkdir()
        (outside / "keep.md").write_bytes(b"private")
        (self.root / "linked").symlink_to(outside, target_is_directory=True)

        with self.assertRaisesRegex(RuntimeError, "changed during file cleanup"):
            module.remove_project_tree(self.root.parent, self.root.name, expected_file_id=-1)
        self.assertTrue(module.remove_project_tree(self.root.parent, self.root.name))
        self.assertEqual((outside / "keep.md").read_bytes(), b"private")
        self.assertFalse(module.remove_project_tree(self.root.parent, self.root.name))

        linked_root = self.root.parent / "linked-project"
        linked_root.symlink_to(outside, target_is_directory=True)
        self.assertTrue(module.remove_project_tree(self.root.parent, linked_root.name))
        self.assertEqual((outside / "keep.md").read_bytes(), b"private")

    def test_cleanup_handles_project_removed_after_identity_check(self):
        module = self.module
        real_open = os.open

        def remove_before_root_open(name, flags, *args, **kwargs):
            if name == self.root.name and kwargs.get("dir_fd") is not None:
                self.root.rmdir()
                raise FileNotFoundError(name)
            return real_open(name, flags, *args, **kwargs)

        with patch.object(module.os, "open", side_effect=remove_before_root_open):
            self.assertFalse(module.remove_project_tree(self.root.parent, self.root.name))

    def test_marker_cleanup_ignores_a_marker_removed_after_listing(self):
        module = self.module
        module.write_pending_marker(self.root.parent, "project-1.pending", "a" * 64)
        real_open_file = module._open_file

        def remove_before_marker_open(parent, name, *, access=os.O_RDONLY):
            if name == "project-1.pending":
                os.unlink(name, dir_fd=parent)
                raise FileNotFoundError(name)
            return real_open_file(parent, name, access=access)

        with patch.object(module, "_open_file", side_effect=remove_before_marker_open):
            module.delete_pending_markers_for_project(self.root.parent, "project-1.")
        self.assertIsNone(module.read_pending_marker(self.root.parent, "project-1.pending"))

    def test_pending_marker_operations_handle_absence_and_remove_only_matching_names(self):
        module = self.module
        self.assertIsNone(module.read_pending_marker(self.root.parent, "missing.pending"))
        self.assertFalse(module.delete_pending_marker(self.root.parent, "missing.pending"))
        module.delete_pending_markers_for_project(self.root.parent, "project-1.")

        module.write_pending_marker(self.root.parent, "project-1.first", "a" * 64)
        module.write_pending_marker(self.root.parent, "other.pending", "b" * 64)
        module.delete_pending_markers_for_project(self.root.parent, "project-1.")
        self.assertIsNone(module.read_pending_marker(self.root.parent, "project-1.first"))
        self.assertEqual(module.read_pending_marker(self.root.parent, "other.pending"), "b" * 64)

    def test_scan_detects_replaced_file_and_ignores_vanished_entry(self):
        module = self.module
        (self.root / "a.md").write_bytes(b"original")
        replacement = self.root / "replacement.tmp"
        replacement.write_bytes(b"replacement")
        real_open = os.open

        def replace_before_open(name, flags, *args, **kwargs):
            if name == "a.md" and kwargs.get("dir_fd") is not None:
                os.replace(replacement, self.root / name)
            return real_open(name, flags, *args, **kwargs)

        with patch.object(module.os, "open", side_effect=replace_before_open):
            with self.assertRaisesRegex(RuntimeError, "changed during file scan"):
                list(module.iter_project_paths(self.root))

        def remove_before_open(name, flags, *args, **kwargs):
            if name == "a.md" and kwargs.get("dir_fd") is not None:
                (self.root / name).unlink()
            return real_open(name, flags, *args, **kwargs)

        with patch.object(module.os, "open", side_effect=remove_before_open):
            self.assertEqual(list(module.iter_project_paths(self.root)), [])

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
