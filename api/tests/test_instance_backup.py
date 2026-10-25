from pathlib import Path

import pytest
import yaml

import instance_backup


def test_backup_verification_and_restore_into_fresh_targets(monkeypatch, tmp_path):
    settings = tmp_path / "settings"
    library = settings / "data" / "projects"
    (library / "project-1").mkdir(parents=True)
    (library / "project-1" / "PROJECT.md").write_text("Project notes", encoding="utf-8")
    (library / "empty-project").mkdir()
    (settings / "knoggin.yml").write_text(
        "developer_settings:\n  documents:\n    project_library_root: data/projects\n",
        encoding="utf-8",
    )
    commands = []

    def postgres(command, _database_url):
        commands.append(command[0])
        if command[0] == "pg_dump":
            Path(command[-1]).write_bytes(b"disposable database dump")

    monkeypatch.setattr(instance_backup, "_run_postgres", postgres)
    monkeypatch.setattr(instance_backup, "_empty_database", lambda _url: None)

    backup = instance_backup.create_backup(
        tmp_path / "backup", database_url="postgresql:///source", settings_dir=settings,
    )
    assert instance_backup.verify_backup(backup)["format_version"] == 1
    assert (backup / "projects" / "project-1" / "PROJECT.md").read_text() == "Project notes"

    restored = tmp_path / "restored"
    target_settings, target_library = instance_backup.restore_backup(
        backup, database_url="postgresql:///empty_target", settings_dir=restored,
    )
    assert (target_library / "project-1" / "PROJECT.md").read_text() == "Project notes"
    assert (target_library / "empty-project").is_dir()
    config = yaml.safe_load((target_settings / "knoggin.yml").read_text())
    assert config["developer_settings"]["documents"]["project_library_root"] == str(target_library)
    assert commands.count("pg_dump") == 1
    assert commands.count("pg_restore") == 4  # three integrity checks and one restore


def test_restore_rejects_modified_backup_or_populated_target(monkeypatch, tmp_path):
    monkeypatch.setattr(instance_backup, "_run_postgres", lambda command, url: (
        Path(command[-1]).write_bytes(b"dump") if command[0] == "pg_dump" else None
    ))
    monkeypatch.setattr(instance_backup, "_empty_database", lambda _url: None)
    settings = tmp_path / "settings"
    settings.mkdir()
    backup = instance_backup.create_backup(
        tmp_path / "backup", database_url="postgresql:///source", settings_dir=settings,
    )
    target = tmp_path / "target"
    target.mkdir()
    (target / "existing.txt").write_text("keep", encoding="utf-8")
    with pytest.raises(ValueError, match="not empty"):
        instance_backup.restore_backup(
            backup, database_url="postgresql:///empty_target", settings_dir=target,
        )
    assert (target / "existing.txt").read_text() == "keep"

    (backup / "database.dump").write_bytes(b"changed")
    with pytest.raises(ValueError, match="checksums"):
        instance_backup.verify_backup(backup)


def test_backup_rejects_symlinked_project_files(monkeypatch, tmp_path):
    settings = tmp_path / "settings"
    library = settings / "data" / "projects"
    library.mkdir(parents=True)
    (library / "outside").symlink_to(tmp_path)
    monkeypatch.setattr(instance_backup, "_run_postgres", lambda command, url: (
        Path(command[-1]).write_bytes(b"dump") if command[0] == "pg_dump" else None
    ))
    with pytest.raises(ValueError, match="symlinks"):
        instance_backup.create_backup(
            tmp_path / "backup", database_url="postgresql:///source", settings_dir=settings,
        )
    assert not (tmp_path / "backup").exists()


def test_absolute_library_path_requires_explicit_restore_destination(monkeypatch, tmp_path):
    settings = tmp_path / "settings"
    settings.mkdir()
    library = tmp_path / "source-library"
    library.mkdir()
    (library / "note.txt").write_text("saved", encoding="utf-8")
    (settings / "knoggin.yml").write_text(
        yaml.safe_dump({"developer_settings": {"documents": {
            "project_library_root": str(library),
        }}}), encoding="utf-8",
    )
    monkeypatch.setattr(instance_backup, "_run_postgres", lambda command, url: (
        Path(command[-1]).write_bytes(b"dump") if command[0] == "pg_dump" else None
    ))
    monkeypatch.setattr(instance_backup, "_empty_database", lambda _url: None)
    backup = instance_backup.create_backup(
        tmp_path / "backup", database_url="postgresql:///source", settings_dir=settings,
    )
    with pytest.raises(ValueError, match="--library-root"):
        instance_backup.restore_backup(
            backup, database_url="postgresql:///target", settings_dir=tmp_path / "new-settings",
        )
    destination = tmp_path / "new-library"
    instance_backup.restore_backup(
        backup, database_url="postgresql:///target", settings_dir=tmp_path / "new-settings",
        library_root=destination,
    )
    assert (destination / "note.txt").read_text() == "saved"
