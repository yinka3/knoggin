"""Offline whole-instance backup and restore for the local API runtime."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import shutil
import subprocess
import tempfile
from datetime import datetime, timezone
from pathlib import Path

import psycopg
import yaml
from psycopg.conninfo import conninfo_to_dict

FORMAT_VERSION = 1
DEFAULT_LIBRARY_ROOT = "data/projects"


def _configured_library_root(settings_dir: Path, config_file: Path) -> str:
    if not config_file.exists():
        return DEFAULT_LIBRARY_ROOT
    if config_file.is_symlink() or not config_file.is_file():
        raise ValueError("Settings file must be a plain file")
    config = yaml.safe_load(config_file.read_text(encoding="utf-8")) or {}
    if not isinstance(config, dict):
        raise ValueError("Settings file must contain a mapping")
    developer = config.get("developer_settings") or {}
    if not isinstance(developer, dict):
        raise ValueError("Developer settings must contain a mapping")
    documents = developer.get("documents") or {}
    if not isinstance(documents, dict):
        raise ValueError("Document settings must contain a mapping")
    configured = documents.get("project_library_root", DEFAULT_LIBRARY_ROOT)
    if not isinstance(configured, str) or not configured.strip():
        raise ValueError("Project library root must be a path")
    return configured


def _resolve_library_root(settings_dir: Path, configured: str) -> Path:
    path = Path(configured).expanduser()
    return (path if path.is_absolute() else settings_dir / path).resolve()


def _pg_environment(database_url: str) -> dict[str, str]:
    values = conninfo_to_dict(database_url)
    names = {
        "host": "PGHOST", "hostaddr": "PGHOSTADDR", "port": "PGPORT",
        "user": "PGUSER", "password": "PGPASSWORD", "dbname": "PGDATABASE",
        "sslmode": "PGSSLMODE", "service": "PGSERVICE", "options": "PGOPTIONS",
        "connect_timeout": "PGCONNECT_TIMEOUT",
    }
    unknown = set(values) - names.keys()
    if unknown:
        raise ValueError("Unsupported database connection options")
    if not values.get("dbname"):
        raise ValueError("Database URL must name a database")
    environment = os.environ.copy()
    for name, key in names.items():
        environment.pop(key, None)
        if name in values:
            environment[key] = str(values[name])
    return environment


def _run_postgres(command: list[str], database_url: str) -> None:
    result = subprocess.run(
        command, env=_pg_environment(database_url), capture_output=True, text=True,
        check=False,
    )
    if result.returncode:
        raise RuntimeError(f"{command[0]} failed: {result.stderr.strip()}")


def _sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _files_under(root: Path):
    if not root.exists():
        return
    if root.is_symlink() or not root.is_dir():
        raise ValueError(f"Expected a plain directory: {root}")
    for current, directories, files in os.walk(root, followlinks=False):
        for name in sorted(directories):
            if (Path(current) / name).is_symlink():
                raise ValueError("Backup cannot contain symlinks")
        for name in sorted(files):
            path = Path(current) / name
            if path.is_symlink() or not path.is_file():
                raise ValueError("Backup cannot contain links or special files")
            yield path


def _file_manifest(root: Path) -> dict[str, dict[str, str | int]]:
    return {
        path.relative_to(root).as_posix(): {
            "size": path.stat().st_size, "sha256": _sha256(path),
        }
        for path in _files_under(root)
        if path.relative_to(root).as_posix() != "manifest.json"
    }


def _directory_manifest(root: Path) -> list[str]:
    directories = []
    for current, names, _ in os.walk(root, followlinks=False):
        for name in names:
            path = Path(current) / name
            if path.is_symlink():
                raise ValueError("Backup cannot contain symlinks")
            directories.append(path.relative_to(root).as_posix())
    return sorted(directories)


def verify_backup(source: Path) -> dict:
    source = source.resolve()
    manifest = json.loads((source / "manifest.json").read_text(encoding="utf-8"))
    if manifest.get("format_version") != FORMAT_VERSION:
        raise ValueError("Unsupported backup format")
    expected = manifest.get("files")
    if not isinstance(expected, dict) or "database.dump" not in expected:
        raise ValueError("Backup manifest is incomplete")
    actual = _file_manifest(source)
    if actual != expected:
        raise ValueError("Backup files or checksums do not match the manifest")
    if _directory_manifest(source) != manifest.get("directories"):
        raise ValueError("Backup directories do not match the manifest")
    _run_postgres(["pg_restore", "--list", str(source / "database.dump")], "dbname=postgres")
    return manifest


def create_backup(output: Path, *, database_url: str, settings_dir: Path) -> Path:
    output = output.expanduser().resolve()
    settings_dir = settings_dir.expanduser().resolve()
    config_file = settings_dir / "knoggin.yml"
    configured_root = _configured_library_root(settings_dir, config_file)
    library_root = _resolve_library_root(settings_dir, configured_root)
    if output.exists():
        raise ValueError("Backup destination already exists")
    if output == settings_dir or output.is_relative_to(settings_dir) or output == library_root or output.is_relative_to(library_root):
        raise ValueError("Backup destination must be outside settings and project files")
    output.parent.mkdir(parents=True, exist_ok=True)
    staging = Path(tempfile.mkdtemp(prefix=".instance-backup-", dir=output.parent))
    try:
        staging.chmod(0o700)
        dump = staging / "database.dump"
        _run_postgres(["pg_dump", "--format=custom", "--no-owner", "--no-acl", "--file", str(dump)], database_url)
        dump.chmod(0o600)
        if config_file.exists():
            shutil.copy2(config_file, staging / "knoggin.yml")
            (staging / "knoggin.yml").chmod(0o600)
        for path in _files_under(library_root):
            target = staging / "projects" / path.relative_to(library_root)
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            target.chmod(0o600)
        for directory in _directory_manifest(library_root) if library_root.exists() else []:
            (staging / "projects" / directory).mkdir(parents=True, exist_ok=True)
        manifest = {
            "format_version": FORMAT_VERSION,
            "created_at": datetime.now(timezone.utc).isoformat(),
            "project_library_root": configured_root,
            "files": _file_manifest(staging),
            "directories": _directory_manifest(staging),
        }
        (staging / "manifest.json").write_text(
            json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
        )
        (staging / "manifest.json").chmod(0o600)
        verify_backup(staging)
        staging.rename(output)
    except BaseException:
        shutil.rmtree(staging)
        raise
    return output


def _empty_database(database_url: str) -> None:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """SELECT EXISTS (
                SELECT 1 FROM pg_class AS relation
                JOIN pg_namespace AS namespace ON namespace.oid = relation.relnamespace
                WHERE namespace.nspname = 'public'
                  AND relation.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
            )"""
        ).fetchone()
    if row is None or row[0]:
        raise ValueError("Restore requires an empty target database")


def _require_empty_target(path: Path) -> None:
    if path.is_symlink() or (path.exists() and (not path.is_dir() or any(path.iterdir()))):
        raise ValueError(f"Restore destination is not empty: {path}")


def restore_backup(
    source: Path, *, database_url: str, settings_dir: Path,
    library_root: Path | None = None,
) -> tuple[Path, Path]:
    source = source.expanduser().resolve()
    manifest = verify_backup(source)
    settings_dir = settings_dir.expanduser().resolve()
    configured_root = manifest["project_library_root"]
    original_root = Path(configured_root).expanduser()
    if original_root.is_absolute() and library_root is None:
        raise ValueError("An absolute project library root needs --library-root")
    library_root = (
        library_root.expanduser().resolve() if library_root is not None
        else _resolve_library_root(settings_dir, configured_root)
    )
    if library_root == settings_dir or settings_dir.is_relative_to(library_root):
        raise ValueError("Project library root cannot contain the settings directory")
    if (
        source == settings_dir or source.is_relative_to(settings_dir)
        or settings_dir.is_relative_to(source)
        or source == library_root or source.is_relative_to(library_root)
        or library_root.is_relative_to(source)
    ):
        raise ValueError("Restore destination cannot contain the backup")
    _require_empty_target(settings_dir)
    _require_empty_target(library_root)
    _empty_database(database_url)

    # Prepare files before changing the database; publication uses only fresh targets.
    settings_dir.mkdir(parents=True, exist_ok=True)
    library_root.mkdir(parents=True, exist_ok=True)
    config_file = source / "knoggin.yml"
    config = yaml.safe_load(config_file.read_text(encoding="utf-8")) if config_file.exists() else {}
    config = config or {}
    if not isinstance(config, dict):
        raise ValueError("Backup settings file must contain a mapping")
    developer = config.get("developer_settings") or {}
    if not isinstance(developer, dict):
        raise ValueError("Backup developer settings must contain a mapping")
    documents = developer.get("documents") or {}
    if not isinstance(documents, dict):
        raise ValueError("Backup document settings must contain a mapping")
    config["developer_settings"] = developer
    developer["documents"] = documents
    documents["project_library_root"] = str(library_root)
    staged_config = settings_dir / ".knoggin.restore.yml"
    staged_config.write_text(yaml.safe_dump(config, sort_keys=False), encoding="utf-8")
    staged_config.chmod(0o600)
    try:
        _run_postgres(["pg_restore", "--no-owner", "--no-acl", "--exit-on-error", "--dbname", _pg_environment(database_url)["PGDATABASE"], str(source / "database.dump")], database_url)
        for directory in manifest["directories"]:
            if directory == "projects" or directory.startswith("projects/"):
                relative = Path(directory).relative_to("projects")
                (library_root / relative).mkdir(parents=True, exist_ok=True)
        for path in _files_under(source / "projects"):
            target = library_root / path.relative_to(source / "projects")
            target.parent.mkdir(parents=True, exist_ok=True)
            shutil.copy2(path, target)
            target.chmod(0o600)
        staged_config.rename(settings_dir / "knoggin.yml")
    except BaseException:
        staged_config.unlink(missing_ok=True)
        raise
    return settings_dir, library_root


def main() -> None:
    parser = argparse.ArgumentParser(description="Offline whole-instance backup and restore")
    commands = parser.add_subparsers(dest="command", required=True)
    backup = commands.add_parser("backup")
    backup.add_argument("output", type=Path)
    backup.add_argument("--settings-dir", type=Path, default=Path(os.environ.get("SETTINGS_DIR", "~/.config/knoggin")))
    verify = commands.add_parser("verify")
    verify.add_argument("source", type=Path)
    restore = commands.add_parser("restore")
    restore.add_argument("source", type=Path)
    restore.add_argument("--settings-dir", type=Path, required=True)
    restore.add_argument("--library-root", type=Path)
    args = parser.parse_args()
    try:
        if args.command == "verify":
            verify_backup(args.source)
            print("Backup verified")
        else:
            database_url = os.environ.get("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL must point to the source or empty target database")
            if args.command == "backup":
                print(f"Backup created: {create_backup(args.output, database_url=database_url, settings_dir=args.settings_dir)}")
            else:
                settings, files = restore_backup(
                    args.source, database_url=database_url, settings_dir=args.settings_dir,
                    library_root=args.library_root,
                )
                print(f"Restored into {settings} and {files}")
    except (OSError, ValueError, RuntimeError, psycopg.Error) as exc:
        parser.exit(1, f"Backup operation failed: {exc}\n")


if __name__ == "__main__":
    main()
