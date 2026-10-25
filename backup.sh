#!/usr/bin/env bash
set -euo pipefail

repo_root="$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)"

case "${1:-}" in
    -h|--help)
        printf 'Usage: %s [backup-directory]\n       %s verify <backup-directory>\n' "$0" "$0"
        exit 0
        ;;
    verify)
        if (( $# != 2 )); then
            printf 'Usage: %s verify <backup-directory>\n' "$0" >&2
            exit 2
        fi
        if ! command -v uv >/dev/null 2>&1; then
            printf 'uv is required to run the backup CLI\n' >&2
            exit 1
        fi
        cd -- "$repo_root"
        exec uv run --package api python -m instance_backup verify "$2"
        ;;
esac

if (( $# > 1 )); then
    printf 'Usage: %s [backup-directory]\n' "$0" >&2
    exit 2
fi
if [[ ! -f "$repo_root/.env" ]]; then
    printf 'Missing %s/.env\n' "$repo_root" >&2
    exit 1
fi
if ! command -v uv >/dev/null 2>&1; then
    printf 'uv is required to run the backup CLI\n' >&2
    exit 1
fi

destination="${1:-$HOME/knoggin-backups/backup-$(date -u +%Y%m%dT%H%M%SZ)}"
cd -- "$repo_root"
exec env -u DATABASE_URL -u SETTINGS_DIR uv run --package api --env-file "$repo_root/.env" python -m instance_backup backup "$destination"
