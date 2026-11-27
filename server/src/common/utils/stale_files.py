"""Remove scratch files and folders a crashed process left behind.

Every name swept here is only ever held while work runs: a Docling extraction,
a staged folder upload, an atomic file write. Normal exits remove them; a hard
kill (crash, power loss, SIGKILL) skips that, so startup sweeps what is old
enough that no live work can still own it.
"""

from __future__ import annotations

import os
import shutil
import stat
import time
from pathlib import Path
from typing import Iterable

from loguru import logger

# Long enough that no extraction, upload or write is still using the entry.
STALE_AFTER_SECONDS = 6 * 60 * 60


def remove_stale_entries(
    directory: Path,
    patterns: Iterable[str],
    *,
    older_than_seconds: float = STALE_AFTER_SECONDS,
    now: float | None = None,
) -> int:
    """Delete entries in ``directory`` matching ``patterns`` and not modified
    for ``older_than_seconds``. Links are removed, never followed. Returns how
    many were removed; one that cannot be removed is logged and skipped."""

    cutoff = (time.time() if now is None else now) - older_than_seconds
    removed = 0
    try:
        candidates = {entry for pattern in patterns for entry in directory.glob(pattern)}
    except OSError:
        return 0
    for entry in sorted(candidates):
        try:
            metadata = entry.lstat()
            if metadata.st_mtime > cutoff:
                continue
            if stat.S_ISDIR(metadata.st_mode):
                shutil.rmtree(entry)
            else:
                os.unlink(entry)
            removed += 1
        except FileNotFoundError:
            continue
        except OSError:
            logger.warning("Could not remove stale scratch entry {}", entry)
    return removed
