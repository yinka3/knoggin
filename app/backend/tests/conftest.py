import sys
from pathlib import Path


BACKEND_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = BACKEND_ROOT.parents[1]

for path in (
    BACKEND_ROOT / "src",
    REPOSITORY_ROOT / "sdk" / "src",
    REPOSITORY_ROOT / "server" / "src",
):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
