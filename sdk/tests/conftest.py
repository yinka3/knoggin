import sys
from pathlib import Path


SDK_ROOT = Path(__file__).resolve().parents[1]
REPOSITORY_ROOT = SDK_ROOT.parent

for path in (SDK_ROOT / "src", REPOSITORY_ROOT / "server" / "src"):
    if str(path) not in sys.path:
        sys.path.insert(0, str(path))
