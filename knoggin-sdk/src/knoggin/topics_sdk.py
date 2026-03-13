import re
from typing import Dict, List, Optional
from loguru import logger

LABEL_PATTERN = re.compile(r'^[a-z][a-z0-9 _-]*$')
MAX_LABEL_LEN = 30

def _validate_label(label: str) -> Optional[str]:
    label = label.strip().lower()
    if not label or len(label) > MAX_LABEL_LEN:
        return None
    if not LABEL_PATTERN.match(label):
        return None
    return label

class TopicBuilder:
    """Fluent builder for topic configurations."""
    def __init__(self):
        self._topics: Dict[str, dict] = {}

    def topic(
        self,
        name: str,
        labels: List[str] = None,
        aliases: List[str] = None,
        hierarchy: Dict = None,
        active: bool = True,
        hot: bool = False,
    ) -> "TopicBuilder":
        if name in ("General", "Identity"):
            logger.warning(f"'{name}' is auto-included. Skipping.")
            return self

        clean_labels = [l for raw in (labels or []) if (l := _validate_label(raw))]

        self._topics[name] = {
            "active": active,
            "labels": clean_labels,
            "aliases": aliases or [],
            "hierarchy": hierarchy or {},
            "hot": hot,
        }
        return self

    def build(self) -> dict:
        return self._topics