"""Reject duplicate evaluation examples before running or scoring them."""


def index_unique(items, key, description):
    indexed = {}
    for item in items:
        item_key = key(item)
        if item_key in indexed:
            raise ValueError(f"Duplicate {description} key")
        indexed[item_key] = item
    return indexed


def validate_case_ids(cases):
    if any(
        not isinstance(case.get("id"), str) or not case["id"].strip() for case in cases
    ):
        raise ValueError("Review cases require non-empty string IDs")
    index_unique(cases, lambda case: case["id"], "review case")
