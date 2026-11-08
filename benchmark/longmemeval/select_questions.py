"""Pick M questions no earlier run has seen, so the score is an honest one."""

import argparse
import json
import random
from collections import defaultdict
from pathlib import Path

import ijson

# Spread across types, with more of the two that need several sessions.
PER_TYPE = {
    "single-session-user": 3,
    "single-session-assistant": 3,
    "single-session-preference": 3,
    "knowledge-update": 3,
    "temporal-reasoning": 4,
    "multi-session": 4,
}


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--exclude", type=Path, nargs="+", required=True,
                        help="JSON lists of question IDs, or dataset files, already used")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--seed", type=int, default=20261104)
    parser.add_argument("--per-type", type=json.loads, default=PER_TYPE,
                        help='JSON count per question type, e.g. {"multi-session": 8}')
    args = parser.parse_args()

    used: set[str] = set()
    for path in args.exclude:
        for item in json.loads(path.read_text()):
            used.add(item["question_id"] if isinstance(item, dict) else item)

    by_type = defaultdict(list)
    with args.data.open("rb") as stream:
        for row in ijson.items(stream, "item"):
            if row["question_id"] not in used:
                by_type[row["question_type"]].append(row)

    rng = random.Random(args.seed)
    chosen = []
    for kind, count in args.per_type.items():
        pool = sorted(by_type[kind], key=lambda row: row["question_id"])
        chosen.extend(rng.sample(pool, count))
    args.out.write_text(json.dumps(chosen, default=str))
    print(json.dumps({row["question_id"]: row["question_type"] for row in chosen}, indent=1))


if __name__ == "__main__":
    main()
