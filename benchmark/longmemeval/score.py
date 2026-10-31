"""Judge a run's answers with LongMemEval's evaluator and print accuracy by type."""

import argparse
import json
import os
import subprocess
import sys
from collections import defaultdict
from pathlib import Path

# Reported first: the types this benchmark is mainly run for.
FIRST = ["knowledge-update", "temporal-reasoning", "abstention"]
JUDGE_MODEL = "gpt-4o-2024-08-06"


def _args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run", type=Path, required=True,
                        help="run directory; with --shards, where the combined run goes")
    parser.add_argument("--shards", type=Path, nargs="+",
                        help="run directories of one run split across processes")
    parser.add_argument("--data", type=Path, required=True, help="the dataset file run")
    parser.add_argument("--evaluator", type=Path,
                        help="LongMemEval's src/evaluation/evaluate_qa.py")
    parser.add_argument("--openrouter", action="store_true",
                        help="send the judge through OpenRouter with OPENROUTER_API_KEY")
    parser.add_argument("--no-judge", action="store_true",
                        help="only summarize existing judge results")
    return parser.parse_args()


def _judge(args, hypotheses: Path) -> None:
    if args.evaluator is None:
        sys.exit("--evaluator is required unless --no-judge is given")
    script = args.evaluator
    env = dict(os.environ)
    if args.openrouter:
        # OpenRouter names the same pinned snapshot with a provider prefix.
        source = script.read_text()
        if f"'{JUDGE_MODEL}'" not in source:
            sys.exit(f"{script} no longer names {JUDGE_MODEL}; update score.py")
        script = args.run / "evaluate_qa_openrouter.py"
        script.write_text(source.replace(f"'{JUDGE_MODEL}'", f"'openai/{JUDGE_MODEL}'"))
        env["OPENAI_API_KEY"] = env["OPENROUTER_API_KEY"]
        env["OPENAI_BASE_URL"] = "https://openrouter.ai/api/v1"
    subprocess.run(
        ["uv", "run", "--no-project", "--with", "openai", "--with", "backoff",
         "--with", "tqdm", "--with", "numpy", "python", str(script),
         "gpt-4o", str(hypotheses), str(args.data.resolve())],
        env=env, check=True,
    )


def _group(question_id: str, question_type: str) -> str:
    return "abstention" if question_id.endswith("_abs") else question_type


def _combine(run: Path, shards: list[Path]) -> None:
    run.mkdir(parents=True, exist_ok=True)
    for name in ("hypotheses.jsonl", "metrics.jsonl", "errors.jsonl"):
        parts = [shard / name for shard in shards if (shard / name).exists()]
        (run / name).write_text("".join(part.read_text() for part in parts))


def main() -> None:
    args = _args()
    if args.shards:
        _combine(args.run, args.shards)
    hypotheses = (args.run / "hypotheses.jsonl").resolve()
    if not args.no_judge:
        _judge(args, hypotheses)
    results = Path(f"{hypotheses}.eval-results-gpt-4o")
    types = {row["question_id"]: row["question_type"]
             for row in json.loads(args.data.read_text())}

    labels: dict[str, list[bool]] = defaultdict(list)
    for line in results.read_text().splitlines():
        row = json.loads(line)
        label = bool(row["autoeval_label"]["label"])
        labels[_group(row["question_id"], types[row["question_id"]])].append(label)

    order = [g for g in FIRST if g in labels] + sorted(set(labels) - set(FIRST))
    every = [label for group in labels.values() for label in group]
    table = {group: {"accuracy": sum(labels[group]) / len(labels[group]),
                     "count": len(labels[group])} for group in order}
    table["overall"] = {"accuracy": sum(every) / len(every), "count": len(every)}

    width = max(map(len, table))
    for group, score in table.items():
        print(f"{group:<{width}}  {score['accuracy']:6.1%}  ({score['count']})")
    (args.run / "scores.json").write_text(json.dumps(table, indent=2))


if __name__ == "__main__":
    main()
