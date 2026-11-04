"""Answer selected LongMemEval questions using projects whose memory is already built."""

import argparse
import asyncio
import json
import logging
import os
from pathlib import Path

from common.utils.time_utils import reset_clock, set_test_clock
from runtime.application import ApplicationRuntime

from dataset import load_questions
from run import _ask

logger = logging.getLogger("longmemeval.answer_existing")


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--data", type=Path, required=True)
    parser.add_argument("--ids-file", type=Path, required=True)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--model", required=True)
    parser.add_argument("--source-run-name", required=True)
    parser.add_argument("--no-message-tools", action="store_true",
                        help="answer from Context, Episodes, and graph without raw-message tools")
    args = parser.parse_args()
    if not os.environ.get("DATABASE_URL"):
        parser.error("DATABASE_URL is required")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(message)s")

    ids = json.loads(args.ids_file.read_text())
    selected = {q.question_id: q for q in load_questions(args.data)
                if q.question_id in ids}
    if len(ids) != len(set(ids)) or set(ids) != set(selected):
        parser.error("ids-file must contain unique question IDs present in the dataset")

    args.out.mkdir(parents=True, exist_ok=True)
    answers_path = args.out / "hypotheses.jsonl"
    done = {json.loads(line)["question_id"] for line in answers_path.read_text().splitlines()} if answers_path.exists() else set()
    runtime = await ApplicationRuntime.start(
        user_name=f"lme_{args.source_run_name}", config_dir=args.out / "config"
    )
    try:
        projects = {row["name"]: row for row in await runtime.projects.list_projects()
                    if row["status"] != "deleted"}
        missing = [qid for qid in ids if f"lme-{qid}" not in projects]
        if missing:
            raise RuntimeError(f"Missing ingested projects: {missing}")
        for index, qid in enumerate(ids, 1):
            if qid in done:
                continue
            question = selected[qid]
            logger.info("[%d/%d] %s using %s", index, len(ids), qid, args.model)
            set_test_clock(question.question_date)
            try:
                answer, usage = await _ask(
                    runtime, projects[f"lme-{qid}"]["id"], question,
                    args.model, args.no_message_tools,
                )
            except Exception as exc:
                logger.exception("%s failed: %s", qid, exc)
                with (args.out / "errors.jsonl").open("a") as file:
                    file.write(json.dumps({"question_id": qid, "error": repr(exc)}) + "\n")
            else:
                with answers_path.open("a") as file:
                    file.write(json.dumps({"question_id": qid, "hypothesis": answer}) + "\n")
                with (args.out / "answer-usage.jsonl").open("a") as file:
                    file.write(json.dumps({"question_id": qid, "usage": usage}) + "\n")
            finally:
                reset_clock()
    finally:
        await runtime.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
