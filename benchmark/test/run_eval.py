import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from typing import List, Tuple
import uuid
from loguru import logger
import os
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from main.setup import _create_user_entity
from shared.service import LLMService
from main.prompts import get_topic_seed_prompt
from shared.resource import ResourceManager
from shared.redisclient import RedisKeys
from agent.streaming import run_stream
from main.context import Context
from shared.schema.dtypes import MessageData
from log.logging_setup import setup_logging

setup_logging(log_level="DEBUG", log_file="benchmark.log", colorize=True)

USER_NAME = "TestUser"
SESSION_ID = str(uuid.uuid4())

DATASET_FILES = {
    "multi": "test_multi_session.json",
    "single": "test_single_user.json",
    "tempo": "test_tempo_session.json",
    "abs": "test_abstinence.json",
    "know": "test_know_updates.json",
    "user-ai": "test_user_assistant.json",
    "user-pref": "test_user_pref_session.json"
}

async def generate_instance_topics(llm: LLMService, haystack_sessions: list) -> dict:
    """Generate topic config from haystack user messages."""
    user_msgs = []
    for session in haystack_sessions:
        for turn in session:
            if turn["role"] == "user":
                user_msgs.append(turn["content"])

    text_block = "\n\n".join(user_msgs)

    system = get_topic_seed_prompt()
    raw = await llm.call_llm(system, text_block)

    if not raw:
        logger.warning("Topic generation failed, using defaults")
        return None

    cleaned = raw.strip()
    if cleaned.startswith("```"):
        cleaned = cleaned.split("\n", 1)[-1]
    if cleaned.endswith("```"):
        cleaned = cleaned.rsplit("```", 1)[0]
    cleaned = cleaned.strip()

    try:
        generated = json.loads(cleaned)
    except json.JSONDecodeError:
        logger.warning(f"Topic generation returned invalid JSON")
        return None

    generated.pop("General", None)
    generated.pop("Identity", None)

    for name, cfg in generated.items():
        cfg.setdefault("labels", [])
        cfg.setdefault("aliases", [])
        cfg.setdefault("hierarchy", {})
        cfg.setdefault("active", True)

    defaults = {
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []}
    }

    return {**defaults, **generated}

async def ingest_haystack(context: Context, haystack_sessions: list, haystack_dates: list):
    total_sessions = len(haystack_sessions)

    for session_idx, session in enumerate(haystack_sessions):
        parts = haystack_dates[session_idx]
        date_str = parts.split(" (")[0]
        time_str = parts.split(") ")[1]
        session_date = datetime.strptime(f"{date_str} {time_str}", "%Y/%m/%d %H:%M")
        session_date = session_date.replace(tzinfo=timezone.utc)

        logger.info(f"Session {session_idx + 1}/{total_sessions} - {len(session)} turns")

        for i, turn in enumerate(session):
            if turn["role"] == "user":
                msg = MessageData(message=turn["content"], timestamp=session_date)
                await context.add(msg)
            else:
                await context.add_assistant_turn(turn["content"], session_date)

            if (i + 1) % 8 == 0:
                await wait_for_batch_drain(context)

        await wait_for_batch_drain(context)
        await wait_for_processing(context)
        logger.info(f"Session {session_idx + 1}/{total_sessions} complete")

    logger.info("Haystack ingestion complete")


async def wait_for_batch_drain(context: Context, timeout: int = 120):
    start = asyncio.get_event_loop().time()

    while asyncio.get_event_loop().time() - start < timeout:
        buffer_len = await context.redis_client.llen(
            RedisKeys.buffer(context.user_name, context.session_id)
        )
        if buffer_len == 0:
            return
        await asyncio.sleep(2)

    logger.warning(f"Batch drain timeout after {timeout}s")


async def wait_for_processing(context: Context, drain_timeout: int = 180, max_retries: int = 10):
    for attempt in range(max_retries):
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < drain_timeout:
            buffer_len = await context.redis_client.llen(
                RedisKeys.buffer(context.user_name, context.session_id)
            )
            if buffer_len == 0:
                break
            await asyncio.sleep(2)

        buffer_len = await context.redis_client.llen(
            RedisKeys.buffer(context.user_name, context.session_id)
        )
        dirty_len = await context.redis_client.scard(
            RedisKeys.dirty_entities(context.user_name, context.session_id)
        )
        merge_len = await context.redis_client.scard(
            RedisKeys.merge_queue(context.user_name, context.session_id)
        )

        if buffer_len == 0 and dirty_len == 0 and merge_len == 0:
            logger.info("All queues processing completely finished.")
            break
            
        logger.info(f"Work still pending (buffer={buffer_len}, dirty={dirty_len}, merge={merge_len}), flushing...")
        
        await context.redis_client.set(
            RedisKeys.checkpoint(context.user_name, context.session_id), 0
        )
        await context._run_session_jobs()
        await asyncio.sleep(2)


async def ask_agent(context: Context, question: str, question_date: str) -> Tuple[str, List]:
    tools_used = []
    response = None

    async for event in run_stream(
        user_query=question,
        user_name=context.user_name,
        session_id=SESSION_ID,
        agent_id="benchmark",
        agent_name="STELLA",
        agent_persona="",
        conversation_history=[],
        hot_topics=[],
        topic_config=context.topic_config,
        llm=context.llm,
        store=context.store,
        ent_resolver=context.ent_resolver,
        redis_client=context.redis_client,
        model=context.model,
        file_rag=context.file_rag,
        user_timezone="UTC",
        mcp_manager=context.mcp_manager
    ):
        evt = event.get("event")

        if evt == "tool_start":
            tools_used.append(event["data"]["tool"])
        elif evt == "response":
            response = event["data"]["content"]
        elif evt == "clarification":
            response = event["data"]["question"]

    return response or "No response generated", tools_used


def answer_key(index, file):
    with open(f"{file}", "r") as f:
        questions = json.load(f)

    q = questions[index]

    print(f"Question: {q['question']}")
    print(f"Expected answer: {q['answer']}")
    print(f"Answer session IDs: {q['answer_session_ids']}")
    print(f"\n{'='*60}\nEVIDENCE TURNS:\n{'='*60}\n")

    for idx, session in enumerate(q["haystack_sessions"]):
        for j, turn in enumerate(session):
            if turn.get("has_answer"):
                print(f"Session {idx}, Turn {j} ({turn['role']}):")
                print(f"{turn['content']}")
                print("-" * 40)

    print("-" * 40)
    print("-" * 40)


async def main():
    if len(sys.argv) < 3:
        print("Usage: python run_eval.py <dataset> <index>")
        print(f"  dataset: {list(DATASET_FILES.keys())}")
        print("  index: question index (0-49)")
        print("Example: python run_eval.py multi 0")
        sys.exit(1)

    dataset = sys.argv[1]
    idx = int(sys.argv[2])

    if dataset not in DATASET_FILES:
        print(f"Unknown dataset: {dataset}")
        print(f"Valid options: {list(DATASET_FILES.keys())}")
        sys.exit(1)

    dataset_file = Path(__file__).parent / DATASET_FILES[dataset]

    with open(dataset_file, "r") as f:
        questions = json.load(f)

    if idx >= len(questions):
        print(f"Index {idx} out of range. Max: {len(questions) - 1}")
        sys.exit(1)

    q = questions[idx]

    logger.info(f"\n{'='*60}")
    logger.info(f"Dataset: {dataset} | Instance {idx}: {q['question_type']}")
    logger.info(f"Question: {q['question']}")
    logger.info(f"Expected: {q['answer']}")
    logger.info(f"Question Date: {q['question_date']}")
    logger.info(f"{'='*60}")

    load_dotenv()

    resource = await ResourceManager.initialize()

    api_key = os.getenv("OPENROUTER_API_KEY")
    if api_key:
        resource.llm_service.update_settings(api_key=api_key)

    await _create_user_entity(resource, USER_NAME)

    topics = await generate_instance_topics(resource.llm_service, q["haystack_sessions"])
    if not topics:
        topics = {
            "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
            "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []}
        }

    context = await Context.create(
        user_name=USER_NAME,
        resources=resource,
        topics_config=topics,
        session_id=SESSION_ID
    )

    context.consumer.update_ingestion_settings(batch_timeout=30.0)

    try:
        await ingest_haystack(context, q["haystack_sessions"], q["haystack_dates"])

        logger.info("Waiting for processing...")
        await asyncio.sleep(30)
        await wait_for_processing(context)
        logger.info("Asking AGENT...")
        response, tools_used = await ask_agent(context, q["question"], q["question_date"])

        logger.info(f"\n{'='*60}")
        logger.info(f"RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Question: {q['question']}")
        logger.info(f"Expected: {q['answer']}")
        logger.info(f"AGENT: {response}")

        answer_key(idx, dataset_file)
        result = {
            "index": idx,
            "dataset": dataset,
            "type": q["question_type"],
            "question": q["question"],
            "question_date": q["question_date"],
            "expected": q["answer"],
            "response": response,
            "tools_used": tools_used
        }

        output_file = Path(__file__).parent / f"eval_result_{dataset}_{idx}.json"
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)

        logger.info(f"\nSaved to {output_file}")

    finally:
        await context.shutdown()
        resource.store.close()


if __name__ == "__main__":
    asyncio.run(main())