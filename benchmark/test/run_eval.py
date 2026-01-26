import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from typing import List, Tuple
import uuid
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from agent.orchestrator import run
from db.store import MemGraphStore
from main.context import Context
from schema.dtypes import MessageData
from answer import answer_key
from log.logging_setup import setup_logging


setup_logging(log_level="DEBUG", log_file="benchmark.log", colorize=True)

USER_NAME = "TestUser"
SESSION_ID = str(uuid.uuid4())
TOPIC_CONFIG  = {
    "Identity": {
        "active": True,
        "labels": ["person"],
        "hierarchy": {},
        "aliases": [],
        "label_aliases": {}
    },
    "General": {
        "active": False,
        "labels": ["person", "place", "organization", "event"],
        "hierarchy": {},
        "aliases": [],
        "label_aliases": {}
    }
}


DATASET_FILES = {
    "multi": "test_multi_session.json",
    "single": "test_single_user.json",
    "tempo": "test_tempo_session.json",
    "abs": "test_abstinence.json",
    "know": "test_know_updates.json",
    "user-ai": "test_user_assistant.json",
    "user-pref": "test_user_pref_session.json"
}


async def ingest_haystack(context: Context, haystack_sessions: list, haystack_dates: list):
    total_sessions = len(haystack_sessions)
    
    for session_idx, session in enumerate(haystack_sessions):
        # Parse session date
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
            
            # Stability drain within large sessions
            if (i + 1) % 8 == 0:
                await wait_for_batch_drain(context)
        
        # Session boundary: drain + jobs
        await wait_for_batch_drain(context)
        await wait_for_processing(context)
        logger.info(f"Session {session_idx + 1}/{total_sessions} complete")
    
    logger.info("Haystack ingestion complete")


async def wait_for_batch_drain(context: Context, timeout: int = 120):
    user = context.user_name
    start = asyncio.get_event_loop().time()
    
    while asyncio.get_event_loop().time() - start < timeout:
        buffer_len = await context.redis_client.llen(f"buffer:{user}")
        if buffer_len == 0:
            return
        await asyncio.sleep(2)
    
    logger.warning(f"Batch drain timeout after {timeout}s")


async def wait_for_processing(context: Context, drain_timeout: int = 180, max_retries: int = 2):
    user = context.user_name
    
    for attempt in range(max_retries):
        # Drain buffer
        start = asyncio.get_event_loop().time()
        while asyncio.get_event_loop().time() - start < drain_timeout:
            buffer_len = await context.redis_client.llen(f"buffer:{user}")
            if buffer_len == 0:
                break
            await asyncio.sleep(2)
        
        # Check if there's still work pending
        buffer_len = await context.redis_client.llen(f"buffer:{user}")
        dirty_len = await context.redis_client.scard(f"dirty_entities:{user}")
        
        if buffer_len == 0 and dirty_len == 0:
            break
            
        if attempt < max_retries - 1:
            logger.warning(f"Work still pending (buffer={buffer_len}, dirty={dirty_len}), retry {attempt + 1}/{max_retries}")
            await asyncio.sleep(5)
    
    await context.redis_client.set(f"checkpoint_count:{user}", 0)
    await context._run_session_jobs()

async def ask_stella(context: Context, question: str, question_date: str) -> Tuple[str, List]:
    result = await run(
        user_query=question,
        user_name=context.user_name,
        conversation_history=[],
        hot_topics=[],
        topic_config=context.topic_config,
        llm=context.llm,
        store=context.store,
        ent_resolver=context.ent_resolver,
        redis_client=context.redis_client,
        date=question_date,
        slim_hot_context=True
    )
    
    response = result.response or (result.question or None)
    tools_used = result.tools_used or []
    return response, tools_used


def answer_key(index, file):

    with open(f"{file}", "r") as f:
        questions = json.load(f)

    q = questions[index]

    print(f"Question: {q['question']}")
    print(f"Expected answer: {q['answer']}")
    print(f"Answer session IDs: {q['answer_session_ids']}")
    print(f"\n{'='*60}\nEVIDENCE TURNS:\n{'='*60}\n")

    for index, session in enumerate(q["haystack_sessions"]):
        for j, turn in enumerate(session):
            if turn.get("has_answer"):
                print(f"Session {index}, Turn {j} ({turn['role']}):")
                print(f"{turn['content']}")
                print("-" * 40)
    
    print("-" * 40)
    print("-" * 40)


async def main():
    if len(sys.argv) < 3:
        print("Usage: python run_eval.py <dataset> <index>")
        print("  dataset: 'multi' or 'single'")
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
    
    executor = ThreadPoolExecutor(max_workers=2)
    store = MemGraphStore()
    context = await Context.create(
        user_name=USER_NAME,
        store=store,
        cpu_executor=executor,
        topics_config=TOPIC_CONFIG,
        session_id=SESSION_ID
    )
    
    try:
        await ingest_haystack(context, q["haystack_sessions"], q["haystack_dates"])

        logger.info("Waiting for processing...")
        await asyncio.sleep(30)
        await wait_for_processing(context)
        logger.info("Asking AGENT...")
        response, tools_used = await ask_stella(context, q["question"], q["question_date"])
        
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
        store.close()


if __name__ == "__main__":
    asyncio.run(main())