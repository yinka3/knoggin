import asyncio
import sys
import json
import argparse
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
import uuid
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from agent.loop import run
from db.memgraph import MemGraphStore
from main.context import Context
from schema.dtypes import MessageData

logger.remove()
logger.add(sys.stdout, level="INFO")

USER_NAME = "Adeyinka"
EVAL_DATE = "2025-12-05 00:30"
SESSION_ID = str(uuid.uuid4())
TOPIC_CONFIG = {
    "Workplace Dynamics": {
        "labels": ["person", "business", "role", "product"],
        "hierarchy": {
            "business": ["role"]
        }
    },
    "Academic Arcs": {
        "labels": ["person", "course", "assignment", "exam", "project", "group"],
        "hierarchy": {
            "course": ["assignment", "exam", "project"]
        }
    },
    "Intramural Sports": {
        "labels": ["person", "team", "sport", "game", "position"],
        "hierarchy": {
            "team": ["position"]
        }
    },
    "Interpersonal Relationships": {
        "labels": ["person", "group", "event"]
    },
    "Family & Heritage": {
        "labels": ["person", "tradition", "place", "pet"]
    },
    "Campus Geography": {
        "labels": ["building", "room", "landmark", "dorm", "facility"]
    },
    "Mental Health & Wellness": {
        "labels": ["person", "condition", "resource", "habit"]
    },
    "Food & Dining": {
        "labels": ["restaurant", "dish", "cuisine", "person"],
        "hierarchy": {
            "restaurant": ["dish"]
        }
    },
    "Entertainment & Media": {
        "labels": ["show", "movie", "music", "party", "person"]
    },
    "Daily Routines": {
        "labels": ["activity", "place", "habit"]
    }
}


async def ingest_haystack(context: Context, messages: list):
    total = len(messages)
    batch_size = 10  # Match BATCH_SIZE from context.py
    
    for i, turn in enumerate(messages):
        ts = datetime.strptime(turn["timestamp"], "%Y-%m-%d %H:%M")
        ts = ts.replace(tzinfo=timezone.utc)
        
        if turn["role"] == "user":
            msg = MessageData(message=turn["content"], timestamp=ts)
            await context.add(msg)
        
        if (i + 1) % 100 == 0:
            logger.info(f"Ingested {i + 1}/{total} turns")
        if (i + 1) % batch_size == 0:
            logger.info(f"Batch {(i + 1) // batch_size} queued, waiting for processing...")
            await wait_for_batch_drain(context)
        else:
            await asyncio.sleep(0.2)
    
    logger.info("Ingestion complete, waiting for final processing...")

async def ask_stella(context: Context, question: str) -> str:
    
    result = await run(
        user_query=question,
        user_name=context.user_name,
        conversation_history=[],
        hot_topics=[],
        topics_config=TOPIC_CONFIG,
        llm=context.llm,
        store=context.store,
        ent_resolver=context.ent_resolver,
        redis_client=context.redis_client,
        date=EVAL_DATE,
        slim_hot_context=True
    )
    
    response = result.get("response") or result.get("question", "No response")
    tools_used = result.get("tools_used", [])
    
    return response, tools_used


async def wait_for_batch_drain(context: Context, timeout: int = 120):
    """Wait for buffer to empty."""
    user = context.user_name
    start = asyncio.get_event_loop().time()
    
    while asyncio.get_event_loop().time() - start < timeout:
        buffer_len = await context.redis_client.llen(f"buffer:{user}")
        
        if buffer_len == 0:
            logger.debug("Buffer drained, continuing...")
            return
        
        await asyncio.sleep(2)
    
    logger.warning(f"Batch drain timeout after {timeout}s")


async def wait_for_processing(context: Context, poll_interval: int = 30, max_wait: int = 600):
    """Wait until buffer is empty and profile job has run."""
    user = context.user_name
    start = asyncio.get_event_loop().time()
    
    while asyncio.get_event_loop().time() - start < max_wait:
        buffer_len = await context.redis_client.llen(f"buffer:{user}")
        
        if buffer_len == 0:
            profile_done = await context.redis_client.get(f"profile_complete:{user}")
            if profile_done:
                logger.info("Processing complete.")
                return
            
            dirty_count = await context.redis_client.scard(f"dirty_entities:{user}")
            if dirty_count == 0:
                logger.info("No pending profile work, continuing...")
                await asyncio.sleep(60)
                return
            
            logger.info(f"Waiting for profile job... ({dirty_count} dirty entities)")
        else:
            logger.info(f"Buffer: {buffer_len}")
        
        await asyncio.sleep(poll_interval)
    
    logger.warning(f"Processing wait timed out after {max_wait}s, continuing anyway")

async def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--skip", action="store_true", help="Skip ingestion")
    args = parser.parse_args()
    
    base_path = Path(__file__).parent.parent
    msgs_file = base_path / "data/custom_benchmark_msgs.json"
    questions_file = base_path / "data/custom_benchmark_questions.json"
    
    with open(msgs_file, "r") as f:
        messages = json.load(f)
    
    with open(questions_file, "r") as f:
        questions = json.load(f)
    
    logger.info(f"Loaded {len(messages)} messages, {len(questions)} questions")
    
    executor = ThreadPoolExecutor(max_workers=5)
    store = MemGraphStore()
    context = await Context.create(
        user_name=USER_NAME,
        store=store,
        cpu_executor=executor,
        topics_config=TOPIC_CONFIG,
        session_id=SESSION_ID
    )
    
    results = []
    
    try:
        if not args.skip:
            logger.info("Starting ingestion...")
            await ingest_haystack(context, messages)
            await wait_for_processing(context)
        else:
            logger.info("Skipping ingestion (--skip)")
        
        logger.info(f"Evaluating {len(questions)} questions...")
        
        for i, q in enumerate(questions):
            logger.info(f"\n{'='*60}")
            logger.info(f"[{i+1}/{len(questions)}] {q['test_case']} ({q['category']})")
            logger.info(f"Q: {q['natural_query']}")
            
            response, tools_used = await ask_stella(context, q["natural_query"])
            
            logger.info(f"Expected: {q['ground_truth']}")
            logger.info(f"Got: {response}")
            
            results.append({
                "index": i,
                "test_case": q["test_case"],
                "category": q["category"],
                "question": q["natural_query"],
                "expected": q["ground_truth"],
                "evidence_turns": q["evidence_turns"],
                "response": response,
                "tools_used": tools_used
            })
            
            await asyncio.sleep(1)
        
        output_file = base_path / "custom_benchmark_results.json"
        with open(output_file, "w") as f:
            json.dump(results, f, indent=2)
        
        logger.info(f"\n{'='*60}")
        logger.info(f"Results saved to {output_file}")
        logger.info(f"Total: {len(results)} questions evaluated")
        
    finally:
        from jobs.cleaner import EntityCleanupJob
        from jobs.base import JobContext
        
        ctx = JobContext(
            user_name=context.user_name,
            redis=context.redis_client,
            idle_seconds=0
        )
        cleaner = EntityCleanupJob(context.user_name, context.store, context.ent_resolver)
        result = await cleaner.execute(ctx)
        logger.info(f"Cleaner: {result.summary}")
        
        await context.shutdown()
        store.close()
        executor.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(main())