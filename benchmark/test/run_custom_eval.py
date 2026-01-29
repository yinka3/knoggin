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
from agent.orchestrator import run
from db.store import MemGraphStore
from main.context import Context
from schema.dtypes import MessageData
from log.logging_setup import setup_logging

setup_logging(log_level="DEBUG", log_file="benchmark.log", colorize=True)

USER_NAME = "Adeyinka"
EVAL_DATE = "2026-08-28 12:00"
SESSION_ID = str(uuid.uuid4())
CONFIG = {
    "Identity": {
        "active": True,
        "labels": ["person"],
        "hierarchy": {},
        "aliases": [],
        "label_aliases": {}
    },
    "Product & Business": {
        "active": True,
        "labels": ["project", "software", "feature", "service", "role", "company", "task", "platform"],
        "hierarchy": {
            "project": ["task", "feature"],
            "company": ["role"]
        },
        "aliases": ["Work", "Startup", "Consulting"],
        "label_aliases": {
            "mvp": "project",
            "stripe": "service",
            "beanstalk consulting": "company",
            "sdr": "role",
            "cold outreach specialist": "role",
            "linkedin": "platform",
            "facebook": "platform",
            "instagram": "platform"
        }
    },
    "Travel": {
        "active": True,
        "labels": ["trip", "city", "country", "landmark", "accommodation", "transport"],
        "hierarchy": {
            "trip": ["city", "accommodation", "landmark"]
        },
        "aliases": ["Vacation", "Trips"],
        "label_aliases": {
            "japan": "country",
            "kyoto": "city",
            "osaka": "city",
            "yellowstone": "landmark",
            "airbnb": "accommodation",
            "ryokan": "accommodation"
        }
    },
    "Creative & Hobbies": {
        "active": True,
        "labels": ["hobby", "tool", "material", "technique", "software", "project"],
        "hierarchy": {
            "hobby": ["tool", "material", "technique"],
            "project": ["material"]
        },
        "aliases": ["Photography", "Crafts", "Sculpting"],
        "label_aliases": {
            "adobe lightroom": "software",
            "clay": "material",
            "sashiko": "technique",
            "road bike": "tool",
            "embroidery": "hobby",
            "sculpture haven": "organization"
        }
    },
    "Entertainment & Media": {
        "active": True,
        "labels": ["movie", "book", "character", "band", "event", "sport_team"],
        "hierarchy": {},
        "aliases": ["Pop Culture", "Sports"],
        "label_aliases": {
            "guardians of the galaxy": "movie",
            "star-lord": "character",
            "edgar allan poe": "person",
            "the 1975": "band",
            "interpol": "band",
            "buccaneers": "sport_team",
            "magic tree house": "book"
        }
    },
    "Food & Drink": {
        "active": True,
        "labels": ["dish", "ingredient", "beverage", "restaurant"],
        "hierarchy": {
            "dish": ["ingredient"]
        },
        "aliases": ["Cooking", "Dining"],
        "label_aliases": {
            "salmon": "ingredient",
            "quinoa": "ingredient",
            "whiskey": "beverage",
            "nikka yoichi": "beverage",
            "pizza": "dish"
        }
    },
    "Health & Wellness": {
        "active": True,
        "labels": ["activity", "practice", "health_condition"],
        "hierarchy": {},
        "aliases": ["Fitness", "Self Care"],
        "label_aliases": {
            "mindfulness": "practice",
            "cycling": "activity",
            "marathon": "activity",
            "dry skin": "health_condition"
        }
    },
    "General": {
        "active": False,
        "labels": ["person", "place", "organization", "event", "date"],
        "hierarchy": {},
        "aliases": [],
        "label_aliases": {}
    }
}

async def ingest_haystack(context: Context, messages: list, session_size: int = 24):
    """Ingest messages in session chunks with processing breaks."""
    total = len(messages)
    
    sessions = [
        messages[i:i + session_size] 
        for i in range(0, total, session_size)
    ]
    
    logger.info(f"Split {total} messages into {len(sessions)} sessions (size={session_size})")
    
    for session_idx, session_msgs in enumerate(sessions):
        session_num = session_idx + 1
        logger.info(f"\n{'='*40}")
        logger.info(f"SESSION {session_num}/{len(sessions)} — {len(session_msgs)} messages")
        logger.info(f"{'='*40}")
        
        for i, turn in enumerate(session_msgs):
            ts = datetime.strptime(turn["timestamp"], "%Y-%m-%d %H:%M")
            ts = ts.replace(tzinfo=timezone.utc)
            
            if turn["role"] == "user":
                msg = MessageData(message=turn["content"], timestamp=ts)
                await context.add(msg)
            
            if (i + 1) % 8 == 0:
                batch_num = (i + 1) // 8
                logger.debug(f"Session {session_num} batch {batch_num} queued")
                await wait_for_batch_drain(context, timeout=120)
            else:
                await asyncio.sleep(0.5)
        
        logger.info(f"Session {session_num} ingestion done, draining...")
        await wait_for_batch_drain(context, timeout=180)
        
        logger.info(f"Session {session_num} waiting for jobs...")
        await wait_for_processing(context)
        logger.info(f"Session {session_num} complete ✓")
    
    logger.info(f"\n{'='*40}")
    logger.info("All sessions ingested")
    logger.info(f"{'='*40}")



async def wait_for_batch_drain(context: Context, timeout: int = 120):
    """Wait for buffer to empty."""
    user = context.user_name
    start = asyncio.get_event_loop().time()
    
    while asyncio.get_event_loop().time() - start < timeout:
        buffer_len = await context.redis_client.llen(f"buffer:{user}")
        
        if buffer_len == 0:
            return
        
        await asyncio.sleep(2)
    
    logger.warning(f"Batch drain timeout after {timeout}s")


async def wait_for_processing(context: Context, drain_timeout: int = 180):
    """Wait for buffer drain, then run session jobs."""
    user = context.user_name
    
    start = asyncio.get_event_loop().time()
    while asyncio.get_event_loop().time() - start < drain_timeout:
        buffer_len = await context.redis_client.llen(f"buffer:{user}")
        if buffer_len == 0:
            break
        logger.debug(f"Buffer draining: {buffer_len}")
        await asyncio.sleep(2)
    else:
        logger.warning(f"Drain timeout after {drain_timeout}s, running jobs anyway")
    
    await context.redis_client.set(f"checkpoint_count:{user}", 0)
    
    logger.info("Running session jobs...")
    await context._run_session_jobs()
    logger.info("Session jobs complete ✓")


async def ask_agent(context: Context, question: str) -> str:
    
    result = await run(
        user_query=question,
        user_name=context.user_name,
        session_id=SESSION_ID,
        conversation_history=[],
        hot_topics=[],
        topic_config=context.topic_config,
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
    
    executor = ThreadPoolExecutor(max_workers=1)
    store = MemGraphStore()
    context = await Context.create(
        user_name=USER_NAME,
        store=store,
        cpu_executor=executor,
        topics_config=CONFIG,
        session_id=SESSION_ID
    )
    
    results = []
    
    try:
        if not args.skip:
            logger.info("Starting ingestion...")
            await ingest_haystack(context, messages)
            await wait_for_processing(context, drain_timeout=300)
        else:
            logger.info("Skipping ingestion (--skip)")
        
        logger.info(f"Evaluating {len(questions)} questions...")
        
        for i, q in enumerate(questions):
            logger.info(f"\n{'='*60}")
            logger.info(f"[{i+1}/{len(questions)}] {q['test_case']} ({q['category']})")
            logger.info(f"Q: {q['natural_query']}")
            
            response, tools_used = await ask_agent(context, q["natural_query"])
            
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