import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone
from concurrent.futures import ThreadPoolExecutor
from loguru import logger

sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from agent.loop import run
from db.memgraph import MemGraphStore
from main.context import Context
from schema.dtypes import MessageData
from answer import answer_key
logger.remove()
logger.add(sys.stdout, level="INFO")

USER_NAME = "TestUser"
DEFAULT_TOPICS = [
    "Shopping",
    "Travel",
    "Career",
    "Health",
    "Fitness",
    "Medical",
    "Entertainment",
    "Media",
    "Events",
    "Hobbies",
    "Sports",
    "Technology",
    "Education",
    "Family",
    "Relationships",
    "Friends",
    "Routines",
    "Pets",
    "Food",
    "Cooking",
    "Home",
    "Finance",
    "Investments",
    "Transportation",
    "Appointments",
    "Projects",
    "Gaming",
    "Wellness",
    "Goals",
    "General",
]

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
    
    for i, session in enumerate(haystack_sessions):
        parts = haystack_dates[i]
        date_str = parts.split(" (")[0]
        time_str = parts.split(") ")[1]
        session_date = datetime.strptime(f"{date_str} {time_str}", "%Y/%m/%d %H:%M")
        session_date = session_date.replace(tzinfo=timezone.utc)
        
        for turn in session:
            if turn["role"] == "user":
                msg = MessageData(message=turn["content"], timestamp=session_date)
                await context.add(msg)
            else:
                await context.add_assistant_turn(turn["content"], session_date)
            
            await asyncio.sleep(0.1)
        
        logger.info(f"Session {i+1}/{total_sessions} complete, waiting for processing...")
        await asyncio.sleep(15)
    
    logger.info("Haystack ingestion complete")


async def ask_stella(context: Context, question: str, question_date: str) -> str:
    topics = context.store.get_topics_by_status()
    active = topics.get("active", []) + topics.get("hot", [])
    
    result = await run(
        user_query=question,
        user_name=context.user_name,
        conversation_history=[],
        hot_topics=[],
        active_topics=active,
        llm=context.llm,
        store=context.store,
        ent_resolver=context.ent_resolver,
        redis_client=context.redis_client,
        date=question_date,
        slim_hot_context=True
    )
    
    return result.get("response") or result.get("question", "No response")


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
    
    executor = ThreadPoolExecutor(max_workers=5)
    store = MemGraphStore()
    context = await Context.create(
        user_name=USER_NAME,
        store=store,
        cpu_executor=executor,
        topics=DEFAULT_TOPICS
    )
    
    try:
        await ingest_haystack(context, q["haystack_sessions"], q["haystack_dates"])
        
        logger.info("Waiting for processing...")
        await asyncio.sleep(30)
        await context._flush_batch_shutdown()
        
        response = await ask_stella(context, q["question"], q["question_date"])
        
        logger.info(f"\n{'='*60}")
        logger.info(f"RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Question: {q['question']}")
        logger.info(f"Expected: {q['answer']}")
        logger.info(f"STELLA: {response}")
        
        answer_key(idx, dataset_file)
        result = {
            "index": idx,
            "dataset": dataset,
            "type": q["question_type"],
            "question": q["question"],
            "question_date": q["question_date"],
            "expected": q["answer"],
            "response": response
        }
        
        output_file = Path(__file__).parent / f"eval_result_{dataset}_{idx}.json"
        with open(output_file, "w") as f:
            json.dump(result, f, indent=2)
        
        logger.info(f"\nSaved to {output_file}")
        
    finally:
        await context.redis_client.aclose()
        store.close()
        executor.shutdown(wait=True)


if __name__ == "__main__":
    asyncio.run(main())