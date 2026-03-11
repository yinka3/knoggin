import asyncio
import sys
import json
from pathlib import Path
from datetime import datetime, timezone, timedelta
from typing import List, Tuple
from loguru import logger
import os
from dotenv import load_dotenv



sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from main.setup import _create_user_entity
from shared.services.llm import LLMService
from shared.infra.resources import ResourceManager
from shared.infra.redis import RedisKeys
from agent.streaming import run_stream
from main.context import Context
from shared.models.schema.dtypes import MessageData
from shared.services.topics import generate_topics
from log.logging_setup import setup_logging

setup_logging(log_level="DEBUG", log_file="benchmark.log", colorize=True)

USER_NAME = "TestUser"

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

    result = await generate_topics(llm, text_block, 4)
    return result

async def ingest_haystack(context: Context, haystack_sessions: list, haystack_dates: list):
    total_sessions = len(haystack_sessions)
    for session_idx, session in enumerate(haystack_sessions):
        parts = haystack_dates[session_idx]
        date_str = parts.split(" (")[0]
        time_str = parts.split(") ")[1]
        session_date = datetime.strptime(f"{date_str} {time_str}", "%Y/%m/%d %H:%M")
        session_date = session_date.replace(tzinfo=timezone.utc)
        user_count = sum(1 for t in session if t["role"] == "user")
        asst_count = len(session) - user_count
        logger.info(f"Session {session_idx + 1}/{total_sessions} - {len(session)} turns ({user_count} user, {asst_count} assistant) -> {user_count} buffered")
        for i, turn in enumerate(session):
            msg_date = session_date + timedelta(seconds=i)
            if turn["role"] == "user":
                msg = MessageData(message=turn["content"], timestamp=msg_date)
                await context.add(msg)
            else:
                await context.add_assistant_turn(turn["content"], msg_date)

        is_last = (session_idx + 1) == total_sessions
        is_every_2 = (session_idx + 1) % 2 == 0
        
        if is_every_2 or is_last:
            await context.consumer.flush()
            await wait_for_processing(context)
            logger.info(f"Session {session_idx + 1}/{total_sessions} block complete")
        else:
            logger.info(f"Session {session_idx + 1}/{total_sessions} added to buffer, skipping full wait")
    logger.info("Haystack ingestion complete")


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

    benchmark_persona = "Analytical Evaluation Agent. Highly logical, precise, and objective. No conversational filler."
    benchmark_instructions = (
        "You are operating in an objective evaluation environment.\n"
        "1. Always start by checking the user's entity profile with fact_check or search_entity.\n"
        "2. Use fact_check as your primary tool for any question about a specific person, place, or thing.\n"
        "3. Fall back to search_messages only after structured tools return nothing.\n"
        "4. Synthesize findings completely but succinctly.\n"
        "5. Do not engage in small talk."
    )
    benchmark_rules = (
        "1. Tool Priority: fact_check first, then search_entity, then get_connections. "
        "search_messages is a last resort only.\n"
        "2. User Profile: Always check the user's own entity profile when questions are about them.\n"
        "3. Multi-session Reasoning: Connect information scattered across multiple past conversations. "
        "Use multiple search queries if necessary.\n"
        "4. Temporal Reasoning: Pay strict attention to timestamps and chronological order.\n"
        "5. Knowledge Updates: Newer information always supersedes older information.\n"
        "6. Abstention: If evidence is insufficient, explicitly state that. Do not hallucinate."
    )

    async for event in run_stream(
        user_query=question,
        user_name=context.user_name,
        session_id=context.session_id,
        agent_id="benchmark",
        agent_name="STELLA",
        agent_persona=benchmark_persona,
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
        mcp_manager=context.mcp_manager,
        agent_instructions=benchmark_instructions,
        agent_rules=benchmark_rules,
        simulated_date=question_date
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

    try:
        topics = await generate_instance_topics(resource.llm_service, q["haystack_sessions"])
    except Exception as e:
        logger.critical(f"Failed to generate topics for this instance. Aborting. Error: {e}")
        return

    context = await Context.create(
        user_name=USER_NAME,
        resources=resource,
        topics_config=topics
    )

    context.consumer.update_ingestion_settings(batch_size=10, batch_timeout=60.0)

    try:
        await ingest_haystack(context, q["haystack_sessions"], q["haystack_dates"])

        logger.info("Waiting for processing...")
        await asyncio.sleep(5)
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
            "session_id": context.session_id,
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
        await resource.shutdown()


if __name__ == "__main__":
    asyncio.run(main())