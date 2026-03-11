import asyncio
import sys
import json
import os
from pathlib import Path
from dotenv import load_dotenv

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from shared.infra.resources import ResourceManager
from main.context import Context
from main.setup import _create_user_entity
from benchmark.test.run_eval import (
    DATASET_FILES, 
    USER_NAME, 
    ask_agent,
    logger
)

async def quick_run_agent(dataset: str, idx: int, session_id: str = None):
    dataset_file = Path(__file__).parent / DATASET_FILES[dataset]
    with open(dataset_file, "r") as f:
        questions = json.load(f)
        
    if idx >= len(questions):
        print(f"Index {idx} out of range. Max: {len(questions) - 1}")
        sys.exit(1)
        
    q = questions[idx]

    # Resolve session_id: CLI arg > eval result file > fail
    if not session_id:
        result_file = Path(__file__).parent / f"eval_result_{dataset}_{idx}.json"
        if result_file.exists():
            with open(result_file, "r") as f:
                prev = json.load(f)
                session_id = prev.get("session_id")
                if session_id:
                    logger.info(f"Reusing session_id from previous eval: {session_id}")

    if not session_id:
        logger.error(
            "No session_id available. Run run_eval.py first to ingest data, "
            "or pass a session_id as the third argument."
        )
        sys.exit(1)
    
    logger.info(f"\n{'='*60}")
    logger.info(f"QUICK RUN - SKIPPING INGESTION")
    logger.info(f"Dataset: {dataset} | Instance {idx}: {q['question_type']}")
    logger.info(f"Session: {session_id}")
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
    
    topics = {
        "General": {"active": True, "labels": [], "hierarchy": {}, "aliases": []},
        "Identity": {"active": True, "labels": ["person"], "hierarchy": {}, "aliases": []}
    }

    context = await Context.create(
        user_name=USER_NAME,
        resources=resource,
        topics_config=topics,
        session_id=session_id
    )

    try:
        logger.info("Asking AGENT directly (bypassing ingest)...")
        response, tools_used = await ask_agent(context, q["question"], q["question_date"])
        
        logger.info(f"\n{'='*60}")
        logger.info(f"RESULTS")
        logger.info(f"{'='*60}")
        logger.info(f"Question: {q['question']}")
        logger.info(f"Expected: {q['answer']}")
        logger.info(f"AGENT: {response}")
        logger.info(f"Tools Used: {tools_used}")
        logger.info(f"{'='*60}")
    finally:
        await context.shutdown()
        await resource.shutdown()

async def main():
    if len(sys.argv) < 3:
        print("Usage: python quick_run.py <dataset> <index> [session_id]")
        print(f"  dataset: {list(DATASET_FILES.keys())}")
        print("  index: question index (0-49)")
        print("  session_id: (optional) reuse a specific session, auto-detected from eval results")
        print("Example: python quick_run.py multi 0")
        sys.exit(1)
        
    dataset = sys.argv[1]
    
    if dataset not in DATASET_FILES:
        print(f"Unknown dataset: {dataset}")
        print(f"Valid options: {list(DATASET_FILES.keys())}")
        sys.exit(1)
        
    idx = int(sys.argv[2])
    session_id = sys.argv[3] if len(sys.argv) > 3 else None
    
    await quick_run_agent(dataset, idx, session_id)

if __name__ == "__main__":
    asyncio.run(main())