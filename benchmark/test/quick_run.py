import asyncio
import json
import sys
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor

sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from agent.loop import run
from db.memgraph import MemGraphStore
from main.context import Context

USER_NAME = "TestUser"

async def answer_question_only(instance_idx: int):
    dataset_file = "benchmark/test/test_multi_session.json" 
    with open(dataset_file, "r") as f:
        questions = json.load(f)
    
    q = questions[instance_idx]
    
    print(f"\nAnswering instance {instance_idx}: {q['question']}")
    
    executor = ThreadPoolExecutor(max_workers=5)
    store = MemGraphStore()
    
    context = await Context.create(
        user_name=USER_NAME,
        store=store,
        cpu_executor=executor,
        topics=[]
    )
    
    try:
        topics = context.store.get_topics_by_status()
        active = topics.get("active", []) + topics.get("hot", [])
        
        result = await run(
            user_query=q['question'],
            user_name=USER_NAME,
            conversation_history=[],
            hot_topics=[],
            active_topics=active,
            llm=context.llm,
            store=context.store,
            ent_resolver=context.ent_resolver,
            redis_client=context.redis_client,
            slim_hot_context=True
        )
        
        response = result.get("response") or result.get("question", "No response")
        
        print(f"Expected: {q['answer']}")
        print(f"Got: {response}")
        
    finally:
        await context.redis_client.aclose()
        store.close()
        executor.shutdown(wait=False)


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python manual_answer.py <instance_index>")
        sys.exit(1)
    
    asyncio.run(answer_question_only(int(sys.argv[1])))