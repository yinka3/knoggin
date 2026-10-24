"""Offline rebuild after an embedding configuration change.

Run with the local engine stopped:
    PYTHONPATH=src ../.venv/bin/python scripts/rebuild_embeddings.py
Uses DATABASE_URL and the same EMBEDDING_* settings as the engine.
"""

import asyncio
import os

from dotenv import load_dotenv

from common.embedding_contract import DEFAULT_EMBEDDING_MODEL
from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from core.knowledge.embedding_service import EmbeddingService
from infrastructure.postgres_client import PostgresClient


async def main() -> None:
    load_dotenv()
    client = PostgresClient(os.environ["DATABASE_URL"])
    embedding = EmbeddingService(
        embedding_model=os.getenv("EMBEDDING_MODEL", DEFAULT_EMBEDDING_MODEL),
    )
    try:
        await client.connect()
        await embedding.load_models()
        print(await EmbeddingRebuilder(client, embedding).rebuild_all_embeddings())
    finally:
        embedding.cleanup()
        await client.close()


if __name__ == "__main__":
    asyncio.run(main())
