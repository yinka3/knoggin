"""Offline rebuild after an embedding configuration change.

Run with the local engine stopped:
    PYTHONPATH=src ../.venv/bin/python scripts/rebuild_embeddings.py
Uses DATABASE_URL and the same KNOGGIN_EMBEDDING_* settings as the engine.
"""

import asyncio
import os

from dotenv import load_dotenv

from core.knowledge.db.embedding_rebuilder import EmbeddingRebuilder
from core.knowledge.services.embedding_service import EmbeddingService
from infrastructure.postgres_client import PostgresClient


async def main() -> None:
    load_dotenv()
    client = PostgresClient(os.environ["DATABASE_URL"])
    embedding = EmbeddingService(
        embedding_model=os.getenv(
            "KNOGGIN_EMBEDDING_MODEL", "dunzhang/stella_en_1.5B_v5"
        ),
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
