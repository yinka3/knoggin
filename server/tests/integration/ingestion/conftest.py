"""Shared real-service fixtures for ingestion integration contracts."""

import json
import os
import uuid
from dataclasses import asdict

import pytest

from core.knowledge.db.writers.project_deletion_writer import ProjectDeletionWriter
from infrastructure.postgres_client import PostgresClient
from tests.fixtures.factories import make_domain_config


@pytest.fixture
async def real_server_scope():
    dsn = os.environ.get(
        "KNOGGIN_TEST_DATABASE_URL",
        "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
    )
    postgres = PostgresClient(dsn=dsn, min_size=1, max_size=2)
    await postgres.connect()
    suffix = uuid.uuid4().hex[:12]
    user_name = "server_acceptance_test_user"
    project_id = f"server-acceptance-project-{suffix}"
    session_id = f"server-acceptance-session-{suffix}"
    await postgres.execute(
        """
        INSERT INTO projects (project_id, user_name, name, domain_config)
        VALUES (%s, %s, %s, %s)
        """,
        (
            project_id,
            user_name,
            "Server acceptance integration",
            json.dumps(asdict(make_domain_config())),
        ),
    )
    await postgres.execute(
        "INSERT INTO sessions (session_id, user_name, project_id) VALUES (%s, %s, %s)",
        (session_id, user_name, project_id),
    )
    try:
        yield {
            "postgres": postgres,
            "user_name": user_name,
            "project_id": project_id,
            "session_id": session_id,
        }
    finally:
        await ProjectDeletionWriter(postgres).delete_project(
            user_name=user_name,
            project_id=project_id,
        )
        await postgres.close()
