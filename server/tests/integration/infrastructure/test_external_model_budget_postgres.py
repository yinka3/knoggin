"""Budget contracts against two canonical tables in an isolated database.

No model calls, AGE, or vector extension are needed. All SQL is exercised by
psycopg; absence of a PostgreSQL test service skips this module's cases.
"""

import asyncio
import os
import re
from contextlib import asynccontextmanager
from pathlib import Path
from uuid import uuid4

import psycopg
import pytest
from psycopg import sql
from psycopg.conninfo import conninfo_to_dict, make_conninfo
from psycopg.rows import dict_row

from common.exceptions import LLMBudgetExceededError
from common.schema.settings import LLMModelPricing, LLMSpendingBudgetSettings
from infrastructure.external_model_budget import ExternalModelSpendingLedger

pytestmark = [pytest.mark.integration, pytest.mark.requires_postgres]


@pytest.fixture(scope="module")
def budget_database_url():
    configured = os.environ.get(
        "KNOGGIN_TEST_DATABASE_URL",
        "postgresql://knoggin:knoggin@localhost:5432/knoggin_db",
    )
    params = conninfo_to_dict(configured)
    params.update(dbname="postgres", connect_timeout="2")
    try:
        admin = psycopg.connect(make_conninfo(**params), autocommit=True)
    except psycopg.OperationalError:
        pytest.skip("PostgreSQL test service is unavailable")
    database = f"knoggin_budget_test_{uuid4().hex[:12]}"
    try:
        admin.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(database)))
    except psycopg.errors.InsufficientPrivilege:
        admin.close()
        pytest.skip("PostgreSQL test role cannot create an isolated database")
    params["dbname"] = database
    target = make_conninfo(**params)
    schema = (
        Path(__file__).resolve().parents[3] / "src/infrastructure/schema.sql"
    ).read_text(encoding="utf-8")
    try:
        with psycopg.connect(target, autocommit=True) as connection:
            for table in ("llm_budget_windows", "llm_budget_reservations"):
                ddl = re.search(
                    rf"CREATE TABLE public\.{table} \(.*?\n\);", schema, re.S
                )
                assert ddl is not None
                connection.execute(ddl.group())
            connection.execute(
                "ALTER TABLE public.llm_budget_windows ADD PRIMARY KEY (reset_key)"
            )
            connection.execute(
                "ALTER TABLE public.llm_budget_reservations ADD PRIMARY KEY (reservation_id)"
            )
            connection.execute(
                "ALTER TABLE public.llm_budget_reservations ADD FOREIGN KEY (reset_key) REFERENCES public.llm_budget_windows(reset_key)"
            )
        yield target
    finally:
        admin.execute(
            sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(database))
        )
        admin.close()


class Database:
    def __init__(self, connection):
        self.connection = connection

    @asynccontextmanager
    async def transaction(self):
        async with self.connection.transaction():
            async with self.connection.cursor() as cursor:
                yield cursor


def settings(key, limit=None, price=1):
    return LLMSpendingBudgetSettings(
        reset_key=key,
        limit_usd=limit,
        fallback_pricing=LLMModelPricing(
            input_usd_per_million_tokens=price, output_usd_per_million_tokens=0
        ),
    )


async def reserve(ledger):
    return await ledger.reserve(
        model="synthetic", estimated_prompt_tokens=100, estimated_output_tokens=0
    )


async def record(ledger, reservation):
    await ledger.record(
        reservation,
        model="synthetic",
        prompt_tokens=100,
        completion_tokens=0,
        approximate_usage=False,
        failed=False,
    )


@asynccontextmanager
async def open_ledger(url, policy):
    connection = await psycopg.AsyncConnection.connect(url, row_factory=dict_row)
    try:
        yield ExternalModelSpendingLedger(policy, postgres_client=Database(connection))
    finally:
        await connection.close()


def run(coro):
    # psycopg async needs a selector loop on Windows; leave global policy intact.
    return asyncio.run(coro, loop_factory=asyncio.SelectorEventLoop)


def test_cap_toggles_keep_origin_and_prior_spend(budget_database_url):
    async def check():
        async with open_ledger(budget_database_url, settings("toggles")) as ledger:
            first = await reserve(ledger)
            assert first.storage == "postgres"
            await ledger.update_settings(settings("toggles", limit=1, price=5))
            await record(ledger, first)
            assert (await ledger.snapshot())["spent_usd"] == pytest.approx(0.0001)
            second = await reserve(ledger)
            await ledger.update_settings(settings("toggles"))
            await record(ledger, second)
            snapshot = await ledger.snapshot()
            assert snapshot["spent_usd"] == pytest.approx(0.0006)
            assert snapshot["reserved_usd"] == 0
            assert snapshot["request_count"] == 2
            await ledger.update_settings(settings("toggles", limit=0.00065))
            with pytest.raises(LLMBudgetExceededError):
                await reserve(ledger)

    run(check())


def test_reset_and_repeated_settlement_use_original_window(budget_database_url):
    async def check():
        async with open_ledger(budget_database_url, settings("old")) as ledger:
            old = await reserve(ledger)
            await ledger.update_settings(settings("new"))
            new = await reserve(ledger)
            await record(ledger, old)
            await record(ledger, old)
            await record(ledger, new)
            snapshot = await ledger.snapshot()
            assert snapshot["spent_usd"] == pytest.approx(0.0001)
            assert snapshot["reserved_usd"] == 0
            assert snapshot["request_count"] == 1
        async with open_ledger(budget_database_url, settings("old")) as reopened:
            assert (await reopened.snapshot())["spent_usd"] == pytest.approx(0.0001)
            assert (await reopened.snapshot())["reserved_usd"] == 0
            assert (await reopened.snapshot())["request_count"] == 1

    run(check())


def test_unsettled_expiry_keeps_estimated_charge(budget_database_url):
    async def check():
        async with open_ledger(budget_database_url, settings("expiry")) as ledger:
            old = await reserve(ledger)
            async with ledger._postgres.transaction() as cur:
                await cur.execute(
                    "UPDATE public.llm_budget_reservations SET expires_at = now() - interval '1 second' WHERE reservation_id = %s",
                    (old.token,),
                )
            new = await reserve(ledger)
            await record(ledger, old)  # Estimate was already charged once.
            await record(ledger, new)
            snapshot = await ledger.snapshot()
            assert snapshot["spent_usd"] == pytest.approx(0.0002)
            assert snapshot["reserved_usd"] == 0
            assert snapshot["request_count"] == 2

    run(check())


def test_concurrent_ledgers_enforce_one_durable_ceiling(budget_database_url):
    async def check():
        async with open_ledger(
            budget_database_url, settings("concurrent", limit=0.00015)
        ) as first:
            async with open_ledger(
                budget_database_url, settings("concurrent", limit=0.00015)
            ) as second:
                results = await asyncio.gather(
                    reserve(first), reserve(second), return_exceptions=True
                )
                admitted = [
                    value for value in results if not isinstance(value, Exception)
                ]
                denied = [
                    value
                    for value in results
                    if isinstance(value, LLMBudgetExceededError)
                ]
                assert len(admitted) == len(denied) == 1
                await record(first, admitted[0])
                assert (await second.snapshot())["spent_usd"] == pytest.approx(0.0001)
                assert (await first.snapshot())["reserved_usd"] == 0

    run(check())


def test_failed_commit_can_retry_without_double_charging(budget_database_url):
    async def check():
        async with open_ledger(budget_database_url, settings("recovery")) as ledger:
            reservation = await reserve(ledger)
            original = ledger._postgres

            class FailCommit(Database):
                @asynccontextmanager
                async def transaction(self):
                    async with super().transaction() as cursor:
                        yield cursor
                        raise RuntimeError("Rollback before commit")

            ledger._postgres = FailCommit(original.connection)
            with pytest.raises(RuntimeError, match="Rollback"):
                await record(ledger, reservation)
            ledger._postgres = original
            assert (await ledger.snapshot())["reserved_usd"] == pytest.approx(0.0001)
            await record(ledger, reservation)
            await record(ledger, reservation)
            assert (await ledger.snapshot())["reserved_usd"] == 0
            assert (await ledger.snapshot())["spent_usd"] == pytest.approx(0.0001)

    run(check())
