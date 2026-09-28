"""Shared AAC-local execution ownership, not agent policy."""

import asyncio

from loguru import logger

from common.utils.lifecycle import settle_owned_task


def config_owner(provider):
    """Prefer an injected config owner over a legacy class provider."""
    return provider if hasattr(provider, "config") else provider.get()


async def execute_aac_run(*, open_run, executor_factory, llm, tools, on_completion,
                          budget, skip_on_error=False):
    """Own tools before run/executor construction and through close completion."""
    response = None
    try:
        try:
            executor = executor_factory(
                open_run(), llm, tools,
                on_successful_completion=on_completion, aac_budget=budget,
            )
            async for event in executor.execute():
                if event.get("event") == "response":
                    response = event.get("data", {}).get("content")
        except Exception as exc:
            if not skip_on_error:
                raise
            logger.warning("AAC seeding failed; skipping opportunity: {}", exc)
    finally:
        cleanup = asyncio.create_task(tools.close())
        try:
            await asyncio.shield(cleanup)
        except asyncio.CancelledError:
            await settle_owned_task(cleanup)
            raise
    return response
