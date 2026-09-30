"""Settle already-owned lifecycle work before propagating a caller's failure."""

import asyncio


async def settle_owned_task(task: asyncio.Future):
    """Join cleanup despite further caller cancellation; do not cancel its owner.

    Call only while handling an existing failure/cancellation. The caller remains
    responsible for re-raising that original outcome after ownership is settled.
    """
    while True:
        try:
            return await asyncio.shield(task)
        except asyncio.CancelledError:
            if task.done():
                return task.result()
