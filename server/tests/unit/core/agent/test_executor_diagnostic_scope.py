import asyncio
from types import SimpleNamespace

from common.utils.diagnostic_context import get_diagnostic_scope
from core.agent.executor import AgentExecutor


def test_executor_stream_can_close_from_another_task_without_leaking_scope():
    async def exercise():
        released = []
        executor = object.__new__(AgentExecutor)
        executor.ctx = SimpleNamespace(
            user_name="ada", project_id="project-1", session_id="session-1",
            run_id="run-1", release=lambda: released.append(True),
        )

        async def produce(*, user_timezone=None):
            assert get_diagnostic_scope()["agent_run"] == "run-1"
            yield {"event": "token", "data": {"content": "hello"}}

        executor._execute_run = produce
        stream = executor.execute()
        assert (await asyncio.create_task(anext(stream)))["event"] == "token"
        await asyncio.create_task(stream.aclose())
        assert released == [True]
        assert get_diagnostic_scope() == {}

    asyncio.run(exercise())
