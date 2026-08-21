from types import SimpleNamespace

import pytest

from common.schema.settings import DeveloperSettings, RootConfig
from runtime.session_runtime_factory import SessionRuntimeFactory
from tests.fixtures.factories import make_domain_config, make_project_state
from tests.fixtures.fakes import (
    FakeKnowledgeStore,
    FakePipeline,
    FakeResources,
    FakeScheduler,
)


class RecordingConfigManager:
    def __init__(self):
        self.config = RootConfig(developer_settings=DeveloperSettings())
        self.subscriptions = []

    def subscribe(self, callback, path):
        self.subscriptions.append((callback, path))

        def unsubscribe():
            pass

        return unsubscribe


class RecordingIngestionPipeline:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self._get_next_ent_id = kwargs.get("get_next_ent_id")
        self.__class__.instances.append(self)

    @property
    def get_next_ent_id(self):
        return self._get_next_ent_id

    @get_next_ent_id.setter
    def get_next_ent_id(self, fn):
        self._get_next_ent_id = fn


class RecordingIngestionWorker:
    instances = []

    def __init__(self, **kwargs):
        self.kwargs = kwargs
        self.get_session_context = kwargs.get("get_session_context")
        self.write_to_graph = kwargs.get("write_to_graph")
        self.started = 0
        self.__class__.instances.append(self)

    def update_settings(self, config):
        self.updated_settings = config

    def start(self):
        self.started += 1


@pytest.fixture
def assembler_harness(monkeypatch):
    RecordingIngestionPipeline.instances = []
    RecordingIngestionWorker.instances = []

    config_manager = RecordingConfigManager()
    resources = FakeResources(knowledge_store=FakeKnowledgeStore())
    entities = object()
    pipeline = FakePipeline()

    async def get_next_ent_id():
        return 42

    shared_processor = RecordingIngestionPipeline(
        project_id="project-1",
        redis_client=resources.redis,
        llm=resources.llm_service,
        entities=entities,
        processor=pipeline,
        compiled_domain=make_domain_config().compile(),
        get_next_ent_id=get_next_ent_id,
    )
    project_state = make_project_state(
        project_id="project-1",
        scheduler=FakeScheduler(),
        postgres=resources.postgres,
        embedding=resources.embedding,
        domain_config=make_domain_config(),
        batch_processor=shared_processor,
        entities=entities,
        pipeline=pipeline,
    )

    monkeypatch.setattr(
        "runtime.session_runtime_factory.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "runtime.session_runtime_factory.IngestionWorker",
        RecordingIngestionWorker,
    )
    return SimpleNamespace(
        assembler=SessionRuntimeFactory("ada", resources),
        config_manager=config_manager,
        project_state=project_state,
        resources=resources,
        batch_processor=shared_processor,
        get_next_ent_id=get_next_ent_id,
    )


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_assemble_wires_runtime_without_launch(
    assembler_harness,
):
    harness = assembler_harness

    ctx = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-1",
        model="model-a",
        agent_id="agent-1",
        enabled_tools=[],
    )

    assert ctx.user_name == "ada"
    assert ctx.session_id == "session-1"
    assert ctx.project_id == "project-1"
    assert ctx.project is harness.project_state
    assert ctx.model == "model-a"
    assert ctx.agent_id == "agent-1"
    assert ctx.enabled_tools == []

    assert harness.resources.redis.evals == []

    assert RecordingIngestionPipeline.instances == [harness.batch_processor]
    processor = harness.batch_processor
    assert ctx.batch_processor is processor
    assert processor.kwargs["project_id"] == "project-1"
    assert processor.kwargs["redis_client"] is harness.resources.redis
    assert processor.kwargs["llm"] is harness.resources.llm_service
    assert processor.kwargs["entities"] is harness.project_state.entities
    assert processor.kwargs["processor"] is harness.project_state.pipeline
    assert processor.kwargs["compiled_domain"] == harness.project_state.compiled_domain
    assert processor.get_next_ent_id is harness.get_next_ent_id

    consumer = RecordingIngestionWorker.instances[0]
    assert ctx.consumer is consumer
    assert consumer.kwargs["knowledge_store"] is harness.resources.knowledge_store
    assert consumer.kwargs["redis"] is harness.resources.redis
    assert consumer.kwargs["processor"] is processor
    assert consumer.get_session_context == ctx.get_conversation_context
    assert consumer.write_to_graph == ctx._write_to_graph_callback
    assert consumer.kwargs["settings"].batch_size == 8
    assert consumer.kwargs["settings"].session_window == 24

    assert ctx.document_service is harness.project_state.document_service
    assert ctx.document_service.project_id == "project-1"
    assert ctx.document_service._embedding is harness.resources.embedding

    assert len(ctx.config_unsubscribers) == 1
    assert harness.config_manager.subscriptions == [
        (consumer.update_settings, "developer_settings.ingestion")
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_sessions_in_same_project_share_document_service(
    assembler_harness,
):
    harness = assembler_harness

    first = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-1",
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    second = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-2",
        model=None,
        agent_id=None,
        enabled_tools=None,
    )

    assert first.document_service is harness.project_state.document_service
    assert second.document_service is harness.project_state.document_service


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_launch_starts_consumer_only(
    assembler_harness,
):
    harness = assembler_harness
    ctx = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-1",
        model=None,
        agent_id=None,
        enabled_tools=None,
    )

    await harness.assembler.launch(ctx)

    assert harness.project_state.scheduler.running is False
    assert harness.project_state.scheduler.started == 0
    assert ctx.consumer.started == 1
    assert harness.resources.knowledge_store.reset_claimed_ingestion_calls == [
        {
            "user_name": "ada",
            "project_id": "project-1",
            "session_id": "session-1",
        }
    ]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_launch_leaves_project_scheduler_untouched(
    assembler_harness,
):
    harness = assembler_harness
    ctx = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-1",
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    harness.project_state.scheduler.running = True

    await harness.assembler.launch(ctx)

    assert harness.project_state.scheduler.started == 0
    assert ctx.consumer.started == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_launch_rejects_missing_consumer_callbacks(
    assembler_harness,
):
    harness = assembler_harness
    ctx = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-1",
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    ctx.consumer.get_session_context = None

    with pytest.raises(RuntimeError, match="consumer.get_session_context callback"):
        await harness.assembler.launch(ctx)

    ctx.consumer.get_session_context = ctx.get_conversation_context
    ctx.consumer.write_to_graph = None

    with pytest.raises(RuntimeError, match="consumer.write_to_graph callback"):
        await harness.assembler.launch(ctx)


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_launch_rejects_missing_entity_id_callback(
    assembler_harness,
):
    harness = assembler_harness
    ctx = await harness.assembler.assemble(
        harness.project_state,
        session_id="session-1",
        model=None,
        agent_id=None,
        enabled_tools=None,
    )
    ctx.batch_processor.get_next_ent_id = None

    with pytest.raises(RuntimeError, match="batch_processor.get_next_ent_id callback"):
        await harness.assembler.launch(ctx)
