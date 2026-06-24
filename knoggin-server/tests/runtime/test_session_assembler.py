from types import SimpleNamespace

import pytest

from common.schema.settings import DeveloperSettings, RootConfig
from knoggin_server.project.state import ProjectState
from knoggin_server.session.boot import SessionAssembler
from tests.fixtures.factories import make_topic_config
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


class RecordingEmitter:
    def __init__(self):
        self.registered_sessions = []

    def register_session(self, project_id, session_id):
        self.registered_sessions.append((project_id, session_id))


class RecordingBatchProcessor:
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


class RecordingBatchConsumer:
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


class RecordingFileRAG:
    instances = []

    def __init__(self, session_id, embedding_service):
        self.session_id = session_id
        self.embedding_service = embedding_service
        self.__class__.instances.append(self)


@pytest.fixture
def assembler_harness(monkeypatch):
    RecordingBatchProcessor.instances = []
    RecordingBatchConsumer.instances = []
    RecordingFileRAG.instances = []

    config_manager = RecordingConfigManager()
    emitter = RecordingEmitter()
    resources = FakeResources(knowledge_store=FakeKnowledgeStore())
    entities = object()
    pipeline = FakePipeline()

    async def get_next_ent_id():
        return 42

    shared_processor = RecordingBatchProcessor(
        project_id="project-1",
        redis_client=resources.redis,
        llm=resources.llm_service,
        entities=entities,
        processor=pipeline,
        topic_config=make_topic_config(),
        get_next_ent_id=get_next_ent_id,
    )
    project_state = ProjectState(
        project_id="project-1",
        topic_config=shared_processor.kwargs["topic_config"],
        entities=entities,
        pipeline=pipeline,
        scheduler=FakeScheduler(),
        user_name="ada",
        redis_client=resources.redis,
        readable_project_ids=["project-1"],
        batch_processor=shared_processor,
    )

    monkeypatch.setattr(
        "knoggin_server.session.boot.ConfigManager.get",
        staticmethod(lambda: config_manager),
    )
    monkeypatch.setattr(
        "knoggin_server.session.boot.DebugEventEmitter.get",
        staticmethod(lambda: emitter),
    )
    monkeypatch.setattr(
        "knoggin_server.session.boot.BatchConsumer",
        RecordingBatchConsumer,
    )
    monkeypatch.setattr(
        "knoggin_server.session.boot.FileRAGService",
        RecordingFileRAG,
    )

    return SimpleNamespace(
        assembler=SessionAssembler("ada", resources),
        config_manager=config_manager,
        emitter=emitter,
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
    )

    assert ctx.user_name == "ada"
    assert ctx.session_id == "session-1"
    assert ctx.project_id == "project-1"
    assert ctx.project is harness.project_state
    assert ctx.model == "model-a"
    assert ctx.active_topics == ["General", "Identity"]

    assert harness.resources.redis.evals == []

    assert RecordingBatchProcessor.instances == [harness.batch_processor]
    processor = harness.batch_processor
    assert ctx.batch_processor is processor
    assert processor.kwargs["project_id"] == "project-1"
    assert processor.kwargs["redis_client"] is harness.resources.redis
    assert processor.kwargs["llm"] is harness.resources.llm_service
    assert processor.kwargs["entities"] is harness.project_state.entities
    assert processor.kwargs["processor"] is harness.project_state.pipeline
    assert processor.kwargs["topic_config"] is harness.project_state.topic_config
    assert processor.get_next_ent_id is harness.get_next_ent_id

    consumer = RecordingBatchConsumer.instances[0]
    assert ctx.consumer is consumer
    assert consumer.kwargs["knowledge_store"] is harness.resources.knowledge_store
    assert consumer.kwargs["redis"] is harness.resources.redis
    assert consumer.kwargs["processor"] is processor
    assert consumer.get_session_context == ctx.get_conversation_context
    assert consumer.write_to_graph == ctx._write_to_graph_callback
    assert consumer.kwargs["batch_size"] == 8
    assert consumer.kwargs["checkpoint_interval"] == 32
    assert consumer.kwargs["session_window"] == 24

    file_rag = RecordingFileRAG.instances[0]
    assert ctx.file_rag is file_rag
    assert file_rag.session_id == "session-1"
    assert file_rag.embedding_service is harness.resources.embedding

    assert len(ctx.config_unsubscribers) == 1
    assert harness.config_manager.subscriptions == [
        (consumer.update_settings, "developer_settings.ingestion")
    ]
    assert harness.emitter.registered_sessions == [("project-1", "session-1")]


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_launch_starts_scheduler_and_consumer(
    assembler_harness,
):
    harness = assembler_harness
    ctx = await harness.assembler.assemble(harness.project_state, "session-1")

    await harness.assembler.launch(ctx)

    assert harness.project_state.scheduler.running is True
    assert harness.project_state.scheduler.started == 1
    assert ctx.consumer.started == 1


@pytest.mark.runtime
@pytest.mark.no_network
async def test_session_assembler_launch_does_not_restart_running_scheduler(
    assembler_harness,
):
    harness = assembler_harness
    ctx = await harness.assembler.assemble(harness.project_state, "session-1")
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
    ctx = await harness.assembler.assemble(harness.project_state, "session-1")
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
    ctx = await harness.assembler.assemble(harness.project_state, "session-1")
    ctx.batch_processor.get_next_ent_id = None

    with pytest.raises(RuntimeError, match="batch_processor.get_next_ent_id callback"):
        await harness.assembler.launch(ctx)
