import pytest

from common.schema.episode.generation import (
    LLMEpisodeDecision,
    LLMEpisodeWindowDecision,
)
from common.schema.settings import EpisodeSettings
from core.knowledge.episodes.generator import EpisodeGenerator
from core.knowledge.episodes.policy import EpisodeGenerationPolicy


class _ScriptedLLM:
    def __init__(self, outputs):
        self.outputs = list(outputs)
        self.calls = []

    async def generate_structured(self, **kwargs):
        self.calls.append(kwargs)
        return self.outputs.pop(0)


class _RecordingEmbeddingService:
    def __init__(self, *, chunk_limit=None):
        self.calls = []
        self.chunk_limit = chunk_limit

    def split_for_embedding(self, text):
        if self.chunk_limit is None:
            return [text]
        return [
            text[index : index + self.chunk_limit]
            for index in range(0, len(text), self.chunk_limit)
        ]

    async def encode(self, texts):
        self.calls.append(list(texts))
        return [[float(index + 1)] * 768 for index in range(len(texts))]


@pytest.mark.no_network
async def test_generator_uses_one_window_call_and_one_embedding_batch():
    llm = _ScriptedLLM(
        [
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="The team selected a bounded episode design.",
                        message_influences=["message:1", "message:2"],
                    ),
                    LLMEpisodeDecision(
                        summary="The team approved the storage budget.",
                        message_influences=["message:3"],
                    ),
                ]
            )
        ]
    )
    embeddings = _RecordingEmbeddingService()
    generator = EpisodeGenerator(llm=llm, embedding_service=embeddings)
    policy = EpisodeGenerationPolicy.capture(settings=EpisodeSettings())

    build = await generator.generate(
        user_name="ada",
        project_id="project-1",
        messages=[
            {
                "message_id": 101,
                "session_id": "session-1",
                "role": "user",
                "content": "Use a bounded episode design.",
                "timestamp_ms": 1,
            },
            {
                "message_id": 102,
                "session_id": "session-1",
                "role": "assistant",
                "content": "The design is bounded by one semantic window.",
                "timestamp_ms": 2,
                "user_msg_id": 101,
            },
            {
                "message_id": 103,
                "session_id": "session-1",
                "role": "user",
                "content": "The storage budget is approved.",
                "timestamp_ms": 3,
            },
        ],
        policy=policy,
    )

    assert len(llm.calls) == 1
    assert "Prior project episodes" not in llm.calls[0]["user"]
    assert "consolidate" not in llm.calls[0]["system"]
    assert not hasattr(generator, "knowledge_store")
    assert len(embeddings.calls) == 1
    assert len(embeddings.calls[0]) == 2
    assert [episode.embedding[0] for episode in build.final_episodes] == [1.0, 2.0]
    assert [
        [message.message_id for message in episode.messages]
        for episode in build.final_episodes
    ] == [[101, 102], [103]]


@pytest.mark.no_network
@pytest.mark.parametrize(
    "repaired_sources",
    [["message:2"], ["message:1", "message:2"]],
)
async def test_narrative_repair_rejects_changed_evidence_before_embedding(
    repaired_sources,
):
    llm = _ScriptedLLM(
        [
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="x" * 4001,
                        message_influences=["message:1"],
                    )
                ]
            ),
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="Repaired prose.",
                        message_influences=repaired_sources,
                    )
                ]
            ),
        ]
    )
    embeddings = _RecordingEmbeddingService()
    with pytest.raises(ValueError, match="changed proposal source references"):
        await EpisodeGenerator(llm=llm, embedding_service=embeddings).generate(
            user_name="ada",
            project_id="project-1",
            messages=[
                {
                    "message_id": message_id,
                    "session_id": "session-1",
                    "role": "user",
                    "content": "Source evidence.",
                    "timestamp_ms": index,
                }
                for index, message_id in enumerate((101, 102), start=1)
            ],
            policy=EpisodeGenerationPolicy.capture(settings=EpisodeSettings()),
        )
    assert len(llm.calls) == 2
    assert embeddings.calls == []


@pytest.mark.no_network
async def test_narrative_repair_rejects_dropped_proposal_before_embedding():
    llm = _ScriptedLLM(
        [
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="x" * 4001,
                        message_influences=["message:1"],
                    )
                ]
            ),
            LLMEpisodeWindowDecision(proposals=[]),
        ]
    )
    embeddings = _RecordingEmbeddingService()
    with pytest.raises(ValueError, match="changed proposal source references"):
        await EpisodeGenerator(llm=llm, embedding_service=embeddings).generate(
            user_name="ada",
            project_id="project-1",
            messages=[
                {
                    "message_id": 101,
                    "session_id": "session-1",
                    "role": "user",
                    "content": "Source evidence.",
                    "timestamp_ms": 1,
                }
            ],
            policy=EpisodeGenerationPolicy.capture(settings=EpisodeSettings()),
        )
    assert embeddings.calls == []


@pytest.mark.no_network
async def test_valid_long_narrative_embeds_every_chunk_and_keeps_full_text():
    summary = "A difficult decision was made. " * 110
    assert len(summary) < 4000
    llm = _ScriptedLLM(
        [
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary=summary,
                        message_influences=["message:1"],
                    )
                ]
            )
        ]
    )
    embeddings = _RecordingEmbeddingService(chunk_limit=400)
    build = await EpisodeGenerator(llm=llm, embedding_service=embeddings).generate(
        user_name="ada",
        project_id="project-1",
        messages=[
            {
                "message_id": 101,
                "session_id": "session-1",
                "role": "user",
                "content": "Source evidence.",
                "timestamp_ms": 1,
            }
        ],
        policy=EpisodeGenerationPolicy.capture(settings=EpisodeSettings()),
    )
    assert build.final_episodes[0].summary == summary.strip()
    assert len(embeddings.calls) == 1
    assert len(embeddings.calls[0]) > 1
    assert all(len(chunk) <= 400 for chunk in embeddings.calls[0])
    assert "".join(embeddings.calls[0]) == "Summary:\n" + summary.strip()
    assert len(build.final_episodes[0].embedding) == 768


@pytest.mark.no_network
async def test_generator_repairs_an_overlong_narrative_without_changing_sources():
    llm = _ScriptedLLM(
        [
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="x" * 4001,
                        message_influences=["message:1"],
                    )
                ]
            ),
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="The deployment remains bounded by one window.",
                        message_influences=["message:1"],
                    )
                ]
            ),
        ]
    )
    embeddings = _RecordingEmbeddingService()
    generator = EpisodeGenerator(llm=llm, embedding_service=embeddings)

    build = await generator.generate(
        user_name="ada",
        project_id="project-1",
        messages=[
            {
                "message_id": 101,
                "session_id": "session-1",
                "role": "user",
                "content": "Keep the deployment episode bounded.",
                "timestamp_ms": 1,
            }
        ],
        policy=EpisodeGenerationPolicy.capture(settings=EpisodeSettings()),
    )

    assert len(llm.calls) == 2
    assert "repairing" in llm.calls[1]["system"]
    assert "consolidation" not in llm.calls[1]["system"]
    assert build.final_episodes[0].summary == (
        "The deployment remains bounded by one window."
    )
    assert len(embeddings.calls) == 1


@pytest.mark.no_network
async def test_generator_rejects_over_capacity_output_without_embedding_it():
    llm = _ScriptedLLM(
        [
            LLMEpisodeWindowDecision(
                proposals=[
                    LLMEpisodeDecision(
                        summary="The model selected too many source messages.",
                        message_influences=["message:1", "message:2"],
                    )
                ]
            )
        ]
    )
    embeddings = _RecordingEmbeddingService()
    generator = EpisodeGenerator(llm=llm, embedding_service=embeddings)

    with pytest.raises(ValueError, match="source message limit"):
        await generator.generate(
            user_name="ada",
            project_id="project-1",
            messages=[
                {
                    "message_id": 101,
                    "session_id": "session-1",
                    "role": "user",
                    "content": "First source.",
                    "timestamp_ms": 1,
                },
                {
                    "message_id": 102,
                    "session_id": "session-1",
                    "role": "assistant",
                    "content": "Second source.",
                    "timestamp_ms": 2,
                    "user_msg_id": 101,
                },
            ],
            policy=EpisodeGenerationPolicy.capture(
                settings=EpisodeSettings(max_episode_source_messages=1)
            ),
        )

    assert len(llm.calls) == 1
    assert embeddings.calls == []
