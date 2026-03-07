# Knoggin

Self-hosted knowledge graph memory for AI agents.

Knoggin extracts entities and relationships from conversations, maintains evolving profiles, and provides grounded context for LLM-powered agents. Privacy-first, self-hosted, explainable.

## Why Knoggin?

Most AI memory systems are black boxes. You feed in conversations and hope the right context comes back. Knoggin takes a different approach.

Instead of summarizing chunks of text, Knoggin **classifies** conversational data against a user-defined schema. You define the structure — entity types, hierarchies, aliases — through topic configuration, and the system categorizes every entity, relationship, and fact it encounters against that schema. Think spaCy-style NER, but powered by LLMs and shaped by your own domain model.

Every entity, relationship, and fact is traceable back to its source message. You can see what the system knows and why it knows it.

Built for developers who want to own their data and understand their agent's memory.

## Features

### Core

- **Schema-driven extraction**: define your own structure through topic configuration. The system classifies entities against your labels, hierarchies, and aliases rather than guessing what matters.
- **Entity extraction & resolution**: identifies people, places, and concepts from text. Deterministic resolution handles typos, nicknames, and inconsistent references using fuzzy matching, vector similarity, and graph signals.
- **Relationship tracking**: builds a graph of connections with message-level evidence and timestamps.
- **Evolving profiles**: entity summaries update as new information arrives. Contradictions are resolved temporally — new facts supersede old ones with full audit trail.
- **Merge proposals**: when the system detects duplicate entities, it generates a merge proposal for human review — like a pull request for your knowledge graph.
- **Topic evolution**: the system periodically re-evaluates your topic configuration based on recent conversations, suggesting new topics or deactivating stale ones.
- **Session memory blocks**: the agent can save and forget persistent notes scoped to your session, injected directly into its context.
- **Agent-ready retrieval**: hybrid search combining semantic similarity, keyword matching, and graph traversal.
- **Developer mode presets**: switch between Speed, Balanced, and Deep Research modes that adjust batch sizes, thresholds, tool limits, and job intervals.

### Integrations

- **Web & news search**: tiered web search (Brave, Tavily, DuckDuckGo) and news search built into the agent's tool set.
- **File uploads & RAG**: upload documents to a session. The agent indexes and searches them alongside your knowledge graph.
- **MCP integration**: connect external tools via the Model Context Protocol. Preset support for Google Workspace, Google Maps, GitHub, Slack, and filesystem access.

### Experimental

- **Autonomous Agent Community (AAC)**: multiple AI agents autonomously discuss your knowledge graph in the background. They can spawn specialist sub-agents, save insights, and build on each other's findings. Think of it as a mini [OpenClaw](https://openclaw.ai) running inside your knowledge graph — agents taking turns making tool calls and LLM requests without your input. **⚠️ AAC consumes API credits in the background.** At default intervals (every 15–30 minutes), each discussion round generates multiple LLM calls across participating agents. Monitor your usage if you enable this, especially in Deep Research mode.

## Quick Start

### Requirements

- Python 3.12+
- Node.js 18+
- Docker (for Memgraph + Redis)
- LLM API key (OpenAI, Anthropic, Google, or any OpenAI-compatible provider via [OpenRouter](https://openrouter.ai))

### Setup

```bash
git clone https://github.com/yourusername/knoggin.git
cd knoggin
```

1. **Start infrastructure:**

```bash
docker-compose up -d
```

2. **Start backend:**

```bash
pip install uv  # if not installed
uv sync
uv run uvicorn api:app --host 0.0.0.0 --port 8000
```

> **GPU acceleration (optional):** If you have a compatible GPU (NVIDIA CUDA, AMD ROCm, or Apple Silicon), enable it for faster embeddings and NER:
>
> ```bash
> KNOGGIN_GPU=true uv run uvicorn api:app --host 0.0.0.0 --port 8000
> ```
>
> You can also adjust the thread pool size with `KNOGGIN_WORKERS=8` (default is 4).

3. **Start frontend:**

```bash
cd frontend
npm install
npm run dev
```

4. Open `http://localhost:5173` and complete the onboarding flow.

### Configuration

Defaults work out of the box. LLM API keys, model selection, and your display name are configured through the onboarding UI.

For custom infrastructure hosts, set `REDIS_HOST`, `REDIS_PORT`, `MEMGRAPH_HOST`, or `MEMGRAPH_PORT` in a `.env` file.

## Architecture

Knoggin separates **write** (extraction) from **read** (retrieval), with background jobs handling maintenance and evolution.

### Write Path: VEGAPUNK

In _One Piece_, Dr. Vegapunk splits his consciousness into satellites, each handling a specialized aspect of his genius. Knoggin borrows this idea — rather than one monolithic prompt, the write path splits cognitive labor across specialized stages. Each does one thing well, and reasoning stays separate from formatting.

_Naming inspired by Dr. Vegapunk from [One Piece](https://en.wikipedia.org/wiki/One_Piece) by Eiichiro Oda._

- **VP-01 (NER)**: Named entity recognition. GLiNER zero-shot detection + LLM reasoning to identify entities and classify them against the active topic schema.
- **Entity Resolution**: Deterministic disambiguation using fuzzy matching, vector similarity, graph co-occurrence, and fact relevance. No LLM call — designed to be fast and predictable.
- **VP-02 (Connections)**: Relationship extraction. Identifies connections between entities with message-level evidence.
- **VP-03 (Profiles)**: Fact extraction. Extracts new facts about entities, handles supersedes and invalidations via temporal contradiction detection.
- **VP-04 (Merge Judgment)**: Evaluates whether two similar entities should be merged. Used by the merge detection job.

### Read Path

The conversational agent uses bounded tool calls to query the graph and synthesize responses with grounded context.

**Graph tools**: `search_entity`, `get_connections`, `get_recent_activity`, `find_path`, `get_hierarchy`

**Message tools**: `search_messages`

**Memory tools**: `save_memory`, `forget_memory`

**File tools**: `search_files`

**Web tools**: `web_search`, `news_search`

**MCP tools**: Dynamically loaded from connected MCP servers

### Background Jobs

An inactivity-based scheduler triggers jobs when the user goes idle, avoiding interference with active conversations.

**Session jobs** (run at extraction checkpoints):

- **Profile Refinement**: entity summaries evolve with new information
- **Merge Detection**: catches duplicates that slip through initial resolution, generates merge proposals for user review
- **Topic Evolution**: re-evaluates topic configuration every N messages based on recent conversation patterns

**Scheduled jobs** (run periodically):

- **Entity Cleanup**: removes orphan entities with no relationships or facts
- **Fact Archival**: deletes invalidated facts past retention period
- **DLQ Replay**: retries failed extraction batches on transient errors
- **AAC Discussion**: triggers autonomous agent community discussions at configurable intervals

### Stack

- **Frontend**: React, Tailwind CSS, shadcn/ui
- **Backend**: Python (FastAPI), Memgraph, Redis, OpenRouter

## Topic Configuration

Topics are central to how Knoggin organizes and retrieves knowledge. Each topic defines:

- **Labels**: entity types valid within the topic (e.g., "person", "company", "project")
- **Hierarchy**: parent/child relationships between entity types (e.g., a "course" contains "exams")
- **Aliases**: alternative names for labels to handle vocabulary variation
- **Active state**: toggle topics on/off to control retrieval scope without deleting data
- **Hot topics**: mark topics for priority context loading into the agent's prompt

Users define their schema through the settings UI. The Topic Evolution job can also suggest schema changes based on recent conversation patterns.

## Limitations

Knoggin's extraction pipeline is powered by LLMs, which are non-deterministic by nature. This means:

- **Extraction quality varies by model.** Stronger models produce more accurate entity recognition. Weaker models will miss entities or create duplicates more frequently.
- **Results are not guaranteed to be identical** across runs, even with the same input. The merge proposal system exists partly to catch inconsistencies the extraction pipeline introduces.
- **The system is as good as the schema you give it.** A well-defined topic configuration with clear labels and hierarchies yields dramatically better results than a vague one.

Knoggin does not claim to be a perfect knowledge base. It makes LLM memory structured, inspectable, and correctable — not infallible.

## License

[AGPL-3.0](./LICENSE)

## Contributing

Interested in contributing? Reach out at adedewe.a@northeastern.edu.
