# Knoggin

Self-hosted knowledge graph memory for AI agents.

Knoggin extracts entities and relationships from conversations, maintains evolving profiles, and provides grounded context for LLM-powered agents. Privacy-first, self-hosted, explainable.

## Why Knoggin?

Most AI memory systems are black boxes. You feed in conversations and hope the right context comes back. Knoggin takes a different approach.

Instead of summarizing chunks of text, Knoggin **classifies** conversational data against a user-defined ontology. You define the schema (entity types, hierarchies, aliases) through topic configuration, and the system aggressively categorizes every entity, relationship, and fact it encounters. Think spaCy-style NER, but powered by LLMs and shaped by your own domain model.

Every entity, relationship, and fact is traceable back to its source message. You can see what the system knows and why it knows it.

Built for developers who want to own their data and understand their agent's memory.

## Features

- **Ontology-driven extraction**: define your own schema through topic configuration. The system classifies entities against your labels, hierarchies, and aliases rather than guessing what matters.
- **Entity extraction & disambiguation**: identifies people, places, and concepts from text. Handles typos, nicknames, and inconsistent references.
- **Relationship tracking**: builds a graph of connections with message-level evidence and timestamps.
- **Evolving profiles**: entity summaries update as new information arrives. Contradictions are resolved temporally using valid_at/invalid_at tracking.
- **Merge proposals**: when the system detects duplicate entities, it generates a merge proposal for human review, like a pull request for your knowledge graph.
- **Topic configuration**: define custom schemas with labels, hierarchies, and aliases. Toggle topics active/inactive to control retrieval scope. Mark topics as "hot" for priority context loading.
- **Topic evolution**: the system periodically re-evaluates your topic configuration based on recent conversations, suggesting new topics or deactivating stale ones.
- **Preferences & Icks**: explicit user preference tracking. Tell the agent what you like and what you hate.
- **Agent-ready retrieval**: hybrid search combining semantic similarity, keyword matching, and graph traversal.
- **Web & news search**: tiered web search (Brave, Tavily, DuckDuckGo) and news search built into the agent's tool set.
- **File uploads & RAG**: upload documents to a session. The agent indexes and searches them alongside your knowledge graph.
- **MCP integration**: connect external tools via the Model Context Protocol. Preset support for Google Workspace, Google Maps, GitHub, Slack, and filesystem access.
- **Autonomous Agent Community (AAC)**: multiple AI agents discuss your knowledge graph in the background. They can spawn specialist sub-agents, save insights, and surface ideas you haven't considered.
- **Session memory blocks**: the agent can save and forget persistent notes scoped to your session, injected directly into its context.
- **Real-time updates**: WebSocket-based streaming for chat responses and background job status.

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

In _One Piece_, Dr. Vegapunk is the world's greatest scientist, so brilliant that his brain grew too large for his body. His solution? Split his consciousness into satellites, each handling a specialized aspect of his genius.

Knoggin borrows this idea. Rather than one monolithic prompt, the write path splits cognitive labor across specialized prompts. Each does one thing well, and reasoning stays separate from formatting.

_Naming inspired by Dr. Vegapunk from [One Piece](https://en.wikipedia.org/wiki/One_Piece) by Eiichiro Oda._

- **VP-01**: Named entity recognition. Identifies entities and classifies them against the active topic schema.
- **VP-02**: Connection extraction. Identifies relationships between entities with evidence.
- **VP-03**: Fact extraction. Extracts new facts about entities, handles supersedes and invalidations.
- **VP-04**: Merge judgment. Evaluates whether two similar entities should be merged.
- **VP-05**: Contradiction detection. Determines if new facts contradict or supersede existing ones.


### Read Path

The conversational agent uses bounded tool calls to query the graph and synthesize responses with grounded context.

**Graph tools**: `search_entity`, `get_connections`, `get_recent_activity`, `find_path`, `get_hierarchy`, `get_hot_topic_context`

**Message tools**: `search_messages`

**Memory tools**: `save_memory`, `forget_memory`, `get_memory_blocks`

**File tools**: `search_files`, `get_file_manifest`

**Web tools**: `web_search`, `news_search`

**MCP tools**: Dynamically loaded from connected MCP servers

### Background Jobs

An inactivity-based scheduler triggers jobs when the user goes idle, avoiding interference with active conversations.

**Session jobs** (run during active use):

- **Profile Refinement**: entity summaries evolve with new information
- **Merge Detection**: catches duplicates that slip through initial disambiguation, generates merge proposals for user review
- **Topic Evolution**: re-evaluates topic configuration every N messages based on recent conversation patterns
- **AAC Discussion**: triggers autonomous agent community discussions at configurable intervals

**Scheduled jobs** (run periodically):

- **Entity Cleanup**: removes orphan entities with no relationships or facts
- **Fact Archival**: archives invalidated facts past retention period
- **DLQ Replay**: retries failed extraction batches on transient errors

### Stack

- **Frontend:** React, Tailwind CSS, Radix UI
- **Backend:** Python (FastAPI), Memgraph, Redis, OpenRouter

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

- **Extraction quality varies by model.** Stronger models (GPT-4o, Claude Sonnet) produce more accurate entity recognition and disambiguation. Weaker models will miss entities or create duplicates more frequently.
- **Results are not guaranteed to be identical** across runs, even with the same input. The merge proposal system exists partly to catch inconsistencies the extraction pipeline introduces.
- **The system is as good as the schema you give it.** A well-defined topic configuration with clear labels and hierarchies yields dramatically better results than a vague one.

Knoggin does not claim to be a perfect knowledge base. It makes LLM memory structured, inspectable, and correctable, not infallible.

## License

[AGPL-3.0](./LICENSE)

## Contributing

Interested in contributing? Reach out at adedewe.a@northeastern.edu.
