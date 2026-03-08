# Knoggin

Self-hosted knowledge graph memory for AI agents.

> Solo project, actively maintained. The core system works but it hasn't been battle-tested by a community yet. Expect rough edges. Bug reports and feedback are welcome.

Knoggin extracts entities and relationships from conversations, maintains evolving profiles, and gives LLM-powered agents grounded context. Everything traces back to its source message. You can see what the system knows and why.

## Why Knoggin?

I wanted a memory system where I could see exactly what my agent knows and why it knows it.

Most memory layers summarize chunks of text and hope the right context resurfaces. Knoggin classifies conversational data against a schema you define instead. You set up entity types, hierarchies, and aliases through topic configuration, and the system categorizes every entity, relationship, and fact against that structure. Think spaCy-style NER, but shaped by your own domain model and enhanced by LLMs.

## Features

- Schema-driven extraction with user-defined entity types, hierarchies, and aliases
- Entity extraction and resolution using fuzzy matching, vector similarity, and graph signals. Handles typos, nicknames, and inconsistent references deterministically.
- Relationship tracking with message-level evidence and timestamps
- Evolving profiles where new facts supersede old ones through temporal contradiction detection, with full audit trail
- Merge proposals for duplicate entities, surfaced for human review
- Topic evolution that periodically re-evaluates your schema based on recent conversations
- Session memory blocks the agent can save/forget, injected into its context
- Hybrid retrieval combining semantic similarity, keyword matching, and graph traversal
- Developer mode presets (Speed, Balanced, Deep Research) that adjust batch sizes, thresholds, and job intervals
- Web and news search (Brave, Tavily, DuckDuckGo) built into the agent's tool set
- File uploads with RAG, indexed and searchable alongside the knowledge graph
- MCP integration for external tools. Presets for Google Workspace, Maps, GitHub, Slack, and filesystem access.

### Experimental

Autonomous Agent Community (AAC): multiple agents discuss your knowledge graph in the background, spawning sub-agents and saving insights autonomously. Inspired by [OpenClaw](https://openclaw.ai), but scoped to your knowledge graph rather than general-purpose task execution. Warning: this consumes HEAVY amounts of API credits if not careful. Monitor usage if you enable it.

## Limitations

Extraction is powered by LLMs, which are non-deterministic. In practice this means:

- Extraction quality varies by model. Weaker models miss entities or create more duplicates.
- Results aren't guaranteed identical across runs with the same input. The merge system exists partly to catch these inconsistencies.
- The system is as good as the schema you give it. Clear labels and hierarchies produce dramatically better results than vague ones.

Knoggin makes LLM memory structured, inspectable, and correctable. Not infallible.

## Quick Start

### Requirements

- Python 3.12+
- Node.js 18+
- Docker (for Memgraph and Redis)
- LLM API key (OpenAI, Anthropic, Google, or any OpenAI-compatible provider via [OpenRouter](https://openrouter.ai))

### Setup

```bash
git clone https://github.com/yinka3/knoggin.git
cd knoggin
```

Start infrastructure:

```bash
docker-compose up -d
```

Start backend:

```bash
pip install uv  # if not installed
uv sync
uv run uvicorn api:app --host 0.0.0.0 --port 8000
```

If you have a compatible GPU (NVIDIA CUDA, AMD ROCm, or Apple Silicon), you can enable it for faster embeddings and NER:

```bash
KNOGGIN_GPU=true uv run uvicorn api:app --host 0.0.0.0 --port 8000
```

Thread pool size is configurable with `KNOGGIN_WORKERS=8` (default 4).

Start frontend:

```bash
cd frontend
npm install
npm run dev
```

Open `http://localhost:5173` and complete the onboarding flow. LLM API keys, model selection, and your display name are all configured there.

For custom infrastructure hosts, set `REDIS_HOST`, `REDIS_PORT`, `MEMGRAPH_HOST`, or `MEMGRAPH_PORT` in a `.env` file.

## Architecture

Knoggin separates write (extraction) from read (retrieval), with background jobs handling maintenance and evolution.

### Write Path: VEGAPUNK

In One Piece, Dr. Vegapunk splits his consciousness into satellites, each handling a specialized aspect of his genius. Knoggin borrows this idea. Rather than one monolithic prompt, the write path splits cognitive labor across specialized stages. Each does one thing well, and reasoning stays separate from formatting.

Naming inspired by Dr. Vegapunk from [One Piece](https://en.wikipedia.org/wiki/One_Piece) by Eiichiro Oda.

- VP-01 (NER): GLiNER zero-shot detection + LLM reasoning to identify and classify entities against the active topic schema.
- Entity Resolution: deterministic disambiguation using fuzzy matching, vector similarity, graph co-occurrence, and fact relevance. No LLM call.
- VP-02 (Connections): relationship extraction with message-level evidence.
- VP-03 (Profiles): fact extraction with temporal contradiction detection for supersedes and invalidations.
- VP-04 (Merge Judgment): evaluates whether two similar entities should be merged. Used by the merge detection job.

### Read Path

The agent uses bounded tool calls to query the graph and synthesize responses.

Graph: `search_entity`, `get_connections`, `get_recent_activity`, `find_path`, `get_hierarchy` · Messages: `search_messages` · Memory: `save_memory`, `forget_memory` · Files: `search_files` · Web: `web_search`, `news_search` · MCP: dynamically loaded from connected servers

### Background Jobs

A lightweight scheduler polls on a fixed interval, but each job defines its own trigger condition: queue depth, dirty entity volume, message count thresholds, elapsed time, or user idle time. Jobs only run when their specific conditions are met.

Session-scoped jobs run at extraction checkpoints: profile refinement (entity summaries evolve with new information), merge detection (catches duplicates that slip through initial resolution), and topic evolution (re-evaluates schema every N messages).

Scheduled jobs run periodically: entity cleanup (orphan removal), fact archival (deletes invalidated facts past retention), DLQ replay (retries failed batches on transient errors), and AAC discussion (triggers autonomous agent rounds at configurable intervals).

### Stack

Frontend: React, Tailwind CSS, shadcn/ui · Backend: Python (FastAPI), Memgraph, Redis, OpenRouter

## Topic Configuration

Topics are central to how Knoggin organizes knowledge. Each topic defines labels (valid entity types), hierarchy (parent/child relationships between types), aliases (alternative names for vocabulary variation), an active state (toggle scope without deleting data), and hot topic status (priority context loading).

You define your schema through the settings UI. The topic evolution job can also suggest changes based on recent conversation patterns.

## License

[AGPL-3.0](./LICENSE)

## Contributing

Interested in contributing? Reach out at adedewe.a@northeastern.edu.
