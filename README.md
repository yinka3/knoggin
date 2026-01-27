# Knoggin

Self-hosted knowledge graph memory for AI agents.

Knoggin extracts entities and relationships from conversations, maintains evolving profiles, and provides grounded context for LLM-powered agents. Privacy-first, self-hosted, explainable.

## Why Knoggin?

Most memory systems are black boxes. You feed in conversations and hope the right context comes back. Knoggin takes a different approach—every entity, relationship, and fact is traceable back to its source message. You can see what the system knows and why it knows it.

Built for developers who want to own their data and understand their agent's memory.

## Features

- **Entity extraction & disambiguation** — Identifies people, places, and concepts from text. Handles typos, nicknames, and inconsistent references.
- **Relationship tracking** — Builds a graph of connections with message-level evidence and timestamps.
- **Evolving profiles** — Entity summaries update as new information arrives. Contradictions are resolved temporally.
- **Topic configuration** — Define custom schemas with labels, hierarchies, and aliases. Toggle topics active/inactive to control what the agent retrieves. Mark topics as "hot" for priority context loading.
- **Agent-ready retrieval** — Hybrid search combining semantic, keyword, and graph traversal.

## Quick Start

> Coming soon — Installation instructions will be available once the UI is complete.

For now, if you want to explore the codebase:
```bash
git clone https://github.com/yourusername/knoggin.git
cd knoggin
```

Requirements:
- Python 3.12+
- Docker (for Memgraph + Redis)
- LLM API key (OpenAI, Anthropic, Google, or any OpenAI-compatible provider)

## Architecture

Knoggin separates **write** (extraction) from **read** (retrieval).

### Write Path — VEGAPUNK

In *One Piece*, Dr. Vegapunk is the world's greatest scientist, so brilliant that his brain grew too large for his body. His solution? Split his consciousness into six satellites, each handling a specialized aspect of his genius.

Knoggin borrows this idea. Rather than one monolithic prompt, the write path splits cognitive labor across specialized prompts. Each does one thing well, and reasoning stays separate from formatting.

*(Please don't sue me, Eiichiro Oda. I'm just a fan who needed a naming convention.)*

- **VP-01**: Named entity recognition
- **VP-02**: Entity disambiguation
- **VP-03**: Connection extraction
- **VP-04**: Profile refinement
- **VP-05**: Merge judgment

### Read Path

The conversational agent uses bounded tool calls to query the graph and synthesize responses with grounded context.

**Tools**: `search_entity`, `search_messages`, `get_connections`, `get_activity`, `find_path`, `get_hierarchy`

### Background Jobs

**Session jobs** (run during active use):
- **Profile Refinement** — Entity summaries evolve with new information
- **Merge Detection** — Catches duplicates that slip through initial disambiguation

**Scheduled jobs** (run periodically):
- **Entity Cleanup** — Removes orphan entities with no relationships
- **Fact Archival** — Archives invalidated facts past retention period
- **Mood Checkpoint** — Tracks emotional patterns from user messages
- **DLQ Replay** — Retries failed batches on transient errors

## Topic Configuration

Topics are central to how Knoggin organizes and retrieves knowledge. Each topic defines:

- **Labels** — Entity types valid within the topic (e.g., "person", "company", "project")
- **Hierarchy** — Parent/child relationships between entity types (e.g., a "course" contains "exams")
- **Aliases** — Alternative names for labels to handle vocabulary variation
- **Active state** — Toggle topics on/off to control retrieval scope without deleting data

Users define their schema manually. A future discovery mode will analyze messages and suggest schema edits via LLM.

## License

Knoggin is licensed under [AGPL-3.0](./LICENSE).

- Self-host, modify, deploy for personal or commercial use
- Contributions welcome under the same license
- If you modify Knoggin and offer it as a network service, you must release your source code under AGPL-3.0

Knoggin uses [Memgraph](https://memgraph.com) as its graph database, licensed separately under the [Business Source License](https://memgraph.com/legal).

## Contributing

Contributions welcome. Please open an issue first to discuss larger changes. Keep PRs focused.

## Support

Questions or issues? Open a GitHub issue.