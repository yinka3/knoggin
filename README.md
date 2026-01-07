# Vestige

A transparent, self-hosted knowledge graph for personal and conversational AI memory.

Vestige helps you discover patterns in your data by extracting entities and relationships from unstructured text, maintaining evolving profiles, and providing grounded context for LLM conversations. Unlike black-box memory systems, Vestige emphasizes **explainability** — you can trace every entity, relationship, and decision back to its source.

Built on principles from [Zep's temporal knowledge graph architecture](https://github.com/getzep/zep), with emphasis on transparency and human oversight.

## Features

**Entity Extraction & Disambiguation**  
Identifies people, places, organizations, and concepts from conversational text. Handles typos, nicknames, and inconsistent casing, resolving them to canonical entities.

**Relationship Tracking**  
Builds a graph of connections with message-level evidence. Relationships have weights and timestamps — you know what's strong and what's stale.

**Topic-Based Access Control**  
Toggle topics active/inactive to restrict what the agent can see. Mark topics as "hot" for priority retrieval.

---

## Architecture

Vestige separates **write** (deterministic extraction) from **read** (bounded retrieval).

### Write Path — VEGAPUNK Satellites

In *One Piece*, Dr. Vegapunk is the world's greatest scientist, so brilliant that his brain grew too large for his body. His solution? Split his consciousness into six satellites, each handling a specialized aspect of his genius.

Vestige borrows this idea. Rather than one monolithic prompt, the write path splits cognitive labor across specialized prompts:

*(Please don't sue me, Eiichiro Oda. I'm just a fan who needed a naming convention.)*

| Satellite | Role |
|-----------|------|
| VEGAPUNK-01 | Named entity recognition |
| VEGAPUNK-02 | Disambiguation reasoning |
| VEGAPUNK-03 | Disambiguation formatting |
| VEGAPUNK-04 | Connection reasoning |
| VEGAPUNK-05 | Connection formatting |
| VEGAPUNK-06 | Profile refinement |
| VEGAPUNK-07 | Summary merging |
| VEGAPUNK-08 | Merge judgment |

Each prompt does one thing well. Reasoning and formatting are deliberately separated.

### Read Path — STELLA

STELLA serves as the conversational agent, using a bounded 5-state machine for retrieval. Tools query the graph; the LLM synthesizes responses with grounded context.

**Tools:**
- `search_messages` — Semantic search over past messages
- `search_entity` — Find entities by name or alias
- `get_connections` — Find related entities
- `get_activity` — Recent interactions involving an entity
- `find_path` — Shortest connection path between two entities
- `finish` — Deliver final response
- `request_clarification` — Ask user for clarity

### SS Agents (Sleepy/Simple)

Background jobs that wake during idle periods:

- **Profile Refinement** — Entity summaries evolve as new information arrives
- **Merge Detection** — Catches duplicates that slip through initial disambiguation
- **Mood Checkpoint** — Tracks emotional patterns from user messages over time
- **DLQ Replay** — Retries failed batches on transient errors

---

## License

Vestige is licensed under the [GNU Affero General Public License v3.0 (AGPL-3.0)](./LICENSE).

**What this means for you:**

- ✅ **Self-hosting** — Free to use, modify, and deploy for personal or commercial use
- ✅ **Contributions** — Welcomed and licensed under AGPL-3.0
- ✅ **Internal use** — Deploy in your organization without restrictions
- ⚠️ **Modified SaaS** — If you modify Vestige and offer it as a network service, you must release your source code under AGPL-3.0
- 📧 **Commercial licensing** — Contact adedewe.a@northeastern.edu for alternative licensing options

**Dependencies:** Vestige uses [Memgraph](https://memgraph.com) as its default graph database, licensed separately under the [Business Source License (BSL)](https://memgraph.com/legal). The database layer can be swapped for other Cypher-compatible stores (Neo4j, etc.) if needed.

---

## Disclaimer

Vestige is a decision-support tool and decision-making system. Users have access for verifying all extracted entities, relationships, and insights.

See [LICENSE](./LICENSE) for full terms.
