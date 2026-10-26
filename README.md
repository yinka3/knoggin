# Knoggin

Knoggin is a memory engine for AI agents that stores and indexes everything on your machine. It turns conversations and project files into entities, relationships, and Episodes, while keeping links to the original sources. Each project supplies its own domain configuration for entity types, topics, aliases, and matching rules.

The project is under active development and is not yet released.

## How it works

### Main information structures

| Structure | What it holds and how it is used |
| --- | --- |
| **Project Brief (`PROJECT.md`)** | User-written goals, background, and preferences for a project. The agent reads it as project guidance. |
| **Context (`CONTEXT.md`)** | The project's current working memory. Knoggin stores it as versioned blocks with evidence links, then renders the latest revision into `CONTEXT.md` for people to read and edit. The agent receives the committed Context when it works in that project. |
| **Run Notebook** | The agent's working memory during one run. It collects results from message, entity, graph, Episode, document, and web tools under stable references, then gives the agent a bounded view of what it has found. It is not persisted after the run. |
| **Semantic window** | A bounded group of completed conversation exchanges. It freezes the source messages and processing settings so Episode, Context, and knowledge updates can move through durable checkpoints. |
| **Episode** | A summary of related conversation messages: what happened, what changed, and what remains unresolved. It keeps links to those messages and can connect to the entities and relationships mentioned in them. |
| **Entity identity and project entity context** | A stable identity lets Knoggin recognize the same entity across a user's projects. Each project keeps its own type, topic, and activity for that identity. |
| **Relationship observations** | Evidence-backed connections between entities in a project. These observations support graph retrieval and can be reviewed when they conflict. |
| **Project files and source references** | Original files stay in the project library; searchable chunks are derived from them. Source references and Context supports let Knoggin trace derived information back to messages or documents. |

In short, source messages and files supply evidence; semantic windows turn conversation history into Episodes, Context revisions, and graph knowledge. Retrieval brings relevant pieces into the Run Notebook while the agent works. Context remains the project's durable memory across runs.

### What Context contains and where it goes

Context is a readable summary of a project's current state, active work, decisions and constraints, preferences, and open questions. Those are the default sections; a project's domain configuration can define different ones. Each piece of text is a block with its own identity and type, such as a user assertion, source-grounded information, an agent-derived note, or a human edit. Supported blocks link back to the messages or assistant-used sources behind them.

Knoggin saves each Context revision in Postgres and renders the latest one as `CONTEXT.md`. A person can edit that file; Knoggin reconciles the edit into a new revision. The agent receives the committed Context in its project prompt as current working memory. It can use Episodes, messages, and documents to check details when needed.

When Context changes, the knowledge stage examines the affected blocks for entity mentions and relationships. It sends their text to GLiNER to find entity spans, alongside known-alias matching and other configured extraction steps. GLiNER's entity labels and descriptions come from the project's domain configuration; Context supplies the text to examine rather than model instructions.

User assertions, source-grounded information, and human edits can contribute to the project's entity contexts and graph observations; agent-derived notes are excluded from that extraction. Links to the blocks and their available evidence let Knoggin update or retire derived observations when a block is changed or removed.

### Ingestion

Knoggin stores conversation messages first, then processes completed exchanges in bounded, project-owned semantic windows. Each window moves through durable stages: it builds Episodes, updates project context, and extracts entities and relationships. The stages can resume after a failure without treating partial work as complete.

Episodes summarize what happened across related messages, including developments and unresolved points. Entity matching uses the project's domain configuration and known aliases, with model-assisted extraction where configured. Knoggin keeps the source messages behind these derived records so an agent can inspect the evidence later.

Project files follow a separate path. Knoggin keeps the files in a confined project library, extracts text and structure, splits them into searchable chunks, and indexes those chunks. The project files remain the source of truth; the index can be rebuilt from them.

### Knowledge storage

Postgres holds messages, Episodes, document metadata and chunks, entity records, relationship observations, and their source references. The local database image includes pgvector for embeddings and Apache AGE for graph data.

Entity identity is shared across a user's projects. Project context, documents, Episodes, and relationship observations stay scoped to their projects. Knoggin records observed relationships with evidence rather than silently treating every extraction as a settled fact. Potential duplicate entities and conflicting relationship evidence can be reviewed through maintenance workflows.

### Retrieval

Conversation search combines full-text and embedding matches and can rerank candidates before returning surrounding message context. Episode search returns compact summaries first; an agent can then open an Episode to read its source messages. Entity search and graph traversal expose related entities and the evidence for their connections.

Document search is a separate path. It combines text and embedding matches over indexed chunks, can rerank results, and supports narrowing the search to a document or project subtree. Reads respect the project's configured visibility into other projects.

## Repository layout

- `server/` contains the Python engine, runtime, storage, and tests. See [server/README.md](server/README.md).
- `api/` contains the local HTTP service. See [api/README.md](api/README.md).
- `app/` contains the Next.js configuration; its frontend source and API integration are in progress.
- `docker/` contains the local Postgres image and initialization scripts.

The engine runs inside an application and does not start an HTTP service on its own. The `api/` package hosts the HTTP service; Docker Compose starts the database only.

## Local setup

You need Python 3.12+, [uv](https://docs.astral.sh/uv/), and Docker Compose. From the repository root:

```powershell
Copy-Item .env.example .env
docker compose up -d
uv sync --package server
```

The copied `.env` works with the local Docker defaults. For a different database password, set both `POSTGRES_PASSWORD` and the password in `DATABASE_URL` to the same value before starting Postgres.

Model files download when first needed. To download them ahead of time, run:

```powershell
uv run --package server python server/scripts/prefetch_models.py
```

Stop Postgres with `docker compose down`. This keeps the database volume; `docker compose down -v` deletes it.

## Where your data goes

Knoggin stores your memory locally: the database, your project files, search indexes, embeddings, reranking, entity extraction (GLiNER), and document parsing all run on your machine.

The language model is the exception. By default Knoggin uses OpenRouter, so this text is sent to the model provider you configure:

- each agent step: the system prompt, Project Context, retrieved results, and recent conversation;
- memory building: each conversation window, for Episodes, Context updates, and relationship extraction;
- web search queries, when web tools are enabled;
- JEV requests, when JEV is enabled (it is off by default).

To keep everything on your machine, point Knoggin at a local model server (see below).

## Using a local model

Knoggin works with any OpenAI-compatible server, such as Ollama, LM Studio, or vLLM. With Ollama:

1. Start Ollama with a context window large enough for Knoggin's prompts. Ollama's default is about 2K tokens and it silently drops anything longer, so set it explicitly:

   ```bash
   OLLAMA_CONTEXT_LENGTH=65536 ollama serve
   ```

   A larger context uses more GPU memory; choose the largest your hardware handles. If a model processes far fewer tokens than Knoggin sent, Knoggin stops with an error that names this setting instead of continuing with a cut prompt.

2. Pick a model that supports tool calling (for example Qwen 2.5/3 or Llama 3.1 and later) and set it in the `llm` settings, either through `PATCH /v1/settings/llm` or in `knoggin.yml`:

   ```yaml
   llm:
     base_url: http://localhost:11434/v1
     agent_model: qwen3:14b
     extraction_model: qwen3:14b
     merge_model: qwen3:14b
     request_timeout_seconds: 900
   ```

   No API key is needed for a local server. Large models on limited hardware can take minutes per call, so raise `request_timeout_seconds` if requests time out.

3. Thinking is limited by default. Local servers can only turn a model's thinking on or off, so Knoggin uses `thinking: planning_only`: the model thinks on the agent's planning step and final answer, and not on tool steps or memory building. On a 27B Qwen this was several times faster with the same results. Set `thinking: off` to disable it everywhere, or `thinking: provider_default` to let the model think on every step; Knoggin then warns that responses will be several times slower.

Repeated agent steps reuse the model's work on the unchanged start of the prompt, so steps after the first in a run are much faster.

## Configuration

`.env.example` lists database, model, and credential settings. Provider API keys (`OPENROUTER_API_KEY`, `BRAVE_API_KEY`, `TAVILY_API_KEY`, `JEV_API_KEY`) are read only from the environment or `.env`; they are never saved in settings. The application runtime also takes a configuration directory and manages `knoggin.yml` there. See [server/README.md](server/README.md) for its runtime entry point.

## Development

```powershell
uv run --package server pytest server/tests
uv run ruff check server/src server/tests
```

## License

[AGPL-3.0](LICENSE)

## Contact

Feedback is welcome at adedewe.a@northeastern.edu.
