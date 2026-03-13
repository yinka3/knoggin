# Knoggin Server

The core engine for Knoggin. Provides the FastAPI backend, knowledge graph extraction pipeline (VEGAPUNK), and background maintenance jobs.

## Setup

### Requirements
- Python 3.12+
- Docker (for Memgraph and Redis)

### Installation

1.  **Clone and enter directory:**
    ```bash
    git clone https://github.com/yinka3/knoggin.git
    cd knoggin/knoggin-server
    ```

2.  **Start infrastructure:**
    ```bash
    docker-compose -f ../docker-compose.yml up -d
    ```

3.  **Sync dependencies:**
    ```bash
    uv sync
    ```

4.  **Run the server:**
    ```bash
    uv run uvicorn api:app --host 0.0.0.0 --port 8000
    ```

    With GPU support:
    ```bash
    KNOGGIN_GPU=true uv run uvicorn api:app --host 0.0.0.0 --port 8000
    ```

## Architecture: VEGAPUNK

In One Piece, Dr. Vegapunk splits his consciousness into satellites, each handling a specialized aspect of his genius. Knoggin borrows this idea. Rather than one monolithic prompt, the write path splits cognitive labor across specialized stages. Each does one thing well, and reasoning stays separate from formatting.

- **VP-01 (NER):** GLiNER zero-shot detection + LLM reasoning to identify and classify entities against the active topic schema.
- **Entity Resolution:** Deterministic disambiguation using fuzzy matching, vector similarity, graph co-occurrence, and fact relevance. No LLM call.
- **VP-02 (Connections):** Relationship extraction with message-level evidence.
- **VP-03 (Profiles):** Fact extraction with temporal contradiction detection for supersedes and invalidations.
- **VP-04 (Merge Judgment):** Evaluates whether two similar entities should be merged. Used by the merge detection job.

## Background Jobs

A lightweight scheduler polls on a fixed interval, but each job defines its own trigger condition: queue depth, dirty entity volume, message count thresholds, elapsed time, or user idle time.

- **Session-scoped jobs:** Profile refinement, merge detection, and topic evolution.
- **Scheduled jobs:** Entity cleanup, fact archival, DLQ replay, and AAC discussion.

## Stack
- **Framework:** FastAPI
- **Database:** Memgraph (Graph), Redis (Cache/Queue)
- **ML/NLP:** GLiNER, sentence-transformers, spacy
- **LLM Integration:** OpenRouter (OpenAI, Anthropic, Gemini, etc.)

