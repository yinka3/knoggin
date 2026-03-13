# Knoggin

Self-hosted knowledge graph memory for AI agents.

> Solo project, actively maintained. The core system works but it hasn't been battle-tested by a community yet. Expect rough edges. Bug reports and feedback are welcome.

Knoggin extracts entities and relationships from conversations, maintains evolving profiles, and gives LLM-powered agents grounded context. Everything traces back to its source message. You can see what the system knows and why.

## Project Structure

This repository is a monorepo containing the following components:

- **[knoggin-server](./knoggin-server/README.md):** The core engine (FastAPI, Memgraph, Redis).
- **[knoggin-sdk](./knoggin-sdk/README.md):** Lightweight Python client for interacting with the server.
- **[frontend](./frontend/README.md):** React-based dashboard and agent interface.
- **knoggin-website:** Documentation and landing page (Hosting coming soon).

## Why Knoggin?

I wanted a memory system where I could see exactly what my agent knows and why it knows it.

Most memory layers summarize chunks of text and hope the right context resurfaces. Knoggin classifies conversational data against a schema you define instead. You set up entity types, hierarchies, and aliases through topic configuration, and the system categorizes every entity, relationship, and fact against that structure. Think spaCy-style NER, but shaped by your own domain model and enhanced by LLMs.

## Quick Start (Combined)

To get the entire system up and running:

1.  **Start Infrastructure:**
    ```bash
    docker-compose up -d
    ```

2.  **Sync Dependencies (Root):**
    ```bash
    uv sync
    ```

3.  **Run Server:**
    ```bash
    cd knoggin-server
    uv run uvicorn api:app --host 0.0.0.0 --port 8000
    ```

4.  **Run Frontend:**
    ```bash
    cd ../frontend
    npm install
    npm run dev
    ```

For detailed documentation on each component, please refer to their respective `README.md` files.

## License

[AGPL-3.0](./LICENSE)

## Contributing

Interested in contributing? Reach out at adedewe.a@northeastern.edu.
