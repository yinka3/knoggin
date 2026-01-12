# Security Policy

## Deployment Model

Vestige is designed for **local, single-user deployment**. All services run on localhost and are not intended to be exposed to external networks. The web interface binds to localhost only.

---

## Data Flow

### What Stays Local

| Data | Storage | Encrypted |
|------|---------|-----------|
| Raw messages | Redis | No* |
| Message embeddings | FAISS (in-memory, rebuilt on startup) | No |
| Entities & relationships | Memgraph | No* |
| Entity profiles & summaries | Memgraph + EntityResolver cache | No* |
| Mood checkpoints | Memgraph | No* |
| Coordination flags | Redis | No |

*\*Relies on localhost isolation and service-level authentication. Data is plaintext at rest.*

### What Leaves Your Machine

Vestige uses [OpenRouter](https://openrouter.ai) to access LLM providers. The following data is sent externally:

| Data Sent | Purpose | Destination |
|-----------|---------|-------------|
| User messages (batched) | Entity extraction, disambiguation | OpenRouter → Gemini |
| Entity names + context | Connection reasoning, profile refinement | OpenRouter → Gemini |
| User query + graph context | Conversational retrieval (STELLA) | OpenRouter → Claude |

**OpenRouter's default policy:** Prompts and completions are not logged unless you opt-in. Metadata (timestamps, token counts) is retained. See [OpenRouter Privacy](https://openrouter.ai/docs/guides/privacy/logging).

**Downstream providers:** Each model provider (Google, Anthropic) has its own data retention policy. OpenRouter tracks these per-endpoint.

### Enabling Zero Data Retention (Optional)

OpenRouter supports a `zdr` parameter that restricts requests to providers with zero data retention policies. To enable:

1. **Account-wide:** Enable in your [OpenRouter privacy settings](https://openrouter.ai/settings/privacy)
2. **Per-request:** Add `"zdr": true` to the `provider` object in API calls

Note: Enabling ZDR may limit available models/endpoints.

---

## Authentication

### Service-Level Auth

Redis and Memgraph require passwords configured in `.env`:

```
REDIS_PASSWORD=<your-password>
MEMGRAPH_USER=<your-user>
MEMGRAPH_PASSWORD=<your-password>
```

This protects against unauthorized local access and browser-based attacks (DNS rebinding).

### Web Interface

The application binds to `localhost` only. There is no user authentication layer — security relies on:

- Localhost binding (not accessible from network)
- Service-level passwords preventing direct database access
- Single-user deployment model

---

## Environment Configuration

Required secrets in `.env`:

```bash
OPENROUTER_API_KEY=     # Your OpenRouter API key
REDIS_PASSWORD=         # Redis authentication
MEMGRAPH_USER=          # Memgraph username
MEMGRAPH_PASSWORD=      # Memgraph password
VESTIGE_USER_NAME=      # Creates root user entity
```

**Do not commit `.env` to version control.**

---

## Do Not

- Expose Redis (port 6379) to the internet
- Expose Memgraph (port 7687) to the internet
- Bind the web interface to `0.0.0.0`
- Share your `.env` file or commit it to version control
- Run Vestige in a multi-tenant environment

If you need network-accessible deployment, additional hardening is required (TLS, reverse proxy auth, firewalls) which is outside the scope of this project.

---

## Not In Scope

- Issues requiring the user to misconfigure their own deployment
- Prompt injection in a single-user context (you control your own data)
- Data sent to OpenRouter/LLM providers (documented above, user choice)
- Vulnerabilities in dependencies that don't affect Vestige's usage

---

## Supported Versions

| Version | Supported |
|---------|-----------|
| 0.1.x   | ✅        |

---

## Reporting a Vulnerability

If you discover a security issue in the Vestige codebase:

1. **Do not** open a public GitHub issue
2. Email: adedewe.a@northeastern.edu
3. Include steps to reproduce
4. Allow 48 hours for initial response

We will acknowledge receipt, investigate, and coordinate disclosure.