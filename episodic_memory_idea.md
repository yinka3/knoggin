# Idea: Replace the Fact Layer with Episodic Memory

## Context

After reviewing the architecture, the current **Fact** layer may be
solving a problem that the existing message + entity + relationship
model already covers.

Current pipeline:

    Messages (canonical)
        ↓
    Entities
        ↓
    Relationships
        ↓
    Facts

Facts are another LLM-generated interpretation of the same evidence.
They require contradiction handling, invalidation, auditing, embeddings,
and refinement jobs while the original messages remain the real source
of truth.

## Proposal

Investigate replacing (or significantly reducing) the Fact layer with an
**Episodic Memory** layer.

Generate an Episode every N messages or at natural conversation
boundaries.

Each episode should contain:

### New developments

-   What was introduced during this episode?

### Updates

-   What changed relative to previous understanding?

### Unresolved / Uncertain

-   What should remain tentative rather than becoming long-term
    knowledge?

## Suggested memory hierarchy

    Messages (canonical evidence)
            ↓
    Episodes (summaries with source message references)
            ↓
    Entities / Relationships (navigation)
            ↓
    Optional rebuildable current-state summaries

## Questions

-   Which components truly depend on FactRecord?
-   Could profile refinement become episode generation?
-   Could contradiction handling become "updates from previous episode"?
-   Which retrieval paths actually benefit from persistent facts?
-   Would episodes reduce complexity while preserving answer quality?

The goal is not to remove provenance. Messages remain canonical. The
question is whether episodes are a better long-term abstraction than
permanent atomic facts.
