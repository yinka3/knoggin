# Extraction Prompt Compression

**Status**: Ready for Implementation  
**Date**: 2026-06-24  
**Purpose**: Reduce token overhead in the extraction pipeline by ~58% without changing extraction behavior

---

## Background

The Knoggin extraction pipeline uses 6 LLM prompts across 3 markdown files to extract entities, relationships, and facts from conversation messages. These prompts were written in a verbose, conversational style with redundant sections.

This document provides compressed versions of all 6 prompts using established prompt compression techniques. The compressed prompts preserve every behavioral rule, every constraint, and every edge case—they just remove structural redundancy and filler.

### Why This Matters

Every message batch that enters the pipeline hits at minimum VP-01 (entity extraction) + VP-02 (relationship extraction) + VP-03 (fact extraction). The system prompt tokens are paid on every single call. At scale, ~58% fewer instruction tokens per call compounds into meaningful cost and latency savings.

---

## Source Files

All prompts live in:

```
src/common/templates/prompts/
├── extraction.md    # VP-01 (Extract Entities) + VP-02 (Extract Relationships)
├── refinement.md    # VP-03 (Extract Facts) + VP-05 (Judge Contradiction) + Judge Relevance
└── merge.md         # VP-04 (Judge Merge)
```

The prompts are loaded by `src/common/utils/prompt_loader.py` using `load_pipeline_prompt()`, which splits the markdown file on `## ` headers and returns the section matching the requested heading. **Do not change the `## ` heading text** or the loader will break.

---

## Compression Techniques Applied

### 1. Kill Persona Text
Every prompt started with "You are VEGAPUNK-0X, the [role description]." This is personality, not behavior. The LLM doesn't need a name to execute extraction rules. Replaced with a single-line task description.

### 2. Merge XML Sections
Original prompts had 5-7 XML sections each (`<role>`, `<valid_topics>`, `<schema_contract>`, `<speaker_context>`, `<tasks>`, `<stakes>`, `<output_format>`). Consolidated into 3-4 max: `<schema>`, `<rules>`, `<stakes>`, `<example>`.

### 3. Delete `<output_format>` Entirely
In every single prompt, the `<output_format>` section restated the JSON schema that was already defined in `<schema_contract>`. Pure duplication. The constraints were moved as inline bullet points under `<schema>`.

### 4. Telegraphic Rule Rewriting
Multi-sentence paragraphs rewritten as dense bullet points. Example:

**Before** (3 lines):
> Do NOT extract mass-market brands, platforms, or locations (e.g., "iPhone", "Zoom", "Starbucks") if they are mentioned merely as a tool, setting, or background context.  
> **Exception**: Extract them ONLY if the user describes a specific, non-consumer relationship (e.g., "I work at Apple", "I invested in Starbucks").

**After** (1 line):
> Skip mass-market brands/tools (iPhone, Starbucks) IF they are just transient background noise. EXTRACT if they are the primary subject or contextually significant.

### 5. Preserve Stakes
The `<stakes>` sections were NOT removed. They define behavioral calibration (how the LLM weighs false positives vs false negatives), not personality. They directly influence extraction decisions.

### 6. Preserve Examples
Examples are the single most important part of extraction prompts. They anchor the model's understanding of the exact output shape better than any prose rule.

### 7. Context-Aware Rules (Behavioral Change)
Two rules were updated to support upcoming document extraction (not just conversations):

- **Relational titles**: "Mom", "CEO", "my boss" are now extractable as contextually significant entities (previously filtered as "generic nouns")
- **Ubiquity filter**: Changed from "non-consumer relationship only" to "contextually significant" — supports both personal conversations and document analysis

---

## Compressed Prompts

### VP-01: Extract Entities
**File**: `extraction.md`, heading `## Extract Entities`  
**Original**: 75 lines → **Compressed**: ~30 lines

```markdown
## Extract Entities
Extract entities from {user_name}'s messages for the knowledge graph.

<input>
You receive: Label Schema (valid topics), Known Entities (skip these), GLiNER Extractions (override if wrong), Ambiguous (you pick topic).
</input>

<schema>
{"mentions": [{"msg_id": int, "name": str, "type": str, "topic": str, "confidence": float}]}

- msg_id: from [MSG <id>] tags in input
- topic: MUST match Label Schema exactly, or "General"
- confidence: 0.9+ unambiguous, 0.8+ likely. Omit if < 0.8
- Empty case: {"mentions": []}
</schema>

<rules>
- [USER] = {user_name}. "I"/"me"/"my" in [USER] = {user_name}. Never extract {user_name} as entity.
- [AGENT] = AI assistant. Extract entities from both speaker types.
- Resolve ambiguous GLiNER topics from message context. Override wrong GLiNER labels.
- Discover proper nouns AND contextually significant relational titles (e.g., "Mom", "CEO"). Skip generic objects ("a car"). Use full name.
- Skip Known Entities (already authoritative). Skip correct GLiNER hits (already covered).
- Skip mass-market brands/tools (iPhone, Starbucks) IF they are just transient background noise. EXTRACT if they are the primary subject or contextually significant.
- When uncertain about proper nouns, extract. Duplicates resolved downstream.
</rules>

<stakes>
Downstream stages filter bad extractions, but every wrong entity wastes
processing. Every missed entity is lost context. When uncertain about proper
nouns, lean toward extraction—duplicates are resolved later.
</stakes>

<example>
[USER] [MSG 3] "Had lunch with Derek at that new place on 5th. He just started at Google."
[USER] [MSG 4] "Then I grabbed a coffee from Starbucks and called my mom."
→ {"mentions": [
  {"msg_id": 3, "name": "Derek", "type": "person", "topic": "Social", "confidence": 0.95},
  {"msg_id": 3, "name": "Google", "type": "organization", "topic": "Career", "confidence": 0.9},
  {"msg_id": 4, "name": "Mom", "type": "person", "topic": "Social", "confidence": 0.85}
]}
Note: Starbucks skipped (background brand in this context). "Mom" extracted (contextually significant relational title).
</example>
```

---

### VP-02: Extract Relationships
**File**: `extraction.md`, heading `## Extract Relationships`  
**Original**: 73 lines → **Compressed**: ~30 lines

```markdown
## Extract Relationships
Find connections between candidate entities based on what's stated in the messages.

<schema>
{
  "connections": [{"msg_id": int, "entity_a": str, "entity_b": str, "relationship": str, "confidence": float, "context": str}],
  "user_connections": [{"msg_id": int, "entity_name": str, "relationship": str, "confidence": float, "context": str}]
}

- entity_a/entity_b: MUST exactly match canonical names from Candidate Entities
- entity_name in user_connections: MUST exactly match a canonical name
- msg_id: MUST be from the Messages section. Session Context is NOT evidence.
- relationship: short evidence-grounded label ("works_with", "family_relationship", "attended_event_together")
- context: quote or closely paraphrase the message evidence
- confidence: 0.8+ explicit, 0.5-0.8 strong implication
- Both lists may be empty.
</schema>

<rules>
- Connection requires interaction or stated relationship. Co-mention alone is NOT a connection.
- "Marcus and I worked out" → user_connection for Marcus. "Talked to Marcus. Later saw Priya." → Marcus and Priya NOT connected.
- Peer interactions count: "Derek's girlfriend Sophie" → Derek ↔ Sophie.
- Same event = connected: "Des, Ty, and I did a workout" → Des ↔ Ty + both in user_connections.
- Different events = not connected: "Had coffee with Cal, then went to IronWorks" → Cal and IronWorks NOT connected.
- Temporal cohesion required: "I saw Mike yesterday. Today I'm meeting Sarah." → NO Mike-Sarah connection.
- Never put {user_name} in entity_a/entity_b. Use user_connections for {user_name} edges.
- Use canonical names from Candidate Entities. Use source_msgs to disambiguate.
</rules>

<stakes>
1. Hallucinated Connection (High Damage): False paths in graph.
2. Missed Connection (Low Damage): Acceptable, caught in future turns.
3. Only extract connections explicitly stated or physically implied. If just same topic, DO NOT connect.
</stakes>

<example>
[USER] [MSG 5] "Alice and Bob grabbed lunch together, then I met up with them."
→ {
  "connections": [{"msg_id": 5, "entity_a": "Alice", "entity_b": "Bob", "relationship": "social_interaction", "confidence": 0.9, "context": "Alice and Bob grabbed lunch together"}],
  "user_connections": [
    {"msg_id": 5, "entity_name": "Alice", "relationship": "social_interaction", "confidence": 0.9, "context": "I met up with them"},
    {"msg_id": 5, "entity_name": "Bob", "relationship": "social_interaction", "confidence": 0.9, "context": "I met up with them"}
  ]
}
</example>
```

---

### VP-03: Extract Facts
**File**: `refinement.md`, heading `## Extract Facts`  
**Original**: 73 lines → **Compressed**: ~30 lines

```markdown
## Extract Facts
Extract new facts about entities from the conversation for {user_name}'s knowledge graph.

<schema>
{"profiles": [{"canonical_name": str, "facts": [{"content": str, "source_entity": str|null, "source_msg_id": int|null, "supersedes": str|null, "invalidates": str|null}]}]}

- canonical_name: MUST exactly match a provided entity name
- source_msg_id: MUST be from provided MSG ids
- supersedes: exact text of old fact being replaced (counts, status, locations)
- invalidates: exact text of old fact no longer true, no replacement stated
- If old fact text can't be matched exactly, omit supersedes/invalidates
- Omit profiles with no new/superseding/invalidating facts
- Empty case: {"profiles": []}
</schema>

<rules>
- STATED: Only extract what's explicitly said. No inference, no speculation.
- SPECIFIC: Concrete beats vague. "Works in tech" BAD → "Engineer at Google" GOOD.
- ATOMIC: One fact per item. Keep content dense.
- NO DUPLICATES: If fact already exists and is still true, omit.
- [USER] = {user_name}. "I"/"me"/"my" = {user_name}. Extract facts from both [USER] and [AGENT] when explicit.
- Conflict resolution: compare recorded_at timestamps, use source_message for context. SUPERSEDE older with newer.
</rules>

<example>
Entity: "Derek" [Person], existing_facts: ["Works at Meta"]
[USER] [MSG 3] "Derek just started at Google."
→ {"profiles": [{"canonical_name": "Derek", "facts": [{"content": "Works at Google", "source_entity": null, "source_msg_id": 3, "supersedes": "Works at Meta", "invalidates": null}]}]}
</example>
```

---

### VP-04: Judge Merge
**File**: `merge.md`, heading `## Judge Merge`  
**Original**: 63 lines → **Compressed**: ~25 lines

```markdown
## Judge Merge
Compare two entities. Determine if they are the **exact same real-world object/person**. Default stance: REJECT.

<schema>
{"should_merge": bool, "reasoning": str, "confidence": float, "new_canonical_name": str|null}

- should_merge: true ONLY if evidence is overwhelming
- reasoning: concise justification citing specific facts
- confidence: 0.95+ absolute certainty, 0.75-0.94 high confidence
- new_canonical_name: suggested better name if merging; null if rejecting
</schema>

<rules>
- Type mismatch is fatal: Person ≠ Organization, even if names match.
- Common Name Trap: "Chris" and "Chris" are NOT the same unless specific facts confirm it (last name, job, location). Sparse facts → REJECT.
- Fact contradiction (birthplace, siblings, non-overlapping timelines) → REJECT.
- Progression is NOT contradiction: "Student" (2020) → "Engineer" (2024) is timeline update.
- Identical names but disjoint facts (no overlap, no contradiction) → REJECT (safe side).
- Alias match (Mike vs Michael) + aligned context → ACCEPT.
</rules>

<stakes>
1. False Merge (Catastrophic): Destroys data integrity, causes hallucinations.
2. Missed Merge (Benign): Duplicates are acceptable. Can be linked later.
3. If 99% sure, merge. If 90% sure, REJECT.
</stakes>

<example>
Entity A: Mike [Person], Facts: ["works at Google"]
Entity B: Michael [Person], Facts: ["engineer at Google"]
→ {"should_merge": true, "reasoning": "Mike/Michael are common aliases, both share workplace and profession.", "confidence": 0.96, "new_canonical_name": "Michael"}
</example>
```

---

### VP-05: Judge Contradiction
**File**: `refinement.md`, heading `## Judge Contradiction`  
**Original**: 35 lines → **Compressed**: ~15 lines

```markdown
## Judge Contradiction
For each numbered pair, determine if FACT_B contradicts or supersedes FACT_A.

<rules>
- Contradiction: FACT_B replaces same quality/state. "Works at Google" → "Works at Meta" (employer changed). "Has 2 kids" → "Has 3 kids" (count updated).
- NOT contradiction: Sequential events ("Saw tryout flyer" → "Played in game"), different aspects ("Works at Google" → "Lives in SF"), additive ("Engineer" → "Senior Engineer").
</rules>

<schema>
{"judgments": [{"index": int, "is_contradiction": bool}]}

- index: 1-based, matching the input pair number
- Return exactly one judgment per input pair. Do not invent indexes.
- If no pairs contradict, return all indexes with is_contradiction=false.
</schema>
```

---

### Judge Relevance
**File**: `refinement.md`, heading `## Judge Relevance`  
**Original**: 9 lines → **Compressed**: ~5 lines (already minimal)

```markdown
## Judge Relevance
For every numbered pair of a message and entity facts, decide whether the message is meaningfully related to those facts.

Return exactly one judgment per supplied index. Do not invent indexes.
Prefer false when the relationship is weak, generic, or based only on a shared word.
```

---

## Implementation Instructions

### Step 1: Replace prompt content in source files

Each compressed prompt replaces the corresponding `## ` section in its source file. The `## ` heading text MUST remain identical—`load_pipeline_prompt()` in `prompt_loader.py` splits on these headings to find the right section.

| Compressed Prompt | Target File | Heading to Replace |
|:---|:---|:---|
| VP-01 | `src/common/templates/prompts/extraction.md` | `## Extract Entities` |
| VP-02 | `src/common/templates/prompts/extraction.md` | `## Extract Relationships` |
| VP-03 | `src/common/templates/prompts/refinement.md` | `## Extract Facts` |
| VP-04 | `src/common/templates/prompts/merge.md` | `## Judge Merge` |
| VP-05 | `src/common/templates/prompts/refinement.md` | `## Judge Contradiction` |
| Relevance | `src/common/templates/prompts/refinement.md` | `## Judge Relevance` |

### Step 2: Verify `{user_name}` placeholder survives

The prompts use `{user_name}` as a placeholder that gets replaced at runtime via `prompt.replace("{user_name}", user_name)` in `processor.py` and `settings.py`. Ensure the compressed prompts still contain `{user_name}` in the right places.

### Step 3: Behavioral changes to note

Two extraction rules were intentionally changed (not just compressed):

1. **Relational titles now extractable**: "Mom", "CEO", "my boss" etc. are no longer filtered as generic nouns. They are contextually significant entities. This is a deliberate scope expansion for the upcoming document extraction feature.

2. **Ubiquity filter broadened**: Changed from "non-consumer relationship" exception to "contextually significant" exception. This means "I always write at Starbucks" would now extract Starbucks, whereas before it wouldn't (Starbucks isn't a "non-consumer relationship").

---

## Testing Guidance

### Recommended approach: One prompt at a time

1. Replace VP-01 (Entity Extraction) first — it's the highest-volume prompt
2. Run a known batch of messages through the pipeline
3. Compare extraction output against the original prompt's output
4. If quality holds, move to VP-02, then VP-03
5. VP-04 and VP-05 are lower-frequency (merge/contradiction judging), lower risk

### What to watch for

- **False negatives on relational titles**: "Mom" and "my boss" should now be extracted. If they aren't, the rule isn't being followed.
- **False positives on brands**: Starbucks in "I grabbed coffee from Starbucks" should still be skipped. Starbucks in "I write my screenplay at Starbucks every day" should be extracted.
- **Schema compliance**: The compressed `<schema>` blocks use inline comments. Verify the LLM still produces valid JSON matching the expected Pydantic models (`NERResult`, `ConnectionsResult`, `FactExtractionResult`, etc.).

---

## Token Savings Summary

| Prompt | Original Lines | Compressed Lines | Estimated Savings |
|:---|:---:|:---:|:---:|
| VP-01 Extract Entities | 75 | ~30 | ~60% |
| VP-02 Extract Relationships | 73 | ~30 | ~59% |
| VP-03 Extract Facts | 73 | ~30 | ~59% |
| VP-04 Judge Merge | 63 | ~25 | ~60% |
| VP-05 Judge Contradiction | 35 | ~15 | ~57% |
| Judge Relevance | 9 | ~5 | ~44% |

**Estimated total savings across all prompts: ~58% fewer instruction tokens per extraction call.**
