# Token Consumption Analysis

**Status**: Working analysis  
**Updated**: 2026-07-03  
**Scope**: Ingestion-side LLM inputs only. Outputs are intentionally out of scope for this layer.

---

## Goal

Measure the tokens Knoggin sends into ingestion LLM calls.

The input side is:

```text
system prompt
+ formatted runtime input
+ retrieved/candidate context
+ schema/topic/config text included before the request
```

Do not estimate token counts by hand. Build representative prompt/input
fixtures, then count them with `scripts/count_prompt_tokens.py`.

---

## Current Pricing Reference

Prices below are public API prices per 1M tokens in USD, gathered from official
pricing pages on 2026-07-03.

These are included to support later cost calculations after input token counts
are measured. This document still focuses on input tokens first.

| Tier | Model | Provider | Input | Output | Pricing basis |
|---|---|---|---:|---:|---|
| You have money | `gpt-5.5-pro` | OpenAI | $30.00 | $180.00 | Standard, short context |
| You have money | `claude-fable-5` | Anthropic | $10.00 | $50.00 | First-party Claude API, standard |
| Strong | `gpt-5.5` | OpenAI | $5.00 | $30.00 | Standard, short context |
| Strong | `claude-opus-4.8` | Anthropic | $5.00 | $25.00 | First-party Claude API, standard |
| Medium | `claude-sonnet-5` | Anthropic | $2.00 | $10.00 | Introductory price through 2026-08-31 |
| Medium | `gemini-3.1-pro-preview` | Google | $2.00 | $12.00 | Standard paid tier, prompts <= 200k tokens |
| Low | `gemini-3.1-flash-lite` | Google | $0.25 | $1.50 | Standard paid tier, text/image/video input |
| Low | `gpt-5.4-nano` | OpenAI | $0.20 | $1.25 | Standard, short context |

Source notes:

- OpenAI pricing page lists flagship model prices per 1M tokens and separates
  Standard, Batch, Flex, and Priority pricing. The table above uses Standard
  short-context pricing for OpenAI models.
- Anthropic pricing page lists model pricing in MTok. The table above uses base
  input and output pricing, not cache-write, cache-hit, batch, or data-residency
  pricing.
- Google Gemini pricing page lists paid-tier per-1M-token pricing by model and
  mode. The table above uses Standard pricing for text inputs.

Pricing source URLs:

- <https://platform.openai.com/docs/pricing>
- <https://docs.anthropic.com/en/docs/about-claude/pricing>
- <https://ai.google.dev/gemini-api/docs/pricing>

---

## Ingestion LLM Input Surfaces

Known ingestion-side structured LLM calls:

| Stage | Prompt family | Main runtime input |
|---|---|---|
| Entity extraction | `extract_entities` | message/source batch, known entities, GLiNER spans, ambiguous candidates, topic labels |
| Fact relevance | `judge_relevance` | message/source text plus candidate entity facts |
| Relationship extraction | `extract_relationships` | resolved entity candidates, message/source batch, session context |
| User profile extraction | `extract_facts` | one user profile, aliases, existing facts, recent conversation |
| Entity profile extraction | `extract_facts` | entity batch, aliases, existing facts, recent conversation |
| Contradiction judgment | `judge_contradiction` | existing fact vs new fact pairs |

All measurements should count the final strings passed as `system=` and `user=`
to `LLMService.generate_structured(...)`.

---

## User Cases For Input Fixtures

Use these cases to build realistic input fixtures. They are not token estimates.

### Case 1: Casual User

Typical behavior:

```text
Frequency: a few times per week
Message style: short, direct
Memory density: low to moderate
Correction rate: low
```

Representative source examples:

```text
Remember Maya owns the design review.
I moved the dentist appointment to Friday.
Use Notion for project notes.
```

Fixture shape:

| Input surface | Fixture size |
|---|---:|
| Entity extraction | 4-8 short messages |
| Relationship extraction | 5-20 resolved entities |
| Fact relevance | 0-8 message/fact pairs |
| Profile extraction | 3-8 dirty entities |
| Contradiction judgment | 0-4 fact pairs |

### Case 2: Daily Conversational User

Typical behavior:

```text
Frequency: daily
Message style: medium-to-long, evolving thoughts
Memory density: dense but manageable
Correction rate: medium
```

Representative source examples:

```text
I think source events should be the durable record, not document chunks.
Actually, do not add source ranking yet. Corrections should happen conversationally.
Let's keep source types to message, doc, and webhook for now.
```

Fixture shape:

| Input surface | Fixture size |
|---|---:|
| Entity extraction | 8-16 medium messages |
| Relationship extraction | 20-60 resolved entities |
| Fact relevance | 15-40 message/fact pairs |
| Profile extraction | 12-24 dirty entities |
| Profile conversation window | 60-100 recent messages |
| Existing facts per entity | 75-125 max |
| Contradiction judgment | 8-24 fact pairs |

### Case 3: Heavy / Burst User

Typical behavior:

```text
Frequency: high and bursty
Message style: many small updates plus occasional long synthesis
Memory density: high and interconnected
Correction rate: medium to high
```

Representative source examples:

```text
Drop source ranking.
Use source_events.
Webhook should be broad.
Actually make sessions the process boundary.
Here is the full ingestion model...
```

Fixture shape:

| Input surface | Fixture size |
|---|---:|
| Interactive entity extraction | 8-12 messages |
| Background source extraction | 20-40 source records |
| Relationship extraction | 50-150 resolved entities |
| Fact relevance | 50-100 message/fact pairs |
| Profile extraction | 24-50 dirty entities |
| Profile source window | 100-200 recent source records |
| Existing facts per entity | 150-300 max |
| Contradiction judgment | 25-80 fact pairs |

---

## Token Counting Utility

Use:

```bash
uv run python scripts/count_prompt_tokens.py path/to/fixture.json
```

Generate the sample system/user input fixtures:

```bash
python3 scripts/generate_token_fixtures.py
```

Count a directory of fixtures:

```bash
python3 scripts/count_prompt_tokens.py --recursive codebase-analysis-docs/token-fixtures
```

Use a tiktoken model or explicit encoding:

```bash
uv run python scripts/count_prompt_tokens.py --model gpt-4o path/to/fixture.json
uv run python scripts/count_prompt_tokens.py --encoding o200k_base path/to/fixture.txt
```

Emit JSON:

```bash
uv run python scripts/count_prompt_tokens.py --json path/to/fixture.json
```

The utility supports:

- plain text files;
- JSON objects with `system` and `user` fields;
- JSON objects with a `messages` list;
- JSONL files;
- directories containing text-like fixture files.

Important limitation:

```text
The script counts visible text only.
```

It does not infer hidden provider chat-envelope tokens, provider-specific
special tokens, tool schema overhead, or non-tiktoken tokenizer differences.
For this phase, that is intentional.

---

## Fixture Format

Sample fixtures live under:

```text
codebase-analysis-docs/token-fixtures/
```

They are generated by:

```text
scripts/generate_token_fixtures.py
```

The generator creates one `system` + `user` JSON file for each ingestion LLM
surface and user case. The message samples intentionally include mixed-width
messages: short commands, medium updates, longer design notes, and rambling
synthesis messages.

Preferred fixture format:

```json
{
  "system": "rendered system prompt here",
  "user": "rendered runtime input here"
}
```

Alternative chat-style format:

```json
{
  "messages": [
    {
      "role": "system",
      "content": "rendered system prompt here"
    },
    {
      "role": "user",
      "content": "rendered runtime input here"
    }
  ]
}
```

For each ingestion stage, generate one fixture per user case:

```text
casual/entity_extraction.json
casual/relationship_extraction.json
casual/profile_extraction.json
daily/entity_extraction.json
daily/relationship_extraction.json
daily/profile_extraction.json
heavy/entity_extraction.json
heavy/relationship_extraction.json
heavy/profile_extraction.json
```

Do not fill these with invented measurements. First render realistic inputs,
then run the counter.

---

## Next Layer

After fixtures exist and are counted, add a measured table:

```text
stage
case
system_tokens
user_tokens
total_input_tokens
model_price_per_1m_input
estimated_input_cost
```

That calculation should use actual counter output, not manual estimates.
