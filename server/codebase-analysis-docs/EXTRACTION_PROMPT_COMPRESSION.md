# Extraction Prompt Compression

Status: proposal, not yet applied.

The live extraction prompts are still the verbose Markdown sections in:

- `src/common/templates/prompts/extraction.md`
- `src/common/templates/prompts/refinement.md`
- `src/common/templates/prompts/merge.md`

They are loaded by `src/common/utils/prompt_loader.py` through named `## `
sections declared in `PIPELINE_PROMPTS`. Do not rename these headings without
updating the loader contract and tests.

## Goal

Reduce repeated instruction tokens in the extraction pipeline while preserving
schema shape and extraction behavior.

High-value targets:

- remove persona/name filler such as `You are VEGAPUNK-*`;
- merge duplicated schema and output-format sections;
- keep examples and calibration rules that affect extraction quality;
- keep required placeholders, especially `{user_name}`;
- keep strict JSON shape requirements aligned with the Pydantic result models.

## Prompt Sections

- `extraction.md#Extract Entities`
- `extraction.md#Extract Relationships`
- `refinement.md#Extract Facts`
- `refinement.md#Judge Contradiction`
- `refinement.md#Judge Relevance`
- `merge.md#Judge Merge`

## Suggested Approach

Apply compression one section at a time and compare outputs on known message
batches before moving to the next section. Entity extraction has the highest
volume and should be tested first.

Watch for:

- schema compliance regressions;
- missed proper nouns or relational titles;
- false positive brand/tool entities;
- relationship extraction drifting from explicit evidence into co-mention;
- contradiction and merge judges becoming less conservative.

## Proposed Behavior Changes

Two possible changes were identified but are not applied yet:

- allow contextually significant relational titles such as `Mom`, `CEO`, or
  `my boss` to be extracted as entities;
- broaden the brand/tool filter from "non-consumer relationship only" to
  "contextually significant."

Treat these as behavior changes, not pure compression. They need test coverage
and product agreement before landing.
