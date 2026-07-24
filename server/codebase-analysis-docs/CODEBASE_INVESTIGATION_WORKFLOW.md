# Codebase Investigation Workflow

This is a repeatable way to understand and improve a subsystem without making
premature changes. It is intended for working through one folder or a small set
of related files at a time.

## 1. Establish Scope

Start with a deliberately small area: one file, a related group of files, or a
single workflow. Do not scan the entire repository unless the change genuinely
crosses those boundaries.

Identify:

- the public entry points and callers;
- direct dependencies and data contracts;
- persistence, queue, cache, and LLM boundaries;
- relevant tests;
- adjacent modules that supply configuration or consume the result.

The goal is to understand why a file exists and what it owns before discussing
whether its implementation is good.

## 2. Read Behavior Before Proposing Changes

Go method by method, starting at the main entry point. For each method, answer:

- What state does it receive and produce?
- What part of the class or workflow does it support?
- What external systems does it call?
- What happens on an expected failure?
- What happens on an unexpected failure?
- Is it a policy decision, orchestration, transformation, or persistence work?

Follow important calls into neighboring files only far enough to verify the
contract. Avoid treating a name or comment as proof of behavior.

## 3. Separate Facts From Product Decisions

First describe what the code does today. Then identify where the behavior is a
product decision rather than a technical necessity.

Examples include:

- whether uncertain matching should reuse data or create a new record;
- whether a background job is volume-triggered, time-triggered, or manual;
- when an LLM may advise versus make an authoritative decision;
- whether a default topic or fallback should create data;
- whether a failure should retry, skip, park work, or stop the workflow.

Resolve these decisions explicitly before implementation. Do not encode an
assumption as fallback logic merely because it might preserve old behavior.

## 4. Look For Unnecessary Complexity

Treat the following as investigation prompts, not automatic defects:

- constructor parameters that duplicate an existing settings object;
- optional prompt fields when prompts are source-controlled;
- compatibility paths for an unreleased system;
- defaults that silently create data or broaden behavior;
- duplicate logging, events, and structured issues for one condition;
- helpers with no callers;
- state fields that are never read;
- fallback logic that hides an internal consistency problem;
- long methods that mix selection, transformation, persistence, and reporting.

The preferred outcome is fewer concepts and clearer ownership, not abstraction
for its own sake.

## 5. Define Failure and Observability Policy

Classify each failure path before changing it:

- Important state transition: emit an event.
- Expected, recoverable domain or validation problem: record a structured issue.
- Unexpected infrastructure or internal failure: log it, retry or route it as
  appropriate, and preserve work when necessary.

Avoid logging and recording the same expected condition unless they serve
different audiences. Avoid emitting high-volume noise that does not help an
operator understand the system.

## 6. Make a Narrow Implementation Plan

Before editing, list the behavioral changes, files affected, and tests needed.
Keep the plan concrete:

- remove or replace obsolete API/configuration surface;
- extract a helper only when it clarifies one responsibility;
- update construction and configuration fan-out together;
- update tests to the new contract rather than preserving removed APIs;
- leave unrelated refactors alone.

For unreleased code, prefer a clean contract over backward compatibility.

## 7. Implement in Coherent Slices

Make one related change at a time. Typical slices are:

1. Change the source-of-truth contract, such as a settings object or prompt
   source.
2. Update constructors and direct call sites.
3. Remove obsolete state, fallback paths, or configuration fields.
4. Update focused tests for the new intended behavior.
5. Run verification before moving to the next concern.

Do not combine a behavioral change with unrelated formatting or architecture
work unless the two are necessary to make the behavior understandable.

## 8. Verify Proportionally

Verification should match the risk and reach of the change:

- run the closest unit or contract tests first;
- run workflow or subsystem tests when data crosses module boundaries;
- run configuration tests when settings or construction changes;
- run lint on the changed files;
- run `git diff --check` before handoff.

If a broader check fails for an unrelated pre-existing issue, state that
clearly. Do not silently claim a full clean run.

## 9. Close Each File Cleanly

Before moving on, summarize:

- what the class or module owns;
- the decisions now reflected in code;
- any remaining known risks or intentionally deferred work;
- tests and checks that passed;
- the next natural area to inspect.

This keeps later work grounded in the decisions already made instead of
reopening the same questions from scratch.

## Guiding Principles

- Read code before designing around it.
- Prefer explicit policy over accidental behavior.
- Prefer deterministic decisions before LLM assistance.
- Treat LLM output as advisory unless the workflow explicitly makes it
  authoritative.
- Prefer false splits to false merges when identity or data integrity is at
  risk.
- Use settings as the single source of truth for tunable operational behavior.
- Keep prompts source-controlled when they are system behavior, not user-owned
  runtime content.
- Preserve failed work when retry or manual inspection is meaningful.
- Keep observability intentional and proportionate.
- Favor removing obsolete code over carrying compatibility paths in an
  unreleased system.
