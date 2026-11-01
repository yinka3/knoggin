# Imported history and live steering

The server and benchmark changes below are implemented. The larger benchmark ingestion still needs fresh databases. Client UX is deferred.

## Implemented: assistant-only history import

- Extend Knoggin's import contract to accept an assistant message without a preceding user message. Preserve its role, content, timestamp, session, and import retry identity. Do not create a fake user message.
- Make canonical message storage, semantic admission, frozen window membership, evidence reads, and downstream extraction accept that standalone assistant turn. Keep paired and user-only turns working.
- Update LongMemEval's loader to preserve leading assistant turns in order. Fail clearly if the server cannot import one; never silently discard it.
- Verify a mixed history with paired turns, consecutive user turns, and assistant-only turns through import, semantic processing, and retrieval. Test interruption and repeat import.
- Use fresh benchmark databases for the larger ingestion. The prior Haiku and Gemini scores omitted 95 leading assistant turns among the 24 scored questions.

## Implemented: multiple steers during one live run

- Persist each steer as a distinct ordered user message with a durable pending state. Keep the active run separate from queued follow-ups.
- At each safe model-step boundary, deliver all pending steers in order and report which steer IDs were applied. A model call already in progress cannot be interrupted.
- If the run ends first, mark remaining steers not applied. Do not silently claim delivery or automatically turn them into follow-ups.
- Ensure applied steer content appears in later conversation and semantic memory without duplicating or rewriting the original user message.
- Verify multiple arrivals during a model call, arrivals near finalization, cancellation, restart/recovery, ordering, and the one-answer-per-run lifecycle. The focused server, Postgres, benchmark, and API checks cover these paths.

## Deferred client UX

- Show each steer as **Pending**, **Applied**, or **Not applied** using server state. Streaming text is not a final answer; the terminal response ends the run.
- For a steer not applied before finalization, offer **Send as follow-up**. Keep a deliberate follow-up visually distinct because it starts a new user turn and receives its own reply.
- Show a wait message only when a bounded pending queue is full. Do not rely on a timer to decide whether the agent saw a steer.
