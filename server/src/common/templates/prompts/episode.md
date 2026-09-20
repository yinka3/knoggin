## Generate Episode
You are creating bounded, project-wide episodic memory for {user_name}.

<task>
Given one eligible project window, return zero to three independent new-episode
proposals. Ungrouped material may be omitted; an empty proposal list is valid.
Keep the combined narrative text in every proposal at or below
{prompt_narrative_chars} characters. The server hard limit is
{max_narrative_chars} characters.
Each proposal may use at most {max_episode_source_messages} source messages and
at most {max_episode_source_tokens} estimated source tokens.
</task>

<grounding>
- The evidence brief is a server-defined catalog. `message:N` entries are the
  only valid references; they are local to this response and never database IDs.
- Session boundaries and the supplied pairing/topic hints are evidence aids,
  not mandatory groups. Decide coherence yourself.
- Every proposal is final for its own selected source messages. Do not revise,
  merge, or refer to an earlier episode.
</grounding>

<decision_rules>
- Create a proposal for a meaningful new topic, decision, development, or
  unresolved thread.
- Do not create a proposal for acknowledgements, filler, or low-signal material.
- Proposals may not share any `message:N` source. Use every selected source
  exactly once in its proposal.
- An `ASSISTANT CLARIFICATION (UNRESOLVED QUESTION)` is unanswered. It may
  support an unresolved thread, but never treat the question or its implied
  premise as an established fact.
- Do not phrase the summary as permanent atomic claims. Write a concise,
  contextual account grounded in the window.
</decision_rules>

<output_contract>
Return exactly the structured response requested by the schema: an array named
`proposals`, containing at most three new-episode proposals.

For every proposal:
- provide `summary` and exactly one `message_influences` item for every
  `message:N` assigned to that proposal, and no unassigned `message:N`
  references;
- keep the proposal within both source limits above.

Do not emit individual skip proposals; use an empty `proposals` array when the
window has no episodic memory to retain.
</output_contract>

## Repair Episode Narrative
You are repairing a proposed episodic-memory response for {user_name}.

<task>
The evidence brief and a readable draft are supplied. Return an equivalent
structured response whose combined narrative text in each proposal is at most
{max_narrative_chars} characters.
</task>

<grounding>
- Preserve the source references for every proposal.
- Compress prose and remove lower-value list items before altering the summary.
- Do not invent references or create additional proposals.
</grounding>

<output_contract>
Return exactly the structured response requested by the schema:

- meet the character limit exactly; the server will reject another overage;
- preserve the structured proposal shape and its existing references.
</output_contract>
