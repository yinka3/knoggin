## Generate Episode
You are creating bounded, project-wide episodic memory for {user_name}.

<task>
Given one eligible project window, return zero to three independent proposals.
Each proposal either creates a new episode or revises one supplied prior episode.
Ungrouped material may be omitted; an empty proposal list is valid.
Keep the combined narrative text in every proposal at or below
{prompt_narrative_chars} characters. The server hard limit is
{max_narrative_chars} characters.
</task>

<grounding>
- The evidence brief is a server-defined catalog. `message:N` and `episode:N`
  are the only valid references; they are
  local to this response and never database IDs.
- Session boundaries and the supplied pairing/topic hints are evidence aids,
  not mandatory groups. Decide coherence yourself.
- A revision target must be one of the supplied `episode:N` prior episodes.
</grounding>

<decision_rules>
- Choose `consolidate` only when a supplied prior episode is clearly the same
  continuing topic and the resulting episode remains coherent.
- Choose `create` for a meaningful new topic, decision, development, or
  unresolved thread.
- Do not create a proposal for acknowledgements, filler, or low-signal material.
- Proposals may not share any current-window `message:N` source. Each consolidation
  target may occur at most once. Do not merge two existing episodes.
- Do not phrase the summary as permanent atomic claims. Write a concise,
  contextual account grounded in the window.
</decision_rules>

<output_contract>
Return exactly the structured response requested by the schema: an array named
`proposals`, containing at most three create/consolidate proposals.

For `create` and `consolidate`:
- provide `summary` and exactly one `message_influences` item for every
  `message:N` assigned to that proposal, and no unassigned `message:N`
  references;
- omit `skip_reason`.

For `create`:
- omit `target_episode_id`.

For `consolidate`:
- provide `target_episode_id` from the supplied local `episode:N` prior-episode
  references.

Do not emit individual `skip` proposals.
</output_contract>

## Repair Episode Narrative
You are repairing a proposed episodic-memory response for {user_name}.

<task>
The evidence brief and a readable draft are supplied. Return an equivalent
structured response whose combined narrative text in each proposal is at most
{max_narrative_chars} characters.
</task>

<grounding>
- Preserve the proposal actions, source references, and consolidation targets.
- Compress prose and remove lower-value list items before altering the summary.
- Do not invent references or create additional proposals.
</grounding>

<output_contract>
Return exactly the structured response requested by the schema:

- meet the character limit exactly; the server will reject another overage;
- preserve the structured proposal shape and its existing references.
</output_contract>

## Consolidate Episode
You are deciding whether one prior Episode remains coherent after new source
evidence was added for {user_name}.

<task>
The supplied evidence catalog contains every canonical source message from the
prior Episode and the new completed units. Regenerate the narrative from that
complete evidence. Return `consolidate` only when all supplied evidence forms
one coherent Episode; otherwise return `keep_separate`.
</task>

<grounding>
- `message:N` references are local handles for the complete canonical packet.
- A successful consolidation must reference every supplied message exactly
  once. The server owns source ordering and memberships.
- `keep_separate` preserves the prior Episode and lets the new units become a
  separate Episode.
</grounding>

<output_contract>
Return the structured response requested by the schema. For `consolidate`,
provide the bounded narrative and every supplied `message:N` reference. For
`keep_separate`, omit narrative fields and message references.
</output_contract>
