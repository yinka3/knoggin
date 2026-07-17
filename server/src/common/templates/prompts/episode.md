## Generate Episode
You are creating bounded episodic memory for {user_name}'s conversation.

<task>
Given one eligible message window, choose exactly one action:
- `create`: create a new episode from this window;
- `consolidate`: merge this window into one supplied prior episode;
- `skip`: this window has too little durable meaning to store.
</task>

<grounding>
- Messages are canonical evidence. The supplied `message_id` values are local
  `mN` references for this call; use only those references.
- `entity_refs_by_message` and `relationship_refs_by_message` are the complete
  available graph memberships for this window. They use local `eN` and `rN`
  references; never invent references.
- `entity_catalog` resolves `eN` references to canonical names, semantic types,
  and aliases. `relationship_catalog` supplies each `rN` relationship type,
  endpoint entities, confidence, context, and `mN` evidence references. Use
  this context to interpret the messages, but keep all selected references
  within the memberships.
- You may select at most two focus entities and only from the supplied entity
  memberships. You may select central relationships only from the supplied
  relationship memberships.
- For `consolidate`, `target_episode_id` must be one of the local `epN`
  references in `prior_episodes`.
</grounding>

<decision_rules>
- Choose `consolidate` only when a supplied prior episode is clearly the same
  continuing topic and the resulting episode remains coherent.
- Choose `create` for a meaningful new topic, decision, development, or
  unresolved thread.
- Choose `skip` for acknowledgements, filler, or other low-signal windows.
- Do not phrase the summary as permanent atomic claims. Write a concise,
  contextual account grounded in the window.
</decision_rules>

<output_contract>
Return exactly the structured response requested by the schema.

For `create` and `consolidate`:
- provide `summary` and exactly one `message_influences` item for every input
  `mN` message reference;
- provide weights greater than or equal to zero;
- omit `skip_reason`.

For `create`:
- omit `target_episode_id`.

For `consolidate`:
- provide `target_episode_id` from the supplied local `epN` prior-episode
  references.

For `skip`:
- provide only `action: "skip"` and `skip_reason`; omit all episode content.
</output_contract>

## Regenerate Consolidated Episode
You are regenerating one existing episodic memory for {user_name}'s conversation.

<task>
The selected target episode and all of its source messages, plus one new eligible
window, have been supplied as one complete source set. Regenerate the current
episode narrative and rank every supplied source message by influence.
</task>

<grounding>
- Messages are canonical evidence. Use every supplied local `mN` message
  reference exactly once in `message_influences`; do not invent references.
- Entity and relationship memberships are closed sets. Focus `eN` entities and
  central `rN` relationships must come only from the supplied memberships.
- Use `entity_catalog` and `relationship_catalog` to interpret the resolved
  entities, relationship types, endpoints, and evidence. They do not authorize
  references outside the supplied memberships.
- The output updates the supplied target episode. Do not choose an action or a
  different target.
</grounding>

<output_contract>
Return exactly the structured response requested by the schema:

- provide a concise contextual `summary`, `new_developments`, `updates`,
  `unresolved`, and `importance`;
- provide exactly one `message_influences` item for every supplied source message;
- select at most two focus entities and only supplied central relationships.
</output_contract>
