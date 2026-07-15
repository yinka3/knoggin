## Generate Episode
You are creating bounded episodic memory for {user_name}'s conversation.

<task>
Given one eligible message window, choose exactly one action:
- `create`: create a new episode from this window;
- `consolidate`: merge this window into one supplied prior episode;
- `skip`: this window has too little durable meaning to store.
</task>

<grounding>
- Messages are canonical evidence. Use only their supplied message IDs.
- `entity_ids_by_message` and `relationship_ids_by_message` are the complete
  available graph memberships for this window. Never invent IDs.
- You may select at most two focus entities and only from the supplied entity
  memberships. You may select central relationships only from the supplied
  relationship memberships.
- For `consolidate`, `target_episode_id` must be one of `prior_episodes`.
</grounding>

<decision_rules>
- Choose `consolidate` only when a supplied prior episode is clearly the same
  continuing topic and the resulting episode remains coherent.
- Choose `create` for a meaningful new topic, decision, development, or
  unresolved thread.
- Choose `skip` for acknowledgements, filler, or other low-signal windows.
- Do not phrase the summary as permanent atomic facts. Write a concise,
  contextual account grounded in the window.
</decision_rules>

<output_contract>
Return exactly the structured response requested by the schema.

For `create` and `consolidate`:
- provide `summary` and exactly one `message_influences` item for every input
  message ID;
- provide weights greater than or equal to zero;
- omit `skip_reason`.

For `create`:
- omit `target_episode_id`.

For `consolidate`:
- provide `target_episode_id` from the supplied prior episodes.

For `skip`:
- provide only `action: "skip"` and `skip_reason`; omit all episode content.
</output_contract>
