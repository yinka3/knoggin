# Data Structure Improvements

Analysis of areas in the codebase where better data structures can improve performance, correctness, or unlock new capabilities like programmatic context assembly.

---

## Identified Areas

### 1. `AgentState.previous_calls` — Bounded Dedup

**File:** `server/src/core/agent/types.py`
**Current:** `Set[Tuple[str, str]]` — grows for the life of a run, every call ever made is remembered equally.
**Better:** A `dict` keyed by call signature, capped at N entries, with oldest-first eviction (insertion order is free in Python 3.7+ dicts).

**You gain:** The agent stops refusing to re-run a tool it called 10 turns ago when context has genuinely changed. A stale hit from early in the run no longer blocks a valid retry late in the run.

**You give up:** The absolute guarantee that the exact same call never fires twice in a run. In practice the agent's `max_calls` budget makes this a non-issue — the dedup window would be capped well below `max_calls`.

**Worth it?** Yes, but it's a behaviour change, not just a perf change. Needs a deliberate decision on window size.

---

### 2. `RetrievedEvidence` — Keyed Dedup + Token Budget

**File:** `server/src/core/agent/types.py`
**Current:** Six `List[Dict]` buckets — appended to freely, no uniqueness guarantees across tool calls.
**Better:** `dict[id, Dict]` per bucket (e.g. keyed by `entity_id` for graph, `message_id` for messages). The `token_count` field already exists on the struct but is unused for trimming.

**You gain:** The prompt renderer stops seeing the same entity or message twice when two tools return overlapping results. Token budget becomes enforceable — lowest-priority items can be trimmed before they hit the context window.

**You give up:** Ordering is no longer implicitly "order tools were called." An explicit score or insertion timestamp would be needed if ordering matters to the renderer.

**Worth it?** High value — directly affects prompt quality and token spend. The `token_count` field being there but unused is a signal this was already planned for.

---

### 3. `EntityManager` — Unified Index

**File:** `server/src/core/knowledge/services/entity_service.py`
**Current:** Three separate `LRUCache` objects (`entity_profiles`, `_name_to_id`, `_id_to_names`) all mutated together under one `RLock`. Eviction from one cache leaves stale pointers in the others.
**Better:** A single `EntityIndex` class that owns all three views and exposes only mutation methods (`register`, `merge`, `remove`). Internally it keeps them coherent. The `RLock` stays but is encapsulated.

**You gain:** The coherence bug is structurally impossible — you can't update one view without the others. Also becomes unit-testable in isolation.

**You give up:** A refactor with meaningful blast radius. Every direct access to `entity_profiles[x]`, `_name_to_id[x]` would need updating. It's not a data structure swap, it's a small encapsulation refactor.

**Worth it?** Yes, but this is the highest-effort item. The current code works — the risk is latent, not actively triggering. Best done alongside future `EntityManager` feature work rather than as a standalone change.

---

### 4. `AgentContext.hot_topic_context` — Priority-Scored Context

**File:** `server/src/core/agent/types.py`
**Current:** `Dict[str, Dict]` — flat, loaded once at agent startup, no ranking.
**Better:** A scored structure like `list[tuple[float, str, Dict]]` (score, topic_name, context) that can be sorted and truncated to a token budget. Score could be cosine similarity between the query embedding and the topic, recency of mentions, or both.

**You gain:** This unlocks programmatic context assembly. Instead of always including all hot topics, the most relevant ones for the current query are included. That's a fundamentally different — and better — agent context model.

**You give up:** A score is needed at load time or query time. If query-time, a small vector comparison step is added before every agent run. If load-time, ranking is static.

**Worth it?** This is the most strategically interesting one. The tradeoff is real — a scoring step is added — but the payoff is that context assembly becomes a first-class operation rather than "dump everything and hope it fits."

---

### 5 & 9. `AgentRunConfig.tool_limits` — Dict Materialised Once

**Files:** `server/src/core/agent/types.py`, `server/src/core/community/community_manager.py`
**Current:** Frozen tuple of tuples, converted to a `dict` inside `get_tool_limit()` on every call. Happens in two places (default config and `COMMUNITY_RUN_CONFIG`).
**Better:** `__post_init__` builds the dict once and stores it as a private attribute. Since the class is `frozen=True`, use `object.__setattr__(self, '_limits_dict', ...)`.

**You gain:** Trivially faster — 25-entry dict construction removed from every tool limit check.

**You give up:** Minor awkwardness from using `object.__setattr__` to bypass the frozen constraint in `__post_init__`.

**Worth it?** Low effort, low risk, small gain. Do it if already touching `AgentRunConfig`, otherwise skip.

---

### 6. `SessionManager._session_locks` — `WeakValueDictionary`

**File:** `server/src/core/session/session_manager.py`
**Current:** `Dict[str, asyncio.Lock]` that grows as sessions are created and is never pruned.
**Better:** `WeakValueDictionary` — the lock evicts automatically when no coroutine holds a live reference. Or simply delete from the dict explicitly in `close_session` (two lines, zero complexity).

**You gain:** No unbounded growth in long-lived processes with many sessions.

**You give up:** `WeakValueDictionary` requires a short critical section to avoid two coroutines creating two locks for the same session simultaneously.

**Worth it?** Moderate. The leak is real but slow. The explicit-delete approach is the simpler fix.

---

### 7. `ProjectManager.active_projects` — Eviction Policy

**File:** `server/src/core/project/project_manager.py`
**Current:** Plain `dict`, no eviction. Idle projects are never unloaded.
**Better:** `OrderedDict` with `move_to_end()` on access gives LRU semantics cheaply. Or a `(last_accessed: float, state: ProjectState)` wrapper on the value.

**You gain:** Idle projects can be detected and evicted, freeing `EntityManager` memory and the associated `Scheduler`.

**You give up:** A policy decision is required — when is a project idle enough to evict? Background jobs still running against it must defer eviction. `active_runtime_sessions_count` on `ProjectState` is the natural guard.

**Worth it?** Yes, but the data structure is the trivial part. The actual investment is in the eviction policy logic.

---

### 8. `TextProcessor._build_phrase_matcher` — Versioned Cache

**File:** `server/src/core/ingestion/services/processor.py`
**Current:** Rebuilds the full spaCy `PhraseMatcher` from the alias dict on every call. No caching. The alias dict can hold up to 3M entries.
**Better:** Store `(_alias_version: int, _cached_matcher: PhraseMatcher)` on the instance. Increment the version when aliases change, invalidate the cache on mismatch.

**You gain:** `PhraseMatcher` construction from a 3M-entry dict is not cheap. Cache it and pay the cost once per alias change rather than once per message processed.

**You give up:** A version counter is needed. The natural place is `EntityManager`, which already owns the aliases and fires `emit_sync` on mutations. Wiring a version increment there adds a small coupling between `EntityManager` and `TextProcessor`.

**Worth it?** Yes — this is the most concrete performance win on the list.

---

## Priority Order

| Priority | Item                                 | Reason                                                     |
| -------- | ------------------------------------ | ---------------------------------------------------------- |
| 1        | #8 — `PhraseMatcher` cache           | Concrete perf win per message processed                    |
| 2        | #2 — `RetrievedEvidence` keyed dedup | Direct prompt quality and token spend improvement          |
| 3        | #4 — `hot_topic_context` scoring     | Unlocks programmatic context assembly                      |
| 4        | #6 / #7 — session/project dict leaks | Low effort, real lifecycle correctness                     |
| 5        | #1 — bounded dedup window            | Behaviour change, needs deliberate decision on window size |
| 6        | #3 — `EntityIndex` unification       | Highest effort, latent not active risk                     |
| 7        | #5/9 — `tool_limits` dict            | Micro-optimisation, do it opportunistically                |
