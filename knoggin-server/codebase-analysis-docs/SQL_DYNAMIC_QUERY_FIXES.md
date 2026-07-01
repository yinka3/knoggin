# Dynamic SQL Construction Fixes

Replace all runtime SQL string assembly with `psycopg.sql` composition.
Add this import to every affected file:

```python
from psycopg import sql
```

`psycopg.sql.SQL` is already available — `psycopg[binary]` is a project dependency.

---

## Pattern reference

```python
# Optional clause
clause = sql.SQL("AND foo = %s") if condition else sql.SQL("")

# Dynamic SET columns (safe identifier quoting)
sql.SQL(", ").join(
    sql.SQL("{} = %s").format(sql.Identifier(col)) for col in cols
)

# Dynamic WHERE AND-chain
sql.SQL(" AND ").join(sql.SQL(frag) for frag in fragments)

# Composed full statement
stmt = sql.SQL("SELECT * FROM t WHERE {where}").format(where=...)
```

All `%s` / `%(name)s` parameter placeholders are **unchanged** — only the structural string injection is replaced.

---

## Fixes

### 1. `session_manager.py:265–279`

Dynamic SET columns built from a dict — column names come from the caller.

```python
# BEFORE
updates = []
params = {"user_name": self.user_name, "session_id": session_id}
for k, v in new_data.items():
    updates.append(f"{k} = %({k})s")
    params[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
if not updates:
    return {}
set_clause = ", ".join(updates)
query = f"UPDATE public.sessions SET {set_clause} WHERE user_name = %(user_name)s AND session_id = %(session_id)s"
await self.pg.execute(query, params)

# AFTER
from psycopg import sql

cols = {}
for k, v in new_data.items():
    cols[k] = json.dumps(v) if isinstance(v, (dict, list)) else v
if not cols:
    return {}

stmt = sql.SQL(
    "UPDATE public.sessions SET {fields} WHERE user_name = %s AND session_id = %s"
).format(
    fields=sql.SQL(", ").join(
        sql.SQL("{} = %s").format(sql.Identifier(k)) for k in cols
    )
)
await self.pg.execute(stmt, [*cols.values(), self.user_name, session_id])
```

---

### 2. `project_manager.py:218–251`

Same dynamic SET pattern for `UPDATE public.projects`.

```python
# BEFORE
updates = []
params = {"user_name": self.user_name, "project_id": project_id}
if name is not None:
    updates.append("name = %(name)s")
    params["name"] = name.strip()
if description is not None:
    updates.append("description = %(description)s")
    params["description"] = description
# ... allowed_projects block (unchanged — it does its own queries) ...
if updates:
    updates.append("updated_at = now()")
    set_clause = ", ".join(updates)
    await self.pg.execute(
        f"UPDATE public.projects SET {set_clause} WHERE user_name = %(user_name)s AND project_id = %(project_id)s",
        params,
    )

# AFTER
from psycopg import sql

col_values = {}
if name is not None:
    col_values["name"] = name.strip()
if description is not None:
    col_values["description"] = description

if col_values:
    stmt = sql.SQL(
        "UPDATE public.projects SET {fields}, updated_at = now()"
        " WHERE user_name = %s AND project_id = %s"
    ).format(
        fields=sql.SQL(", ").join(
            sql.SQL("{} = %s").format(sql.Identifier(k)) for k in col_values
        )
    )
    await self.pg.execute(stmt, [*col_values.values(), self.user_name, project_id])
```

---

### 3. `entity_reader.py:163–213` — `list_entities`

Dynamic WHERE clause assembled into an f-string for both count and data queries.

```python
# BEFORE
where_clauses = ["(e.project_id = ANY(%s) OR e.entity_id = %s)"]
params = [visible_project_ids, IDENTITY_ENTITY_ID]
if entity_type:
    where_clauses.append("e.type = %s")
    params.append(entity_type)
if search:
    where_clauses.append("lower(e.canonical_name) LIKE lower(%s)")
    params.append(f"%{search}%")
if topic:
    where_clauses.append("e.topic = %s")
    params.append(topic)
where_str = "WHERE " + " AND ".join(where_clauses) if where_clauses else ""
count_query = f"SELECT count(*) AS total FROM entities e {where_str}"
data_query = f"""
SELECT ...
FROM entities e
LEFT JOIN facts f ...
{where_str}
GROUP BY ...
"""

# AFTER
from psycopg import sql

where_frags = [sql.SQL("(e.project_id = ANY(%s) OR e.entity_id = %s)")]
params = [visible_project_ids, IDENTITY_ENTITY_ID]
if entity_type:
    where_frags.append(sql.SQL("e.type = %s"))
    params.append(entity_type)
if search:
    where_frags.append(sql.SQL("lower(e.canonical_name) LIKE lower(%s)"))
    params.append(f"%{search}%")
if topic:
    where_frags.append(sql.SQL("e.topic = %s"))
    params.append(topic)

where_clause = sql.SQL("WHERE ") + sql.SQL(" AND ").join(where_frags)

count_query = sql.SQL("SELECT count(*) AS total FROM entities e {where}").format(
    where=where_clause
)
data_query = sql.SQL("""
SELECT
    e.entity_id AS id,
    ...
FROM entities e
LEFT JOIN facts f ON f.entity_id = e.entity_id AND f.invalid_at IS NULL AND f.project_id = ANY(%s)
{where}
GROUP BY e.entity_id
ORDER BY e.last_mentioned_ms DESC NULLS LAST
OFFSET %s
LIMIT %s
""").format(where=where_clause)

count_row = await self.client.fetch_one(count_query, tuple(params))
...
entities_res = await self.client.fetch_all(
    data_query, (visible_project_ids, *params, offset, limit)
)
```

---

### 4. `fact_reader.py:76–115` and `117–177` — `get_facts_for_entity` / `get_facts_for_entities`

`active_sql` is a literal string fragment injected via f-string. Both methods use the same pattern.

```python
# BEFORE
active_sql = "AND invalid_at IS NULL" if active_only else ""
query = f"""
SELECT ...
FROM facts
WHERE entity_id = %s
  AND project_id = ANY(%s)
{active_sql}
ORDER BY valid_at DESC, fact_id
"""

# AFTER
from psycopg import sql

active_clause = sql.SQL("AND invalid_at IS NULL") if active_only else sql.SQL("")
query = sql.SQL("""
SELECT ...
FROM facts
WHERE entity_id = %s
  AND project_id = ANY(%s)
{active}
ORDER BY valid_at DESC, fact_id
""").format(active=active_clause)
```

Apply the same change to the `get_facts_for_entities` method (line 132) — identical pattern.

---

### 5. `graph_reader.py:141–198` — `get_recent_project_messages`

Optional `before_clause` injected via f-string. The param tuple is also conditionally built.

```python
# BEFORE
before_clause = "AND message_id <= %s" if before_message_id is not None else ""
query = f"""
SELECT ...
FROM messages
WHERE user_name = %s
AND project_id = %s
{before_clause}
ORDER BY message_id DESC
LIMIT %s
"""
query_params = (
    (params["user_name"], params["project_id"], params["before_message_id"])
    if before_message_id is not None
    else (params["user_name"], params["project_id"])
)
query_params = (*query_params, params["limit"])

# AFTER
from psycopg import sql

before_clause = (
    sql.SQL("AND message_id <= %s") if before_message_id is not None else sql.SQL("")
)
query = sql.SQL("""
SELECT ...
FROM messages
WHERE user_name = %s
  AND project_id = %s
  {before}
ORDER BY message_id DESC
LIMIT %s
""").format(before=before_clause)

query_params = [user_name, project_id]
if before_message_id is not None:
    query_params.append(before_message_id)
query_params.append(limit)
```

---

### 6. `projection_rebuilder.py` — `_fetch_messages`, `_fetch_entities`, `_fetch_relationships`, `_fetch_facts`

All four methods build a `filters` list and join it with `" AND ".join(filters)` inside an f-string. In every case the filters list is **fixed at two entries** — the dynamic builder is unnecessary.

Replace each f-string with a plain static query string. Example for `_fetch_messages`:

```python
# BEFORE
filters = ["project_id = %s"]
params = [project_id]
filters.append("user_name = %s")
params.append(user_name)
await cur.execute(f"""SELECT ... FROM messages WHERE {" AND ".join(filters)} ...""", tuple(params))

# AFTER  (remove filters list entirely)
await cur.execute(
    """
    SELECT ...
    FROM messages
    WHERE project_id = %s
      AND user_name = %s
    ORDER BY user_name, session_id, message_id
    """,
    (project_id, user_name),
)
```

Apply the same simplification to `_fetch_entities`, `_fetch_relationships`, and `_fetch_facts`. Check each filter list before removing — they are all statically two entries.

---

### 7. `core_utils.py:184–195` — `fetch_conversation_turns`

```python
# BEFORE
query = """SELECT ... FROM public.messages WHERE user_name = %(user_name)s AND session_id = %(session_id)s"""
params = {"user_name": user_name, "session_id": session_id, "limit": num_turns}
if up_to_msg_id:
    query += " AND message_id <= %(up_to_msg_id)s "
    params["up_to_msg_id"] = up_to_msg_id
query += " ORDER BY message_id DESC LIMIT %(limit)s "

# AFTER
from psycopg import sql

up_to_clause = (
    sql.SQL("AND message_id <= %s") if up_to_msg_id else sql.SQL("")
)
query = sql.SQL("""
    SELECT message_id, role, content, timestamp_ms as timestamp, user_msg_id, metadata
    FROM public.messages
    WHERE user_name = %s AND session_id = %s
    {up_to}
    ORDER BY message_id DESC LIMIT %s
""").format(up_to=up_to_clause)

params = [user_name, session_id]
if up_to_msg_id:
    params.append(up_to_msg_id)
params.append(num_turns)

rows = await pg_client.fetch_all(query, params)
```

---

### 8. `tool_queries.py` — three methods with optional topic filter + ORDER BY append

All three follow the same pattern: a base query, an optional `AND target.topic = ANY(%s)` appended, then a fixed `ORDER BY ... LIMIT` appended. Fix is the same for all three.

#### `search_entity` (line 305)

```python
# BEFORE
if active_topics:
    entity_sql += " AND e.topic = ANY(%s)"
    params = (..., active_topics)
else:
    params = (...)

# AFTER
from psycopg import sql

topic_clause = (
    sql.SQL("AND e.topic = ANY(%s)") if active_topics else sql.SQL("")
)
entity_query = sql.SQL("""
    SELECT ...
    FROM entities e
    WHERE e.entity_id = ANY(%s)
    {topic}
""").format(topic=topic_clause)

params = [visible_project_ids, visible_project_ids, visible_project_ids, entity_ids]
if active_topics:
    params.append(active_topics)
```

#### `get_related_entities` (lines 499–524)

```python
# BEFORE
if active_topics is not None:
    query += " AND target.topic = ANY(%s)"
    params = (..., active_topics)
else:
    params = (...)
query += " ORDER BY r.weight DESC, r.last_seen_ms DESC LIMIT %s"
params = (*params, limit)

# AFTER
topic_clause = (
    sql.SQL("AND target.topic = ANY(%s)") if active_topics is not None else sql.SQL("")
)
query = sql.SQL("""
    SELECT ...
    {topic}
    ORDER BY r.weight DESC, r.last_seen_ms DESC LIMIT %s
""").format(topic=topic_clause)

params = [visible_project_ids, entity_names, ...]  # existing fixed params
if active_topics is not None:
    params.append(active_topics)
params.append(limit)
```

#### `get_recent_activity` (lines 590–613)

Same pattern as `get_related_entities`. Apply identically.

---

### 9. `document_service.py` — four query-building methods

All four use the same `query += " AND ..."` / `sql += " AND ..."` pattern followed by a fixed suffix appended at the end. Use `psycopg.sql` to compose the optional clauses and use a `sql.SQL.join` for the suffix.

The fix structure is the same for all four. Shown once in full for `_list_folder_documents` (line 2063); apply the same structure to:
- `list_folder_uploads` (~line 1935)
- `list_documents` (~line 2225)  
- `search` (~line 2334)

```python
# BEFORE (_list_folder_documents)
params: list = [self.project_id, folder_root_id, session_id]
if path_prefix is not None:
    escaped = self._escape_like(path_prefix)
    query += (
        " AND (pd.relative_path = %s "
        "OR pd.relative_path LIKE %s ESCAPE '\\')"
    )
    params.extend([path_prefix, f"{escaped}/%"])
query += """
    GROUP BY pd.document_id
    ORDER BY pd.relative_path, pd.document_id
"""

# AFTER
from psycopg import sql

path_clause = sql.SQL("")
if path_prefix is not None:
    escaped = self._escape_like(path_prefix)
    path_clause = sql.SQL(
        "AND (pd.relative_path = %s OR pd.relative_path LIKE %s ESCAPE '\\\\')"
    )

query = sql.SQL("""
    SELECT ...
    FROM public.project_documents AS pd
    LEFT JOIN public.document_chunks AS dc ON dc.document_id = pd.document_id
    WHERE pd.project_id = %s
      AND pd.folder_root_id = %s
      AND (
          pd.visibility_scope = 'project'
          OR (pd.visibility_scope = 'session' AND pd.session_id = %s)
      )
    {path}
    GROUP BY pd.document_id
    ORDER BY pd.relative_path, pd.document_id
""").format(path=path_clause)

params = [self.project_id, folder_root_id, session_id]
if path_prefix is not None:
    params.extend([path_prefix, f"{escaped}/%"])
```

For `list_documents` and `search`, which have multiple optional clauses, collect them into a list and join:

```python
optional_clauses = []
params: list = [self.project_id, session_id]

if visibility_scope is not None:
    optional_clauses.append(sql.SQL("AND pd.visibility_scope = %s"))
    params.append(visibility_scope)
if folder_root_id is not None:
    optional_clauses.append(sql.SQL("AND pd.folder_root_id = %s"))
    params.append(folder_root_id.strip())
if normalized_prefix is not None:
    escaped = self._escape_like(normalized_prefix)
    optional_clauses.append(
        sql.SQL("AND (pd.relative_path = %s OR pd.relative_path LIKE %s ESCAPE '\\\\')")
    )
    params.extend([normalized_prefix, f"{escaped}/%"])

query = sql.SQL("""
    SELECT ...
    WHERE ...
    {filters}
    GROUP BY pd.document_id
    ORDER BY pd.created_at DESC, pd.document_id DESC
    LIMIT %s
""").format(
    filters=sql.SQL(" ").join(optional_clauses) if optional_clauses else sql.SQL("")
)
params.append(limit)
```

---

## Priority assessment

| Fix | Worth doing? | Reason |
|---|---|---|
| `session_manager.py` SET clause (fix 1) | **Yes** | Column names from a caller-supplied dict — `sql.Identifier` prevents injection |
| `project_manager.py` SET clause (fix 2) | **Yes** | Same reason |
| Everything else (fixes 3–9) | **Optional** | All injected fragments are hardcoded string literals chosen by `if` branches, not user input. Only values go through `%s`, which psycopg already parameterises safely. The `psycopg.sql` wrapper adds noise with no safety benefit. |

Fixes 1 and 2 are the only ones with a real justification. The rest are purely cosmetic.

---

## Embedding batching opportunities

Model is already loaded in FP16 (`torch_dtype: torch.float16` in `embedding_service.py`). ONNX export is a separate orthogonal win — apply via `optimum-cli` without touching any of the below.

### `entity_service.py` — `register_entity` (line 356)

Called once per new entity, but in the ingestion pipeline new entities arrive as a batch. Fix: add a `register_entity_batch` method that collects all `text_to_embed` strings, calls `encode([...])` once, then iterates to do the cache writes. Keep `register_entity` as a thin delegate.

**Scope:** `entity_service.py` + call sites in `pipeline_service.py`. Contained.

### `entity_service.py` — `compute_embedding` (line 403)

Same shape as above. The profile refinement job calls this per entity. Add a `compute_embeddings_batch` variant, call it wherever multiple profile updates are triggered together.

**Scope:** `entity_service.py` + the profile job call sites. Contained.

### `entity_service.py` — `_find_candidates` (line 311)

`encode_single` only fires here if no `precomputed_embedding` was passed in. The right fix is to ensure callers in the ingestion pipeline always pass a pre-computed embedding rather than letting resolution compute it on demand. Check call sites in `pipeline_service.py` — if they already pass embeddings, this path is already dead.

**Scope:** check call sites only. Possibly a no-op fix.

### `fact_resolution.py` — `apply_fact_changes` (line 107)

`encode_single` is inside a `for` loop but the embedding is immediately consumed by `detect_contradictions` on the next line, and `active_existing` is mutated each iteration based on the result. The loop is stateful — you can't trivially hoist all encodings out front without splitting the loop into two passes:

1. Pre-compute all embeddings: `embeddings = await embedding_service.encode([f.content for f in merge_result.new_contents])`
2. Iterate with the pre-computed embeddings alongside the stateful contradiction logic.

The contradiction detection would need to accept a pre-computed embedding rather than calling encode itself. More involved than the entity service ones but still contained within `fact_resolution.py`.

**Scope:** `fact_resolution.py` only. Moderate effort.


---

# Naming Notes

Some core class names don't match their actual responsibility. Known cases:

- `Context` — is the full session runtime, not a data bag. Candidate rename:
  `Session`.
- `BatchConsumer` — is a Redis drain loop, not a queue consumer. Candidate
  rename: `IngestionWorker`.
- `BatchProcessor` — is the full NLP ingestion pipeline with mutable state.
  Candidate rename: `IngestionPipeline`.
- `EntityManager` — is an in-memory entity resolution cache with fuzzy and
  vector matching. Candidate rename: `EntityResolver`.
- `DebugEventEmitter` — is the production pipeline event system, not a debug
  tool. Candidate rename: `EventEmitter`.

These have not been renamed yet. When reading or changing these classes, use
the descriptions above as the mental model.
