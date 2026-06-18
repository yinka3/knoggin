# Folder Upload Filtering and Project-Scoped FileRAG

## Summary

This feature makes it easy for a user to add a whole folder of files to FileRAG without manually opening each subfolder and selecting individual files. The system scans an uploaded folder recursively, applies project-specific filter rules, ignores unwanted files and directories, shows a reviewable preview, and ingests only the accepted files into FileRAG.

The first target audience is coders because code projects commonly contain a mix of valuable source files and noisy folders such as dependency caches, build outputs, virtual environments, binary artifacts, logs, and generated files. The same idea can later support researchers, students, operators, writers, and other users who work with document-heavy folders.

The intended architecture change is that FileRAG becomes project-scoped rather than session-scoped. Files are stored under a project, but each file can carry `session_id` metadata and a visibility scope so files uploaded from a session can still be visible in the project files area while remaining retrievable only by that session.

Backward compatibility is not a requirement for this feature because there are no existing users to preserve.

## Purpose

The purpose is to remove friction from uploading useful file context while protecting the system from unnecessary or harmful ingestion.

Today, adding a folder-like set of documents requires the user to manually navigate through folders and select files. That is slow, error-prone, and especially painful for codebases where useful files are distributed across many directories. It also places the burden of knowing what to exclude on the user every time.

This feature moves that work into the system:

- The user uploads a folder once.
- The system traverses allowed subfolders and detects candidate files.
- Project-specific filters exclude unimportant or risky files.
- `.gitignore` can be respected by default.
- The user can preview and customize the result.
- Accepted files are indexed into FileRAG with enough metadata to preserve folder context.

The end result is a FileRAG workflow that feels closer to adding a project than adding a pile of unrelated files.

## Core User Benefit

The main benefit is that users can provide richer context with less manual effort.

For coders, this means the assistant can understand a codebase from real project structure:

- `src/project_manager.py` is different from `tests/project_manager.py`.
- `backend/config.py` is different from `worker/config.py`.
- A file's location helps explain its purpose.
- Retrieval can cite or display a meaningful path rather than only a filename.

For non-coders, the same model helps with organized document collections:

- research folders,
- class notes,
- product specs,
- meeting archives,
- policy documents,
- exported support docs,
- writing projects,
- design documentation.

The user does not need to decide every file manually, and the system avoids wasting storage, embedding cost, and retrieval space on files that should never be indexed.

## Primary Use Cases

### Uploading a Code Project

A developer uploads a project folder. The system ignores `.git`, `node_modules`, `.venv`, `dist`, `build`, `__pycache__`, logs, binaries, and files excluded by `.gitignore`. It ingests source files, Markdown docs, configs, and other useful text files.

Subfolders are included automatically as long as the folder itself is not hidden, blocked by project filters, excluded by `.gitignore`, or stopped by safety limits such as maximum folder depth.

This lets the assistant answer questions such as:

- "Where is project membership handled?"
- "What files are involved in session startup?"
- "Explain the flow from upload to retrieval."
- "Which module should I edit for project-level FileRAG?"

### Uploading a Session-Specific Working Set

A user uploads files while inside a session. Those files appear in the project's files area for visibility and organization, but retrieval access remains limited to that session.

This supports a workflow where the project owns the storage container, while the session controls access:

```text
project_id = current project
visibility_scope = session
session_id = current session
```

Other sessions in the same project should not retrieve those files unless the files are promoted to project-wide visibility.

### Reusing Project Filter Settings

A project remembers its filter settings. The first upload can show a preview so the user can tune the rules. Later uploads can use the saved settings and be auto-accepted when the user trusts the configuration.

This is useful for repeated codebase uploads, incremental imports, and projects where the same folder structure appears again and again.

### Re-Uploading a Changed Folder

A user uploads the same folder again after files changed. The system should eventually detect unchanged files, new files, and changed files using hashes and relative paths.

The preferred long-term behavior is merge/update:

- unchanged files are skipped,
- new files are added,
- changed files replace their current active version,
- missing files are not automatically deleted unless the user explicitly chooses a sync-style behavior.

## Important Concepts

### `relative_path`

`relative_path` is the file path inside the uploaded folder or project. It does not store the user's full local machine path.

Example folder:

```text
my-app/
  README.md
  src/main.py
  src/utils/time.py
  tests/test_time.py
```

Stored paths:

```text
README.md
src/main.py
src/utils/time.py
tests/test_time.py
```

This helps the system preserve project structure and disambiguate files with the same name.

### `folder_root_id`

`folder_root_id` identifies one folder upload batch. Every accepted file from the same folder upload receives the same `folder_root_id`.

This lets the system:

- list files from one upload together,
- delete an uploaded folder as a unit,
- compare a later upload against a previous one,
- show where a file came from,
- support future folder-level history or sync behavior.

### `content_hash`

`content_hash` identifies the exact file contents. It is useful for duplicate detection and re-upload behavior.

When `relative_path` is the same and `content_hash` is unchanged, the file can be skipped. When `relative_path` is the same but `content_hash` changed, the file should be treated as a new version of the same logical file.

### `visibility_scope`

`visibility_scope` controls who can retrieve a file.

Suggested values:

```text
project
session
```

Project-visible files can be retrieved by relevant sessions in the project. Session-visible files are stored under the project but only retrievable by the originating session.

## FileRAG Scope Change

The current implementation describes FileRAG as session-scoped. This feature changes the desired model:

```text
Current direction:
FileRAG storage = session-scoped

New direction:
FileRAG storage = project-scoped
FileRAG access = project/session visibility rules
```

Project-scoped FileRAG should use `project_id` as the required storage and retrieval boundary. `session_id` becomes provenance and access-control metadata rather than the top-level storage owner.

The retrieval rule should be explicit:

```text
project_id = current_project
AND (
  visibility_scope = "project"
  OR session_id = current_session
)
AND is_latest = true
AND is_deleted = false
```

This avoids leaking session-only uploads into other sessions while still allowing the project files area to show all files that belong to the project.

## Suggested Metadata Model

Each ingested file should carry metadata similar to:

```text
file_id
project_id
session_id
visibility_scope
folder_root_id
folder_name
relative_path
original_name
extension
size_bytes
content_hash
version
is_latest
is_deleted
uploaded_at
updated_at
source_kind
```

Suggested `source_kind` values:

```text
manual_file_upload
folder_upload
session_upload
project_upload
```

Chunks should inherit the metadata needed for retrieval filters and citations:

```text
file_id
project_id
session_id
visibility_scope
folder_root_id
relative_path
content_hash
version
is_latest
is_deleted
```

## Filtering Model

Filters should be project-specific. Different projects need different rules, especially across programming languages and document types.

Suggested project filter settings:

```text
project_id
respect_gitignore
ignored_patterns
allowed_extensions
blocked_extensions
blocked_file_names
blocked_directory_names
max_file_size_bytes
max_total_upload_size_bytes
max_file_count
max_folder_depth
auto_accept_enabled
updated_at
```

`.gitignore` should be respected by default because it is a strong signal for code projects. Users should be able to disable it per project because there are cases where ignored files are still useful to the assistant.

Possible issue cases for `.gitignore`:

- generated docs are ignored by Git but useful for context,
- local notes are ignored but intentionally uploaded,
- a non-code folder happens to contain a `.gitignore`,
- the user wants to include files normally omitted from source control.

## Default Ignore Rules

The default filter should be cautious. It should avoid indexing files that are usually large, repetitive, generated, sensitive, or not useful for semantic retrieval.

Common ignored directories:

```text
.git
.hg
.svn
node_modules
dist
build
target
.next
.nuxt
.cache
.pytest_cache
.ruff_cache
.mypy_cache
__pycache__
.venv
venv
env
.tox
coverage
.idea
.vscode
```

Common ignored files and patterns:

```text
*.pyc
*.pyo
*.log
*.tmp
*.lock
*.min.js
*.map
*.sqlite
*.db
*.zip
*.tar
*.gz
*.png
*.jpg
*.jpeg
*.gif
*.mp4
*.mov
```

Sensitive defaults:

```text
.env
.env.*
*.pem
*.key
id_rsa
id_ed25519
credentials.json
secrets.*
```

Sensitive files should be blocked by default. If override support is added later, it should be deliberate and highly visible in the backend/API contract.

## Upload Lifecycle

The backend workflow should separate scanning from ingestion.

### 1. Scan

The system reads folder paths, names, sizes, extensions, and ignore rules. It should avoid chunking or embedding during scan.

Folder scanning should be recursive. If a subfolder is allowed, the scanner should descend into it and continue applying the same rules. If a subfolder is hidden or filtered out, the scanner should skip the whole directory before reading the files inside it. This prevents expensive scans of directories that are already known to be irrelevant, such as dependency folders, build folders, caches, and hidden metadata directories.

Scan output should include:

- accepted candidate files,
- excluded file counts,
- excluded directory counts,
- exclusion reasons,
- total candidate size,
- total excluded size if known,
- rule sources such as default ignore, project filter, `.gitignore`, file size, or binary detection.

### 2. Preview

The first upload should produce a preview that the user can customize. The preview should be represented as backend/API data so later clients can decide how to display or auto-accept it.

The preview should answer:

- which files would be ingested,
- which files would be ignored,
- why files were ignored,
- what settings were used,
- what the expected ingestion size is.

### 3. Accept

When accepted, the system ingests only accepted files. Excluded files are not stored, chunked, embedded, or added to FileRAG.

### 4. Auto-Accept

Project settings can allow auto-accepting a scan result with saved filters. This is useful after the user has already tuned and trusted the project's rules.

Auto-accept should still apply all guardrails such as file count, file size, binary detection, and sensitive-file blocking.

### 5. Ingest

Accepted files are read, chunked, embedded, and written into project-scoped FileRAG with file and chunk metadata.

## Excluded Files

Excluded files should be totally ignored for ingestion. They should not be embedded and their contents should not be stored.

The system may store lightweight summary information for observability:

```text
excluded_count
excluded_bytes
excluded_reason_counts
```

Example reason counts:

```text
gitignore: 120
default_directory_ignore: 86
blocked_sensitive_file: 4
too_large: 3
unsupported_extension: 32
binary_file: 18
```

The system should not retain excluded file contents, especially because some excluded files may be huge or sensitive.

## Re-Upload and Merge Behavior

The ideal future behavior is to merge new and changed files instead of blindly creating duplicate uploads.

Suggested matching logic:

```text
same project_id
same visibility_scope
same session_id when visibility_scope = session
same folder_root_id or same logical folder identity
same relative_path
```

Then compare `content_hash`:

- same hash: skip as unchanged,
- different hash: create a new active version,
- new relative path: add as new file,
- missing relative path: keep existing file unless explicit sync mode is selected.

Avoid automatic deletion in the first implementation. Deletion-on-missing is useful but risky. It should be a separate sync mode rather than default merge behavior.

## Version History

Version history is useful, but it should be introduced carefully because every version can create more chunks, embeddings, storage usage, and retrieval ambiguity.

Suggested behavior:

- latest active version is used for normal retrieval,
- older versions are retained only for history, diff, restore, or audit,
- project settings define the retention count,
- a sensible default is 10 versions,
- an upper bound between 25 and 100 can be considered based on storage cost.

Normal retrieval should filter:

```text
is_latest = true
is_deleted = false
```

This prevents old versions from appearing in normal answers unless the user explicitly asks for history-aware behavior.

## Benefits

### Less Manual Work

Users can upload a folder once instead of manually selecting files from many subdirectories.

### Better Context Quality

The system ingests source files and useful documents while excluding noisy, generated, duplicate, binary, or sensitive files.

### Better Retrieval Grounding

`relative_path` gives answers better citations and context. The assistant can explain not only what file was used, but where the file lives in the project.

### Safer Storage and Embedding Costs

Ignoring excluded files before ingestion prevents large folders, dependency trees, build outputs, and binary artifacts from consuming storage and embedding resources.

### Project-Level Organization

Project-scoped FileRAG makes files part of the project knowledge space while still allowing session-only access when needed.

### Future Re-Upload Support

`folder_root_id`, `relative_path`, and `content_hash` create a path toward merge, sync, and version history without needing to redesign file identity later.

## Agent-Facing File Tools

Project-scoped FileRAG and folder uploads give the agent enough structure to work with files more like a read-only project filesystem instead of a flat pile of uploaded documents.

The agent should not receive tools that change file visibility or promote session-only files to project-wide access. Access-changing actions should stay user-controlled. The agent's file tools should be read-only and should obey the same retrieval rule used by FileRAG:

```text
project_id = current_project
AND (
  visibility_scope = "project"
  OR session_id = current_session
)
AND is_latest = true
AND is_deleted = false
```

### `list_files`

Lists files available to the current agent context.

Suggested shape:

```text
list_files(folder_root_id=None, path_prefix=None, visibility_scope=None, limit=50)
```

Useful when the user asks what files exist, whether a project contains docs, or what files are available under a specific folder.

The response should include enough metadata for the agent to choose follow-up tools:

```text
file_id
relative_path
original_name
extension
size_bytes
chunk_count
visibility_scope
folder_root_id
uploaded_at
```

### `list_folder_tree`

Returns the visible folder structure for an uploaded folder or path prefix.

Suggested shape:

```text
list_folder_tree(folder_root_id=None, path_prefix=None, max_depth=3)
```

This helps the agent orient itself before searching. It is especially useful for code projects because the tree can show major areas such as `src`, `tests`, `docs`, and config files.

Example response:

```text
src/
  knoggin_server/
    project/
    session/
    knowledge/
tests/
  runtime/
  knowledge/
README.md
```

### `search_files`

Searches visible files for relevant chunks.

Suggested updated shape:

```text
search_files(query, file_name=None, relative_path=None, path_prefix=None, limit=5)
```

The current system already has `search_files(query, file_name=None, limit=5)`. The folder-aware version should add `relative_path` and `path_prefix` so the agent can search within exact paths or subtrees instead of only filtering by exact filename.

### `read_file`

Reads a specific visible file or a bounded line range from that file.

Suggested shape:

```text
read_file(file_id=None, relative_path=None, start_line=None, end_line=None)
```

This is valuable for coding workflows. Search can find relevant chunks, but the agent may need a complete file section to explain behavior, trace implementation details, or answer questions about exact code.

The tool should enforce size and line limits so the agent cannot accidentally pull a very large file into context.

### `get_file_info`

Returns metadata for one visible file.

Suggested shape:

```text
get_file_info(file_id=None, relative_path=None)
```

Useful for citations, debugging retrieval, and answering questions about where a file came from.

Suggested response fields:

```text
file_id
project_id
session_id
visibility_scope
folder_root_id
relative_path
extension
size_bytes
content_hash
version
is_latest
uploaded_at
updated_at
source_kind
```

### `list_folder_uploads`

Lists uploaded folder batches visible to the current context.

Suggested shape:

```text
list_folder_uploads(visibility_scope=None, limit=25)
```

This helps the agent handle questions like "use the codebase folder I uploaded earlier" or "what folder uploads are available in this project?"

Suggested response fields:

```text
folder_root_id
folder_name
file_count
total_size_bytes
uploaded_at
source_kind
visibility_scope
session_id
```

### `get_folder_upload_summary`

Returns a summary of one folder upload batch.

Suggested shape:

```text
get_folder_upload_summary(folder_root_id)
```

This can include accepted file counts, excluded file counts, excluded directory counts, exclusion reason counts, total size, and a top-level folder tree.

### Future Version and Change Tools

If version history and merge behavior are added, the agent can also receive read-only tools for understanding changes:

```text
list_file_versions(file_id=None, relative_path=None)
compare_file_versions(file_id=None, relative_path=None, from_version=None, to_version=None)
list_changed_files(folder_root_id=None, since_upload_id=None)
```

These should be added after the underlying version model exists. Normal retrieval should still use only latest active files unless the user explicitly asks about history or changes.

### Recommended Agent Tool MVP

The first agent-facing tool set should be:

```text
list_files
list_folder_tree
search_files
read_file
get_file_info
```

This gives the agent enough capability to inspect available files, understand folder structure, search content, read exact files when needed, and cite file metadata correctly.

## File Focus Mode

File Focus Mode is a later agent-aware capability that lets a user tell the system that one file, a group of files, a folder, or a folder upload batch is the active working context for a period of time.

The purpose is to make references like "this file", "that module", "the uploaded doc", or "the ingestion folder" resolve cleanly across multiple turns without forcing the user to repeat paths every message.

Example user requests:

```text
For this session, focus on src/knoggin_server/ingestion/.
For the next 30 minutes, I am talking about file_rag.py.
Review these three files until I say stop.
Clear file focus.
Switch focus to the tests folder.
```

This should not mean dumping large file metadata into every prompt. The system should store richer focus state internally and expose only a tiny summary to the agent.

### Focus State

Suggested focus metadata:

```text
focus_id
project_id
session_id
mode: soft | pinned
target_type: file | folder | folder_upload | file_set
file_ids
relative_paths
path_prefixes
folder_root_id
reason
created_at
expires_at
expires_after_turns
created_from_message_id
```

Each message can carry a lightweight reference to the active focus:

```text
file_focus_id
```

The full focus object can be resolved by the backend when needed.

### Soft vs Pinned Focus

Soft focus is inferred by the system. For example, if the user uploads a file and immediately asks questions about it, the system can treat that uploaded file as the likely focus.

Pinned focus is explicitly requested by the user. For example, "for this session" or "for the next 30 minutes" should create stronger focus that persists until it expires or the user clears it.

### Deterministic Focus Resolver

The main behavior should be handled by a deterministic resolver rather than by asking the LLM to infer focus from a large metadata blob.

Suggested helper:

```text
resolve_file_focus(session_id, message_text) -> FileFocusContext
```

The resolver should answer:

- is there active file focus,
- did the user mention a different file or folder,
- did the user ask to clear or switch focus,
- should this message use focused file defaults,
- which file IDs, relative paths, path prefixes, or folder upload IDs should apply.

### Tiny Agent Prompt Hint

The agent should receive a compact human-readable summary only when focus is active.

Example:

```text
Active file focus:
- path_prefix: src/knoggin_server/ingestion/
- mode: pinned
- expires: this session
```

For a single file:

```text
Active file focus:
- relative_path: src/knoggin_server/knowledge/services/file_rag.py
- mode: soft
```

This keeps the agent oriented without overloading the prompt.

### Focus-Aware Tool Defaults

The most important behavior is that file tools should use active focus as default filters when the user's request appears file-related.

Example:

```text
search_files(query="where does retry happen?")
```

can be resolved by the backend as:

```text
search_files(
  query="where does retry happen?",
  path_prefix="src/knoggin_server/ingestion/"
)
```

if folder focus is active.

The same applies to `list_files`, `list_folder_tree`, `read_file`, and `get_file_info`. The focus should help tools resolve "this file" or "this folder" without requiring the LLM to manually pass every filter.

### Bias, Not A Cage

File focus should bias retrieval, not trap the agent.

If the user asks about the focused file, the system should apply focus defaults. If the user asks to compare the focused file with another path, the agent should use both. If the user asks an unrelated project question, the system should not force all retrieval through the active file focus.

### Recommended Timing

File Focus Mode should come after the backend has folder-aware metadata and read-only file tools.

Recommended order:

1. Project-scoped FileRAG.
2. Folder uploads and filters.
3. Folder-aware agent file tools.
4. File Focus Mode.

## Non-Goals For The First Version

The first version does not need to include:

- frontend design,
- backward compatibility with the current session-scoped FileRAG tables,
- full Git-like commits,
- complex diff visualization,
- automatic deletion of missing files during re-upload,
- project-wide sharing of session-only uploads,
- indexing excluded file contents,
- preserving excluded files for audit,
- File Focus Mode.

## Recommended MVP

The first complete backend-focused MVP should include:

1. Project-scoped FileRAG storage metadata.
2. Session metadata for session-originated files.
3. `visibility_scope` retrieval filtering.
4. Folder upload scan support with `relative_path`.
5. Project-specific filter settings.
6. `.gitignore` support enabled by default.
7. A preview data model with included and excluded file summaries.
8. Ingestion of accepted files only.
9. Exclusion reasons.
10. Content hashing for duplicate detection.

After the MVP, add:

1. Auto-accept using saved project filters.
2. Hash-based unchanged-file skipping.
3. Merge/update behavior for changed files.
4. Optional version history.
5. Optional user-controlled project promotion for session-only files.
6. Optional sync mode that deletes or marks missing files.
7. File Focus Mode with deterministic focus resolution and focus-aware tool defaults.

## Risks and Design Watchpoints

### Access Leakage

The biggest correctness risk is accidentally retrieving session-only files from another session in the same project. Retrieval filters must always include project and visibility rules.

### Hidden Storage Growth

Folder uploads can add many chunks quickly. File count, file size, total size, and version history limits are necessary.

### Expensive Previews

Preview generation should not chunk or embed files. It should be a lightweight scan.

### Over-Trusting Ignore Rules

`.gitignore` is useful but not perfect. The preview exists so users can see what was skipped and customize project rules.

### Secrets

Coder folder uploads can accidentally contain secrets. Sensitive file blocking should be a default guardrail.

### File Identity Ambiguity

File names alone are not enough. The system needs `relative_path` and `content_hash` to understand whether a file is duplicate, changed, or distinct.

### Prompt Bloat

File Focus Mode should not add large metadata objects to every prompt. The agent should receive a tiny focus summary, while deterministic backend helpers resolve the full focus state and apply tool defaults.

## Final Product Framing

This feature turns FileRAG from "upload individual files" into "attach useful project context." For coders, it makes Knoggin feel more aware of the codebase as a real folder structure instead of a flat list of documents. For other users, it makes organized document collections easier to bring into the assistant without overwhelming retrieval.

The best first version is backend-focused: project-scoped FileRAG, session visibility metadata, folder scan, project filters, `.gitignore`, preview data, accepted-file ingestion, and content hashes. That gives the system the important architecture and user value now while leaving merge, sync, history, folder-aware agent tools, and File Focus Mode as natural next steps.
