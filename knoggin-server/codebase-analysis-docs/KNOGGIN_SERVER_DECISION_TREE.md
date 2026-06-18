# Knoggin Server Decision Tree

This is a choose-your-own-adventure map of `knoggin-server`.

It answers two questions:

1. Where should I go in the codebase for a given kind of change?
2. What decisions does the engine make at runtime?

The server is an embeddable engine package, not an HTTP application. An external
API or SDK composes its managers and services.

## 1. Pick Your Destination

Start here when deciding which part of the codebase to inspect or change.

```mermaid
flowchart TD
    START{"What are you trying to change?"}

    START -->|Boot or shared dependencies| RES["ResourceManager<br/>infrastructure/resources.py"]
    START -->|Projects or visibility| PROJ["ProjectManager + ProjectState<br/>project/"]
    START -->|Session lifecycle| SESSION["SessionManager + Context + SessionAssembler<br/>session/"]
    START -->|How messages become knowledge| INGEST["BatchConsumer + BatchProcessor + TextProcessor<br/>ingestion/"]
    START -->|Entities, facts, graph, or retrieval| KNOW["Knowledge services + db readers/writers<br/>knowledge/"]
    START -->|How the assistant answers| AGENT["Orchestrator + AgentExecutor + Tools<br/>agent/"]
    START -->|Periodic maintenance| JOBS["Scheduler + BaseJob implementations<br/>infrastructure/job + */jobs/"]
    START -->|Autonomous discussions| AAC["CommunityManager + CommunityStore + AACJob<br/>community/"]
    START -->|Config, contracts, or scope rules| COMMON["ConfigManager + schemas + scoping<br/>common/"]

    RES --> R1{"Which dependency?"}
    R1 -->|Redis and fast state| REDIS["infrastructure/redis_client.py"]
    R1 -->|Postgres, AGE, pgvector| GRAPH["infrastructure/graph_client.py<br/>postgres_client.py<br/>schema.sql"]
    R1 -->|LLM calls| LLM["infrastructure/llm_client.py"]
    R1 -->|Embeddings and reranking| EMB["knowledge/services/embedding_service.py"]

    KNOW --> K1{"Read or write?"}
    K1 -->|Read| READERS["knowledge/db/readers/<br/>knowledge/db/tool_queries.py"]
    K1 -->|Write| WRITERS["knowledge/db/writers/<br/>knowledge/db/write_graph_db.py"]
    K1 -->|Resolve or manage concepts| SERVICES["knowledge/services/"]
    K1 -->|Repair AGE projection| REBUILD["knowledge/db/projection_rebuilder.py"]
```

## 2. Engine Boot

`ResourceManager` owns expensive process-wide dependencies. Project and session
runtime objects are assembled only after those resources are ready.

```mermaid
flowchart TD
    START["External caller starts engine"] --> EXISTS{"ResourceManager already initialized?"}
    EXISTS -->|Yes| RETURN["Return singleton"]
    EXISTS -->|No| ENV{"DATABASE_URL configured?"}
    ENV -->|No| CONFIG_ERR["Raise ConfigurationError"]
    ENV -->|Yes| DEVICE{"KNOGGIN_GPU requested and available?"}
    DEVICE -->|CUDA or MPS| GPU["Select accelerator"]
    DEVICE -->|No| CPU["Select CPU"]
    GPU --> INIT
    CPU --> INIT

    INIT["Create executor, GraphClient, Redis, LLM, embeddings"] --> LOAD["Load tokenizer, embedding models, spaCy, and GLiNER concurrently"]
    LOAD --> OK{"All critical models loaded?"}
    OK -->|No| TEARDOWN["Tear down partial resources<br/>raise DependencyError"]
    OK -->|Yes| CONNECT["Connect Postgres pools and load AGE"]
    CONNECT --> ENTITY["Create global EntityManager"]
    ENTITY --> READY["ResourceManager ready"]
```

## 3. Project and Session Lifecycle

The main runtime ownership rule is:

- `ResourceManager`: process-wide dependencies.
- `ProjectState`: topic config, entity resolver, ingestion processor, scheduler.
- `Context`: one active session, consumer, conversation state, and file RAG.

```mermaid
flowchart TD
    REQUEST{"Session request"}
    REQUEST -->|Create| VALID_PROJECT{"project_id present and project exists?"}
    REQUEST -->|Resume| ACTIVE{"Session already active?"}

    VALID_PROJECT -->|No| REJECT["Reject request"]
    VALID_PROJECT -->|Yes| ACQUIRE["Acquire project for session"]

    ACTIVE -->|Yes| RETURN_ACTIVE["Return active Context"]
    ACTIVE -->|No| META{"Session metadata exists and has project_id?"}
    META -->|No metadata| NOT_FOUND["Return None"]
    META -->|Invalid project_id| INVALID["Raise ValueError"]
    META -->|Valid| ACQUIRE

    ACQUIRE --> IDENTITY{"Identity invariant initialized?"}
    IDENTITY -->|No| ENSURE_ID["Persist shared identity entity as ID 1<br/>normal entity sequence starts at ID 2"]
    IDENTITY -->|Yes| PROJECT_STATE
    ENSURE_ID --> PROJECT_STATE

    PROJECT_STATE{"ProjectState already active?"}
    PROJECT_STATE -->|Yes| INC["Increment active runtime session count"]
    PROJECT_STATE -->|No| BOOT_PROJECT["Load readable projects and topic config<br/>build EntityManager, TextProcessor, BatchProcessor<br/>register project jobs"]

    INC --> ASSEMBLE
    BOOT_PROJECT --> ASSEMBLE["SessionAssembler creates Context"]
    ASSEMBLE --> WIRE["Wire project BatchProcessor<br/>session BatchConsumer<br/>session FileRAGService"]
    WIRE --> SCHED{"Project scheduler running?"}
    SCHED -->|No| START_SCHED["Start scheduler"]
    SCHED -->|Yes| START_CONSUMER
    START_SCHED --> START_CONSUMER["Start session BatchConsumer"]
    START_CONSUMER --> LIVE["Context is live"]

    LIVE --> CLOSE{"Close or delete?"}
    CLOSE -->|Close| STOP["Stop session workers<br/>release ProjectState reference"]
    STOP --> LAST{"Last active runtime session?"}
    LAST -->|No| KEEP_PROJECT["Keep project runtime and jobs active"]
    LAST -->|Yes| SHUT_PROJECT["Stop scheduler and remove active ProjectState"]
    CLOSE -->|Delete data| DELETE["Delete session Redis keys, file RAG data,<br/>metadata, and project membership"]
```

## 4. User Message Learning Path

The conversation loop and learning loop share the same `Context`, but ingestion
runs asynchronously through a Redis buffer.

```mermaid
flowchart TD
    MSG["Context.add(user message)"] --> DUP{"Same session + content + timestamp<br/>seen in dedup window?"}
    DUP -->|Yes| EXISTING["Return existing message ID"]
    DUP -->|No| ASSIGN["Allocate global message ID"]
    ASSIGN --> REDIS["Cache canonical message and timeline position in Redis"]
    REDIS --> BUFFER["Append scoped message to Redis ingestion buffer"]
    BUFFER --> SIGNAL["Record project activity, signal consumer, refresh TTLs"]

    SIGNAL --> WAKE{"Why did consumer wake?"}
    WAKE -->|Message signal| DRAIN["Drain up to batch_size"]
    WAKE -->|Timeout, explicit flush, or shutdown| DRAIN

    DRAIN --> VALID{"Any valid buffered messages?"}
    VALID -->|No, only corrupt entries| TRIM_BAD["Trim corrupt entries and continue"]
    VALID -->|Yes| CONTEXT["Load surrounding conversation context"]
    CONTEXT --> PROCESS["BatchProcessor.run"]

    PROCESS --> MENTIONS["Known alias matching + GLiNER"]
    MENTIONS --> LLM_NER{"LLM NER enabled?"}
    LLM_NER -->|No| DET_ONLY["Use deterministic mentions"]
    LLM_NER -->|Yes| VP01["Ask LLM for uncovered or ambiguous mentions"]
    VP01 --> LLM_OK{"LLM extraction succeeded?"}
    LLM_OK -->|No| DET_ONLY
    LLM_OK -->|Yes| FILTER["Validate confidence, topic, type, and duplicates"]
    DET_ONLY --> ACTIVE_FILTER
    FILTER --> ACTIVE_FILTER{"Mention belongs to an active topic?"}
    ACTIVE_FILTER -->|No| DROP_MENTION["Drop mention"]
    ACTIVE_FILTER -->|Yes| ANY{"Any accepted mentions?"}
    DROP_MENTION --> ANY
    ANY -->|No| NO_GRAPH["Successful batch with no graph mutations"]
    ANY -->|Yes| RESOLVE["Resolve each mention"]

    RESOLVE --> CANDIDATE{"Existing candidate above threshold<br/>and schema/context checks pass?"}
    CANDIDATE -->|Yes| REUSE["Reuse entity; optionally add alias"]
    CANDIDATE -->|No| NEW["Allocate and cache new project entity"]
    REUSE --> CONNECTIONS
    NEW --> CONNECTIONS["Extract entity and user relationships"]
    CONNECTIONS --> CONN_OK{"Connection extraction succeeded?"}
    CONN_OK -->|No| PROCESS_FAIL["Mark processing failure"]
    CONN_OK -->|Yes| RESULT["Return scoped BatchResult"]

    NO_GRAPH --> RESULT
    RESULT --> SAVE_MSG["Always persist source message logs"]
    PROCESS_FAIL --> DLQ_PROCESS["Write processing-stage DLQ entry"]
    SAVE_MSG --> MSG_OK{"Message log write succeeded?"}
    MSG_OK -->|No| DLQ_MSG["Write message-log-stage DLQ entry"]
    MSG_OK -->|Yes| HAS_WRITES{"Batch has graph writes?"}
    HAS_WRITES -->|No| CHECKPOINT["Advance checkpoint and last-processed IDs"]
    HAS_WRITES -->|Yes| WRITE["Build and execute GraphMutationPlan"]
    WRITE --> WRITE_OK{"Graph write succeeded?"}
    WRITE_OK -->|No| PURGE["Purge phantom new entities from resolver cache"]
    PURGE --> DLQ_GRAPH["Write graph-write-stage DLQ entry"]
    WRITE_OK -->|Yes| DIRTY["Mark safe entities dirty for profile refinement"]
    DIRTY --> CHECKPOINT

    DLQ_PROCESS --> DLQ_OK{"DLQ write succeeded?"}
    DLQ_MSG --> DLQ_OK
    DLQ_GRAPH --> DLQ_OK
    DLQ_OK -->|Yes| TRIM["Trim source batch from buffer"]
    DLQ_OK -->|No| RETAIN["Keep messages buffered for retry"]
    CHECKPOINT --> TRIM
```

## 5. Graph Mutation Decisions

`GraphMutationPlan` is the safety boundary between extraction and persistence.

```mermaid
flowchart TD
    BATCH["Scoped BatchResult"] --> SCOPE{"user_name, session_id, project_id present?"}
    SCOPE -->|No| FAIL["Raise ValueError"]
    SCOPE -->|Yes| VALIDATE["Validate referenced existing entity IDs"]
    VALIDATE --> AVAILABLE{"Validation available?"}
    AVAILABLE -->|No| ASSUME["Warn and assume candidates are valid"]
    AVAILABLE -->|Yes| ZOMBIES{"Resolver IDs missing from canonical SQL?"}
    ZOMBIES -->|Yes| FILTER["Remove zombie IDs from resolver cache and plan"]
    ZOMBIES -->|No| SAFE
    ASSUME --> SAFE["Build safe entity set"]
    FILTER --> SAFE

    SAFE --> PROFILES{"Profiles exist for new or alias-updated entities?"}
    PROFILES -->|No| SKIP_ENTITY["Skip incomplete entity write"]
    PROFILES -->|Yes| ENTITY_WRITE["Build entity write with embedding and scope"]
    SKIP_ENTITY --> REL
    ENTITY_WRITE --> REL{"Both relationship endpoints resolve safely?"}
    REL -->|No| SKIP_REL["Record skipped relationship"]
    REL -->|Yes| EVIDENCE["Attach user/session/message evidence reference"]

    EVIDENCE --> PLAN
    SKIP_REL --> PLAN{"Plan has any writes?"}
    PLAN -->|No| DONE_EMPTY["Mark graph work skipped"]
    PLAN -->|Yes| EXEC["Update aliases, write entities and relationships"]
    EXEC --> REDIS{"Redis client supplied and dirty entities exist?"}
    REDIS -->|Yes| MARK["Mark project dirty entities<br/>clear profile-complete flag"]
    REDIS -->|No| SUMMARY
    MARK --> SUMMARY["Return GraphWriteSummary"]
```

## 6. Persistence Ownership

The storage model has a deliberate hierarchy:

```mermaid
flowchart TD
    WRITE{"What kind of state is this?"}
    WRITE -->|Canonical knowledge| SQL["Postgres relational tables<br/>messages, entities, aliases, facts,<br/>relationships, evidence refs, hierarchy"]
    WRITE -->|Graph traversal projection| AGE["Apache AGE<br/>projected nodes and edges"]
    WRITE -->|Search projection| SEARCH["pgvector + tsvector tables<br/>entity_search, fact_search, message_search"]
    WRITE -->|Fast runtime state| REDIS["Redis<br/>sessions, projects, buffers, config,<br/>queues, counters, memory, events"]
    WRITE -->|Uploaded file retrieval| FILES["Session-scoped FileRAG storage"]

    SQL --> SOURCE["Source of truth"]
    AGE --> REBUILD["Can be rebuilt per project from SQL"]
    SEARCH --> DERIVED["Derived query indexes"]
    REDIS --> EPHEMERAL["Operational and durable runtime metadata"]
```

Important scope decisions:

- The identity entity is always entity ID `1` in reserved scope `__identity__`.
- A normal entity belongs to a project and records its source session.
- Readable project IDs are `__identity__`, the current project, then explicitly
  allowed projects.
- Source evidence is always addressed by user, session, and message ID.
- Project jobs use project scope; live ingestion keeps the originating session
  scope.

## 7. Agent Answering Path

The agent is a bounded tool loop. The Architect plans and approves final
answers; the Librarian performs cheaper investigative steps.

```mermaid
flowchart TD
    QUERY["Orchestrator.run_stream"] --> SERVICES["Build MemoryManager and Tools from active Context"]
    SERVICES --> IDENTITY{"Requested agent config found?"}
    IDENTITY -->|Yes| CONFIG["Use saved name, persona, model, temperature, tools"]
    IDENTITY -->|No| DEFAULT["Use runtime defaults or explicit overrides"]
    CONFIG --> PRELOAD
    DEFAULT --> PRELOAD["Load memories, directives, file manifest, and hot-topic context"]
    PRELOAD --> LOOP["Start bounded AgentExecutor loop"]

    LOOP --> LIMIT{"Attempts or consecutive-error limit reached?"}
    LIMIT -->|Yes| EVIDENCE{"Any evidence accumulated?"}
    EVIDENCE -->|Yes| FALLBACK_SUMMARY["Generate evidence-based fallback response"]
    EVIDENCE -->|No| FALLBACK_CLARIFY["Ask user to rephrase"]

    LIMIT -->|No| MODE{"First turn, replanning, or final synthesis?"}
    MODE -->|Yes| ARCH["Architect mode<br/>agent model + high reasoning"]
    MODE -->|No| LIB["Librarian mode<br/>extraction model + medium reasoning"]
    ARCH --> STEP
    LIB --> STEP["LLM must return tool calls"]

    STEP --> OUTPUT{"What did the model request?"}
    OUTPUT -->|Raw text only| FORMAT["Return formatting error to loop"]
    OUTPUT -->|Clarification| CLARIFY["Yield clarification and stop"]
    OUTPUT -->|Replanning| REPLAN["Set Architect mode for next turn"]
    OUTPUT -->|submit_answer from Librarian| REVIEW["Promote to Architect for final synthesis"]
    OUTPUT -->|submit_answer from Architect| ANSWER["Yield final response and sources"]
    OUTPUT -->|Investigative tools| TOOL_GUARDS{"Call limits, duplicates, and arguments valid?"}

    TOOL_GUARDS -->|No| TOOL_ERR["Record tool error"]
    TOOL_GUARDS -->|Yes| EXEC_TOOL["Execute tools sequentially"]
    EXEC_TOOL --> ACC["Accumulate messages, entities, graph, facts, files, and sources"]
    ACC --> SIZE{"Evidence over context limit?"}
    SIZE -->|Yes| SUMMARIZE["Summarize and retain a small raw evidence tail"]
    SIZE -->|No| EMPTY
    SUMMARIZE --> EMPTY{"Consecutive tool rounds empty?"}
    TOOL_ERR --> EMPTY
    EMPTY -->|Threshold reached| REPLAN
    EMPTY -->|No| LOOP
    FORMAT --> LOOP
    REPLAN --> LOOP
    REVIEW --> LOOP
```

Tool destinations:

| Need | Main tool path |
| --- | --- |
| Conversation evidence | `search_messages` |
| Entity profile | `search_entity` |
| Neighbors and relationships | `get_connections` |
| Time-oriented evidence | `get_recent_activity` |
| Claim verification | `fact_check` |
| Multi-hop connection | `find_path` |
| Parent or child structure | `get_hierarchy` |
| Uploaded documents | `search_files` |
| External information | `web_search`, `news_search` |
| Session memory | `save_memory`, `forget_memory` |
| AAC behavior | `save_insight`, `spawn_specialist` |

## 8. Background Job Decisions

Each active project has one scheduler. It checks jobs once at startup and every
30 seconds afterward. Enabled jobs run when either their domain trigger passes
or their declared scheduler cadence is due. The scheduler owns cadence
timestamps and advances them only after successful results; job `should_run`
checks do not mutate cadence state. Local task tracking and a token-owned Redis
lease prevent overlapping runs in the same process or across server workers.

Archival retains its profile-complete domain trigger in addition to its fallback
cadence. It consumes the profile marker with a compare-and-delete only after a
successful archival pass, so failed work and newer markers remain retryable.

```mermaid
flowchart TD
    TICK["Project scheduler tick"] --> RUNNING{"Same job already running?"}
    RUNNING -->|Yes| SKIP["Skip this tick"]
    RUNNING -->|No| ENABLED{"Optional job enabled?"}
    ENABLED -->|No| SKIP
    ENABLED -->|Yes| KIND{"Which job?"}

    KIND -->|Profile refinement| PROFILE{"Dirty entities exist and<br/>volume or idle threshold met?"}
    PROFILE -->|Yes| RUN_PROFILE["Refine profiles and facts<br/>queue updated entities for merge"]
    PROFILE -->|No| SKIP

    KIND -->|Merge detection| MERGE{"Merge queue non-empty?"}
    MERGE -->|Yes| RUN_MERGE["Detect duplicates and hierarchy candidates"]
    MERGE -->|No| SKIP

    KIND -->|Topic evolution| TOPIC{"Project message interval reached<br/>and all session buffers empty?"}
    TOPIC -->|Yes| RUN_TOPIC["Propose and sanitize topic config changes"]
    TOPIC -->|No| SKIP

    KIND -->|DLQ replay| DLQ{"Replay interval elapsed?"}
    DLQ -->|Yes| RUN_DLQ["Retry scoped processing, message log, or graph stage"]
    DLQ -->|No| SKIP

    KIND -->|Entity cleanup| CLEAN{"Cleanup interval elapsed?"}
    CLEAN -->|Yes| RUN_CLEAN["Remove invalid or stale orphan entities<br/>except merge-pending entities"]
    CLEAN -->|No| SKIP

    KIND -->|Fact archival| ARCHIVE{"Profile completed or fallback interval elapsed?"}
    ARCHIVE -->|Yes| RUN_ARCHIVE["Delete old invalidated facts"]
    ARCHIVE -->|No| SKIP

    KIND -->|AAC discussion| AAC{"Community enabled and interval elapsed?"}
    AAC -->|Yes| RUN_AAC["Trigger discussion and prune old discussions"]
    AAC -->|No| SKIP

    RUN_PROFILE --> RESULT
    RUN_MERGE --> RESULT
    RUN_TOPIC --> RESULT
    RUN_DLQ --> RESULT
    RUN_CLEAN --> RESULT
    RUN_ARCHIVE --> RESULT
    RUN_AAC --> RESULT["Emit success, failure, or timeout event"]
```

## 9. The Shortest Mental Model

```mermaid
flowchart LR
    USER["User"] --> CONTEXT["Session Context"]
    CONTEXT -->|Answer now| AGENT["Agent tool loop"]
    CONTEXT -->|Learn later| BUFFER["Redis ingestion buffer"]
    BUFFER --> PIPELINE["Extract and resolve"]
    PIPELINE --> SQL["Canonical Postgres knowledge"]
    SQL --> AGE["AGE traversal projection"]
    SQL --> SEARCH["Vector and text search projections"]
    SQL --> JOBS["Project maintenance jobs"]
    AGE --> AGENT
    SEARCH --> AGENT
    JOBS --> SQL
```

In one sentence: Knoggin answers from project-scoped evidence while a separate
asynchronous learning loop turns new messages into a source-grounded,
repairable knowledge index.

## Code Anchors

- [`src/infrastructure/resources.py`](../src/infrastructure/resources.py)
- [`src/knoggin_server/project/project_manager.py`](../src/knoggin_server/project/project_manager.py)
- [`src/knoggin_server/project/state.py`](../src/knoggin_server/project/state.py)
- [`src/knoggin_server/session/session_manager.py`](../src/knoggin_server/session/session_manager.py)
- [`src/knoggin_server/session/boot.py`](../src/knoggin_server/session/boot.py)
- [`src/knoggin_server/session/context.py`](../src/knoggin_server/session/context.py)
- [`src/knoggin_server/ingestion/services/batch_consumer.py`](../src/knoggin_server/ingestion/services/batch_consumer.py)
- [`src/knoggin_server/ingestion/services/pipeline_service.py`](../src/knoggin_server/ingestion/services/pipeline_service.py)
- [`src/knoggin_server/ingestion/services/processor.py`](../src/knoggin_server/ingestion/services/processor.py)
- [`src/knoggin_server/knowledge/db/write_graph_db.py`](../src/knoggin_server/knowledge/db/write_graph_db.py)
- [`src/infrastructure/graph_client.py`](../src/infrastructure/graph_client.py)
- [`src/infrastructure/schema.sql`](../src/infrastructure/schema.sql)
- [`src/knoggin_server/agent/orchestrator.py`](../src/knoggin_server/agent/orchestrator.py)
- [`src/knoggin_server/agent/executor.py`](../src/knoggin_server/agent/executor.py)
- [`src/infrastructure/job/scheduler.py`](../src/infrastructure/job/scheduler.py)
