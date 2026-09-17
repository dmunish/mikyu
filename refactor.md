# mikyu — Refactor & Hardening Plan

## Premise

mikyu is pre-release. No users, no migration obligations, no compatibility contracts. This plan executes **one break**, taken deliberately, exactly once.

Three commitments:

1. **One storage engine, two genuinely separate drivers.** Today the log and task drivers can share one physical queue and politely skip each other's records. After this plan, they cannot. `engine.Storage` is unchanged; the coupling built on top of it is removed.
2. **Kind is structural.** A queue's kind is the directory it lives in. No sidecar, no magic prefix, no cross-driver discriminator. The filesystem is the source of truth.
3. **Simplicity is a feature.** Every mechanism that exists only to reconcile the two drivers over a shared stream is deleted. Every mechanism that survives is justified by an end user's benefit.

Nothing in this plan adds a distributed concept, a broker, or a second storage engine. The system remains single-node, local-first, embedded-first.

---

# Part 1 — The Design

## 1.1 Directory layout

Every queue is exactly one kind. The parent directory **is** the kind.

```
<data_dir>/
  log/
    events/
      00000000000000000000.log
      00000000000000000000.idx
      ...
  task/
    jobs/
      00000000000000000000.log
      00000000000000000000.idx
      ...
  queues.json          # cache of {name → kind, spec}; path is authoritative
  mikyu.lock
  manager-offsets.dat
```

- A directory under `log/` cannot be a task queue.
- `queues.json` caches kind for `List` and `Describe`. If the cache disagrees with the path, the path wins and the registry is rewritten.
- `log/events` and `task/events` are both legal; the bare name `events` refers to neither until disambiguated by kind.
- Moving `log/events` to `task/events` is a user error. On load, the task driver scans records, finds no task record kind tag, and reports `ErrCorruptRecord` with a message that names the directory. (See §2.4.)

## 1.2 Naming

**Types are interfaces at the boundary; concrete types are unexported.**

| Layer | Log | Task |
|---|---|---|
| Interface (SDK-visible) | `logqueue.Log` | `taskqueue.Queue` |
| Concrete (internal) | `logqueue.queue` | `taskqueue.queue` |
| Constructor | `logqueue.New(...) (Log, error)` | `taskqueue.New(...) (Queue, error)` |
| Element struct | `engine.Record` / `mikyu.Record` | `taskqueue.Task` / `mikyu.Task` |
| Element reference | — | `taskqueue.Ref` / `mikyu.TaskRef` |
| Bare identifier | — | `taskqueue.ID` / `mikyu.TaskID` |

**Method names.**

| Before | After |
|---|---|
| `engine.Message` | `engine.Record` |
| `protocol.LogRecord` | `protocol.Record` |
| `WriteLogRecord` / `ReadLogRecord` | `WriteRecord` / `ReadRecord` |
| `Manager.CreateQueue` | `Manager.Create` |
| `Manager.GetQueue` | `Manager.Get` |
| `Manager.DeleteQueue` | `Manager.Delete` |
| `Manager.ListQueues` | `Manager.List` |
| `retention.DriverProvider` | `retention.FloorProvider` |
| `retention.QueueTarget` | `retention.Target` |
| `retention.Target.Drivers []DriverProvider` | `retention.Target.Driver FloorProvider` |
| `retention.Coordinator.ReconcileQueueAt` | `ReconcileAt` |

**SDK surface (unchanged verbs, retyped).**

```go
// mikyu.Client — all four names stay.
CreateLogQueue(ctx, name, opts...) (LogQueue, error)
CreateTaskQueue(ctx, name, opts...) (TaskQueue, error)
LogQueue(name) (LogQueue, error)
TaskQueue(name) (TaskQueue, error)
```

`Create*` returns the handle it just created. `LogQueue`/`TaskQueue` return a handle to an existing queue or `ErrQueueNotFound`. There is no upsert. The distinction between *declare* and *get a handle* is a feature: boot declares, runtime gets. (See §1.6.)

Type aliases in the facade:

```go
type LogQueue  = logqueue.Log
type TaskQueue = taskqueue.Queue
type Record    = engine.Record
type Task      = taskqueue.Task
type TaskRef   = taskqueue.Ref
type TaskID    = taskqueue.ID
```

`mikyu.LogQueue` and `mikyu.TaskQueue` are interfaces. Users can substitute fakes and wrappers exactly as today.

## 1.3 Manager — one lookup, strict access

`Manager.Get` auto-determines kind by path, because the path is authoritative:

```go
type Kind uint8
const (
    KindLog  Kind = 1
    KindTask Kind = 2
)

// Get returns the storage for name and the kind the directory says it is.
// Checks log/<name> first, then task/<name>. Returns ErrQueueNotFound if neither exists.
func (m *Manager) Get(name string) (Storage, Kind, error)

func (m *Manager) Create(ctx context.Context, name string, kind Kind, cfg Config) (Storage, error)
func (m *Manager) Delete(ctx context.Context, name string) error
func (m *Manager) List(ctx context.Context) ([]QueueInfo, error)

type QueueInfo struct {
    Name string
    Kind Kind
}
```

Typed access is enforced at the SDK layer:

```go
func (c *embeddedClient) LogQueue(name string) (logqueue.Log, error) {
    storage, kind, err := c.node.Manager().Get(name)
    if err != nil { return nil, err }
    if kind != manager.KindLog { return nil, ErrWrongKind }
    return logqueue.New(name, storage), nil
}
```

A task-queue name asked of `LogQueue` returns `ErrWrongKind` naming the directory. Never a silent reinterpretation.

## 1.4 Opcodes

Continuous nibble blocks, one subsystem each. Frozen going forward.

| Range | Subsystem |
|---|---|
| `0x00–0x0F` | Admin |
| `0x10–0x1F` | Log |
| `0x20–0x2F` | Task |
| `0x30–0x3F` | Dead-letter |
| `0x70–0x7F` | Protocol |
| `0xF0–0xFF` | Error |

**Admin (`0x00–0x0F`)**

| Code | Op | Purpose |
|---|---|---|
| `0x00` | `OpCreate` | Create a queue (kind in body) |
| `0x01` | `OpDelete` | Delete a queue |
| `0x02` | `OpList` | List queues |
| `0x03` | `OpUpdate` | Update a queue's spec |
| `0x04` | `OpDescribe` | Describe a queue |
| `0x05` | `OpTruncate` | Truncate a log queue |
| `0x06` | — | Reserved |
| `0x07` | `OpMetrics` | Live telemetry snapshot |
| `0x08` | `OpSchema` | Parameter table |
| `0x09–0x0B` | — | Reserved |
| `0x0C` | `OpShutdown` | Graceful shutdown |
| `0x0D–0x0F` | — | Reserved |

**Log (`0x10–0x1F`)**

| Code | Op | Purpose |
|---|---|---|
| `0x10` | `OpAppend` | Append one record |
| `0x11` | `OpAppendBatch` | Append many records |
| `0x12` | `OpRead` | Read one record by offset |
| `0x13` | `OpReadBatch` | Read many records by offset |
| `0x14` | `OpLastOffset` | Last offset |
| `0x15` | `OpMinOffset` | Minimum readable offset |
| `0x16` | `OpCommit` | Commit consumer-group offset |
| `0x17` | `OpCommitted` | Read consumer-group offset |
| `0x18` | `OpFetch` | Long-polling fetch |
| `0x19–0x1F` | — | Reserved |

**Task (`0x20–0x2F`)**

| Code | Op | Purpose |
|---|---|---|
| `0x20` | `OpEnqueue` | Enqueue a task |
| `0x21` | `OpDequeue` | Dequeue (long-poll) |
| `0x22` | `OpAck` | Acknowledge |
| `0x23` | `OpNack` | Negative acknowledge |
| `0x24` | `OpExtend` | Extend visibility |
| `0x25` | `OpArchive` | Archive (in-flight or DLQ) |
| `0x26–0x2F` | — | Reserved |

**Dead-letter (`0x30–0x3F`)**

| Code | Op | Purpose |
|---|---|---|
| `0x30` | `OpDeadLetters` | List DLQ entries |
| `0x31` | `OpDeadLetterAck` | Discard a DLQ entry |
| `0x32–0x3F` | — | Reserved |

**Protocol (`0x70–0x7F`)**: `0x7E OpAuth`, rest reserved.

**Error (`0xF0–0xFF`)**: `0xFF OpError`, rest reserved.

Error *codes* keep their existing block structure (`0x01–0x0F` admin, `0x11–0x1F` storage, `0x21–0x2F` task, `0x31–0x3F` dead-letter, `0x41–0x4F` server). New codes assigned in Phase 3 fill existing blocks.

## 1.5 Durability invariants

**Single writer.** Exactly one goroutine writes bytes to the WAL file. Every other path enqueues. Same rule for offset-store compaction and each queue's segment writer. Documented in `wal.go`, enforced by a test hook.

**Directory fsyncs at four sites.**

1. After creating a new segment file.
2. After `os.Rename` in offset-store compaction.
3. After `os.Rename` in `queues.json` writes.
4. After `Close()` truncates the active segment.

A `syncDir(parent *os.Root)` helper mirrors the existing per-OS `syncData` files (`engine/sync_*.go`). On Windows it is a no-op with a documented reason.

**Shutdown contract.**

```
OpShutdown received
  → Server.Stop         (stop accepting, drain in-flight handlers)
  → Node.Close          (stop new ops, release queues)
  → per-queue           (stop WAL flusher: drain pending, final sync)
  → per-queue           (close active segment: truncate to pos, fsync dir)
  → release directory lock
```

Parked waiters:

- In-flight `WAL.push` — completes or fails with `ErrQueueClosed`.
- Parked `Fetch` — wakes with `ErrQueueClosed`.
- Parked `Dequeue` — wakes with `ErrQueueClosed`.
- Parked `Tail` / `Stream` — yields `ErrQueueClosed` once, stops.

**Backpressure.** `asyncChan` capacity is `BatchSize`. At full, the producer blocks. Cancellation returns `ctx.Err()`. No drop, no silent error.

**Error taxonomy.** Every sentinel has exactly one wire code; every wire code maps to exactly one sentinel. A test fails on any gap. Messages carry sentinel and context: `"queue %q is a task queue; append requires a log queue"`, not `"wrong kind"`.

New codes:

| Sentinel | Block |
|---|---|
| `ErrWrongKind` | `0x4X` server |
| `ErrDiskFull` | `0x1X` storage |
| `ErrCorruptRecord` | `0x1X` storage |
| `ErrCorruptCheckpoint` | `0x1X` storage |
| `ErrQueueFull` | `0x2X` task |
| `ErrRecordTooLarge` | `0x1X` storage |
| `ErrRecordCompacted` | `0x1X` storage |

## 1.6 The two-phase client lifecycle

```go
// Boot: declare. Returns the handle directly.
events, err := db.CreateLogQueue(ctx, "events", mikyu.WithSegmentSize(1<<26))
jobs,   err := db.CreateTaskQueue(ctx, "jobs",
    mikyu.WithDefaultVisibility(time.Minute),
    mikyu.WithDefaultMaxAttempts(5),
)

// Runtime: get handles. No writes, no surprises.
events, err := db.LogQueue("events")
jobs,   err := db.TaskQueue("jobs")
```

Why not fold them into a single upsert:

- Options on an existing queue raise an unresolvable contract problem (ignore silently, mutate on read, or error on drift — each is a real footgun).
- A read-only "give me a handle" call must never write. Admin dashboards and health checks call `LogQueue`; if it can recreate a deleted queue, it silently masks deletion.
- Two concurrent `LogQueue(name, opts...)` calls race into `ErrQueueExists`; the retry path grows a kind-check and a spec-drift check. A two-line get-then-create at the application layer is easier to write correctly than a correct upsert in the SDK.

If the pattern turns out to be ubiquitous, a named helper can be added later with an explicit contract. Starting strict is reversible; starting with upsert is not.

## 1.7 Features

Four features ride the break. All opt-in. All single-node.

**Task queues — stalled-job model.** A visibility-expired lease is a distinct failure from an explicit nack. `Task.Stalled uint32` increments each time a task is reclaimed after its visibility expired. Persisted in checkpoints and DLQ records. Metric `task_stalls` on the queue scope. A stall counts against `MaxAttempts` (which is already true — expiry increments `Attempts`). `Stalled` does **not** participate in the strict lease check: it is observation, not a delivery token.

**Task queues — bounded state.** New spec fields:

- `task.max_in_flight` — default 100 000. `Enqueue` beyond returns `ErrQueueFull`.
- `task.max_dlq` — default 100 000. Overflow drops the oldest DLQ entry and increments a `dlq_overflow` counter. Lossy by design.
- Cap of `0` means unlimited; documented as opting out of the safety property.

Active and DLQ counts travel in the checkpoint so recovery can verify them.

**Task queues — archive as an explicit action.** Archiving is a user-triggered terminal operation, a peer to `Ack`.

```
process(task)
  ├── Ack(ref)         → discard
  ├── Archive(ref)     → preserve to <name>-archive
  └── Nack(ref)        → retry, eventually DLQ
                          ├── AckDeadLetter(ref)  → discard
                          └── Archive(ref)        → preserve
```

Single method: `taskqueue.Queue.Archive(ctx, ref Ref) error`. The ref resolves to an in-flight task (search `active` first, run the strict check) or a DLQ entry (search `dlq` next, check `Offset` and `Attempts`). One method, one wire op (`OpArchive`), one envelope, one code path.

`<name>-archive` is lazily created as a log queue on first `Archive` if and only if no queue by that name exists. Uses the node's **session defaults** — same defaults any user-created queue gets, consistent with the goal.md rule that session defaults are ambient. If the user wants a different configuration, they create `<name>-archive` explicitly as a log queue *before* the first `Archive` call; the lazy auto-create only fires when the name is free.

Envelope written to the archive:

```
{
  task_id,
  source_queue,
  key,
  payload,
  attempts,
  stalled,
  delivery_id,      // empty for DLQ
  offset,           // original offset in the source task queue
  archived_from,    // "inflight" | "dlq"
  timestamp
}
```

Failure semantics: the archive append is synchronous with the state change. Acquire the task-queue lock, validate the ref, append to the archive log, and only on success remove the task from its current state and mark `dirty`. If the append fails, the task stays where it was and the caller sees the error.

At `Fsync: true`, archive costs ~293 µs; at `Fsync: false`, ~26 µs. For high-throughput paths, use `Ack` for most tasks and `Archive` selectively. Documented as guidance, not enforced.

**Log queues — opt-in compaction.** For each record key, retain only the most recent record. Offsets are preserved; superseded records become gaps.

```go
log.Append(ctx, payload, WithKey([]byte("user:1")))
client.UpdateQueue(ctx, "users", mikyu.WithCompaction(true))
```

Compaction runs only on **closed segments, at roll time**. Never the active segment. A closed segment is fully written and no live tail is reading it — a hard, local, verifiable boundary. No cleaner thread, no watermarks.

Record format gains an optional group field:

```
[4B body_len][4B crc][2B id_len][16B uuid][8B ts][2B group_len][group][payload]
```

`group_len == 0` for records without a key. The engine exposes compaction as a generic grouping function:

```go
type CompactionPolicy struct {
    ExtractGroup func(payload []byte) []byte
}
```

During roll-time compaction, the engine calls `ExtractGroup` on each record's payload, compares the returned bytes for equality, and keeps the latest per group. The engine knows grouping. It does not know what a key is. `logqueue` defines "key" as the user's bytes and wires the extractor to read the group field.

The key index is `map[string]uint64` of latest-offset-per-group, rebuilt by scanning segments on `openQueue`. Target working set: ≤1 M unique keys. A metric and log line report the index size at open time if it exceeds this.

Reads: `Read(offset)` of a compacted offset returns `engine.ErrRecordCompacted`. `Stream` and `Tail` skip compacted offsets silently. Group offsets are never renumbered; a group's committed offset stays valid; `MinOffset` reports the oldest non-truncated offset.

**Keyed records — one noun, two meanings.**

Same SDK name, same wire representation, different internal meaning.

- **taskqueue:** identity metadata. Stored in the driver-owned task record body. Propagated to the archive envelope. No engine involvement.
- **logqueue:** compaction group. Stored in the engine record's group field, read by the compaction extractor.

The user sees one concept: a key identifies the subject of a record. The engine sees grouping and nothing more.

**The composition.** With `WithKey` on enqueue, `Archive` instead of `Ack`, and compaction enabled on `<name>-archive`, the archive becomes a materialized table of latest outcome per subject. Three explicit user actions, no config bundle. This is the README's headline example.

## 1.8 Wire protocol changes

**Breaking:**

- `OpCreate` body: `u8 kind`, `bytes(name)`, `u16 specVersion`, `bytes(json Spec)`.
- `QueueDescription` gains `kind`.
- `OpList` response carries per-entry `kind`.
- `QueueView.Kind` becomes `"log" | "task" | "internal"`.
- `OpCreateQueue` → `OpCreate`; `OpListQueues` → `OpList`; `OpDescribeQueue` → `OpDescribe`.
- New ops: `OpMinOffset`, `OpArchive`.
- `OpArchive` uses the same `TaskRef` envelope (mode byte + payload) as `Ack`. Mode 0 searches `active` first (strict check), then `dlq` (Offset + Attempts). Mode 1 searches `active` first, then `dlq`. Missing ID returns `ErrTaskNotFound`; a stale mode-0 ref returns `ErrTaskStale` (from active) or `ErrDeadLetterStale` (from DLQ).
- `specVersion` bumped. Old clients rejected with `ErrCodeSpecTooNew`. No shim.
- Checkpoint format version bumped to `3`.
- Log record format gains group field.
- Task record drops magic; keeps a 1-byte kind tag.
- New error codes from §1.5.

**Unchanged:** frame layout, `OpAuth`, `OpFetch`, `OpEnqueue`, `OpDequeue`, `OpAck`, `OpNack`, `OpExtend`, `OpDeadLetters`, `OpDeadLetterAck`, `OpSchema`, `OpMetrics`, `OpShutdown`, error code block structure.

---

# Part 2 — Phased Implementation

Six phases. Each is one commit series (or one feature branch). Later phases assume earlier phases have merged.

---

## Phase 1 — Rename pass

Pure mechanical renames. No behavior change. Type-checking and existing tests must pass unchanged at the end.

### New

- `internal/engine/sync_dir.go` — declares `syncDir(root *os.Root) error`, delegates to per-OS files.
- `internal/engine/sync_dir_unix.go` — `fsync` on the root's `Fd()`.
- `internal/engine/sync_dir_windows.go` — no-op with a documented reason.

```go
// sync_dir.go
func syncDir(root *os.Root) error { return syncDirOS(root) }
```

### Updated

- `engine.Message` → `engine.Record`. All usages.
- `protocol.LogRecord` → `protocol.Record`.
- `protocol.WriteLogRecord` / `ReadLogRecord` → `WriteRecord` / `ReadRecord`.
- `logqueue.LogQueue` → `logqueue.Log` (now the interface). Concrete type renamed `logqueue.queue` (unexported). `logqueue.ClientLogQueue` is deleted — the interface *is* `Log`.
- `logqueue.NewLogQueue` → `logqueue.New` returning `Log`.
- `logqueue.Log.Fetch` moved onto the `Log` interface.
- `taskqueue.TaskQueue` → `taskqueue.Queue` (interface). Concrete type `taskqueue.queue` (unexported).
- `taskqueue.NewTaskQueue` → `taskqueue.New` returning `Queue`.
- `taskqueue.TaskRef` → `taskqueue.Ref`; `taskqueue.TaskID` → `taskqueue.ID`.
- `Manager.CreateQueue` → `Manager.Create`; `GetQueue` → `Get`; `DeleteQueue` → `Delete`; `ListQueues` → `List`.
- `retention.DriverProvider` → `retention.FloorProvider`; `QueueTarget` → `Target`; `Drivers []DriverProvider` → `Driver FloorProvider`; `ReconcileQueueAt` → `ReconcileAt`.
- SDK aliases: `mikyu.LogQueue = logqueue.Log`, `mikyu.TaskQueue = taskqueue.Queue`, `mikyu.Record = engine.Record`, `mikyu.TaskRef = taskqueue.Ref`, `mikyu.TaskID = taskqueue.ID`.

Interface definition shape:

```go
// internal/queue/logqueue/logqueue.go
type Log interface {
    Name() string
    Append(ctx context.Context, payload []byte) (uint64, error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (engine.Record, error)
    ReadBatch(ctx context.Context, start uint64, n int) ([]engine.Record, error)
    LastOffset(ctx context.Context) (uint64, error)
    MinOffset(ctx context.Context) (uint64, error)
    Truncate(ctx context.Context, minOffset uint64) (int64, error)
    Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error]
    Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error]
    Group(name string) Group
}

type queue struct { /* same fields as today */ }
func New(name string, storage engine.Storage) Log { return &queue{...} }
```

### Deleted

- `logqueue.ClientLogQueue` (superseded by `Log`).
- `taskqueue.ClientTaskQueue` (superseded by `Queue`).
- Any code referencing the old names.

### Testing

- Existing test suite passes with renames applied.
- `TestNoLegacyNames` (audit) extended to ban the old identifiers.
- `TestSchemaGateParity` and `TestSchemaIsTotalAndStable` unchanged and passing.
- No new test files. This phase is a rename.

---

## Phase 2 — The split and wire reorganization

The one break. Directory-as-kind; the shared-queue machinery deleted; opcode layout frozen in its final shape.

### New

- `internal/node/kind.go`:

```go
type Kind uint8
const (
    KindLog  Kind = 1
    KindTask Kind = 2
)

func (k Kind) String() string {
    switch k {
    case KindLog:  return "log"
    case KindTask: return "task"
    default:       return "unknown"
    }
}

func dirFor(k Kind) string {
    switch k {
    case KindLog:  return "log"
    case KindTask: return "task"
    }
    return ""
}
```

- `internal/manager/manager.go` — the storage manager moves out of `engine` if it hasn't already. Method signatures:

```go
func (m *Manager) Get(name string) (engine.Storage, Kind, error)
func (m *Manager) Create(ctx context.Context, name string, kind Kind, cfg engine.Config) (engine.Storage, error)
func (m *Manager) Delete(ctx context.Context, name string) error
func (m *Manager) List(ctx context.Context) ([]QueueInfo, error)
```

`Get` checks `log/<name>` first, then `task/<name>`. `Create` writes to `<dirFor(kind)>/<name>`. Registry is cache-only; mismatch corrected to the path and rewritten.

### Updated

- `queues.json` schema: `{version: 2, queues: {name: {kind: "log"|"task", spec: {...}}}}`.
- `node.CreateQueue` → `node.CreateLogQueue` and `node.CreateTaskQueue`, each returning the driver.
- `node.LogQueue(name)` returns `(logqueue.Log, error)`; `node.TaskQueue(name)` returns `(taskqueue.Queue, error)`. Both enforce kind via `Manager.Get`.
- `retention.Coordinator` loses the multi-driver arbitration loop:

```go
// Before: min over []DriverProvider
// After: a single FloorProvider per target
type Target struct {
    Name    string
    Storage engine.Storage
    Driver  FloorProvider
    Policy  Policy
}
```

Reconcile consults the single driver's `TruncateFloor` and clamps accordingly.

- Opcode table reorganized per §1.4. Existing `0x01–0x08` admin ops are renumbered; `OpMinOffset` and `OpArchive` are added in their blocks.
- `OpCreate` body: `u8 kind`, `bytes(name)`, `u16 specVersion`, `bytes(json)`.
- `OpList` response: `u32 count`, then per-entry `bytes(name) u8 kind`.
- `QueueView.Kind` becomes `"log" | "task" | "internal"`.
- SDK `Client` interface gains the four-way split (below).

```go
type Client interface {
    io.Closer
    CreateLogQueue(ctx context.Context, name string, opts ...QueueOption) (LogQueue, error)
    CreateTaskQueue(ctx context.Context, name string, opts ...QueueOption) (TaskQueue, error)
    LogQueue(name string) (LogQueue, error)
    TaskQueue(name string) (TaskQueue, error)
    UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error
    DescribeQueue(ctx context.Context, name string) (QueueDescription, error)
    DeleteQueue(ctx context.Context, name string) error
    ListQueues(ctx context.Context) ([]QueueInfo, error)
    Truncate(ctx context.Context, name string, minOffset uint64) (int64, error)
    Metrics(ctx context.Context) (Metrics, error)
    Schema(ctx context.Context) ([]Param, error)
    SubscribeSnapshots() (<-chan Metrics, func())
}
```

### Deleted

- `MIKYUTQ\x01` magic constant and every reference.
- The 8-byte prefix on task records. Task records keep a 1-byte kind tag: `recordTask`, `recordDLQ`, `recordDLQAck`, `recordCPStart`, `recordCPChunk`, `recordCPCommit`.
- `taskqueue.refreshLocked`'s "foreign record" skip branch. An unknown task record kind is now `ErrCorruptRecord`.
- `retention.Target.Drivers []DriverProvider` and the `min(floors)` loop.
- `QueueView.Kind: "shared"` and the `mergeScopes` coupling that produced it.
- `Manager.GetQueue`, `Manager.CreateQueue`, `Manager.DeleteQueue`, `Manager.ListQueues`.
- `NewLogQueue`, `NewTaskQueue`.
- Every code path that opens a queue without a kind.

### Testing

- **New:** `TestKindByPath` — creates `log/events` and `task/events`, asserts `Manager.Get` reports the right kind for each.
- **New:** `TestMoveQueueDirectoryFailsLoad` — moves a log queue's directory under `task/`, opens the node, asserts `ErrCorruptRecord` on first task-driver access with a message naming the directory.
- **New:** `TestWrongKindRefused` — `db.LogQueue("jobs")` returns `ErrWrongKind` when `jobs` is a task queue.
- **New:** `TestCreateReturnsHandle` — `db.CreateLogQueue(ctx, "q")` returns a usable handle on the same call.
- **New:** `TestOpcodeNibbleGrouping` — every opcode in the frozen table lives in exactly its declared nibble; no duplicates.
- **New:** `TestListCarriesKind` — remote `ListQueues` returns kind for each entry; embedded matches.
- **Updated:** `TestRegistryRoundTrip`, `TestRegistryRejectsUnknownFields` — schema v2.
- **Updated:** `TestManagerRejectsEscape` — `Create(ctx, "../escape", ...)` still fails.
- **Updated:** `TestParityEndToEnd` — passes with new SDK signatures.
- **Deleted:** `TestTaskQueueIgnoresForeignLogRecords`, `TestTaskQueueCheckpointIncludesForeignRecords` — no longer meaningful.
- **Deleted:** `TestEmbeddedRetentionCoordinatesLogAndTaskDrivers` — replaced by `TestRetentionHonorsSingleDriver`.

Phase 2 is the largest single change. Land it as one commit with all renames and signature changes together, so nothing is half-split.

---

## Phase 3 — Durability hardening

No new features. Every item is an invariant made explicit and tested.

### New

- Single-writer test hook in `engine/wal.go`:

```go
var writerHook func()

func (w *wal) writeAt(p []byte, off int64) (int, error) {
    if writerHook != nil { writerHook() }
    return w.file.WriteAt(p, off)
}
```

`TestSingleWriter` installs a hook that records the goroutine ID and fails if two distinct IDs ever appear.

- `TestDirectoryFsyncSites` — a build-tag-instrumented `syncDir` records each call; the test asserts the four sites fire during a scripted workload (create queue, append, roll, compact offsets, close).

### Updated

- `wal.go`: single-writer invariant documented at the type. Every `push`/`pushBatch` continues to enqueue only; `flush` and `drainAndFlushAsync` are the only callers of `writeAt`.
- `offsets.go`: compaction documented and instrumented the same way. `compactLocked` is the only writer.
- `segment.go`: `writeIndexAt` and `writeIndex` documented as called only from the WAL flusher.
- Four sites call `syncDir(parentRoot)` after their rename or create step.
- `wal.go` doc comment: async backpressure contract (batch-size capacity, block at full, `ctx.Err()` on cancel).
- `server.go` / `node.go` doc comment: shutdown contract with the four wake conditions.
- `internal/client/errors.go` and `internal/server/errors.go` extended with the new codes from §1.5.
- Error message text at every site that raises a new sentinel.

### Deleted

- Any place that silently swallows an `fsync` failure.

### Testing

- **New:** `TestSingleWriter` (above).
- **New:** `TestDirectoryFsyncSites` (above).
- **New:** `TestBackpressureBlocksProducer` — fill `asyncChan`, assert the next push blocks; cancel and assert `ctx.Err()`.
- **New:** `TestShutdownWakesParkedFetch` — park a `Fetch`, call `Node.Close`, assert `ErrQueueClosed`.
- **New:** `TestShutdownWakesParkedDequeue` — same shape.
- **New:** `TestShutdownWakesTail` — same shape; the iterator yields `ErrQueueClosed` once and terminates.
- **New:** `TestErrorTaxonomyTotal` — iterate every sentinel declared in the error packages, look each up in the code map, assert an entry exists; iterate the map, assert each code maps to a declared sentinel. Any gap fails.
- **Updated:** `TestQueueRealCrashRecovery` — still passes; now also asserts the fsync sites fired.

---

## Phase 4 — Task queue evolution and archive

Bounded state, stalled jobs, keyed tasks, and the archive feature land together because they all touch the task record format and the checkpoint (version bump to 3).

### New

- `taskqueue.Task.Stalled uint32` field.
- `taskqueue.Queue.Archive(ctx, ref Ref) error`.
- `spec.TaskSpec.MaxInFlight *uint32` and `spec.TaskSpec.MaxDLQ *uint32`.
- `mikyu.WithMaxInFlight(n)`, `mikyu.WithMaxDLQ(n)`.
- `mikyu.WithKey(k []byte)` — returns an option consumed by `Enqueue`. Stored in the task record body.
- `OpArchive` handler in `internal/server/handlers_task.go`.
- `internal/queue/taskqueue/archive.go` with the archive logic:

```go
func (q *queue) Archive(ctx context.Context, ref Ref) error {
    q.mu.Lock()
    defer q.mu.Unlock()
    if q.closed || q.closing { return ErrTaskClosed }
    if err := q.refreshLocked(ctx); err != nil { return err }

    // Resolve: active first (strict), then dlq.
    if state, err := q.resolveLeaseLocked(ref); err == nil {
        return q.archiveActiveLocked(ctx, state, "inflight")
    } else if !errors.Is(err, ErrTaskNotFound) {
        return err // stale or not-inflight: don't fall through
    }
    if dl, ok := q.dlq[ref.taskID()]; ok {
        return q.archiveDLLocked(ctx, dl, "dlq")
    }
    return ErrTaskNotFound
}

func (q *queue) archiveActiveLocked(ctx context.Context, state *taskState, from string) error {
    env := encodeArchiveEnvelope(state.task, q.name, from)
    if _, err := q.archiveLog.Append(ctx, env); err != nil { return err }
    delete(q.active, state.task.ID)
    q.dirty++
    q.broadcastLocked()
    q.pruneScheduleLocked()
    return nil
}
```

- Archive envelope encoder/decoder in `internal/queue/taskqueue/archive.go`.
- `node.archiveLogFor(name)` — lazily creates `<name>-archive` as a log queue on first use, using session defaults. If the name is already taken by a task queue, returns `ErrWrongKind`.

```go
func (n *Node) archiveLogFor(ctx context.Context, source string) (logqueue.Log, error) {
    name := source + "-archive"
    storage, kind, err := n.mgr.Get(name)
    switch {
    case err == nil && kind == KindLog:
        return n.logQueue(name)
    case err == nil && kind != KindLog:
        return nil, fmt.Errorf("%w: %q exists as a %s queue", ErrWrongKind, name, kind)
    case errors.Is(err, engine.ErrQueueNotFound):
        return n.CreateLogQueue(ctx, name) // session defaults
    default:
        return nil, err
    }
}
```

- CLI: `mikyu archive <task|json|id> --queue <name>`. Accepts an in-flight ref or a DLQ ref.
- REPL: `archive` verb dispatch.

### Updated

- Task record body: drops the 8-byte magic prefix, keeps a 1-byte kind tag, adds `key_len u32` + key bytes, adds `stalled u32`.
- Checkpoint format version 3: adds `stalled`, `active_count`, `dlq_count`; recovery verifies counts against the decoded maps.
- DLQ record format: adds `stalled` and `key`.
- `Task` struct and wire DTO: `Stalled`, `Key` fields.
- `OpArchive` payload reuses the `TaskRef` envelope; the server routes mode 0 to active-then-dlq, mode 1 to active-then-dlq.
- `Enqueue`: checks `len(active) >= maxInFlight` before append; returns `ErrQueueFull`.
- DLQ admission: if `len(dlq) >= maxDLQ`, drop the oldest entry by offset and increment `dlq_overflow`.
- `taskqueue.Queue.Ack` and `AckDeadLetter` unchanged — they still discard.

### Deleted

- The `MIKYUTQ\x01` magic constant, now fully superseded by the kind tag.
- Any code path that appends to a task queue without checking `max_in_flight`.

### Testing

- **New:** `TestStalledIncrementsOnVisibilityExpiry` — enqueue, dequeue, sleep past visibility, dequeue again; assert `Stalled == 1`, `Attempts == 2`.
- **New:** `TestStalledNotInLeaseIdentity` — a stale `Task` ref with a different `Stalled` value but matching `Offset`/`Attempts`/`DeliveryID` still acks.
- **New:** `TestMaxInFlightRefuses` — enqueue `maxInFlight + 1`; last returns `ErrQueueFull`.
- **New:** `TestMaxDLQDropsOldest` — configure `maxDLQ = 2`, DLQ three tasks, assert the oldest is gone and `dlq_overflow == 1`.
- **New:** `TestArchiveInFlight` — enqueue, dequeue, archive; assert task is gone from `active`, the envelope is readable from `<name>-archive`, and the envelope's `archived_from == "inflight"`.
- **New:** `TestArchiveFromDLQ` — nack to DLQ, archive the DLQ entry; envelope's `archived_from == "dlq"`, `delivery_id == ""`.
- **New:** `TestArchiveAutoCreatesLogQueue` — first archive creates `<name>-archive` as a log queue with session defaults.
- **New:** `TestArchiveExplicitPreCreationWins` — pre-create `<name>-archive` with custom retention; archive uses it.
- **New:** `TestArchiveRefusesWrongKind` — pre-create `<name>-archive` as a task queue; archive returns `ErrWrongKind`.
- **New:** `TestArchiveFailureLeavesTaskInPlace` — inject an append failure on the archive log; assert the task is still in `active`.
- **New:** `TestCheckpointRecoveryVerifiesCounts` — write a checkpoint with mismatched counts; recovery returns `ErrCorruptCheckpoint`.
- **New:** `TestWithKeyTaskRoundTrip` — enqueue with a key, dequeue, assert `Key` matches; archive, assert the envelope carries it.

---

## Phase 5 — Log queue evolution: keyed records and compaction

### New

- `engine.Record` gains an optional `Group []byte` field.
- Log record format: `[4B body_len][4B crc][2B id_len][16B uuid][8B ts][2B group_len][group][payload]`. `group_len == 0` means no key.
- `engine.CompactionPolicy`:

```go
type CompactionPolicy struct {
    ExtractGroup func(payload []byte) []byte
}

// compactClosedSegment drops every record whose group has a newer record in a
// later segment. Reads through the segment's index and rewrites the .log and
// .idx in place.
func (q *Queue) compactClosedSegment(seg *segment, policy CompactionPolicy) error
```

- `engine.Queue.keyIndex` — `map[string]uint64` maintained across closed segments. Built at `openQueue` by a single pass. Bounded to a documented target of 1 M entries; a metric and a `log.Printf` warn when exceeded.
- `logqueue` wires the extractor to read the record's group field.
- `spec.EngineSpec.Compaction *bool` — create-only, default off.
- `mikyu.WithCompaction(bool)` — log queue only; error on task queue at the SDK boundary.
- `mikyu.WithKey(k []byte)` on the log side returns an `AppendOption` consumed by `Append` / `AppendBatch`.
- `ErrRecordCompacted` sentinel, wire code `0x1X`.
- `OpMinOffset` handler.

### Updated

- `encodeRecord` and `decodeRecord` carry the group field.
- `queue.roll()` calls `compactClosedSegment` on the segment it just sealed, if the queue's config enables compaction. Never on the active segment.
- `queue.Read(offset)` returns `ErrRecordCompacted` for a compacted offset.
- `logqueue.Stream` / `Tail` skip compacted offsets silently.
- `openQueue` rebuilds the key index in a single pass over closed segments.
- `remoteLogQueue.MinOffset` calls `OpMinOffset` instead of reading the gauge from `Metrics`.

### Deleted

- The old implicit `MinOffset`-via-`Metrics` path.

### Testing

- **New:** `TestCompactionKeepsLatestPerKey` — append three records under the same key across a roll; assert only the last survives.
- **New:** `TestCompactionOnlyClosedSegments` — append to a single active segment; assert nothing is dropped.
- **New:** `TestReadCompactedOffsetReturnsErrRecordCompacted` — distinct from `ErrOffsetTruncated`.
- **New:** `TestStreamSkipsCompactedOffsets` — `Stream` yields only live records.
- **New:** `TestGroupOffsetsSurviveCompaction` — commit a group offset, compact, assert the committed offset is unchanged and reads from it work.
- **New:** `TestKeyIndexRebuildsOnOpen` — append with keys, close, reopen, assert the index is rebuilt and compaction still works.
- **New:** `TestKeyIndexSizeWarning` — configure >1 M distinct keys in a synthetic workload; assert the warning fires (or is skipped under a build tag to keep CI fast).
- **New:** `TestOpMinOffsetRoundTrip` — remote `MinOffset` returns the same value as embedded.
- **Updated:** `TestLogQueue_TruncateFloor_ConsumerGroups` — passes with the new `MinOffset` semantics.

---

## Phase 6 — Surface, wire finalization, docs

Everything user-facing. The wire layout is already frozen; this phase locks in the SDK, CLI, and REPL.

### New

- `mikyu --version` reports the new `specVersion`.
- CLI verbs: `create --type log|task <name>` (mandatory `--type`), `archive <ref>`, `append --key`, `enqueue --key`.
- REPL: `archive` verb, `use` inference of kind, `tail` refuses on task, `dequeue` refuses on log.
- README: the §1.7 composition example as the headline.
- `docs/getting-started.md` — the two-phase lifecycle, embedded → daemon → remote.

### Updated

- CLI `list` shows kind per row.
- CLI `describe` shows compaction status.
- CLI `metrics` shows `task_stalls`, `dlq_overflow`.
- SDK re-exports: `mikyu.ErrWrongKind`, `mikyu.ErrRecordCompacted`, `mikyu.ErrQueueFull`, `mikyu.ErrDiskFull`, `mikyu.ErrCorruptRecord`.
- `internal/audit/legacy_test.go` ban list extended: `LogQueue`, `TaskQueue`, `TaskRef`, `TaskID`, `Message` (in engine context), `MIKYUTQ`, `GetQueue`, `CreateQueue`, `DeleteQueue`, `ListQueues`, plus the old method names. Whitelist: `mikyu.LogQueue`, `mikyu.TaskQueue` (SDK aliases only).

### Deleted

- Any documentation referring to `OpCreateQueue`, `OpListQueues`, `OpDescribeQueue`.
- The old `queue("jobs")` sketch in the SDK docs (goal.md's `db.queue("jobs")` was aspirational; the two-phase lifecycle supersedes it).

### Testing

- **New:** `TestParityEveryOp` — for every opcode in the frozen table, drive it through embedded and remote clients and assert byte-identical responses (after stripping timestamps and machine-local values).
- **New:** `TestParityMixedKinds` — a directory with both log and task queues; `List`, `Describe`, and `Metrics` report correct kinds on both surfaces.
- **New:** `TestParityEveryError` — for every sentinel, force the server to raise it and assert the client decodes it to the same sentinel with the detail preserved.
- **New:** `TestCLIArchiveRoundTrip` — enqueue, dequeue, archive, read the archive log; the payload and envelope fields survive.
- **New:** `TestREPLKindInference` — `use events` then `append` works; `use jobs` then `dequeue` works; `use events` then `dequeue` returns a clear error.
- **New:** `TestCLICreateRequiresType` — `create events` without `--type` fails; `create --type log events` succeeds.
- **Updated:** `TestSchemaOverWire` — passes; wire schema matches after the spec version bump.
- **Updated:** `TestEmbeddedMetricsMatchesRemote` — extended to cover both kinds.
- **Updated:** `TestSubscribeSnapshotsRemoteHonorsInterval` — unchanged; still passes.

### Definition of done

- All renames landed; audit gate tightened; no legacy names in the codebase.
- Two separate driver directories; no shared physical queue; magic deleted.
- Opcode layout matches §1.4; `TestOpcodeNibbleGrouping` passes.
- Single-writer invariant tested.
- Directory fsyncs in place at all four sites.
- Shutdown and backpressure contracts documented and tested.
- Error taxonomy total; new sentinels and codes present; messages written.
- Task state bounded; checkpoint version 3; `queues.json` v2.
- Stalled-job model present with metric and DLQ field.
- `Archive` implemented end-to-end; lazy auto-create of `<name>-archive`; envelope verified.
- `WithKey` on both drivers; engine knows grouping, not keys.
- Log compaction implemented; `ErrRecordCompacted` distinct from `ErrOffsetTruncated`; consumer offsets unaffected.
- Wire bumped; old clients refused cleanly.
- Parity harness covers every op, both kinds, both modes.
- CLI and REPL surfaces match §1.7.
- README documents the composition example from §1.7.
- `mikyu --version` reports the new spec version.

---

# Deferred

Real, named, out of scope for this break.

- **Torn-write recovery audit** per file type — post-split, standalone.
- **Disk-full behavioral policy** beyond assigning `ErrDiskFull` — the response (does `Append` return immediately? does the queue lock?) is its own design.
- **Startup time profiling** — 250 ms is acceptable; if `OpenManager` I/O dominates, it is a `pprof` investigation later.
- **First-60-seconds measurement** — after the SDK surface freezes.
- **Documentation rewrite** beyond README.
- **Redis-style consumer-group recovery** — not adopted. "Resume from committed offset" is sufficient; the application decides how to handle read-but-uncommitted.
- **BullMQ group / fair scheduling** — `WithKey` enables it later; not built now.
- **Persisted compaction key index** — in-memory rebuild on open is sufficient at current scale.
- **Async archive** — `Archive` is synchronous. At high throughput, use `Ack` for most tasks and `Archive` selectively. Async archive with a bounded queue and a documented delivery guarantee is a future feature.
- **`EnsureLogQueue` / `EnsureTaskQueue` helpers** — the two-line get-then-create is left to the caller. If the pattern proves ubiquitous, a named helper can be added with an explicit contract.
- **Multi-node anything** — permanently out of scope.

---

# Summary

**One break.** Structural split, naming cleanup, opcode reorganization, hardening, and four features (stalled jobs, bounded state, explicit archive, compaction) ride it together.

**One invariant.** Single writer, made explicit and tested.

**One composition.** Task keys plus explicit archive plus log compaction equals a materialized outcome table per subject.

**One noun.** *Key* names the same user-facing concept on both drivers. The engine knows grouping; the task driver knows identity. Neither knows what the other means by it. The user sees one thing.

**One lifecycle.** Boot declares with `Create*`; runtime gets with `LogQueue`/`TaskQueue`. Two phases, no upsert, no surprises.