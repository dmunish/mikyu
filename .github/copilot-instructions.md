# Mikyu — Architecture & Implementation Specification

## 1. Project Overview

**mikyu** is a high‑performance, log‑structured message queue written from scratch in Go. It presents **two distinct delivery semantics over one physical storage engine**:

- **Log queues** — append‑only streams with consumer‑group offset tracking, replay, continuous tail, and truncation. Used for event logs, CDC pipelines, and replayable streams.
- **Task queues** — at‑least‑once delivery with visibility leases, exponential backoff, scheduling, stale‑lease protection, a durable per‑queue dead‑letter queue, and chunked checkpoint‑based crash recovery. Used for work pools and job processing.

Both drivers sit on the same `engine.Storage` interface and can share a single physical queue directory: a log producer and a task enqueuer may write to the same segments, and each driver **ignores records it does not own** (log records lack the task magic header; task records are foreign to log readers).

The project ships as a **single binary** with four roles:

| Role | Entry | Description |
|---|---|---|
| Embedded engine | `mikyu.Open(dir, opts...)` | In‑process `engine.Manager`, no network, ref‑counted per data directory. |
| Remote client | `mikyu.Dial(target, opts...)` | Pooled TCP / Unix / Named‑Pipe wire client. |
| Daemon | `mikyu daemon` | Long‑lived server process over TCP, Unix socket, or Windows named pipe. |
| CLI / REPL | `mikyu <cmd>`, `mikyu shell` | Stateless one‑shot CLI and a styled interactive REPL. |

### 1.1 Core Guarantees

- **Durable‑before‑ack (default, `Fsync: true`).** `Append`/`AppendBatch` block until the WAL flusher has `syncData`'d the record bytes and index entry. The returned offset is crash‑safe.
- **Optional crash‑loss window (`Fsync: false`).** The record is published to the ring cache and `publishedOffset` immediately; the flusher writes via `WriteAt` without `syncData`. Trades a small crash window for ~3.5× lower latency. Bytes still land in the OS page cache.
- **Zero‑copy reads.** Closed segments are `mmap`'d read‑only. `Read` on a closed segment returns a sub‑slice of mapped memory with no heap allocation.
- **Ring cache.** The last 16,384 messages are cached in a lock‑protected ring for ~100 ns/op tail reads.
- **Long‑poll tailing.** `Fetch` blocks on the queue's `notify` channel for up to its `maxWait`, waking on every `AppendBatch` broadcast. `Stream` reuses the same primitive with `maxWait=0` for a non‑blocking drain; `Tail` re‑arms a 30 s window per iteration, so an idle stream is one request per window instead of a 5 ms polling loop.
- **Chunked, self‑describing checkpoints.** Task state is snapshotted into `recordCPStart` / `recordCPChunk` / `recordCPCommit` records; the newest valid commit wins on recovery, and corrupt or incomplete checkpoints are skipped in favor of earlier ones.
- **Single source of truth for configuration.** Every queue and session parameter lives in `spec.Table` with a canonical key, path, kind, layer, mutability, env name, flag name, and option name. All layers (SDK, CLI, env, YAML, wire, storage) are driven from that one table.
- **Enforced architecture.** `.go-arch-lint.yml` defines the package‑level dependency graph; `internal/audit` gates legacy names and comment style. CI fails on a violation.

### 1.2 Concurrency & Performance Snapshot

| Path | Result |
|---|---|
| Append (durable, `Fsync: true`) | ≈ 292.6 µs/op (64 B payloads) |
| Append (async, `Fsync: false`) | ≈ 25.9 µs/op (≈ 3.5× faster) |
| Ring‑cache `Read` | ≈ 100 ns/op, 0 allocs |
| `encodeRecord` | ≈ 887 ns/op, 0 allocs |
| WAL steady‑state flusher | 0 allocs/op, 3 B/op (double‑buffered swap) |
| 64 KB protocol frame | 38.5 µs → 3.5 µs (pooled writer) |
| Index RAM | `O(log S)` per read; footprint scales with segment count, not message count |

---

## 2. Architectural Principles

1. **Layer purity.** `spec` and `engine` are bottoms. `spec` imports nothing internal. `engine` imports only `spec`. `protocol` is pure DTOs over `bytes`/`frames`. `node` never touches `protocol`/`server`/`client`. `client` and `server` never import each other. `ui/*` reaches the world only through `mikyu`, `client`, `command`, `daemon`, `spec`. This graph is enforced.
2. **One storage engine, two policy drivers.** `engine.Storage` is the only contract drivers see. `logqueue` and `taskqueue` implement different semantics over identical bytes.
3. **One configuration table.** `spec.Table` is the canonical parameter list. Every other surface is generated from it.
4. **The registry is a config artifact.** Per‑queue specs live in `queues.json`. The filesystem is the source of truth for existence; missing directories purge entries, orphan directories are adopted.
5. **The data directory is a lock.** `node.Acquire` takes an exclusive `flock` on `<dir>/mikyu.lock`. Two processes on the same dir fail fast with `ErrLocked`.
6. **Path escape is a compile‑time concern.** Every file operation in `engine` uses `os.Root` scoped to the data directory.
7. **Refcount over teardown.** Embedded clients share one `node.Node` per absolute data directory. `Close` decrements; the last one releases the manager, drivers, lock, and retention loop.
8. **Errors are stable sentinels.** Wire codes map 1:1 to Go sentinels. Messages are human text, never contracts.

---

## 3. Layered Architecture & The Dual‑Driver Nature

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Layer 6 — Access & Deployment                                              │
│    internal/ui/cli     internal/ui/repl     internal/ui/command             │
│    internal/daemon     cmd/mikyu                                            │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 5 — Client & Process Core                                            │
│    internal/client (embedded | remote)     internal/node (registry, pool)   │
│    internal/config (yaml + env)                                             │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 4 — Drivers                                                          │
│    internal/queue/logqueue      internal/queue/taskqueue                    │
│    internal/queue/group         internal/queue/retention                    │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 3 — Transport                                                        │
│    internal/transport/protocol      internal/transport/ipc                  │
│    internal/server                  internal/daemon (lifecycle)             │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 2 — Configuration & Schema                                           │
│    internal/spec (Spec, Session, Table, Value, Duration, codec, validate)   │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 1 — Storage Engine                                                   │
│    internal/engine.Manager → engine.Queue                                   │
│      segments (active + mmap), WAL (sync + async), raw 16‑byte index,       │
│      ring cache (16,384), manager‑scoped OffsetStore (compacting)           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 The Two Drivers

Both drivers take an `engine.Storage` handle. A **single physical queue** can be registered with both drivers simultaneously; each driver writes its own record shape and ignores records it does not recognize.

| Aspect | `logqueue.LogQueue` | `taskqueue.TaskQueue` |
|---|---|---|
| Record shape | Raw `encodeRecord` from `engine` | `taskRecordMagic` + kind byte + task payload |
| Foregin records | Yielded as opaque `engine.Message` | Ignored in `refreshLocked` (unknown kind) |
| Consumer state | `group.Base` (HighWater) in memory | `active` map + `scheduleHeap` + `dlq` |
| Durability anchor | Consumer group commit offsets | Chunked checkpoints (`recordCPCommit`) |
| Retention floor | `min(HighWater)` across groups | `min(checkpointOffset, active offsets, uncheckpointed DLQ)` |
| Primary API | `Append`, `Read`, `Fetch`, `Stream`, `Tail`, `Group` | `Enqueue`, `Dequeue`, `Ack`, `Nack`, `Extend`, `DeadLetters` |

### 3.2 Retention Arbitration Between Drivers

```
logqueue.TruncateFloor   ──┐
                           ├── retention.Coordinator.ReconcileQueueAt(ctx, name, requested)
taskqueue.TruncateFloor  ──┘            │
                                        ▼
                            min(floors)  clamped to ≤ requested
                                        │
                            if safeFloor == 0 → no-op
                                        │
                            engine.Storage.TruncateBefore(safeFloor)
```

The coordinator holds a `Policy{Age, Bytes}` per queue. A policy shrink (`SetPolicy` reports it) triggers an immediate out‑of‑band reconcile, bounded by a 5‑minute context. Driver floors **cap** truncation but never mandate it: without a policy or an explicit `requested` floor, nothing is deleted.

---

## 4. The Configuration Singularity — `internal/spec`

Every configurable value in mikyu is one row of `spec.Table`:

```go
type Param struct {
    Key        string   // canonical flat key: identity of env, flag, option, patch
    Path       string   // nested location, e.g. "engine.batch_size"
    Kind       Kind     // int | uint32 | duration | bytes | bool | string | timestamp
    Layer      Layer    // call | spec | session
    Mutability Mutability // mutable | create-only | not-applicable
    EnvName    string   // MIKYU_<KEY>
    FlagName   string   // <key-with-dashes>
    OptionName string   // WithPascalCase
    Apply        func(*Spec, Value)
    ApplySession func(*Session, Value)
}
```

Everything downstream is driven from the table:

- **SDK options** (`WithBatchSize`, `WithSegmentSize`, …) are `OptionName` string‑matched in `internal/spec/parity_test.go` and `mikyu/surface_test.go`.
- **CLI flags** (`--batch-size`, `--segment-size`) come from `FlagName`.
- **Environment variables** (`MIKYU_BATCH_SIZE`) come from `EnvName`.
- **YAML keys** (`defaults.engine.batch_size`) come from `Path`.
- **Wire patches** are the JSON encoding of a sparse `Spec`.
- **`queues.json`** stores the same sparse `Spec` shape.
- **`update` flags** are filtered to `spec.Updatable()`; attempting a create‑only key returns `spec.ErrImmutableField` naming the key.

### 4.1 Layers

| Layer | Where it can be set | Persistence |
|---|---|---|
| `LayerCall` | Per call (`WithVisibility`, `WithRetryAfter`, `WithMaxAttempts`, `WithScheduledAt`) | Never persisted. Not a `Spec` field. No env var. |
| `LayerSpec` | Per queue (`CreateQueue`/`UpdateQueue` options, `defaults:` block in YAML, `MIKYU_*` env) | `queues.json` and wire JSON. Has `Spec` fields. |
| `LayerSession` | Per process / session (`--dir`, `--target`, `MIKYU_TARGET`, etc.) | `mikyu.yaml` `server:` block, or env. Never reaches the wire queue spec. |

### 4.2 Effective Spec Resolution

```
effective = stored.Spec.Merge(session_defaults).Merge(spec.Builtin())
```

- `stored.Spec` is the per‑queue sparse spec from `queues.json`.
- `session_defaults` is the process's `WithSessionSpec` (or the YAML `defaults:` block, or env overrides).
- `spec.Builtin()` is the total built‑in layer.

`DescribeQueue` returns **all four**: the stored spec, the effective spec, the session defaults it inherits, and the current storage stats. Tests assert that embedded, remote, CLI, and YAML defaults surfaces all produce identical effective JSON.

### 4.3 Built‑ins (One Source of Truth)

| Key | Value |
|---|---|
| `engine.batch_size` | `256` |
| `engine.max_interval` | `10ms` |
| `engine.min_interval` | `30µs` |
| `engine.segment_size` | `64 MiB` |
| `engine.segment_age` | `1h` |
| `engine.max_message_bytes` | `1 MiB` |
| `engine.encrypted` | `false` |
| `engine.fsync` | `true` |
| `retention.age` / `retention.bytes` | `0` (unlimited) |
| `task.default_visibility` | `30s` |
| `task.default_max_attempts` | `5` |
| `server.target` | `tcp://127.0.0.1:7420` |
| `server.data_dir` | `./data` |
| `server.max_conns` | `10` |

`Spec.Builtin()` and `Spec.BuiltinSession()` are total: every `LayerSpec`/`LayerSession` field is set. `spec.Spec.Effective` is therefore always a fully resolved spec.

### 4.4 Sparse Encoding

All `Spec` fields are pointers. `EncodeSpec` omits unset fields (`{"engine":{"fsync":true}}`), and `DecodeSpec` uses `DisallowUnknownFields` + `MaxSpecPayload = 1 MiB` + trailing‑data rejection. Same schema for JSON (wire) and YAML (config), enforced by `TestSpecTagsIdentical`.

---

## 5. Repository Layout

```
github.com/dmunish/mikyu/
├── .go-arch-lint.yml               # Enforced package graph
├── mikyu.go                        # Public SDK facade
├── options.go                      # Re-exported option constructors
├── typed.go                        # Typed[T] log wrapper + Codec[T]
├── mikyu_test.go / surface_test.go # SDK surface contract
│
├── cmd/mikyu/main.go               # Single-binary entrypoint
│
└── internal/
    ├── audit/                      # Source-code gates (no legacy names, comment style)
    │
    ├── spec/                       # THE config schema
    │   ├── spec.go                 # Spec, EngineSpec, RetentionSpec, TaskSpec, Session
    │   ├── table.go                # Table, Param, Kind, Layer, Mutability, Value
    │   ├── codec.go                # Duration, Encode/DecodeSpec, MaxSpecPayload
    │   ├── env.go                  # ApplyEnv / ApplyEnvSession
    │   ├── errors.go               # ErrInvalidConfig, ErrImmutableField, ...
    │   └── validate.go             # ValidateCreate / ValidateUpdate
    │
    ├── config/                     # mikyu.yaml + env loading
    │   └── config.go               # Load, DefaultConfig, legacy-key errors
    │
    ├── engine/                     # Storage engine (see §6)
    │   ├── storage.go  manager.go  queue.go  wal.go  segment.go
    │   ├── index.go  encode.go  cache.go  offsets.go  compact.go
    │   ├── read.go  append.go  fetch.go  offset_api.go  message.go  config.go
    │   ├── sync_{darwin,fdatasync,windows}.go
    │   └── encrypt_{linux,windows,other}.go
    │
    ├── enginetest/memstorage.go    # In-memory engine.Storage for synctest bubbles
    │
    ├── queue/
    │   ├── group/group.go          # Base: HighWater, Mark, Commit, Close
    │   ├── logqueue/               # Log driver (api.go, logqueue.go, group.go,
    │   │                            # retention.go, stream.go)
    │   ├── taskqueue/              # Task driver (taskqueue.go, task.go, enqueue.go,
    │   │                            # dequeue.go, ack.go, nack.go, extend.go, dlq.go,
    │   │                            # lease.go, scheduler.go, checkpoint.go,
    │   │                            # compaction.go, id.go)
    │   └── retention/coordinator.go # Policy, QueueTarget, Coordinator
    │
    ├── transport/
    │   ├── protocol/               # frames.go, bytes.go, opcodes.go, errors.go,
    │   │                            # task.go, log.go, doc.go
    │   └── ipc/ipc.go              # Unix socket / named pipe listener & dialer
    │
    ├── node/                       # Process-wide shared local node
    │   ├── node.go                 # Node: manager + drivers + retention + registry
    │   ├── pool.go                 # Acquire / refcount / ErrSessionDefaultsMismatch
    │   ├── registry.go             # queues.json load/save/reconcile
    │   ├── lock.go  lock_unix.go  lock_windows.go
    │
    ├── server/                     # Wire server
    │   ├── server.go  dispatch.go  tls.go  errors.go
    │   ├── handlers_admin.go  handlers_log.go  handlers_task.go
    │
    ├── client/                     # Embedded and remote clients
    │   ├── embedded.go  embedded_queues.go  options.go  errors.go  types.go
    │   ├── remote.go  remote_logqueue.go  remote_taskqueue.go  remote_group.go
    │
    ├── daemon/                     # Daemon lifecycle
    │   ├── daemon.go  detach_unix.go  detach_windows.go
    │
    └── ui/
        ├── command/                # Shared verb + flag helpers (command.go,
        │                            # task.go, flags.go, verbs.go)
        ├── cli/                    # One-shot CLI (cli.go, task.go)
        └── repl/                   # Interactive REPL (repl.go, task.go, style.go,
                                     # vt_{other,windows}.go)
```

The package graph is enforced by `.go-arch-lint.yml`; the allowed dependency edges are:

```
spec, engine, protocol, group, enginetest   → bottoms
ipc                                         → protocol
logqueue                                    → engine, group
taskqueue, retention                        → engine
node                                        → spec, engine, logqueue, taskqueue, retention
client, server                              → spec, engine, node, {log,task}queue, ipc, protocol
config                                      → spec
daemon                                      → spec, config, engine, node, server, protocol
mikyu                                       → spec, client, engine, node, logqueue, taskqueue
command                                     → mikyu, client, daemon, spec
cli, repl                                   → mikyu, client, daemon, command, spec
cmd                                         → mikyu, daemon, cli, repl
```

---

## 6. Storage Engine — `internal/engine`

### 6.1 `Storage` Interface

```go
type Storage interface {
    Name() string
    Append(ctx context.Context, payload []byte) (uint64, error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (Message, error)
    ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error)
    Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Message, error)
    LastOffset() uint64
    CommitOffset(ctx context.Context, group string, offset uint64) error
    Committed(ctx context.Context, group string) (uint64, error)
    MaxAppendPayloadBytes() int64
    MinOffset() uint64
    Segments() []SegmentInfo
    TruncateBefore(ctx context.Context, offset uint64) (int64, error)
    FloorOlderThan(ctx context.Context, olderThan time.Time) (uint64, error)
    FloorForSize(ctx context.Context, maxBytes int64) (uint64, error)
}
```

`Fetch` is the streaming primitive both `logqueue.Stream` and `logqueue.Tail` build on; it is the only read method on `Storage` that blocks.

### 6.2 `Config` (per queue)

```go
type Config struct {
    BatchSize       int
    MaxInterval     time.Duration
    MinInterval     time.Duration
    SegmentSize     int64
    MaxMessageBytes int64
    SegmentAge      time.Duration
    Encrypted       bool
    Fsync           bool
}
```

`withDefaults()` fills zeros: `BatchSize=256`, `MaxInterval=10ms`, `MinInterval=30µs` (negative clamps to 0 = "no linger"), `SegmentSize=64 MiB`, `MaxMessageBytes=1 MiB`, `SegmentAge=1h`. `ApplyConfig` merges mutable fields into a live queue and calls `wal.UpdateConfig`. Create‑only fields (`SegmentSize`, `MaxMessageBytes`, `Encrypted`, `Fsync`) are silently ignored on the live path.

### 6.3 `Manager`

Owns a data directory, an `*os.Root` scoped to it, one shared `*OffsetStore`, and a `map[string]*Queue`.

- `OpenManager(dir, cfg, perQueue)` — creates the dir, opens the offset store, walks subdirectories, opens each queue with its per‑queue config override.
- `CreateQueue(name, opts...)` — respects the per‑queue `Config` if given; errors with `ErrQueueExists` otherwise.
- `GetQueue`, `ListQueues` (sorted), `DeleteQueue` (close + `root.RemoveAll`).
- `EngineStats()` returns a shallow aggregate with `ActiveQueues`, `TotalBytes`, and per‑queue `QueueStats`.

### 6.4 `Queue`

```go
type Queue struct {
    name              string
    dir               string
    cfg               Config
    active            *segment
    segments          []*segment
    nextOffset        uint64   // next logical offset to assign
    minOffset         uint64   // lowest readable offset on disk
    publishedOffset   uint64   // high-water mark visible to readers
    w                 *wal
    offsets           *OffsetStore
    cache             *ringCache
    nextPreallocBytes int64
    closed            bool
    mu                sync.RWMutex
}
```

`openQueue` creates the queue directory **through `os.Root` scoped to the parent**, so a hostile name cannot escape the data dir. All file I/O in `segment` uses the same mechanism.

#### 6.4.1 Append & AppendBatch

`Append` delegates to `AppendBatch`. The batch path stages per‑segment sub‑batches so a WAL write never straddles a roll:

1. Validate `len(payload) <= MaxMessageBytes`; encode with `uuid.NewV7()` and a shared timestamp; reject records larger than `SegmentSize` with `ErrRecordTooLarge`.
2. Stage into a `stagedBatch{w, seg, recs, positions, recLens, offsets, ids, totalLen, startIdx}`.
3. On roll (size or age), commit the staged group through either:
   - **`commitSync`** (`Fsync: true`): `wal.pushBatch` publishes sequence numbers; the caller unlocks and waits on `wal.wait(seq)` (or delivers the wake token), then `roll()`s and starts a new group.
   - **`commitAsync`** (`Fsync: false`): clone payloads into the ring cache, publish records on `wal.asyncChan`, advance `publishedOffset`, then `roll()`.
4. Final staged group is committed after the loop.

#### 6.4.2 Read & ReadBatch

`Read`:

1. `q.cache.Get(offset)` — hit returns the shallow message.
2. `q.locateSegment(offset)` — `slices.BinarySearchFunc` over `q.segments` base offsets (O(log S)). Handles `offset < minOffset` (`ErrOffsetTruncated`), `offset ≥ publishedOffset` (`ErrOffsetNotFound`).
3. `q.readFromSegment(seg, offset)` — reads from `seg.mmapData` when mapped, else `seg.logFile.ReadAt`; `decodeRecord` validates CRC and length.

`ReadBatch` walks offsets in order, reusing the current segment pointer between consecutive offsets and short‑circuiting to `locateSegment` only on boundary crossings.

#### 6.4.3 Fetch

`Fetch` is the blocking, notify‑driven read primitive that `logqueue.Stream` and `logqueue.Tail` sit on. It is a single loop over the queue's `notify` channel:

1. Snapshot `closed`, `minOffset`, `publishedOffset`, and the current `notify` channel under `RLock`.
2. `closed` → `ErrQueueClosed`; `offset < minOffset` → `ErrOffsetTruncated`.
3. `offset < publishedOffset` → delegate to `ReadBatch(ctx, offset, maxCount)` and return.
4. No data and `maxWait <= 0` → `ErrNoData`.
5. Otherwise arm a timer for the remaining budget and block on `{ctx.Done(), notify, timer.C}`. A notify wake means an `AppendBatch` reached the publish point and the loop re‑checks; a timer fire returns `ErrNoData`; ctx cancellation returns `ctx.Err()`.

`notify` is broadcast by `broadcastLocked()` — called by both the sync and async `AppendBatch` paths after `publishedOffset` advances — so a parked reader wakes the instant a new record is visible. `Fetch` never touches `segments` directly; it hands off to `ReadBatch` once data is present, which keeps the mmap/read logic in one place.

#### 6.4.4 Segment Rolling & Preallocation

`nextPreallocBytes` starts at `min(8 MiB, SegmentSize)` and adapts inside `roll()`:

- Segment filled in under `fastFillThreshold` (10 s) → double, capped at `min(256 MiB, SegmentSize)`.
- Segment filled after `slowFillThreshold` (5 m) → halve, floored at the 8 MiB min.
- The active segment is truncated to its written `pos` before `enableMMap`.

This keeps small queues small and large queues fast.

#### 6.4.5 Close & ApplyConfig

`Close` stops the WAL, closes every segment, marks `closed`. `ApplyConfig` updates `BatchSize`, `MaxInterval`, `MinInterval`, `SegmentAge` on the live queue and pushes the flusher config via `wal.UpdateConfig`.

### 6.5 WAL — `wal.go`

```go
type wal struct {
    cfg  Config
    file *os.File

    mu          sync.Mutex
    cond        *sync.Cond
    buf         []byte
    pending     []pendingEntry
    swapBuf     []byte
    swapPending []pendingEntry
    pushed      int64
    published   int64
    err         error

    asyncChan     chan asyncEntry
    wake          chan struct{}
    configChanged chan struct{}
    shutdown      chan struct{}
    done          chan struct{}
    stopOnce      sync.Once
}
```

**Sync path.** `push`/`pushBatch` stage bytes + pending entries into `buf`/`pending` under lock; the flusher (`syncLoop`) selects over `asyncChan`, `ticker.C` (`MaxInterval`), `wake`, `configChanged`, `shutdown`. `adaptiveFlush` sleeps `MinInterval` before calling `flush()` when the current batch is below `BatchSize`. `flush` swaps `buf ↔ swapBuf` and `pending ↔ swapPending`, drains the wake token under lock, calls `file.WriteAt`, and (if `Fsync`) `syncData`; then broadcasts on `cond` and reclaims the swap buffers. Producers `wait(seq)` on `cond`.

**Async path.** `queue.AppendBatch` writes records onto `wal.asyncChan` and clones into the ring cache. `drainAndFlushAsync(first)` groups up to `BatchSize` entries, writes each record at its recorded `pos`, writes the corresponding `.idx` entry, and advances `published` contiguously.

**Wake‑token correctness.** The `wake` channel has buffer 1 and is drained inside `flush()` under lock, preventing producer hangs when `adaptiveFlush` is mid‑linger.

**Platform `syncData`:**

| OS | Call |
|---|---|
| Darwin | `unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)` (drive cache) |
| Linux/BSD | `unix.Fdatasync(fd)` |
| Windows | `file.Sync()` |

### 6.6 Segment — `segment.go`

Fields: `.log` file handle, `.idx` file handle, `pos`, `preallocSize`, `createdAt`, `mmapData`, `idxMMap`.

- `openSegment(dir, baseOff, preallocSize)` preallocates the `.log` file if empty.
- `enableMMap()` maps both files read‑only **only if `Stat().Size() > 0`** (Windows robustness).
- `recover()` tries `tryFastRecovery` (validates last index entry, copies raw entries); on failure, `rebuild()` re‑walks the `.log` file, validates each CRC‑checked record, rewrites `.idx`, truncates torn tails.
- `readIndexEntry(offset)` reads 16 bytes from `idxMMap` (closed) or `idxFile.ReadAt`.
- `lastTimestamp()` returns the timestamp of the newest record (used by `FloorOlderThan`).
- `destroy()` unmaps, closes, and unlinks both files through an `os.Root`.
- `Close()` truncates the active `.log` to `pos`, unmaps, closes.

### 6.7 Record Format — `encode.go`

```
[4B Body Length][4B CRC32][2B ID Length (=16)][16B Raw UUID v7][8B Unix Nanoseconds][Payload]
```

CRC‑32/IEEE covers the body (`rec[8:]`). `minRecordSize = 34`. `decodeRecord` returns `(id string, ts time.Time, payload []byte, err error)`.

### 6.8 Ring Cache — `cache.go`

`slots [16384]Message`, indexed by `offset & (capacity-1)` under `sync.RWMutex`. `EvictBefore(minOffset)` zeros slots below the new floor. `Get(offset)` validates `msg.Offset == offset` and non‑zero timestamp to defend against stale slots.

### 6.9 Offset Store — `offsets.go`

One `manager-offsets.dat` file persists `(queue:group) → uint64` as `[2B keyLen][key][8B offset]` tuples. `recover()` streams valid records and stops on torn tails. `Commit` writes, flushes, `Sync`s, and every `compactAppendsThreshold = 5000` appends triggers `compactLocked`, which rewrites the file atomically via `os.Rename` from `manager-offsets.dat.tmp`.

### 6.10 Retention Primitives — `compact.go`

- `MinOffset()`, `Segments()` return snapshots.
- `FloorOlderThan(ctx, olderThan)` walks **closed** segments oldest → newest; a segment qualifies only if its last record is strictly before `olderThan`. The active segment is never included.
- `FloorForSize(ctx, maxBytes)` drops whole oldest closed segments while total size exceeds `maxBytes`.
- `TruncateBefore(ctx, targetOffset)` unlinks closed segments with `nextOffset <= targetOffset`; stops at the first straddling segment; updates `minOffset`; calls `cache.EvictBefore`.

### 6.11 Kernel Directory Encryption — `encrypt_*.go`

| OS | Mechanism |
|---|---|
| Linux | `SYS_IOCTL` with `FS_IOC_SET_ENCRYPTION_POLICY`; `AES_256_XTS` contents, `AES_256_CTS` filenames. Tolerates `ENOTTY`, `EOPNOTSUPP`, `EEXIST`. |
| Windows | `EncryptFileW` from `advapi32.dll` (NTFS EFS). |
| Other (macOS/BSD) | Logs a warning and returns `nil`. |

---

## 7. Log Queue Driver — `internal/queue/logqueue`

### 7.1 Interface — `api.go`

```go
type ClientLogQueue interface {
    Name() string
    Append(ctx context.Context, payload []byte) (uint64, error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (engine.Message, error)
    ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error)
    LastOffset(ctx context.Context) (uint64, error)
    MinOffset(ctx context.Context) (uint64, error)
    Truncate(ctx context.Context, minOffset uint64) (int64, error)
    Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error]
    Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error]
    Group(name string) Group
}
```

The interface deliberately exposes only `engine.Message`, not `engine.Storage`, keeping the engine internal. `LogQueue.Fetch` exists on the concrete type and is used by the server's `OpFetch` handler; it is not part of the client interface because remote callers reach it through `Tail`/`Stream`.

### 7.2 Behavior — `logqueue.go` & `stream.go`

- `LogQueue` holds a `map[string]*queueGroup` for cached consumer groups.
- **`Fetch(ctx, offset, maxCount, maxWait)`** is the streaming primitive. It returns `ErrOffsetTruncated` when `offset < MinOffset()`, returns up to `maxCount` records when `offset < publishedOffset`, blocks on the queue's `notify` channel for up to `maxWait` when the reader has caught up, and returns `ErrNoData` when the wait elapses with nothing new. `LogQueue.Fetch` is a thin delegate to `storage.Fetch`.
- **`Stream(ctx, startOffset)`** drains every existing record from `startOffset` and returns on `ErrNoData`. It passes `maxWait=0`, which turns `Fetch` into a strictly non‑blocking drain: the first empty iteration ends the iterator. Any other error is yielded once and stops the iterator.
- **`Tail(ctx, startOffset)`** yields existing records, then blocks in `Fetch` for up to `tailWait = 30 s` per iteration; `ErrNoData` simply re‑arms the wait, so an idle tail costs one long‑poll per 30 s window instead of the previous 5 ms polling loop. A `ctx` cancellation is yielded once and stops the iterator. `notify` is broadcast on every `AppendBatch` publish, so a tail wakes the instant new records become visible; the queue stays alive across in‑flight WAL writes because the wait is signal‑driven, not offset‑polled. This is tested by `TestLogQueueTailBugProof`, `TestLogQueueTailContinuous`, and `TestTailBatchDeliversAll` under `testing/synctest`.
- `Group(name)` returns a cached `*queueGroup` (a `group.Base` with `CommitFn` bound to `LogQueue.CommitOffset`).

### 7.3 Retention — `retention.go`

```go
func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, bool, error)
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error)
```

`TruncateFloor` returns `min(HighWater across all groups)` and a `constrained` flag. No groups → `(0, false)`. When `Truncate(0)` is called with no explicit offset, the driver substitutes its own floor if constrained; otherwise the call is a no‑op.

---

## 8. Task Queue Driver — `internal/queue/taskqueue`

### 8.1 State — `taskqueue.go`

```go
type TaskQueue struct {
    name    string
    storage engine.Storage

    mu                sync.Mutex
    active            map[string]*taskState     // id → in-flight or scheduled task
    dlq               map[string]Task
    dlqUncheckpointed map[string]struct{}
    schedule          scheduleHeap              // min-heap by (due, seq)
    sequence          uint64                    // monotonic seq for tie-breaking
    scanNext          uint64                    // next offset to scan
    checkpointOffset  uint64                    // last committed checkpoint start
    dirty             uint64                    // ops since last checkpoint

    notify          chan struct{}               // broadcast on every state change
    stopCh          chan struct{}
    wg              sync.WaitGroup
    closing, closed bool

    defaultVisibility  time.Duration
    defaultMaxAttempts uint32
}
```

Lifecycle: `NewTaskQueue` opens the storage, calls `loadCheckpointLocked`, sets `scanNext = MinOffset()` if zero, calls `refreshLocked` to replay post‑checkpoint records, and starts `checkpointLoop` (5 s ticker). `Close` stops the loop, writes a final checkpoint if dirty, marks closed.

`SetDefaults(Defaults{Visibility, MaxAttempts})` updates the queue's defaults for future enqueues; in‑flight tasks keep their captured values.

### 8.2 Types — `task.go`

```go
type Task struct {
    ID          string
    Payload     []byte
    Attempts    uint32
    MaxAttempts uint32
    Visibility  time.Time
    ScheduledAt time.Time
    DeliveryID  string    // random per delivery; empty until first lease
    Offset      uint64    // engine offset of the task record
}

type TaskRef interface{ taskRef(); taskID() string }
type TaskID string       // bare ID → skip strict lease check
// Task also implements TaskRef → strict check on Offset+Attempts+DeliveryID
```

Defaults: `defaultVisibilityTimeout = 30s`, `defaultMaxAttempts = 5`, `defaultRetryDelay = 1s`, `defaultMaxRetryDelay = 5m`, `defaultCheckpointInterval = 5s`, `checkpointEvery = 128`, `checkpointChunkBytes = 64 KiB`.

Options:

- **`EnqueueOption`** — `WithVisibility(d)`, `WithMaxAttempts(n)`, `WithScheduledAt(t)`. Zero/unset values inherit queue defaults.
- **`NackOption`** — `WithRetryAfter(d)`.

`ResolveNackOptions` computes exponential backoff `defaultRetryDelay * 2^(Attempts-1)` capped at `defaultMaxRetryDelay`.

### 8.3 Enqueue — `enqueue.go`

Resolves options under lock, generates a 32‑char hex ID from `crypto/rand` (`id.go`), appends `encodeTaskRecord(id, maxAttempts, visibilityTimeout, scheduledAt, payload)`, installs a `taskState{task, nextAt: scheduledAt, visibilityTimeout}`, pushes onto the schedule, sets `scanNext = offset + 1`, increments `dirty`, broadcasts.

### 8.4 Dequeue — `dequeue.go`

```go
func (q *TaskQueue) TryDequeue(ctx context.Context) (Task, error)      // one pass
func (q *TaskQueue) Dequeue(ctx context.Context) (Task, error)         // blocks
```

`Dequeue` loops on `takeDueLocked(now)`:

1. Pop stale entries (version mismatch or missing from `active`).
2. If the due task is in‑flight on its **final** attempt → dead‑letter: append `recordDLQ`, move to `dlq`, mark `dlqUncheckpointed`, broadcast, continue.
3. Otherwise assign a fresh `DeliveryID`, set `inFlight=true`, `Attempts++`, `Visibility = now + visibilityTimeout`, bump version, re‑push onto schedule, broadcast, return.

`Dequeue` parks on `notify` or a timer until `waitUntil`. Context cancellation returns `ctx.Err()`.

### 8.5 Lease Resolution — `lease.go`

`resolveLeaseLocked(ref)` is the single source of truth for lease validity:

- `nil` → `ErrInvalidTask`.
- Not in `active` → `ErrTaskNotFound`.
- Not `inFlight` → `ErrTaskNotInflight`.
- Visibility expired → `ErrTaskStale`.
- **Strict mode** (`ref` is `Task`): requires `Offset`, `Attempts`, and non‑empty `DeliveryID` to match exactly; mismatch → `ErrTaskStale`.
- **Loose mode** (`ref` is `TaskID`): skips the strict comparison; the visibility check still applies.

`broadcastLocked` closes the current `notify` channel and allocates a fresh one, waking every parked `Dequeue`.

### 8.6 Ack / Nack / Extend / DLQ

- **`ack.go`** — `resolveLeaseLocked`, delete from `active`, `dirty++`, broadcast, prune schedule.
- **`nack.go`** — `resolveLeaseLocked`; if `Attempts >= MaxAttempts`, dead‑letter; otherwise reset `inFlight`, clear `DeliveryID`, set `nextAt = now + retryAfter`, bump version, re‑push, broadcast.
- **`extend.go`** — only extends `Visibility` forward (never shortens); never increments `Attempts`.
- **`dlq.go`** — `DeadLetters` returns an offset‑sorted snapshot; `AckDeadLetter` verifies `Offset`/`Attempts` when `ref` is a `Task` (else `ErrDeadLetterStale`), appends `recordDLQAck`, and deletes.

### 8.7 Scheduler — `scheduler.go`

`scheduleHeap` is a binary min‑heap ordered by `(due, seq)`. `pushScheduleLocked` bumps `q.sequence` and pushes. `pruneScheduleLocked` rebuilds the heap when `len(schedule) > len(active)*3 + 1024`.

### 8.8 Checkpointing — `checkpoint.go`

Record kinds (8‑byte magic `MIKYUTQ\x01`, then one kind byte):

| Kind | Name | Payload |
|---|---|---|
| 1 | `recordTask` | id, maxAttempts, visibilityTimeout, scheduledAt, payload |
| 2 | `recordCPStart` | coveredThrough (u64), checkpointVersion (u32 = 2) |
| 3 | `recordCPChunk` | index (u32), data (bytes) |
| 4 | `recordCPCommit` | startOffset (u64), totalChunks (u32), coveredThrough (u64), version (u32) |
| 5 | `recordDLQ` | offset, id, attempts, maxAttempts, scheduledAt, payload |
| 6 | `recordDLQAck` | offset, id, attempts |

**`checkpointLocked`**:

1. `refreshLocked` to include foreign records appended since the last task op.
2. Snapshot `active` (sorted by offset) + `dlq` (sorted by offset) via `encodeSnapshot`.
3. Append `recordCPStart(coveredThrough)`.
4. Split the snapshot into ≤ `min(MaxAppendPayloadBytes() - header, checkpointChunkBytes)` chunks; append `recordCPChunk(i, data)` for each.
5. Append `recordCPCommit(startOffset, totalChunks, coveredThrough)`.
6. Verify `commitOffset == startOffset + totalChunks + 1`; on mismatch return `ErrCheckpointCorrupt`.
7. Clear `dlqUncheckpointed` and reset `dirty`.

If the queue is empty, one empty chunk is written so `totalChunks ≥ 1` always holds.

**`loadCheckpointLocked`** scans backward from `LastOffset()` for the newest valid commit record. It validates: commit bounds, chunk count ≤ `maxCheckpointChunks = 1<<20`, covered bytes ≤ `maxCheckpointBytes = 512 MiB`, chunk indices match, snapshot decodes cleanly. Any failure → try an earlier commit. Success → rebuild `active`, `dlq`, and set `scanNext = off + 1`.

**`refreshLocked`** walks `scanNext` forward. `recordTask`, `recordDLQ`, `recordDLQAck` are decoded; `recordCP*` are ignored; **anything else is treated as a foreign log record and skipped**. This is how log and task drivers share storage safely.

---

## 9. Cross‑Driver Retention — `internal/queue/retention`

```go
type DriverProvider interface {
    TruncateFloor(ctx context.Context) (floor uint64, constrained bool, err error)
}

type Policy struct { Age time.Duration; Bytes int64 }

type QueueTarget struct {
    Name    string
    Storage engine.Storage
    Drivers []DriverProvider
    Policy  Policy
    mu      sync.Mutex
}

type Coordinator struct {
    mu       sync.RWMutex
    targets  map[string]*QueueTarget
    interval time.Duration
}
```

- `RegisterTarget(*QueueTarget)` — replaces the target.
- `RegisterDriver(name, storage, driver)` — idempotent append to the target's list.
- `UnregisterDriver` / `UnregisterTarget`.
- `SetPolicy(name, Policy) (shrunk bool)` — compares against the previous policy using `isShrink`, which reports `true` when a zero (=unlimited) dimension becomes bounded or a finite bound decreases. `tighter` handles zero as "above every finite bound".
- `ReconcileQueueAt(ctx, name, requested)` — computes `policyFloor` (via `FloorOlderThan` / `FloorForSize`, at least `requested`), then clamps by `min(all driver floors)`. Zero → no‑op. Otherwise `storage.TruncateBefore(safeFloor)`.
- `ReconcileAll(ctx)` — iterates every target, ignoring per‑queue errors.
- `Start(ctx)` — background ticker loop.

The node wires `SetPolicy` to `UpdateQueue`; a shrink triggers an out‑of‑band reconcile bounded by the node's retention context (5 minutes).

---

## 10. Transport — `internal/transport`

### 10.1 Protocol — `internal/transport/protocol`

#### 10.1.1 Frames — `frames.go`

```
+-----------------+---------------+----------------------+
| Length (uint32) | Opcode (u8)   | Payload              |
| 4 bytes         | 1 byte        | (Length - 1) bytes   |
+-----------------+---------------+----------------------+
```

`MaxFrameBytes = 256 MiB`. `WriteFrame` grabs a pooled `[]byte` (32 KiB seed), grows it in‑place while retaining the pointer, writes `[length][op][payload]`, returns the buffer to the pool. `ReadFrame` validates length, allocates exactly `length-1` bytes, reads in one `io.ReadFull`. `ReadFrameInto` supports buffer reuse for the server's per‑connection read loop.

#### 10.1.2 Bytes — `bytes.go`

`Reader` provides bounds‑checked `ReadU16`/`ReadU32`/`ReadU64`/`ReadBytes`; `Buffer` provides symmetric writers. All integers are big‑endian.

#### 10.1.3 Opcodes — `opcodes.go`

Opcodes are **nibble‑grouped** by subsystem:

| Subsystem | Range | Ops |
|---|---|---|
| Admin | `0x0X` | `OpCreateQueue`, `OpDeleteQueue`, `OpListQueues`, `OpUpdateQueue`, `OpDescribeQueue`, `OpTruncate`, `OpInfo`, `OpShutdown` (0x0C) |
| Log | `0x1X` | `OpAppend`, `OpAppendBatch`, `OpRead`, `OpReadBatch`, `OpLastOffset`, `OpCommit`, `OpCommitted`, `OpFetch` |
| Task | `0x2X` | `OpEnqueue`, `OpDequeue`, `OpAck`, `OpNack`, `OpExtend`, `OpDeadLetters`, `OpDeadLetterAck` |
| Protocol‑level | — | `OpError` (0xFF), `OpReservedHandshake` (0x7E) |

The table is **frozen**: opcodes are never reused. `TestOpcodeNibbleGrouping` asserts every op lives in its declared nibble and no collisions exist.

`ParseTarget` supports `tcp://`, `unix://`, `pipe://`, and bare `host:port` (treated as TCP).

#### 10.1.4 Errors — `errors.go`

Codes are blocked by subsystem nibble: `0x01–0x0F` admin, `0x11–0x1F` storage, `0x21–0x2F` task, `0x31–0x3F` dead‑letter. `OpError` frames carry one code plus a human message; the code is the stable contract.

#### 10.1.5 Task Wire — `task.go`

`EncodeTask` / `DecodeTask` serialize `Task` including `DeliveryID` and `Offset`. `DecodeTask` rejects malformed payloads (`ErrInvalidTask`), including non‑zero `Visibility` without a `DeliveryID`. `EncodeTaskRef` / `DecodeTaskRef` prefix a **mode byte**:

- `TaskRefModeTask = 0` → full task record (server runs the strict lease check).
- `TaskRefModeTaskID = 1` → bare ID (server skips the strict check).

`EncodeTaskDequeue` clamps negative timeouts to 0 so they decode as "non‑blocking" instead of a 64‑bit millisecond value. `EncodeTaskList` / `DecodeTaskList` handle `DeadLetters` responses with a `10,000,000` cap.

#### 10.1.6 Log Wire — `log.go`

The log wire DTO is `LogRecord{Offset, ID, Timestamp, Payload}`.

- `WriteLogRecord(b, rec)` / `ReadLogRecord(r)` — one record as `u64 offset`, `bytes ID`, `u64 UnixNano timestamp`, `bytes payload`.
- `WriteLogRecordList` / `ReadLogRecordList` — `u32 count` (cap `65536`) followed by that many records. Over‑large counts decode as `ErrTruncatedBody`.
- `EncodeFetch(b, queue, start, maxCount, maxWait)` / `DecodeFetch(r)` — the `OpFetch` request: `bytes(queue) u64 start u32 maxCount u64 maxWait` (`maxWait` as a `time.Duration` in nanoseconds). A negative `maxWait` on the wire decodes to a negative duration, which the server clamps to 0 before dispatch.

`OpFetch` requests carry a `u64` wait in nanoseconds; the server caps it at `maxFetchWait = 5 min` so a hostile client cannot pin a handler indefinitely.

### 10.2 IPC — `internal/transport/ipc`

`Listen(target)` returns a `*Listener` wrapping either a Unix domain socket (with stale file removal and `chmod 0600`) or a Windows named pipe. `Listener.Close()` unlinks the Unix socket file. `DialContext(ctx, target)` maps `pipe://` onto the `unix` network on POSIX (as a Unix socket) and onto `\\.\pipe\…` on Windows.

### 10.3 Server — `internal/server`

```go
type Server struct {
    node      *node.Node
    listener  net.Listener
    tlsConfig *tls.Config
    mu        sync.Mutex
    stopCh    chan struct{}
    doneCh    chan struct{}
}
```

- `getOrCreateLogQueue` / `getOrCreateTaskQueue` delegate to the node's stable cached handles.
- `ListenAndServe(target)` picks `ipc.Listen` for `unix`/`pipe` or `net.Listen` for `tcp`, wraps `tcp` with TLS if configured, and calls `Serve`.
- `Serve(ln)` accepts and spawns `serveConn` per connection.
- `serveConn` reads frames on a **single reader goroutine** feeding an unbuffered channel; the main goroutine dispatches each frame and writes the response. This gives one frame of lookahead and natural backpressure, and lets a dead connection cancel a long‑polling handler. `OpShutdown` sets `shouldShutdown`; the goroutine then launches `s.Stop()` and returns.
- `Stop()` closes the stop channel and the listener; safe to call from any goroutine.

`dispatch.go` routes every opcode. `handlers_admin.go` covers queue lifecycle and `Info`/`Describe`; `handlers_log.go` covers append/read/read‑batch/last‑offset/commit/committed/fetch; `handlers_task.go` covers enqueue/dequeue/ack/nack/extend/dead‑letters. Every response is either a success frame reusing the request opcode, or `OpError`.

`handleFetch` clamps the client's requested wait at `maxFetchWait = 5 min` and then calls `LogQueue.Fetch`; it serializes the result as a `LogRecordList` frame. A non‑blocking call (`wait == 0`) that finds no data returns `ErrCodeNoData`, which the client maps back to `engine.ErrNoData`; the long‑polling `Tail` path therefore treats `ErrNoData` as "re‑arm the window" rather than an error.

`handleDequeue` caps server‑side long‑poll at `maxDequeueWait = 5 min` and reports `ErrCodeTaskNotAvailable` on clean context deadline, so clients re‑arm.

`tls.go` `DefaultTLSConfig(certFile, keyFile)` loads a keypair or generates an ECDSA P‑256 self‑signed cert (`localhost`, `127.0.0.1`, `::1`, 1‑year validity), with `MinVersion: tls.VersionTLS13`.

`errors.go` `taskErr` maps each driver/spec/node sentinel to its wire code. Unknown errors map to `ErrCodeUnknown`.

---

## 11. Process Core — `internal/node`

`node.Node` is the shared local core: one data directory, its queue registry, cached drivers, and retention lifecycle.

```go
type Node struct {
    mgr        *engine.Manager
    retention  *retention.Coordinator
    logQueues  map[string]*logqueue.LogQueue
    taskQueues map[string]*taskqueue.TaskQueue
    mu         sync.RWMutex

    retentionCtx    context.Context
    retentionCancel context.CancelFunc
    retentionWG     sync.WaitGroup
    reconcileWG     sync.WaitGroup
    closed          bool

    dir      string
    defaults spec.Spec
    registry *registry
    lock     *dirLock
    openedAt time.Time
}
```

- `openNode(dir, defaults)` — `MkdirAll`, acquire the directory lock, load and reconcile `queues.json`, build per‑queue engine configs from the effective spec, open the manager, and start the retention loop.
- `Acquire(dir, defaults)` in `pool.go` — ref‑counted process pool keyed by absolute path. Mismatched session defaults → `ErrSessionDefaultsMismatch`. The release func is idempotent via `sync.Once`.
- `CreateQueue(ctx, name, s)` — `spec.ValidateCreate`, refuse duplicates, create the engine queue, persist the spec to `queues.json`, cache the log driver, register both the driver and policy.
- `UpdateQueue(ctx, name, patch)` — `spec.ValidateUpdate`, merge patch over stored, persist, apply mutable engine config, keep cached driver and retention target in sync, and (if the policy shrank) launch a bounded immediate reconcile.
- `DescribeQueue(ctx, name)` — returns stored, effective, defaults, and stats.
- `Info(ctx)` — version from `debug.ReadBuildInfo`, uptime from `openedAt`, data dir, aggregate storage, per‑queue stats.
- `LogQueue` / `TaskQueue` — double‑checked locking, cached handles, retention registered on first use.
- `DeleteQueue` — closes the task driver, drops both cached handles, unregisters retention, removes the directory, updates the registry.
- `Truncate` — materializes both drivers, calls `retention.ReconcileQueueAt(ctx, name, minOffset)`.
- `ListQueues`, `TaskDefaults`, `PolicyFor`, `ToEngineConfig` — helpers.

`registry.go` — `queues.json` with `{version, queues}` shape, `DisallowUnknownFields`, atomic temp+rename writes, and `reconcileRegistry` purging entries whose directories are missing and adopting orphan directories on session defaults. Corrupt registry → `ErrRegistryCorrupt` at startup.

`lock.go` + `lock_{unix,windows}.go` — `flock(LOCK_EX|LOCK_NB)` on Unix, `LockFileEx` on Windows, on `<dir>/mikyu.lock`. Failure → `ErrLocked`.

---

## 12. Client — `internal/client`

### 12.1 Options — `options.go`

`SessionConfig` carries `Spec`, `Target`, `DataDir`, `MaxConns`, `TLSEnabled`, `TLSCertFile`, `TLSKeyFile`. `WithSessionSpec` replaces the inherited defaults. `TLSConfig()` builds a `tls.Config` from a cert pair, using the client cert as its own root, `MinVersion: tls.VersionTLS13`.

`QueueOption func(*spec.Spec)` — every queue option writes exactly one field, so the spec is sparse by construction. `applyQueueOptions` folds them.

### 12.2 Embedded — `embedded.go`, `embedded_queues.go`

`embeddedClient` holds `*node.Node` and the release func from `node.Acquire`. Every method delegates to the node. `Close` releases the reference.

### 12.3 Remote — `remote.go`, `remote_logqueue.go`, `remote_taskqueue.go`, `remote_group.go`

`remoteClient` holds a pooled `chan *clientConn` and a `connSem` counting semaphore sized to `MaxConns`:

- `acquire(ctx)` — take a semaphore slot, then pop from the pool or dial fresh.
- `dialConn` — `ipc.DialContext` for `unix`/`pipe`; `tls.Dialer` or `net.Dialer` for `tcp` with a 5 s timeout.
- `release(ch)` — return to pool unless closed or full.
- `Close` — flip `closed`, drain the pool.
- `exchange(ctx, op, body)` — acquire, set deadline from `ctx`, run a cancellation watcher that forces the socket deadline to a poisoned time on `ctx.Done()`, write frame, read frame, decode `OpError`. On cancellation or error, `discard` the connection (never return a connection with an unread response to the pool).

`remoteLogQueue` implements `ClientLogQueue`. Its internal `fetch(ctx, start, maxCount, maxWait)` is the mirrored `OpFetch` primitive:

- The server budget is `wait = min(maxWait, ctx deadline remaining)`.
- The call deadline is `wait + pollMargin` (`5 s`), so a stalled server cannot hold a socket past the poll window.
- `Stream` calls `fetch(ctx, curr, streamBatch=1024, 0)` — the server's non‑blocking path — and returns on `engine.ErrNoData`.
- `Tail` calls `fetch(ctx, curr, streamBatch, longPoll=30 s)`; `engine.ErrNoData` is treated as "poll elapsed, re‑arm", so an idle tail spends one request per 30 s window. The whole iterator yields on parent‑context cancellation, restoring the parent error after the call returns.

`remoteTaskQueue` implements `TaskQueue` with a **long‑poll loop** in `Dequeue`:

- Server budget per call: `longPollTimeout = 30s`, capped by `ctx` deadline.
- Client deadline: `wait + longPollMargin` (`5s`).
- On `ErrTaskNotAvailable` (poll elapsed) or client‑side deadline, re‑arm immediately.
- On parent `ctx.Done()`, return the context error.

`remoteGroup` embeds `group.Base` with a `CommitFn` that exchanges `OpCommit`.

`errors.go` `decodeProtocolError` maps each wire code back to the exact Go sentinel, preserving server detail after the sentinel text (so `ErrImmutableField: …segment_size…` still satisfies `errors.Is`). `ErrCodeNoData` maps to `engine.ErrNoData`.

---

## 13. User Surface — `internal/ui`

### 13.1 Verb Table — `internal/ui/command/verbs.go`

The canonical CLI/REPL verb list:

| Kind | Verbs |
|---|---|
| Log | `list`, `create`, `update`, `describe`, `delete`, `append`, `read`, `tail`, `truncate` |
| Task | `enqueue`, `dequeue`, `ack`, `nack`, `extend`, `dlq`, `dlq-ack` |
| Admin | `info`, `shutdown` |

`command.Verbs` is the single source; `cli.handlers` and `repl.replHandlers` are keyed by the same names, and `TestNamingLaw` in `mikyu/surface_test.go` asserts no retired vocabulary (`Produce`, `Publish`, `Pop`, `Stats`, `DequeueNow`, `FetchOffset`, `OriginalOffset`, …) leaks into any verb or method.

### 13.2 Shared Command Helpers — `internal/ui/command`

- Log: `List`, `Create`, `Update`, `Describe`, `Delete`, `Append`, `Read`, `Tail`, `Truncate`, `Info`, `Shutdown`.
- Task: `Enqueue`, `Dequeue`, `TaskAck`, `TaskNack`, `TaskExtend`, `TaskDeadLetters`, `TaskAckDeadLetter`.
- `ResolvePayload` — string, `@file`, or `-` (stdin).
- `FormatSingleLinePayload` — escapes CR/LF/TAB, strips control chars, and truncates to a terminal‑width cell count via Lipgloss.
- `TightensRetention(current, patch)` — used to decide whether to prompt before an irreversible update.
- Flags: `SessionFlags`, `QueueFlagSet`, `QueuePatchFlagSet`, `EnqueueFlagSet`, `NackFlagSet`, plus `ParseAnywhere` for flags before/after positionals. All flag sets are driven by `spec.Params(layer)`.
- `SpecOption(spec.Spec)` — the one bridge that hands a fully‑built spec to the SDK.
- `OpenClient(session)` — embedded if `--dir`, otherwise ensures the daemon is running and dials.

### 13.3 CLI — `internal/ui/cli`

`App{Stdin, Stdout, Stderr, Confirm}` — testable stream surface. `Run(target, args)` parses global flags, looks up the verb, optionally prints per‑verb help from `spec.Params`, opens the client, and dispatches. `confirm` uses the injected `Confirm` in tests, otherwise prompts on a TTY and requires `-y/--yes` on non‑interactive input. Exit codes come from `cmd/mikyu` (`0` success, `1` general error, `2` usage error via `daemon.ErrUsage`).

`task.go` renders tasks as Lipgloss cards on a TTY and as JSON otherwise; `--json` always forces JSON. Task records round‑trip through `command.ParseTaskRef`, so `dequeue --json | ack` works end‑to‑end with the strict lease check preserved.

### 13.4 REPL — `internal/ui/repl`

`replSession` carries the client, target, dir, active queue, last task queue, last task ID, and a `done` flag. `dispatch` maps the first token:

- Session commands: `use`, `help`, `exit`/`quit`.
- Every verb from `command.Verbs`.
- Retired names (`produce`, `publish`, `pop`, `stats`) are explicitly unreachable (`TestREPLMatchesCLIVerbs`).

`resolveQueue` and `queueFor` let `ack`/`nack`/`extend`/`dlq`/`dlq-ack` fall back to the active queue and then to the last dequeued task's queue. `taskRef` lets `ack`/`nack`/`extend` reuse `lastTaskID` when no argument is given.

`style.go` — fire‑gradient ASCII banner, command reference table, task/dead‑letter cards, confirmation prompt theme. `vt_windows.go` enables `ENABLE_VIRTUAL_TERMINAL_PROCESSING` on Windows.

### 13.5 Entrypoint — `cmd/mikyu/main.go`

```go
run(args):
  no args               → repl.RunREPL(defaultTarget, nil)
  "daemon" / "server"   → daemon.Run(args[1:])
  "shell" / "repl"      → repl.RunREPL(defaultTarget, args[1:])
  "cli"                 → cli.RunCLI(defaultTarget, args[1:])
  otherwise             → cli.RunCLI(defaultTarget, args)
```

`exitCode(err)` maps `nil → 0`, `daemon.ErrUsage → 2`, else `1`.

---

## 14. Daemon — `internal/daemon`

```go
func Run(args []string) error
func EnsureDaemonRunning(target string) error
func IsDaemonAlive(target string) bool
func SendShutdownSignal(target string) error
```

- `Run` resolves config (`resolveConfig` layers CLI flags > `MIKYU_*` env > `mikyu.yaml` > compiled defaults, only applying **explicitly set** flags via `fs.Visit`), opens the node, reconciles declared queues, optionally wraps the server in TLS, serves, and blocks on `SIGINT`/`SIGTERM` or the serve result. A bind failure is reported through a buffered channel so deferred cleanups run.
- `EnsureDaemonRunning` probes the target; if dead, spawns a detached copy of `os.Executable()` with `daemon` and waits up to 3 s.
- `setDetachedProcessAttr` uses `Setsid: true` on Unix, `CREATE_NO_WINDOW` on Windows.
- `SendShutdownSignal` writes an `OpShutdown` frame.

---

## 15. Configuration — `internal/config`

`Config{Server spec.Session, Defaults spec.Spec, Queues map[string]spec.Spec}`.

- `Load(flagPath)` resolves the config file (flag → `./mikyu.yaml` → `$XDG_CONFIG_HOME/mikyu/mikyu.yaml`, bootstrapping a commented template if absent), rejects legacy YAML keys (`max_segment_bytes`, `max_segment_age`) and legacy env names (`MIKYU_ENGINE_*`, `MIKYU_SERVER_*`, `MIKYU_ENGINE_FLUSH_INTERVAL`) with a hard error naming the replacement, parses with `DisallowUnknownField`, and applies `spec.ApplyEnvSession` + `spec.ApplyEnv`.
- `DefaultConfig()` returns built‑in session defaults plus an empty queue map.
- `DefaultConfigYAML` is the bootstrapped template.

---

## 16. Configuration & Environment Reference

### 16.1 YAML Schema

```yaml
server:
  target: "tcp://127.0.0.1:7420"
  data_dir: "./data"
  tls_enabled: false
  tls_cert_file: ""
  tls_key_file: ""
  max_conns: 10

defaults:              # session defaults inherited by new queues
  engine:
    batch_size: 256
    max_interval: "10ms"
    min_interval: "30us"
    segment_size: 67108864
    segment_age: "1h"
    max_message_bytes: 1048576
    encrypted: false
    fsync: true
  retention:
    age: "0s"
    bytes: 0
  task:
    default_visibility: "30s"
    default_max_attempts: 5

queues: {}             # declarative pre-created queues (sparse Specs)
```

### 16.2 Env Overrides

Every `LayerSpec` and `LayerSession` param has a `MIKYU_<KEY>` env var. Examples: `MIKYU_TARGET`, `MIKYU_DATA_DIR`, `MIKYU_MAX_CONNS`, `MIKYU_BATCH_SIZE`, `MIKYU_SEGMENT_SIZE`, `MIKYU_FSYNC`, `MIKYU_RETENTION_AGE`, `MIKYU_DEFAULT_VISIBILITY`, `MIKYU_DEFAULT_MAX_ATTEMPTS`. Legacy `MIKYU_ENGINE_*` and `MIKYU_SERVER_*` names are hard errors naming the replacement.

---

## 17. Wire Protocol Summary

```
Client                                         Server
  │  WriteFrame(conn, Op, body)                 │
  ├────────────────────────────────────────────►│
  │                                             │ dispatch → handler
  │                                             │
  │  ReadFrame(bufio.Reader)                    │
  │◄────────────────────────────────────────────┤
  │  OpError → decodeProtocolError              │
```

- **Admin frames** carry `bytes(name) u16 specVersion bytes(json Spec)`. The server checks `specVersion` **before** touching the JSON, so a future schema is rejected without interpretation (`ErrCodeSpecTooNew`).
- **`OpDescribeQueue`** response is `u16 specVersion bytes(json QueueDescription)`.
- **`OpTruncate`** request is `bytes(name) u64 minOffset`; response is `u64 reclaimedBytes`.
- **Log frames** carry `bytes(name)` and payload/offset fields as needed. `OpFetch` is `bytes(name) u64 start u32 maxCount u64 maxWait`; the response is a length‑prefixed `LogRecordList`. `OpFetch` is the only log read that long‑polls: the server clamps `maxWait` at `maxFetchWait = 5 min` and reports `ErrCodeNoData` on a clean empty window, so the client's `Tail` re‑arms.
- **Task frames** carry `bytes(name)`, then a mode‑tagged `TaskRef`, then op‑specific data. Responses carry a wire `Task` or a task list.
- **`OpError`** carries `u8 code bytes(message)`; the code is the stable contract.

---

## 18. Storage Layout & Record Format

```
<data_dir>/
├── mikyu.lock                  # Exclusive directory lock
├── queues.json                 # Per-queue sparse specs (atomic write)
├── queues.json.tmp             # Rename scratch
├── manager-offsets.dat         # Compacted consumer-group offsets
├── manager-offsets.dat.tmp     # Rename scratch
└── <queue_name>/
    ├── 00000000000000000000.log   # Log segment (pre-allocated while active)
    ├── 00000000000000000000.idx   # Fixed-width index (16 B/record)
    ├── 00000000000000001024.log   # Next segment (base offset in name)
    ├── 00000000000000001024.idx
    └── ...
```

Every **engine record**:

```
[4B Body Length][4B CRC32][2B ID Length (16)][16B Raw UUID v7][8B Unix Nanoseconds][Payload]
```

Every **task record** additionally carries the 8‑byte magic `MIKYUTQ\x01` and a kind byte, so a task driver can ignore foreign log records and vice versa.

---

## 19. Task Queue Semantics (Precise)

| Operation | Effect |
|---|---|
| `Enqueue` | Appends `recordTask`. Installs a scheduled `active` state. `Attempts = 0`, `DeliveryID = ""`. |
| `Dequeue` / `TryDequeue` | Pops due tasks. On the **final** attempt expiring, dead‑letters instead of redelivering. Otherwise bumps `Attempts`, assigns a fresh `DeliveryID`, sets `Visibility = now + visibilityTimeout`. |
| `Ack` | Requires an active lease (strict if `Task`). Removes from `active`, broadcast. |
| `Nack` | Requires an active lease. If `Attempts >= MaxAttempts`, dead‑letters; else resets to pending with `RetryAfter` (default exponential backoff). |
| `Extend` | Requires an active lease. Only extends forward. Never increments `Attempts`. |
| `DeadLetters` | Offset‑sorted snapshot. |
| `AckDeadLetter` | Verifies `Offset`/`Attempts` when strict; appends `recordDLQAck`. |
| `Close` | Stops the checkpoint loop; writes a final checkpoint if dirty. |

**Retry backoff**: `defaultRetryDelay * 2^(Attempts-1)`, capped at `defaultMaxRetryDelay` (5 min).

**Stale‑lease protection** is layered:

- Wire: `TaskRef` mode byte selects strict vs loose.
- Server: `decodeTaskRef` forwards the mode; `fromWireTask` copies the payload.
- Driver: `resolveLeaseLocked` enforces `Offset`, `Attempts`, and non‑empty `DeliveryID` in strict mode.
- CLI/REPL: `ParseTaskRef` accepts either a JSON task record or a bare ID, so `dequeue --json` feeds straight into `ack` with the strict check intact.

---

## 20. Retention & Truncation Flow

```
logqueue.TruncateFloor  ──┐                        (per-queue Policy: Age, Bytes)
                          │
taskqueue.TruncateFloor ──┤
                          ├──► Coordinator.ReconcileQueueAt(ctx, name, requested)
                          │        │
                          │        ├─ policyFloor = max(FloorOlderThan, FloorForSize, requested)
                          │        ├─ safeFloor   = min(driver floors)   (or policyFloor if none)
                          │        ├─ if safeFloor == 0 → no-op
                          │        └─ storage.TruncateBefore(safeFloor)
                          ▼
            Closed segments with nextOffset ≤ safeFloor are unmapped,
            unlinked, and ring-cache-evicted. The active segment survives.
```

The `node.UpdateQueue` path wires a policy shrink to an immediate reconcile bounded by `retentionCtx` (5‑minute timeout) so a tightening isn't deferred to the 30 s ticker.

---

## 21. Cross‑Cutting Concerns

### 21.1 Concurrency

| Subsystem | Synchronization |
|---|---|
| `engine.Queue` | `sync.RWMutex` over segments, offsets, WAL pointer. |
| `engine.wal` | `sync.Mutex` + `sync.Cond` for sync path; buffered `asyncChan` for async path; wake token drained inside `flush`. |
| `ringCache` | `sync.RWMutex` over the slot slice. |
| `OffsetStore` | `sync.RWMutex`; writes flushed and `Sync`'d under lock. |
| `taskqueue.TaskQueue` | Single `sync.Mutex` over all state and checkpoint writes; `notify` broadcast on every state change. |
| `retention.Coordinator` | `sync.RWMutex` over the target map; per‑target `sync.Mutex`. |
| `group.Base` | `sync.Mutex` over `HighWater`. |
| `node.Node` | `sync.RWMutex` over driver maps and registry. |
| `remoteClient` | `chan *clientConn` pool + `chan struct{}` semaphore + `atomic.Bool` closed. |
| `server.Server` | `sync.Mutex` on listener; per‑connection goroutines. |

### 21.2 Error Model

- Public methods honor `ctx.Err()` first.
- `spec`, `engine`, `taskqueue`, `node` each export their own sentinels.
- `server.errors.taskErr` maps every sentinel to a stable wire code.
- `client.decodeProtocolError` maps every wire code back to the same sentinel and preserves detail after `": "`.
- The CLI/REPL render `daemon.ErrUsage` with exit code 2; everything else is a general error.

### 21.3 Security

- Directory encryption: `fscrypt` (Linux), NTFS EFS (Windows), warning fallback elsewhere.
- TLS 1.3 minimum for TCP when enabled; IPC bypasses TLS.
- Self‑signed ECDSA certificates generated automatically when no keypair is supplied.
- Unix sockets `chmod 0600`; stale socket files removed on `Listen`.
- Every `engine` path flows through `os.Root` so no name can escape the data directory.

### 21.4 Configuration Precedence

```
flags > MIKYU_* env > mikyu.yaml > spec.Builtin()/BuiltinSession()
```

Only explicitly‑set flags override lower layers (via `fs.Visit`).

---

## 22. Architectural Guards — `internal/audit`

Two source‑level tests enforce invariants that the compiler cannot:

1. **`TestNoLegacyNames`** — walks every non‑test Go file in the module and fails on any banned identifier: `Produce`, `ProduceBatch`, `Publish`, `Pop`, `Stats`, `TruncateQueue`, `FetchOffset`, `OriginalOffset`, `TaskOption`, `FailOption`, `WithScheduleAt`, `WithVisibilityTimeout`, `MaxSegmentAge`, `MaxSegmentBytes`, `DequeueNow`, `OpProduce`, `OpPublish`, `OpPop`, `OpStats`, `OpTail`, `OpFetchOffset`, and any `OpTask*ByID`.
2. **`TestInternalCommentStyle`** — walks `internal/` and `cmd/`: every package clause and exported symbol carries at most one line of doc comment. The `mikyu` facade at the repo root is exempt because it is what end users read.

Additionally, `mikyu/surface_test.go` asserts:

- Every verb in `command.Verbs` has a CLI handler and a REPL handler.
- No retired verb leaks into `command.LookupVerb`, `cli.handlers`, or `repl.replHandlers`.
- The set of `With*` constructors exactly matches `spec.Table`'s `OptionName` set plus `WithSessionSpec`.
- `Client`, `LogQueue`, `TaskQueue`, `Group` expose no `*ID` sibling methods and no `Close`.
- Every exported `Err*` sentinel is distinct (no `errors.Is` equality, no shared message).

`spec/parity_test.go` additionally cross‑checks that every `spec.Table` row is reachable from every layer its `Layer` allows: SDK option, CLI flag, env var, YAML, `queues.json`, and wire JSON.

---

## 23. Deployment Modes

1. **Embedded.** `mikyu.Open(dir, opts...)` → ref‑counted `node.Node` over an `engine.Manager`. No network, no daemon, no TLS.
2. **Remote.** `mikyu.Dial(target, opts...)` → pooled wire client. Lazy connect on first operation. Supports `tcp://`, `unix://`, `pipe://`.
3. **Daemon.** `mikyu daemon` (or `daemon.Run`) → foreground server; `EnsureDaemonRunning` spawns a detached copy on first CLI/REPL use.
4. **CLI / REPL.** `mikyu <verb>` or `mikyu shell`. Both drive the same `command` helper layer.

Embedded and remote clients produce **identical** JSON for `DescribeQueue` and `Info`, verified by `internal/client/describe_test.go` and `mikyu/surface_test.go`.

---

## 24. Performance Characteristics

| Path | Result |
|---|---|
| Append (durable, `Fsync: true`) | ≈ 292.6 µs/op (64 B) |
| Append (async, `Fsync: false`) | ≈ 25.9 µs/op (**≈ 3.5× faster**) |
| Ring‑cache `Read` | ≈ 100 ns/op, 0 allocs |
| `encodeRecord` | ≈ 887 ns/op, 0 allocs |
| WAL flusher steady state | 0 allocs/op, 3 B/op (double‑buffered swap) |
| 64 KB frame write | 38.5 µs → 3.5 µs (pooled writer) |
| Index memory | `O(log S)` per read; footprint ∝ segment count |
| Checkpoint writes | Every 5 s tick or 128 dirty ops |
| `remoteTaskQueue.Dequeue` idle | One long‑poll request per 30 s window (not 10 ms polls) |
| `remoteLogQueue.Tail` idle | One long‑poll request per 30 s window (not 10 ms polls) |
| Embedded `logqueue.Tail` idle | One `Fetch` block per 30 s window; wakes on `notify` broadcast, no polling |

Benchmarks live behind the `bench` build tag: `internal/engine/stress_bench_test.go`, `internal/queue/taskqueue/bench_test.go`, `internal/client/remote_dequeue_test.go`.

---

## 25. Public API Quick Reference

### 25.1 Facade — `package mikyu`

```go
// Lifecycle
client, err := mikyu.Open("/var/lib/mikyu")            // embedded
client, err := mikyu.Dial("tcp://host:7420")            // remote
defer client.Close()

// Client
client.CreateQueue(ctx, "events", mikyu.WithSegmentSize(64<<20))
client.UpdateQueue(ctx, "events", mikyu.WithRetentionAge(24*time.Hour))
d, _    := client.DescribeQueue(ctx, "events")
names, _:= client.ListQueues(ctx)
client.DeleteQueue(ctx, "events")
client.Truncate(ctx, "events", minOffset)
info, _ := client.Info(ctx)

// Log queue
lq, _ := client.LogQueue("events")
off, _ := lq.Append(ctx, []byte("hello"))
off, _ := lq.AppendBatch(ctx, [][]byte{…})
msg, _ := lq.Read(ctx, off)
msgs, _ := lq.ReadBatch(ctx, off, 10)
last, _ := lq.LastOffset(ctx)
for m, err := range lq.Stream(ctx, 0) { … }   // drains existing records, returns
for m, err := range lq.Tail(ctx, 0) { … }     // 30 s long-poll, wakes on append
g := lq.Group("consumers"); g.Mark(off); g.Commit(ctx); g.Committed(ctx)

// Task queue
tq, _ := client.TaskQueue("jobs")
task, _ := tq.Enqueue(ctx, payload,
    mikyu.WithVisibility(30*time.Second),
    mikyu.WithMaxAttempts(3),
    mikyu.WithScheduledAt(time.Now().Add(time.Minute)))
got, _ := tq.Dequeue(ctx)
tq.Ack(ctx, got)
tq.Nack(ctx, got, mikyu.WithRetryAfter(time.Second))
tq.Extend(ctx, got, time.Minute)
dls, _ := tq.DeadLetters(ctx)
tq.AckDeadLetter(ctx, dls[0])

// Typed log wrapper
codec := mikyu.Codec[Event]{Marshal: …, Unmarshal: …}
tq2 := mikyu.NewTyped(lq, codec)
tq2.Append(ctx, event)
```

### 25.2 Options

- **Client:** `WithSessionSpec`, `WithTarget`, `WithDataDir`, `WithMaxConns`, `WithTLSEnabled`, `WithTLSCertFile`, `WithTLSKeyFile`.
- **Queue (spec):** `WithBatchSize`, `WithMaxInterval`, `WithMinInterval`, `WithSegmentSize`, `WithSegmentAge`, `WithMaxMessageBytes`, `WithEncrypted`, `WithFsync`, `WithRetentionAge`, `WithRetentionBytes`, `WithDefaultVisibility`, `WithDefaultMaxAttempts`.
- **Enqueue (call):** `WithVisibility`, `WithMaxAttempts`, `WithScheduledAt`.
- **Nack (call):** `WithRetryAfter`.

### 25.3 Errors

| Category | Sentinels |
|---|---|
| Spec | `ErrInvalidConfig`, `ErrImmutableField`, `ErrSpecTooNew`, `ErrSpecTooLarge` |
| Node | `ErrLocked`, `ErrRegistryCorrupt`, `ErrSessionDefaultsMismatch` |
| Engine | `ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`, `ErrOffsetTruncated`, `ErrMessageTooLarge`, `ErrQueueClosed`, `ErrCorruptRecord`, `ErrNoData` |
| Task | `ErrTaskNotAvailable`, `ErrTaskNotFound`, `ErrTaskNotInflight`, `ErrTaskStale`, `ErrTaskClosed`, `ErrInvalidTask`, `ErrCheckpointCorrupt`, `ErrDeadLetterNotFound`, `ErrDeadLetterStale` |
| Protocol | `ErrProtocol`, `ErrTruncatedBody` |

All of the above are re‑exported from `package mikyu` and checked for uniqueness by `TestSentinelUniqueness`.

---

## 26. CLI / REPL Verbs

| Verb | Args | Purpose |
|---|---|---|
| `list` | — | List queues |
| `create` | `[flags] <queue>` | Create a queue (all spec flags) |
| `update` | `[flags] <queue>` | Patch mutable fields only |
| `describe` | `<queue> [--json]` | Stored + effective spec + stats |
| `delete` | `<queue> [-y]` | Delete a queue and its data |
| `append` | `<queue> <payload\|@file\|->` | Append a log record |
| `read` | `<queue> <offset>` | Read a log record |
| `tail` | `<queue> [offset]` | Stream log records |
| `truncate` | `<queue> [offset] [-y]` | Truncate through the retention coordinator |
| `enqueue` | `[flags] <queue> <payload\|@file\|->` | Enqueue a task |
| `dequeue` | `<queue> [--timeout 30s] [--json]` | Lease the next due task |
| `ack` | `<queue> <task\|json\|id>` | Acknowledge a task |
| `nack` | `<queue> <task\|json\|id> [--retry-after 5s]` | Return for redelivery |
| `extend` | `<queue> <task\|json\|id> <timeout>` | Extend a lease |
| `dlq` | `<queue> [--json]` | List dead‑letter tasks |
| `dlq-ack` | `<queue> <task\|json\|id>` | Remove a dead‑letter task |
| `info` | `[--json]` | Server identity + storage state |
| `shutdown` | `[-y]` | Stop the daemon |

REPL‑only session commands: `use <queue>` (set active), `help`, `exit`/`quit`.

---

## 27. Glossary

- **Engine record** — the raw bytes produced by `encodeRecord`; carries a UUID v7, timestamp, and CRC32.
- **Task record** — an engine record whose payload begins with `MIKYUTQ\x01` and a kind byte.
- **Foreign record** — an engine record a driver does not own; silently skipped by the other driver.
- **Fetch** — the engine's single blocking read: returns immediately when data is present, otherwise parks on the queue's `notify` channel for up to `maxWait`. Both `logqueue.Stream` (`maxWait=0`) and `logqueue.Tail` (`maxWait=30s`) are thin iterators over it.
- **Effective spec** — stored spec merged over session defaults over built‑ins.
- **TaskRef** — `Task` (strict lease check) or `TaskID` (loose check).
- **DeliveryID** — a random per‑delivery identifier; changes on each lease.
- **Checkpoint** — a chunked, self‑describing snapshot of task state and DLQ.
- **Truncate floor** — the highest offset a driver permits retention to delete.
- **Policy shrink** — a retention change that lowers a finite bound, or makes an unbounded dimension bounded; triggers an immediate reconcile.
- **Session defaults** — process‑wide `spec.Spec` inherited by every new queue in a process.
- **Verb** — one CLI/REPL command, defined once in `command.Verbs`.