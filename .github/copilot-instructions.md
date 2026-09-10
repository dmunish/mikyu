# Mikyu Technical Specification & Codebase Architecture

## 1. Project Overview

**mikyu** is a high‑performance, log‑structured message queue written from scratch in Go. It unifies two distinct delivery semantics over a single storage engine:

- **Log‑oriented queues** — append‑only streams with consumer‑group offset tracking, used for event logs, replayable streams, and CDC pipelines.
- **Task‑oriented queues** — at‑least‑once delivery with visibility leases, retries with exponential backoff, scheduling, stale‑lease protection, durable per‑queue dead‑letter queues, and checkpoint‑based crash recovery.

Both drivers sit on the same `engine.Storage` interface and can even share a single physical queue directory (a physical queue can be read as a log and as a task source simultaneously; foreign records are ignored by each driver).

The project ships as a **single binary** that behaves as:

- an **in‑process embedded engine** (`mikyu.Open`),
- a **background network daemon** over TCP / Unix Domain Sockets / Windows Named Pipes (`mikyu daemon`),
- a **stateless one‑shot CLI** (`mikyu <command>`),
- an **interactive REPL shell** (`mikyu`, `mikyu shell`, `mikyu repl`) with Lipgloss styling and `huh` confirmation prompts.

`mikyu.Dial()` produces a remote `Client` that speaks a binary length‑prefixed frame protocol over a pooled TCP/IPC connection.

### 1.1 Core Architectural Guarantees & Technical Principles

- **Durable‑before‑ack semantics (default):** every `Produce` / `ProduceBatch` invocation blocks until the record bytes and index metadata are durably synced to disk via the Write‑Ahead Log (`wal`) flusher. The returned offset is therefore crash‑safe.
- **Optional crash‑loss window (`Fsync: false`):** a per‑queue `Fsync` configuration flag selects the async fast path. In async mode the WAL flusher performs `WriteAt` without `syncData`, and `publishedOffset` is advanced before records are physically durable. Readers immediately observe the record via the ring cache and via `ReadAt` against the still‑warm page cache. This trades a small crash‑loss window for **~3.5× lower latency** (≈25.9 µs vs ≈292.6 µs for 64 B records). Data is still written to the OS page cache in both modes — only the physical flush differs.
- **Zero‑copy reads:** closed historical log segments are `mmap`'d read‑only into the process address space. `Read` on a closed segment returns a sub‑slice of the mapped memory without heap allocation. Active segments are read through `ReadAt` syscalls on the open file descriptor. A read/write‑protected **ring cache** of the last **16,384** messages provides a fast tail path measured at ~100 ns/op with zero allocations.
- **Dynamic dual‑threshold WAL flushing:** the WAL flusher (`internal/engine/wal.go`) triggers a disk sync when either:
  1. pending record count reaches `BatchSize`, or
  2. elapsed wall time exceeds `MaxInterval`.

  A **micro‑linger** (`MinInterval`) briefly waits to group concurrent producers into one batch. The flusher runs in a dedicated goroutine and swaps buffers under a `sync.Mutex`, so producers never block on disk I/O — they wait on a `sync.Cond` for the flusher to publish their sequence number.
- **Two WAL paths (`Fsync` true / false):**
  - **Sync path** (`Fsync: true`): producers stage bytes + pending entries into `wal.buf`/`wal.pending` via `push` / `pushBatch`, then `wait(seq)` on a `sync.Cond` until `published > seq`. The flusher calls `syncData` (platform‑appropriate: `F_FULLFSYNC` on Darwin, `Fdatasync` on Linux/BSD, `File.Sync` on Windows) before broadcasting.
  - **Async path** (`Fsync: false`): producers publish the record via `wal.asyncChan` (a 65,536‑entry buffered channel) and immediately advance `publishedOffset`. The flusher drains up to `BatchSize` entries per tick, writes them to the segment file, writes index entries, then broadcasts. `queue.AppendBatch` writes the record bytes into `wal.buf` only in the sync path; in the async path it clones the payload directly into the ring cache.
- **Raw 16‑byte UUID v7 encoding:** messages are encoded using Go standard library `uuid.UUID` (`uuid.NewV7()`) raw byte arrays, avoiding string formatting allocations and reducing per‑record overhead by 20 bytes.
- **`iter.Seq2[Message, error]` streaming:** `Stream()` and `Tail()` are exposed as native Go 1.23+ range‑over‑func iterators, giving lazy evaluation and clean cancellation via `context.Context`.
- **Transparent kernel directory encryption:** `EnsureDirectoryEncrypted` applies OS‑level policies — `fscrypt` `FS_IOC_SET_ENCRYPTION_POLICY` on Linux, NTFS EFS `EncryptFileW` on Windows — while maintaining `mmap` performance. Unsupported kernels (e.g. macOS) log a warning and continue.
- **TLS 1.3 transport:** TCP connections can enforce TLS 1.3 with an optional auto‑generated ECDSA self‑signed certificate for zero‑config local security; local IPC bypasses TLS overhead entirely.
- **Declarative configuration & auto‑bootstrapping:** `internal/config` parses `mikyu.yaml` via `goccy/go-yaml`, auto‑bootstraps a commented default config when missing (`./mikyu.yaml` → `$XDG_CONFIG_HOME/mikyu/mikyu.yaml`), applies environment‑variable overrides, and reconciles declared queues on daemon launch.
- **Single binary, four roles:** the `cmd/mikyu` entrypoint routes into `daemon`, `repl`, and `cli` based on arguments. Running `mikyu` with no arguments probes the local IPC socket, auto‑spawns a detached background daemon if offline, and immediately attaches an interactive REPL.

### 1.2 Concurrent Design Characteristics

- **Continuous log tailing** (`logqueue.LogQueue.Tail`): treats `offset < last` as the bound; on transient errors (`ErrOffsetNotFound`, `ErrCorruptRecord`, `io.EOF`, `io.ErrUnexpectedEOF`) it sleeps 5 ms and retries, keeping streams alive across in‑flight WAL writes.
- **Reader safety:** the `publishedOffset` high‑water mark is updated only after WAL disk sync completes (sync mode) or immediately on ring‑cache publish (async mode). Readers check against this value, eliminating `io.EOF` races against concurrent writes.
- **WAL wake‑token handling:** wake‑channel draining happens inside `flush()` under lock, preventing producer hangs when `adaptiveFlush` is mid‑linger.
- **Manager lock contention:** `Stats()` takes only `RLock()` and reads in‑memory counters for O(1) metrics retrieval.
- **Duplicate client handles:** double‑checked locking in `embeddedClient.LogQueue` / `TaskQueue` and `server.getOrCreateLogQueue` / `getOrCreateTaskQueue` guarantees a single cached wrapper instance per queue even under concurrent access.
- **Zero‑allocation encoding:** `encodeRecord` writes directly into a pre‑sized slice, achieving ≈887 ns/op encoding.
- **Double‑buffered WAL flusher:** steady‑state allocations eliminated (0 allocs/op, 3 B/op) by swapping `buf ↔ swapBuf` and `pending ↔ swapPending`.
- **Protocol frame efficiency:** `WriteFrame` retains enlarged pooled buffers; `ReadFrame` streams payloads into exact‑sized slices. 64 KB frames see a 90.7 % latency reduction (38.5 µs → 3.5 µs) with zero extra heap allocations.
- **Index RAM reduction:** `O(log S)` segment binary search plus fixed‑width on‑disk index arithmetic; heap footprint scales with segment count, not message count.
- **Offset log compaction:** the manager‑level offset file is compacted via atomic rename every 5,000 commits, bounding file size to active consumer groups.
- **Windows `mmap` robustness:** `enableMMap` skips zero‑byte `.idx` files, preventing initialization errors on Windows.
- **Retention coordination:** `retention.Coordinator` computes a single safe truncation floor across all registered drivers (log consumer groups, task checkpoint anchors) and issues `storage.TruncateBefore` only at that floor.

---

## 2. Repository Tree

```
github.com/dmunish/mikyu/
├── go.mod
├── go.sum
├── README.md
│
├── mikyu.go                     # Public SDK: Client, LogQueue, TaskQueue, Message, Task, Group aliases
├── options.go                   # Re-exported client/queue functional options
├── typed.go                     # Generic Typed[T] queue wrapper & Codec[T] interface
├── mikyu_test.go
│
├── cmd/
│   └── mikyu/
│       └── main.go              # Single-binary entrypoint: daemon | shell | repl | cli | default
│
└── internal/
    ├── cli/                     # One-shot stateless CLI handler
    │   ├── cli.go               # RunCLI, subcommand router, confirmation prompts
    │   ├── task.go              # CLI task commands + JSON output
    │   └── cli_test.go
    │
    ├── client/                  # Client drivers (embedded & remote)
    │   ├── embedded.go          # In-process embedded client (engine.Manager + retention)
    │   ├── options.go           # Client/Queue config structs & functional options
    │   ├── remote.go            # Pooled TCP/IPC wire client
    │   └── embedded_retention_test.go
    │
    ├── command/                 # Stateless helpers shared by CLI and REPL
    │   ├── command.go           # List, Create, Delete, Produce, Read, Tail, Stats, Truncate, Shutdown
    │   ├── task.go              # Publish, Pop, TaskAck/Nack/Extend/DeadLetters/AckDeadLetter
    │   ├── flags.go             # --flag / --flag=value extractors
    │   ├── command_test.go
    │   └── task_test.go
    │
    ├── config/                  # YAML + env configuration manager
    │   ├── config.go            # Load, DefaultConfig, env overrides, ToEngineConfig
    │   └── config_test.go
    │
    ├── daemon/                  # Background daemon lifecycle
    │   ├── daemon.go            # Run, EnsureDaemonRunning, IsDaemonAlive, SendShutdownSignal
    │   ├── detach_unix.go
    │   ├── detach_windows.go
    │   └── daemon_test.go
    │
    ├── engine/                  # Storage engine (WAL, segments, index, offsets, cache)
    │   ├── manager.go           # Multi-queue manager & EngineStats
    │   ├── queue.go             # Per-queue append, zero-copy read, roll, truncate
    │   ├── wal.go               # Dual-threshold flusher, sync + async paths
    │   ├── segment.go           # Active file I/O + read-only mmap handling
    │   ├── index.go             # In-memory & raw on-disk index entry structs
    │   ├── offsets.go           # Manager-level consumer group offset store
    │   ├── encode.go            # CRC32 framing, raw UUID v7, timestamp
    │   ├── cache.go             # Ring cache (16,384 messages)
    │   ├── compact.go           # SegmentInfo, MinOffset, Segments, TruncateBefore
    │   ├── storage.go           # Storage interface
    │   ├── sync_darwin.go       # F_FULLFSYNC
    │   ├── sync_fdatasync.go    # unix.Fdatasync
    │   ├── sync_windows.go      # file.Sync
    │   ├── encrypt_linux.go     # fscrypt ioctl
    │   ├── encrypt_windows.go   # NTFS EFS EncryptFileW
    │   ├── encrypt_other.go     # Warning fallback
    │   ├── *_test.go
    │   └── stress_bench_test.go
    │
    ├── group/                   # Concurrency-safe consumer group base
    │   ├── group.go             # Base with HighWater, Mark, Commit, Close
    │   └── group_test.go
    │
    ├── ipc/                     # Unix domain socket / Windows named pipe helpers
    │   ├── ipc.go               # Listen, Dial, DialContext, Listener cleanup
    │   └── ipc_test.go
    │
    ├── logqueue/                # Log-oriented driver
    │   ├── api.go               # ClientLogQueue interface
    │   ├── logqueue.go          # LogQueue: Produce, Read, Stream, Tail, Group, Truncate
    │   ├── group.go             # queueGroup wrapping group.Base
    │   ├── retention.go         # LogRetentionPolicy, TruncateFloor, Truncate
    │   └── *_test.go
    │
    ├── protocol/                # Wire format
    │   ├── opcodes.go           # Op* constants, ErrCode* constants, ParseTarget
    │   ├── frames.go            # Length-prefixed frame reader & pooled writer
    │   ├── bytes.go             # Bounds-checked Reader & Buffer
    │   ├── task.go              # Task wire encoding/decoding
    │   └── *_test.go
    │
    ├── repl/                    # Interactive REPL session
    │   ├── repl.go              # Session struct, dispatch, log commands, help table
    │   ├── task.go              # REPL task commands + card rendering
    │   ├── style.go             # Lipgloss palette, banner, help formatting
    │   ├── vt_other.go          # VT no-op (POSIX)
    │   ├── vt_windows.go        # ENABLE_VIRTUAL_TERMINAL_PROCESSING
    │   └── repl_test.go
    │
    ├── retention/               # Cross-driver retention arbitration
    │   ├── coordinator.go       # Coordinator, QueueTarget, DriverProvider
    │   └── coordinator_test.go
    │
    ├── server/                  # Wire server
    │   ├── server.go            # Listener, worker goroutines, graceful Stop
    │   ├── handlers.go          # Opcode dispatch + handlers
    │   ├── tls.go               # TLS 1.3 + self-signed ECDSA cert generator
    │   └── *_test.go
    │
    └── taskqueue/               # Task-oriented driver
        ├── taskqueue.go         # TaskQueue: Enqueue, Dequeue, Ack, Fail, Extend, DLQ
        ├── task.go              # Task struct, options, errors
        ├── checkpoint.go        # Durable checkpoint encoding/decoding
        ├── scheduler.go         # Heap-based scheduling
        ├── compaction.go        # TruncateFloor for retention coordination
        ├── id.go                # Random task ID generation
        ├── bench_test.go
        └── taskqueue_test.go
```

---

## 3. Layered Architecture & Dual‑Driver Nature

Mikyu is built on a strict layered architecture. Every layer is reusable across deployment modes and driver semantics.

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Layer 6 — Access & Deployment                                              │
│    internal/cli        internal/repl        internal/daemon   cmd/mikyu     │
│    internal/command                                                          │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 5 — Client                                                           │
│    internal/client/embedded     ←──── in-process    ─────► engine.Manager    │
│    internal/client/remote       ←──── wire frames   ─────► server.Server     │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 4 — Drivers                                                          │
│    internal/logqueue     ──► Log semantics (stream, tail, groups)            │
│    internal/taskqueue    ──► Task semantics (at-least-once, leases, DLQ)     │
│    internal/group        ──► Shared concurrency-safe high-watermark base     │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 3 — Retention Coordination                                           │
│    internal/retention.Coordinator                                           │
│      • collects TruncateFloor() from every registered driver                 │
│      • enforces min(floors), clamped by any caller-requested floor           │
│      • calls engine.Storage.TruncateBefore                                   │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 2 — Transport                                                        │
│    internal/protocol     internal/server      internal/ipc                  │
│    internal/daemon (lifecycle)                                              │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 1 — Storage Engine                                                   │
│    internal/engine.Manager                                                  │
│      internal/engine.Queue (per queue)                                      │
│        • WAL flusher (sync + async paths)                                   │
│        • Segments (active write + read-only mmap)                           │
│        • Raw fixed-width .idx index                                          │
│        • Ring cache (16,384 msgs)                                            │
│        • OffsetStore (manager-scoped, compacting)                            │
└─────────────────────────────────────────────────────────────────────────────┘
```

**Driver duality.** `internal/logqueue` and `internal/taskqueue` both take an `engine.Storage` handle. They can be registered on the **same physical queue**, in which case:

- the log driver treats every append as an opaque log record,
- the task driver tags its own records with an 8‑byte `taskRecordMagic` header and silently ignores any record that does not carry it (`TestTaskQueueIgnoresForeignLogRecords`).

This lets a single physical queue serve both a log stream and a task work pool with shared storage, shared retention, and shared `OffsetStore`.

---

## 4. Public API Surface (`package mikyu`)

### 4.1 Type Aliases & Re‑exports (`mikyu.go`, `options.go`, `typed.go`)

```go
// Domain models (aliases into internal types)
type Message    = engine.Message      // {Offset uint64; ID string; Timestamp time.Time; Payload []byte}
type SystemStats = engine.EngineStats // {ActiveQueues int; TotalBytes int64; Queues map[string]QueueStats}
type QueueStats  = engine.QueueStats  // {MessageCount, SegmentCount, SizeBytes, LastOffset, MinOffset}
type Group       = logqueue.Group
type Task        = taskqueue.Task
type TaskOption  = taskqueue.TaskOption
type FailOption  = taskqueue.FailOption
type LogQueue    = logqueue.ClientLogQueue
type TaskQueue   = client.TaskQueue   // alias → taskqueue.ClientTaskQueue

// ClientConfig / QueueConfig / ClientOption / QueueOption = re-export from internal/client

// Client is the top-level SDK interface.
type Client interface {
    io.Closer
    CreateQueue(ctx context.Context, name string, opts ...QueueOption) error
    LogQueue(name string) (LogQueue, error)
    TaskQueue(name string) (TaskQueue, error)
    ListQueues(ctx context.Context) ([]string, error)
    DeleteQueue(ctx context.Context, name string) error
    TruncateQueue(ctx context.Context, name string, minOffset uint64) (int64, error)
    Stats(ctx context.Context) (SystemStats, error)
}

func Open(dir string, opts ...ClientOption) (Client, error)   // embedded
func Dial(target string, opts ...ClientOption) (Client, error) // remote
```

The `Typed[T]` wrapper composes a `Codec[T]{Marshal, Unmarshal}` with a `LogQueue` to produce/read typed values without changing the wire or storage formats.

### 4.2 LogQueue Interface (`internal/logqueue/api.go`)

```go
type ClientLogQueue interface {
    Name() string
    Produce(ctx context.Context, payload []byte) (uint64, error)
    ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
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

### 4.3 TaskQueue Interface (`internal/taskqueue/task.go`)

```go
type ClientTaskQueue interface {
    Close() error
    Name() string

    Enqueue(ctx context.Context, payload []byte, opts ...TaskOption) (Task, error)
    Dequeue(ctx context.Context) (Task, error)

    Ack(ctx context.Context, task Task) error
    Fail(ctx context.Context, task Task, opts ...FailOption) error
    Extend(ctx context.Context, task Task, timeout time.Duration) error

    DeadLetters(ctx context.Context) ([]Task, error)
    AckDeadLetter(ctx context.Context, task Task) error

    // By-ID convenience APIs (for stateless admin/CLI usage)
    AckID(ctx context.Context, taskID string) error
    FailID(ctx context.Context, taskID string, opts ...FailOption) error
    ExtendID(ctx context.Context, taskID string, timeout time.Duration) error
    AckDeadLetterID(ctx context.Context, taskID string) error
}
```

### 4.4 Group Interface (`internal/logqueue/group.go`)

```go
type Group interface {
    io.Closer
    Name() string
    Mark(offset uint64)
    Commit(ctx context.Context) error
    FetchOffset(ctx context.Context) (uint64, error)
}
```

### 4.5 Functional Options

- **Client options** (`internal/client/options.go`): `WithBatchSize`, `WithMaxInterval` (alias `WithFlushInterval`), `WithMinInterval`, `WithMaxConns`, `WithClientDirectoryEncryption`, `WithClientTLS`, `WithFsync`.
- **Queue options**: `WithQueueBatchSize`, `WithQueueMaxInterval`, `WithQueueMinInterval`, `WithMaxSegmentBytes`, `WithMaxMessageBytes`, `WithMaxSegmentAge`, `WithDirectoryEncryption`, `WithQueueFsync`.
- **Task options**: `WithVisibilityTimeout`, `WithMaxAttempts`, `WithScheduleAt`, `WithRetryAfter`.

### 4.6 Re‑exported Error Variables

| Category | Variables |
|---|---|
| Queue | `ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`, `ErrOffsetTruncated`, `ErrMessageTooLarge`, `ErrQueueClosed`, `ErrCorruptRecord` |
| Task | `ErrTaskNotAvailable`, `ErrTaskNotFound`, `ErrTaskNotInflight`, `ErrTaskStale`, `ErrTaskClosed`, `ErrInvalidTask`, `ErrCheckpointCorrupt`, `ErrDeadLetterNotFound`, `ErrDeadLetterStale` |
| Client | `ErrTaskNotSupported` (retained for source compatibility; task queues are now fully supported) |

---

## 5. Package‑by‑Package Specification

### 5.1 `internal/engine` — Storage Engine

#### 5.1.1 `storage.go` — The `Storage` Interface

```go
type Storage interface {
    Name() string
    Append(ctx context.Context, payload []byte) (uint64, error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (Message, error)
    ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error)
    LastOffset() uint64
    CommitOffset(ctx context.Context, group string, offset uint64) error
    FetchOffset(ctx context.Context, group string) (uint64, error)

    MaxAppendPayloadBytes() int64

    MinOffset() uint64
    Segments() []SegmentInfo
    TruncateBefore(ctx context.Context, offset uint64) (reclaimedBytes int64, err error)
}
```

`Message` is `{Offset uint64; ID string; Timestamp time.Time; Payload []byte}`.

#### 5.1.2 `manager.go` — Multi‑Queue Lifecycle

`Manager` owns a directory, a shared `OffsetStore`, and a map of `*Queue`. On `OpenManager(dir, cfg)` it:

1. creates `dir` (with optional `EnsureDirectoryEncrypted`),
2. opens the manager‑level `OffsetStore`,
3. walks subdirectories and calls `openQueue` for each,
4. returns a `Manager`.

`CreateQueue(name, opts...)` respects per‑queue `engine.Config` overrides. `GetQueue`, `ListQueues`, `DeleteQueue`, and `Stats` mirror the public `Client` API. `Stats()` takes only `RLock` and reads each queue's in‑memory counters for O(1) metrics:

```go
type EngineStats struct {
    ActiveQueues int
    TotalBytes   int64
    Queues       map[string]QueueStats
}
type QueueStats struct {
    MessageCount uint64
    SegmentCount int
    SizeBytes    int64
    LastOffset   uint64
    MinOffset    uint64
}
```

#### 5.1.3 `queue.go` — Per‑Queue Core

`Queue` fields:

```go
type Queue struct {
    name              string
    dir               string
    cfg               Config
    active            *segment
    segments          []*segment
    nextOffset        uint64
    minOffset         uint64
    publishedOffset   uint64
    w                 *wal
    offsets           *OffsetStore
    cache             *ringCache
    nextPreallocBytes int64
    closed            bool
    mu                sync.RWMutex
}
```

**`Config`** (per queue): `BatchSize`, `MaxInterval`, `MinInterval`, `MaxSegmentBytes`, `MaxMessageBytes`, `MaxSegmentAge`, `Encrypted`, `Fsync`. `withDefaults()` supplies non‑zero defaults; `MinInterval == 0` disables linger, negative values are clamped to 0.

**Segment rolling policy.** `nextPreallocBytes` starts at `min(8 MiB, MaxSegmentBytes)` and adapts inside `roll()`:

- segment filled under `fastFillThreshold` (10 s) → double `nextPreallocBytes` (capped at `min(256 MiB, MaxSegmentBytes)`),
- segment filled after `slowFillThreshold` (5 m) → halve (floored at the 8 MiB min),
- the active segment is truncated to its written size before `enableMMap()`.

**`AppendBatch`** stages per‑segment sub‑batches so a WAL write never straddles a roll. For each payload it:

1. validates `len(payload) <= MaxMessageBytes`,
2. encodes with `uuid.NewV7()` and a shared timestamp,
3. stages into a `stagedBatch{w, seg, recs, positions, recLens, offsets, ids, totalLen, startIdx}`,
4. when a roll is required (size or age), commits the staged group via `commitSync` (Fsync=true) or `commitAsync` (Fsync=false), then `roll()`s and starts a new group,
5. final staged group is committed after the loop.

The sync path calls `wal.pushBatch` and then either unlocks to wait on `wal.wait(seq)` or delivers the wake token. The async path clones payloads into the ring cache, publishes the record onto `wal.asyncChan`, and advances `publishedOffset` immediately.

**`Read`** is:

1. `q.cache.Get(offset)` — ring cache hit returns a shallow message,
2. `q.locateSegment(offset)` — O(log S) binary search over `q.segments` base offsets,
3. `q.readFromSegment(seg, offset)` — reads from `seg.mmapData` (closed segment) or `seg.logFile.ReadAt` (active segment), then `decodeRecord`.

**`ReadBatch`** walks offsets in order, reusing the current segment pointer between consecutive offsets and short‑circuiting to `locateSegment` only on boundary crossings.

**`TruncateBefore`** is implemented in `compact.go`. It walks closed segments whose `nextOffset <= targetOffset`, calls `seg.destroy()` to unmap and unlink, updates `minOffset`, and calls `cache.EvictBefore(minOffset)`. The active segment is never truncated.

#### 5.1.4 `wal.go` — Write‑Ahead Log

```go
type wal struct {
    cfg         Config
    file        *os.File

    mu          sync.Mutex
    cond        *sync.Cond
    buf         []byte
    pending     []pendingEntry
    swapBuf     []byte
    swapPending []pendingEntry
    pushed      int64
    published   int64
    err         error

    asyncChan chan asyncEntry   // async (Fsync=false) fast path
    wake      chan struct{}
    shutdown  chan struct{}
    done      chan struct{}
    stopOnce  sync.Once
}
```

- `push` / `pushBatch` append into `buf`/`pending` under lock and report `wake = len(pending) >= BatchSize`.
- `syncLoop` selects over `asyncChan`, `ticker.C` (`MaxInterval`), `wake`, and `shutdown`.
- `adaptiveFlush` sleeps `MinInterval` before `flush()` when the current pending count is below `BatchSize`.
- `flush` swaps buffers, performs `file.WriteAt`, and if `Fsync` is enabled calls `syncData`; broadcasts on `cond` and drains any pending wake token.
- `drainAndFlushAsync(first)` groups up to `BatchSize` entries from `asyncChan`, writes each record at its `pos`, writes the corresponding `.idx` entry via `writeIndexAt`, and advances `published` contiguously.
- `stop()` uses `stopOnce` to close `shutdown`, wait for `done`, and return the last error.

**Platform‑specific `syncData`:**

| OS | Implementation |
|---|---|
| Darwin | `unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)` (drive cache flush) |
| Linux/BSD | `unix.Fdatasync(fd)` |
| Windows | `file.Sync()` |

#### 5.1.5 `segment.go` — Segments & mmap

`segment` holds the `.log` file, the `.idx` file, `pos` (written bytes), `preallocSize`, `createdAt`, and two `mmap.MMap` regions (`mmapData`, `idxMMap`).

- `openSegment` preallocates the `.log` file to `preallocSize`.
- `enableMMap()` opens a read‑only `mmap` for both files **only if the file size is > 0** (Windows robustness).
- `recover()` first tries `tryFastRecovery` (validates last index entry and copies all raw entries); on failure it calls `rebuild()` which re‑walks the `.log` file, validates each CRC‑checked record, and rewrites the `.idx`.
- `readIndexEntry(offset)` reads 16 bytes from `idxMMap` (closed segment) or `idxFile.ReadAt`.
- `destroy()` unmaps, closes, and unlinks both files, returning reclaimed bytes.
- `Close()` truncates the active `.log` to `pos`, unmaps any `mmap` regions, and closes both file handles.

#### 5.1.6 `encode.go` — Record Format

```
[4B Body Length][4B CRC32][2B ID Length (16)][16B Raw UUID v7][8B Unix Nanoseconds][Payload]
```

`crc32.ChecksumIEEE` covers `record[8:]` (the body). `decodeRecord` validates length, CRC, ID length, then returns `(id string, ts time.Time, payload []byte, err error)`. Constants: `minRecordSize = 34`, `offUUID = 10`, `offTS = 26`, `offBody = 34`.

#### 5.1.7 `cache.go` — Ring Cache

`ringCache` is a `slots []Message` sized to `defaultRingCacheCapacity = 16384`, indexed by `offset & (capacity-1)` under `RWMutex`. `EvictBefore(minOffset)` zeros any slot below the truncation floor.

#### 5.1.8 `offsets.go` — Manager‑Level Offset Store

A single `manager-offsets.dat` file persists `(queue:group) → uint64` entries as `[2B keyLen][key][8B offset]` tuples. `recover()` streams valid records and stops silently on torn tails. `Commit` appends and `Sync`s the file, then triggers `compactLocked()` every `compactAppendsThreshold = 5000` appends, which rewrites the file atomically via `os.Rename`. `Fetch` returns the in‑memory offset.

#### 5.1.9 `compact.go` — Segment Metadata & Truncation

```go
type SegmentInfo struct {
    BaseOffset uint64
    NextOffset uint64
    SizeBytes  int64
    CreatedAt  time.Time
    IsActive   bool
}

func (q *Queue) MinOffset() uint64
func (q *Queue) Segments() []SegmentInfo
func (q *Queue) TruncateBefore(ctx context.Context, targetOffset uint64) (int64, error)
```

`TruncateBefore` only deletes **closed** segments whose `nextOffset <= targetOffset`; on the first segment spanning the target it stops. It updates `minOffset` and calls `cache.EvictBefore`.

#### 5.1.10 `encrypt_*.go` — Kernel Directory Encryption

| OS | Mechanism |
|---|---|
| Linux | `unix.Syscall(SYS_IOCTL, fsIocSetEncryptionPolicy, …)` with `FS_ENCRYPTION_MODE_AES_256_XTS` for contents and `AES_256_CTS` for filenames. Silently tolerates `ENOTTY`, `EOPNOTSUPP`, `EEXIST`. |
| Windows | `EncryptFileW(path)` from `advapi32.dll` (NTFS EFS). |
| Other (macOS/BSD) | Logs a warning and returns `nil`. |

---

### 5.2 `internal/logqueue` — Log Driver

#### 5.2.1 `logqueue.go`

`LogQueue` wraps an `engine.Storage` and caches `queueGroup` handles in `groups map[string]*queueGroup`.

- `Produce` / `ProduceBatch` / `Read` / `ReadBatch` forward directly to `storage`.
- `Stream(ctx, startOffset)` yields messages by walking `storage.Read` until `ErrOffsetNotFound`, then returns.
- `Tail(ctx, startOffset)` polls `storage.LastOffset()`; when `offset < last` it reads and yields; otherwise sleeps 5 ms. On transient errors (`ErrOffsetNotFound`, `ErrCorruptRecord`, `io.EOF`, `io.ErrUnexpectedEOF`) it sleeps 5 ms and retries the same offset, keeping tail streams alive across concurrent WAL writes.
- `Group(name)` returns a cached `queueGroup` (a `group.Base` with `CommitFn` bound to `LogQueue.CommitOffset`).
- `Close()` is a no‑op; physical storage is owned by the manager.

#### 5.2.2 `retention.go`

```go
type LogRetentionPolicy struct { TTL time.Duration }

func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, error)
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error)
```

`TruncateFloor` computes `max(ttlFloor, consumerFloor)`:

- **consumerFloor** = minimum `HighWater` across all in‑memory consumer groups (0 if no groups exist — treated as "no constraint"),
- **ttlFloor** = the `NextOffset` of the last non‑active segment whose `CreatedAt` is older than `policy.TTL`.

If `minOffset == 0` is passed to `Truncate`, the driver substitutes its own `TruncateFloor` result before delegating to `storage.TruncateBefore`.

#### 5.2.3 `api.go` — Interface Contract

`ClientLogQueue` is the sole interface upper layers depend on, keeping `engine` types out of the public SDK.

---

### 5.3 `internal/taskqueue` — Task Driver

#### 5.3.1 `taskqueue.go`

`TaskQueue` implements at‑least‑once delivery over `engine.Storage`. State transitions are serialized under `mu`; task writes and checkpoint writes share the same critical section so a checkpoint never overtakes the driver.

```go
type TaskQueue struct {
    name    string
    storage engine.Storage

    mu                sync.Mutex
    active            map[string]*taskState
    dlq               map[string]Task
    dlqUncheckpointed map[string]struct{}
    schedule          scheduleHeap
    sequence          uint64
    scanNext          uint64

    checkpointOffset uint64
    dirty            uint64
    notify           chan struct{}
    stopCh           chan struct{}
    wg               sync.WaitGroup
    closing          bool
    closed           bool
}
```

**Lifecycle:** `NewTaskQueue(name, storage)` opens the queue, calls `loadCheckpointLocked`, sets `scanNext` from `MinOffset()` if needed, then `refreshLocked()` to replay any records appended after the checkpoint, and finally starts the `checkpointLoop` goroutine.

**`Enqueue`** resolves `TaskOption`s → `(visibilityTimeout, maxAttempts, scheduledAt)`, generates a random 16‑byte hex ID (`id.go`), appends `encodeTaskRecord` to storage, installs an `active` map entry, pushes onto `schedule`, increments `scanNext`, and broadcasts.

**`Dequeue`** loops over `takeDueLocked` until a task is due, using `time.NewTimer` around `waitUntil` and waking on `notify` or context cancel.

**`takeDueLocked(now)`** iterates the schedule heap:

- pops stale entries (version mismatch or missing from `active`),
- if the due task is in‑flight on its **final** attempt → dead‑letter, append `encodeDeadLetterRecord`, move to `dlq`, mark `dlqUncheckpointed`, broadcast, and continue,
- otherwise assign a fresh `DeliveryID`, `inFlight = true`, `Attempts++`, `Visibility = now + visibilityTimeout`, push back onto the schedule.

**`Ack` / `Fail` / `Extend`** validate the lease with `validateLeaseLocked` (task ID + offset + `Attempts` + `DeliveryID` must all match) before mutating state. `Fail` respects exponential backoff from `ResolveFailOptions` and dead‑letters once `Attempts >= MaxAttempts`. `Extend` only extends forward.

**By‑ID variants** (`AckID`, `FailID`, `ExtendID`, `AckDeadLetterID`) resolve the current in‑flight lease via `currentLeaseLocked(taskID)` and are used by stateless CLI/REPL admin operations.

**`Close`** stops the checkpoint loop, writes a final checkpoint, and marks `closed`.

**`checkpointLoop`** ticks every `defaultCheckpointInterval = 5 s` and writes a checkpoint when `dirty > 0`.

#### 5.3.2 `task.go` — Type Definitions

```go
type Task struct {
    ID          string
    Payload     []byte
    Attempts    uint32
    MaxAttempts uint32
    Visibility  time.Time
    ScheduledAt time.Time
    DeliveryID  string
    offset      uint64
}
func (t Task) OriginalOffset() uint64
```

Defaults: `defaultVisibilityTimeout = 30s`, `defaultMaxAttempts = 5`, `defaultRetryDelay = 1s`, `defaultMaxRetryDelay = 5m`, `defaultCheckpointInterval = 5s`, `checkpointEvery = 128`, `checkpointChunkBytes = 64 KiB`.

`ResolveFailOptions(task)` produces exponential backoff: `delay *= 2` per retry, capped at `defaultMaxRetryDelay`.

#### 5.3.3 `checkpoint.go` — Durable Snapshots

Records carry an 8‑byte magic header (`M I K Y U T Q 1`) followed by a kind byte:

| Kind | Meaning |
|---|---|
| 1 `recordTask` | Task enqueue |
| 2 `recordCPStart` | Checkpoint start (covered‑through offset, version) |
| 3 `recordCPChunk` | Checkpoint chunk `(index, data)` |
| 4 `recordCPCommit` | Checkpoint commit `(startOffset, totalChunks, coveredThrough, version)` |
| 5 `recordDLQ` | Dead‑letter move |
| 6 `recordDLQAck` | Dead‑letter acknowledgement |

`encodeSnapshot` writes a deterministic, offset‑sorted snapshot of `active` states (including `nextAt`, `inFlight`, `visibilityTimeout`, `DeliveryID`) plus the DLQ. `checkpointLocked`:

1. `refreshLocked` to cover foreign records appended since the last task op,
2. snapshot `active` + `dlq`,
3. `append(recordCPStart)`,
4. split the snapshot into ≤ `min(MaxAppendPayloadBytes() - header, checkpointChunkBytes)` chunks,
5. `append(recordCPChunk)` per chunk,
6. `append(recordCPCommit)`,
7. verify `commitOffset == startOffset + totalChunks + 1`,
8. clear `dlqUncheckpointed` and reset `dirty`.

`loadCheckpointLocked` scans backward from `LastOffset()` for the newest valid commit record, verifies the start, chunk indices, and snapshot integrity, then rebuilds `active` + `dlq`. Corrupt or incomplete checkpoints are silently skipped in favor of an earlier valid one.

`refreshLocked` walks `scanNext` forward, decoding `recordTask`, `recordDLQ`, and `recordDLQAck`; any unknown kind (e.g. a foreign log record) is ignored, allowing the queue to share storage with `logqueue`.

#### 5.3.4 `scheduler.go` — Heap Scheduling

```go
type scheduleItem struct {
    id      string
    due     time.Time
    version uint64
    seq     uint64
}
```

A `scheduleHeap` (`Less` compares `due`, tie‑breaks on `seq`). `pushScheduleLocked` bumps `q.sequence` and pushes. `pruneScheduleLocked` rebuilds the heap when `len(schedule) > len(active)*3 + 1024` to bound memory.

#### 5.3.5 `compaction.go` — `TruncateFloor`

```go
func (q *TaskQueue) TruncateFloor(ctx context.Context) (uint64, error) {
    // closed → ErrTaskClosed
    // checkpointOffset == 0 → 0 (no safe truncation yet)
    // floor = min(checkpointOffset, min active offset, min uncheckpointed DLQ offset)
}
```

Once all active tasks advance past the last checkpoint and the DLQ is fully checkpointed, the floor equals the last checkpoint start offset.

---

### 5.4 `internal/retention` — Cross‑Driver Retention

```go
type DriverProvider interface {
    TruncateFloor(ctx context.Context) (uint64, error)
}

type QueueTarget struct {
    Name    string
    Storage engine.Storage
    Drivers []DriverProvider
    mu      sync.Mutex
}

type Coordinator struct {
    mu       sync.RWMutex
    targets  map[string]*QueueTarget
    interval time.Duration
}
```

Key methods:

- `RegisterTarget(*QueueTarget)` — replaces the driver set for a queue.
- `RegisterDriver(name, storage, driver)` — idempotently appends a driver to the target's list.
- `UnregisterDriver(name, driver)` — removes one driver; deletes the target when empty.
- `UnregisterTarget(name)` — removes the queue entirely.
- `ReconcileQueue(ctx, name)` — delegates to `ReconcileQueueAt(ctx, name, 0)`.
- `ReconcileQueueAt(ctx, name, requested)` — computes `min(floors)` across all drivers, clamps to any non‑zero `requested` floor, and calls `target.Storage.TruncateBefore(safeFloor)`. Returns 0 reclaimed when `safeFloor == 0` or no drivers exist.
- `ReconcileAll(ctx)` — iterates every target and returns reclaimed bytes per queue.
- `Start(ctx)` — background ticker loop calling `ReconcileAll`.

The coordinator is deliberately driver‑agnostic: `logqueue.LogQueue` and `taskqueue.TaskQueue` both satisfy `DriverProvider` via their `TruncateFloor` implementations, and the coordinator's identity‑based driver deduplication relies only on comparable interfaces.

---

### 5.5 `internal/group` — Consumer Group Base

```go
type Base struct {
    Name      string
    Mu        sync.Mutex
    HighWater uint64
    Closed    bool
    CommitFn  CommitFunc
}
```

- `Mark(offset)` — sets `HighWater = offset + 1` if `offset >= HighWater` (monotonic).
- `Commit(ctx)` — snapshots `HighWater` under lock, calls `CommitFn` unlocked.
- `Close()` — idempotent; commits once before returning.

`logqueue.queueGroup` and `client.remoteGroup` both embed `Base`, so embedded and remote consumer groups share identical concurrency semantics.

---

### 5.6 `internal/protocol` — Wire Format

#### 5.6.1 `frames.go`

```
+-----------------+---------------+----------------------+
| Length (uint32) | Opcode (u8)   | Payload              |
| 4 bytes         | 1 byte        | (Length - 1) bytes   |
+-----------------+---------------+----------------------+
```

`MaxFrameBytes = 256 MiB`. `WriteFrame` grabs a `[]byte` from a `sync.Pool` seeded with 32 KiB buffers, grows it if needed while retaining the pointer for reuse, writes `[length][op][payload]`, and returns the buffer to the pool. `ReadFrame` reads the header, validates bounds, allocates exactly `length-1` bytes for the payload, and reads it in one `io.ReadFull`.

#### 5.6.2 `bytes.go`

`Reader` provides bounds‑checked `ReadU32`, `ReadU64`, `ReadBytes`. `Buffer` provides `WriteU32`, `WriteU64`, `WriteBytes` (length‑prefixed).

#### 5.6.3 `opcodes.go`

```
0x01 CreateQueue   0x02 Produce    0x03 Read          0x04 Tail
0x05 CommitOffset  0x06 FetchOffset 0x07 ProduceBatch  0x08 ReadBatch
0x09 ListQueues    0x0A DeleteQueue 0x0B Stats         0x0C Shutdown
0x0D TaskEnqueue   0x0E TaskDequeue 0x0F TaskAck       0x10 TaskFail
0x11 Truncate      0x12 TaskExtend  0x13 TaskDeadLetters
0x14 TaskDeadLetterAck
0x15 TaskAckByID   0x16 TaskFailByID 0x17 TaskExtendByID
0x18 TaskDeadLetterAckByID
0xFF Error
```

Error codes: `Unknown`, `QueueNotFound`, `QueueExists`, `OffsetNotFound`, `OffsetTruncated`, `TaskNotAvailable`, `TaskNotFound`, `TaskNotInflight`, `TaskStale`, `TaskClosed`, `InvalidTask`, `DeadLetterNotFound`, `DeadLetterStale`.

`ParseTarget` supports `tcp://`, `unix://`, `pipe://` schemes, plus bare `host:port` (treated as TCP).

#### 5.6.4 `task.go`

`EncodeTask` / `DecodeTask` serialize `Task` including `DeliveryID` and `OriginalOffset` so remote servers can reject stale leases. `EncodeTaskList` / `DecodeTaskList` handle `DeadLetters` responses. `EncodeTaskID` / `DecodeTaskID` are used for By‑ID opcodes.

---

### 5.7 `internal/server` — Wire Server

```go
type Server struct {
    mgr       *engine.Manager
    listener  net.Listener
    tlsConfig *tls.Config
    queues    map[string]*logqueue.LogQueue
    tasks     map[string]*taskqueue.TaskQueue
    mu        sync.RWMutex
    stopCh    chan struct{}
    doneCh    chan struct{}

    retention       *retention.Coordinator
    retentionCancel context.CancelFunc
    retentionWG     sync.WaitGroup
}
```

**`getOrCreateLogQueue` / `getOrCreateTaskQueue`** use double‑checked locking to guarantee stable wrappers per queue and register each with the retention coordinator.

**`ListenAndServe(target)`** parses the target, creates an `ipc.Listener` for `unix`/`pipe` or a plain `net.Listener` for `tcp`, optionally wraps with `tls.NewListener`, then `Serve`.

**`Serve(ln)`** accepts connections, spawning `serveConn` per connection.

**`serveConn`** loops: `protocol.ReadFrame` → `dispatch` → `protocol.WriteFrame`. If `dispatch` reports `shouldShutdown`, it launches `s.Stop()` and `s.mgr.Close()` in a goroutine and returns.

**`dispatch`** routes every opcode to its handler. Task handlers (`handleTaskEnqueue`, `handleTaskDequeue`, `handleTaskAck`, `handleTaskFail`, `handleTaskExtend`, `handleTaskDeadLetters`, `handleTaskDeadLetterAck`, and their By‑ID variants) all resolve queues through `getOrCreateTaskQueue` and return errors through `taskErr`, which maps `taskqueue` errors to specific wire error codes.

**`handleTruncate`** materializes both drivers before calling `s.retention.ReconcileQueueAt(ctx, name, minOffset)`.

#### `tls.go`

`DefaultTLSConfig(certFile, keyFile)` loads an X.509 keypair or generates a self‑signed ECDSA P‑256 certificate (`localhost`, `127.0.0.1`, `::1`, 1‑year validity). TLS `MinVersion` is `tls.VersionTLS13`.

---

### 5.8 `internal/ipc` — Local IPC

`Listen(target)` returns a `*Listener` wrapping either a Unix Domain Socket (with stale file removal and `chmod 0600`) or a Windows named pipe. `Listener.Close()` unlinks the Unix socket file.

`DialContext(ctx, target)` maps `pipe://` to the underlying `unix` network on both POSIX (where it becomes a Unix socket) and Windows (where the address is normalized to `\\.\pipe\...`).

---

### 5.9 `internal/daemon` — Lifecycle

```go
func Run(configPath string) error
func EnsureDaemonRunning(target string) error
func IsDaemonAlive(target string) bool
func SendShutdownSignal(target string) error
```

- `Run` loads config, opens the engine, reconciles declarative queues, optionally wraps the server in TLS, runs the server, and blocks on `SIGINT`/`SIGTERM` or `srv.Done()`.
- `EnsureDaemonRunning` probes the target; if dead, spawns a detached copy of `os.Executable()` with `daemon` and waits up to 3 s for it to come online.
- `setDetachedProcessAttr` uses `Setsid: true` on Unix and `CREATE_NO_WINDOW` on Windows.

---

### 5.10 `internal/config` — Declarative Configuration

`DefaultConfig()` supplies safe defaults:

```yaml
server:
  target: "tcp://127.0.0.1:7420"
  data_dir: "./data"
engine:
  batch_size: 256
  max_interval: "10ms"
  min_interval: "30us"
  max_segment_bytes: 67108864
  max_message_bytes: 1048576
  max_segment_age: "1h"
  encrypted: false
  fsync: true
queues: []
```

`resolveOrBootstrapPath` finds a config at the flag path, `./mikyu.yaml`, or `$XDG_CONFIG_HOME/mikyu/mikyu.yaml`, writing the default template if none exist. `applyEnvOverrides` recognizes `MIKYU_SERVER_TARGET`, `MIKYU_SERVER_DATA_DIR`, `MIKYU_ENGINE_BATCH_SIZE`, `MIKYU_ENGINE_MAX_INTERVAL` (legacy alias `MIKYU_ENGINE_FLUSH_INTERVAL`), `MIKYU_ENGINE_MIN_INTERVAL`, `MIKYU_ENGINE_MAX_SEGMENT_BYTES`, `MIKYU_ENGINE_MAX_MESSAGE_BYTES`, `MIKYU_ENGINE_MAX_SEGMENT_AGE`, `MIKYU_ENGINE_ENCRYPTED`, and `MIKYU_ENGINE_FSYNC`.

`ToEngineConfig` and `ToQueueEngineConfig` merge defaults with per‑queue overrides (respecting pointer fields for `Encrypted` / `Fsync`).

---

### 5.11 `internal/client` — Embedded & Remote Drivers

#### 5.11.1 `options.go`

`Config` carries client‑wide defaults and remote connection parameters: `BatchSize`, `MaxInterval`, `MinInterval`, `MaxConns`, `DefaultMaxSegmentBytes`, `DefaultMaxMessageBytes`, `DefaultMaxSegmentAge`, `Encrypted`, `Fsync`, `TLSConfig`. `QueueConfig` mirrors per‑queue overrides. `DefaultClientConfig()` produces the same defaults advertised in the YAML template.

#### 5.11.2 `embedded.go`

`embeddedClient` owns an `engine.Manager`, a `queues map[string]*logqueue.LogQueue`, a `tasks map[string]TaskQueue`, and a background `retention.Coordinator`.

- `NewEmbedded` builds an `engine.Config` from the client `Config` and starts the retention loop.
- `CreateQueue` builds a per‑queue `engine.Config`, creates the `engine.Queue`, wraps it in `logqueue.NewLogQueue`, and registers it with retention.
- `LogQueue` uses `getOrCreateLogQueue` (double‑checked locking).
- `TaskQueue` lazily constructs `taskqueue.NewTaskQueue` on the existing storage and registers it with retention.
- `TruncateQueue` materializes both drivers, then calls `retention.ReconcileQueueAt(ctx, name, minOffset)`.
- `DeleteQueue` closes the task queue, drops cached handles, unregisters the retention target, and calls `mgr.DeleteQueue`.
- `Stats` copies the manager stats into a shallow new map.
- `Close` cancels the retention loop, closes all task queues, and closes the manager.

#### 5.11.3 `remote.go`

`remoteClient` holds a buffered `pool chan *clientConn`, a `connSem chan struct{}` sized to `MaxConns`, and an `atomic.Bool closed` flag.

- `acquire(ctx)` takes a slot on `connSem`, tries `pool`, otherwise `dialConn(ctx)`.
- `dialConn` uses `ipc.DialContext` for `unix`/`pipe` and either `tls.Dialer` or `net.Dialer` for TCP, with a 5 s timeout.
- `release(ch)` returns the connection to `pool` unless closed or the pool is full.
- `Close()` flips the closed flag and drains the pool.
- `exchange(ctx, op, body)` acquires a connection, sets the deadline from `ctx`, writes a frame, reads the response, and decodes `OpError` into a Go error via `decodeProtocolError`.

`remoteLogQueue` implements `ClientLogQueue`; `remoteTaskQueue` implements `ClientTaskQueue` with polling `Dequeue` (10 ms backoff on `ErrTaskNotAvailable`); `remoteGroup` embeds `group.Base` with a `CommitFn` that exchanges `OpCommitOffset`.

---

### 5.12 `internal/command`, `internal/cli`, `internal/repl` — Access Layers

#### 5.12.1 `internal/command`

Stateless helpers used by both CLI and REPL:

- Log: `List`, `Create`, `Delete`, `Produce`, `Read`, `Tail`, `Stats`, `Truncate`, `Shutdown`, `ResolvePayload`, `GetTerminalWidth`, `FormatSingleLinePayload`.
- Task: `Publish`, `Pop`, `TaskAck`, `TaskNack`, `TaskExtend`, `TaskDeadLetters`, `TaskAckDeadLetter`.
- Flag extraction (`flags.go`): `ExtractDuration`, `ExtractOptionalDuration`, `ExtractUint32`, `ExtractOptionalUint32`, `ExtractTime`, `ExtractOptionalTime` — all support `--flag value` and `--flag=value`, and remove the flag from the slice.

Destructive operations (`Delete`, `Truncate`, `Shutdown`) accept a `confirmFn` so both CLI and REPL can share the same logic with different confirmation prompts.

#### 5.12.2 `internal/cli`

`RunCLI(target, args)` ensures the daemon is running, dials the client, and dispatches on `args[0]` to a subcommand handler. Log subcommands: `list`, `create`, `delete`, `produce`, `read`, `tail`, `stats`, `truncate`, `shutdown`. Task subcommands: `publish`, `pop`, `ack`, `nack`, `extend`, `dlq`, `dlq-ack`.

`confirmCLI` prints a prompt on a terminal and requires `-y`/`--yes`/`-f`/`--force` in non‑interactive mode. `printCLITask` / `printCLITasks` render Lipgloss cards on a terminal and JSON otherwise.

#### 5.12.3 `internal/repl`

`replSession` maintains `client`, `target`, `ctx`, `activeQueue`, `lastTaskQueue`, `lastTaskID`. Dispatch is a switch over the first token (`exit`/`quit`, `help`, `list`, `use`, `create`, `delete`, `produce`, `read`, `tail`, `stats`, `truncate`, `publish`, `pop`, `ack`, `nack`, `extend`, `dlq`, `dlq-ack`, `shutdown`).

`style.go` defines the Lipgloss palette: fire‑gradient ASCII banner, command table styling, confirmation arrow, task/dead‑letter card borders. `PrintREPLBanner` prints the banner, a short command reference table, a quickstart hint, and the connection status.

`resolveQueue` in `task.go` allows the REPL to remember the active queue so commands like `ack` and `nack` can omit the queue name. `shortID` truncates IDs for display.

`vt_windows.go` enables `ENABLE_VIRTUAL_TERMINAL_PROCESSING` on stdout/stderr.

---

### 5.13 `cmd/mikyu/main.go`

```go
const defaultTarget = "tcp://127.0.0.1:7420"

func run(args []string) error {
    switch {
    case len(args) == 0:              return repl.RunREPL(defaultTarget)
    case args[0] == "daemon" || args[0] == "server": return daemon.Run(args[1:])
    case args[0] == "shell" || args[0] == "repl":    return repl.RunREPL(defaultTarget)
    case args[0] == "cli":            cli.RunCLI(defaultTarget, args[1:])
    default:                          cli.RunCLI(defaultTarget, args)
    }
}
```

---

## 6. Cross‑Cutting Concerns

### 6.1 Concurrency Model

| Subsystem | Synchronization |
|---|---|
| `engine.Queue` | `sync.RWMutex` guarding segments, `minOffset`, `publishedOffset`, `active`, `nextOffset`, WAL pointer. |
| `engine.wal` | `sync.Mutex` + `sync.Cond` for the sync path; buffered `asyncChan` for the async path. |
| `ringCache` | `sync.RWMutex`; slots are read/written independently. |
| `OffsetStore` | `sync.RWMutex`; writes are flushed and `Sync`'d under lock. |
| `taskqueue.TaskQueue` | Single `sync.Mutex` guarding all state transitions and checkpoint writes; `notify` channel broadcast on every state change. |
| `retention.Coordinator` | `sync.RWMutex` on the target map; per‑target `sync.Mutex` on driver lists. |
| `group.Base` | `sync.Mutex` for `HighWater`. |
| `remoteClient` | Buffered channel pool + `connSem` semaphore; `atomic.Bool` closed flag. |
| `server.Server` | `sync.RWMutex` on queue/task maps; double‑checked locking in `getOrCreate*`. |

### 6.2 Error Handling

- Public methods honor `ctx.Err()` and return the context error.
- `engine` errors are the canonical queue error set; `taskqueue` errors are task‑specific.
- `protocol.decodeProtocolError` maps wire codes back to the exact Go sentinel.
- `server.taskErr` maps each `taskqueue` error to the matching wire code.
- The REPL and CLI render errors with styled red or JSON output depending on TTY detection.

### 6.3 Security & Encryption

- Directory encryption: `fscrypt` (Linux) / NTFS EFS (Windows) / warning fallback.
- TLS 1.3 for TCP when configured (`tls.VersionTLS13` minimum); IPC bypasses TLS.
- Self‑signed ECDSA certificates are generated automatically when no keypair is supplied.
- Unix sockets are `chmod 0600` and stale sockets are removed on `Listen`.

### 6.4 Configuration & Environment

Covered in §5.10. All engine knobs (`BatchSize`, `MaxInterval`, `MinInterval`, `MaxSegmentBytes`, `MaxMessageBytes`, `MaxSegmentAge`, `Encrypted`, `Fsync`) are configurable at daemon, client, and per‑queue scope, with env overrides for daemon scope.

### 6.5 Deployment Modes

1. **Embedded** — `mikyu.Open(dir, opts...)` → in‑process `engine.Manager`, no network.
2. **Remote Client** — `mikyu.Dial(target, opts...)` → pooled wire client.
3. **Daemon** — `daemon.Run(configPath)` → background server with auto‑spawn via `EnsureDaemonRunning`.

---

## 7. Storage Layout & Record Format

```
<data_dir>/
├── manager-offsets.dat      # Compacted consumer-group offset store
├── manager-offsets.dat.tmp  # Atomic-rename scratch file
└── <queue_name>/
    ├── 00000000000000000000.log   # Log segment (pre-allocated while active)
    ├── 00000000000000000000.idx   # Fixed-width index (16 bytes per record)
    ├── 00000000000000001024.log
    ├── 00000000000000001024.idx
    └── ...
```

Segment base offsets are zero‑padded 20‑digit decimals. Every record on disk:

```
[4B Body Length][4B CRC32][2B ID Length (16)][16B Raw UUID v7][8B Unix Nanoseconds][Payload]
```

- `crc32.ChecksumIEEE` covers `[CRC32+4 : end]`.
- `minRecordSize = 34`.
- Payload is bounded by `MaxMessageBytes`.

The `.idx` file is an array of `{pos int64, len int64}` pairs. `rebuild()` re‑derives the index by walking the `.log` file and verifying CRC; `tryFastRecovery` short‑circuits when the last index entry points inside a valid record.

---

## 8. Retention & Truncation Flow

```
Consumer Groups (logqueue)             Active Tasks + DLQ (taskqueue)
        │                                       │
        │ TruncateFloor()                       │ TruncateFloor()
        ▼                                       ▼
┌───────────────────────────────┐   ┌───────────────────────────────────┐
│ min(HighWater across groups)  │   │ checkpointOffset constrained by   │
│ max with TTL-expired segments │   │ active task offsets and           │
│                               │   │ uncheckpointed DLQ offsets        │
└───────────────┬───────────────┘   └───────────────┬───────────────────┘
                │                                   │
                └──────────────┬────────────────────┘
                               ▼
                retention.Coordinator.ReconcileQueueAt
                  • min(all driver floors)
                  • clamped by any nonzero `requested`
                  • engine.Storage.TruncateBefore(floor)
                               ▼
                     Closed segments whose nextOffset <= floor
                     are unmapped, unlinked, and ring-cache-evicted
```

`truncate` from the CLI/REPL/daemon goes through the coordinator, so both drivers are always consulted. The active segment is never truncated; segment rolls are the physical granularity.

---

## 9. Task Queue Semantics (Precise)

| Operation | Effect |
|---|---|
| `Enqueue` | Appends a `recordTask`. Installs an `active` state at `ScheduledAt`. `Attempts = 0`. |
| `Dequeue` | Pops the due task from the heap; on the **final** attempt expiry, dead‑letters instead of redelivering. Otherwise assigns a fresh `DeliveryID`, sets `Attempts++`, and a `Visibility` deadline. |
| `Ack` | Verifies `(ID, offset, Attempts, DeliveryID)`; removes from `active`; broadcasts. |
| `Fail` | Same verification. If `Attempts >= MaxAttempts`, appends `recordDLQ`, moves to `dlq`, marks `dlqUncheckpointed`. Otherwise resets to pending with `RetryAfter` delay, bumps version, pushes back onto the schedule. |
| `Extend` | Only extends `Visibility` forward; never increments attempts. |
| `AckID` / `FailID` / `ExtendID` | Resolve the current lease by ID; enforce that a lease exists and is unexpired. |
| `DeadLetters` | Sorted snapshot of the durable DLQ. |
| `AckDeadLetter` / `AckDeadLetterID` | Verify `(offset, Attempts, MaxAttempts)` and append `recordDLQAck`. |
| `Close` | Stop checkpoint loop; write a final checkpoint if dirty. |

Backoff: `defaultRetryDelay * 2^(Attempts-1)`, capped at `defaultMaxRetryDelay` (5 min).

---

## 10. Wire Protocol Summary

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

Frames are length‑prefixed (`uint32` big‑endian `= 1 + payload length`). Responses always reuse the request opcode on success; failures return `OpError` with a one‑byte error code and a human message.

Task wire bodies always include the delivery ID and the engine offset so the server can reject stale acknowledgements with `ErrCodeTaskStale`.

---

## 11. Performance Characteristics (Measured)

| Path | Result |
|---|---|
| Produce (durable, `Fsync: true`) | ≈292.6 µs/op for 64 B payloads |
| Produce (async, `Fsync: false`) | ≈25.9 µs/op for 64 B payloads (**≈3.5× faster**) |
| Ring‑cache read | ≈100 ns/op, zero allocations |
| `encodeRecord` | ≈887 ns/op, zero allocations |
| WAL steady‑state | 0 allocs/op, 3 B/op (double‑buffered swap) |
| 64 KB protocol frame | 38.5 µs → 3.5 µs (90.7 % reduction, zero extra heap allocations) |
| Index memory | `O(log S)` segments; no per‑message in‑memory index slice |
| Task enqueue / dequeue+ack | High concurrent rates; checkpoint writes batched every 5 s or 128 dirty ops |

---

## 12. Appendix — Public API Quick Reference

**Client lifecycle**

```go
client, _ := mikyu.Open("/var/lib/mikyu")     // embedded
client, _ := mikyu.Dial("tcp://host:7420")     // remote
defer client.Close()
```

**Log queue**

```go
client.CreateQueue(ctx, "events")
q, _ := client.LogQueue("events")
off, _ := q.Produce(ctx, []byte("hello"))
msg, _ := q.Read(ctx, off)
for m, err := range q.Tail(ctx, 0) { … }
g := q.Group("consumers"); g.Mark(off); g.Commit(ctx)
```

**Task queue**

```go
tq, _ := client.TaskQueue("jobs")
task, _ := tq.Enqueue(ctx, payload, mikyu.WithVisibilityTimeout(30*time.Second), mikyu.WithMaxAttempts(3))
got, _ := tq.Dequeue(ctx)
tq.Ack(ctx, got)
tq.Fail(ctx, got, mikyu.WithRetryAfter(time.Second))
tq.Extend(ctx, got, time.Minute)
dls, _ := tq.DeadLetters(ctx)
tq.AckDeadLetter(ctx, dls[0])
```

**Maintenance**

```go
client.TruncateQueue(ctx, "events", minOffset)  // goes through retention coordinator
client.DeleteQueue(ctx, "events")
st, _ := client.Stats(ctx)
```

**Configuration knobs**

`BatchSize`, `MaxInterval` (`WithFlushInterval`), `MinInterval`, `MaxConns`, `DefaultMaxSegmentBytes`, `DefaultMaxMessageBytes`, `DefaultMaxSegmentAge`, `Encrypted`, `Fsync`, `TLSConfig`. Per‑queue overrides via `WithQueueBatchSize`, `WithQueueMaxInterval`, `WithQueueMinInterval`, `WithMaxSegmentBytes`, `WithMaxMessageBytes`, `WithMaxSegmentAge`, `WithDirectoryEncryption`, `WithQueueFsync`.

**Task knobs**

`WithVisibilityTimeout`, `WithMaxAttempts`, `WithScheduleAt`, `WithRetryAfter`.

---