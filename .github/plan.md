# Mikyu Revised Architecture Plan

This document lays out the refactored architecture for Mikyu, incorporating the twin‑driver design (log and task), unified public API, strict separation of concerns, policy-agnostic storage compaction, and preparation for future metrics/logging and cross‑language SDKs. The access modes (CLI, REPL, TUI) are moved into dedicated `internal` packages, leaving `cmd/mikyu` as a thin entry point.

---

## 1. Layered Architecture Overview



```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Access Modes (CLI, REPL, TUI, GUI) – each in its own internal package  │
├─────────────────────────────────────────────────────────────────────────┤
│  Public Client API (github.com/dmunish/mikyu)                           │
│  - Client, LogQueue, TaskQueue, Group, Message, Task, etc.              │
├─────────────────────────────────────────────────────────────────────────┤
│  Consumption Drivers (internal/logqueue, internal/taskqueue)            │
│  - LogDriver: streaming, consumer groups, TTL retention                 │
│  - TaskDriver: visibility, retries, DLQ, checkpoints, compaction        │
├─────────────────────────────────────────────────────────────────────────┤
│  Core Engine (internal/engine)                                          │
│  - Storage Interface, Truncation Primitives, Manager, Queue             │
├─────────────────────────────────────────────────────────────────────────┤
│  Infrastructure (future: internal/metrics, internal/logstore)           │
│  - Cross-cutting: metrics registry, crash-safe logging                  │
│  - Wired via interfaces, no hot-path impact                             │
└─────────────────────────────────────────────────────────────────────────┘

```

The public API sits at the top, accessible both to access modes and to external SDKs. Internal packages are layered: drivers depend on the engine, the server/protocol layer orchestrates, and infrastructure modules are dependencies injected via interfaces.

---

## 2. Revised Directory Tree

```text
github.com/dmunish/mikyu/
├── go.mod
├── go.sum
├── README.md
│
├── mikyu.go                  # Public types: Client, LogQueue, TaskQueue, Group, Message, Task
├── options.go                # ClientOption, QueueOption, TaskOption, etc.
├── typed.go                  # Generic Typed[T] wrapper (works with LogQueue)
├── embedded.go               # embeddedClient: implements Client using engine + drivers
├── remote.go                 # remoteClient: implements Client via protocol frames
│
├── internal/
│   ├── config/               # YAML config, env overrides
│   │   └── config.go
│   │
│   ├── engine/               # Pure storage engine (no consumption semantics)
│   │   ├── storage.go        # Storage interface (Append, Read, TruncateBefore, MinOffset)
│   │   ├── manager.go        # Manager: open/create queues, lifecycle
│   │   ├── queue.go          # Queue: concrete implementation of Storage & TruncateBefore
│   │   ├── wal.go            # WAL flusher (async, batched)
│   │   ├── segment.go        # Log segment management, mmap, unmap/unlink helpers
│   │   ├── index.go          # In-memory + on-disk index entries
│   │   ├── offsets.go        # Offset store for consumer groups
│   │   ├── retention.go      # RetentionCoordinator floor manager
│   │   ├── encode.go         # Record framing, CRC32, UUID v7
│   │   ├── encrypt_*.go      # OS-specific directory encryption
│   │   └── sync_*.go         # OS-specific sync helpers
│   │
│   ├── logqueue/             # Log driver (streaming)
│   │   ├── logqueue.go       # LogQueue struct, implements log-side API
│   │   ├── group.go          # Consumer group implementation
│   │   ├── retention.go      # TTL and consumer group offset evaluation
│   │   └── message.go        # Message type (public equivalent)
│   │
│   ├── taskqueue/            # Task driver (visibility, retries, DLQ)
│   │   ├── taskqueue.go      # TaskQueue struct, implements task-side API
│   │   ├── task.go           # Task type and options
│   │   ├── scheduler.go      # In-memory scheduling, retry logic
│   │   ├── checkpoint.go     # Snapshot/checkpoint for fast boot & safe truncation offset
│   │   └── compaction.go     # Computes safe truncation offset from active task map
│   │
│   ├── protocol/             # Wire protocol (versioned)
│   │   ├── opcodes.go        # Opcodes for log + task operations
│   │   ├── frames.go         # Length-prefixed frame reader/writer
│   │   ├── bytes.go          # Bounds-checked binary helpers
│   │   └── errors.go         # Wire error encoding/decoding
│   │
│   ├── server/               # Network server (TCP/IPC/TLS)
│   │   ├── server.go         # Listener loop, graceful stop
│   │   ├── handlers.go       # Dispatch opcodes to engine/drivers
│   │   └── tls.go            # TLS config, self-signed certs
│   │
│   ├── ipc/                  # Local IPC (unix sockets, windows pipes)
│   │   └── ipc.go
│   │
│   ├── cli/                  # Stateless one-shot commands (list, produce, read, ...)
│   │   ├── cli.go
│   │   └── helpers.go        # Confirm prompts, payload resolution
│   │
│   ├── repl/                 # Interactive REPL shell
│   │   ├── repl.go           # REPL loop, command dispatch
│   │   ├── banner.go         # Lipgloss banner and styles
│   │   ├── autospawn.go      # Ensure daemon running, spawn if needed
│   │   └── vt_*.go           # Terminal setup (Windows vs other)
│   │
│   ├── daemon/               # Background server daemon
│   │   ├── daemon.go         # Run function: config load, engine, server, signals
│   │   └── detach_*.go       # OS-specific process detachment
│   │
│   ├── tui/                  # Interactive dashboard (future)
│   │   └── tui.go            # Placeholder for TUI logic
│   │
│   ├── metrics/              # Future: metrics registry, collectors, snapshots
│   │   └── metrics.go        # (planned)
│   │
│   └── logstore/             # Future: crash-safe logging
│       └── logstore.go       # (planned)
│
└── cmd/
    └── mikyu/
        └── main.go           # Thin entry point: flags, subcommand routing

```

---

## 3. Public API (Root Package `mikyu`)

### Types

```go
type Message struct {
    Offset    uint64
    ID        string
    Timestamp time.Time
    Payload   []byte
}

type Task struct {
    ID          string
    Payload     []byte
    Attempts    uint32
    MaxAttempts uint32
    Visibility  time.Time
    ScheduledAt time.Time
}

type SystemStats struct {
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

### Client Interface

```go
type Client interface {
    io.Closer

    // Queue management
    CreateQueue(ctx context.Context, name string, opts ...QueueOption) error
    DeleteQueue(ctx context.Context, name string) error
    ListQueues(ctx context.Context) ([]string, error)
    Stats(ctx context.Context) (SystemStats, error)

    // Driver access
    LogQueue(name string) (LogQueue, error)
    TaskQueue(name string) (TaskQueue, error)
}

```

---

## 4. Internal APIs & Policy-Agnostic Storage



### 4.1 Engine (`internal/engine`)



The engine exposes policy-agnostic storage and truncation primitives. It does not decide *when* to delete segments; it executes deletion when instructed by driver drivers or the retention manager.

```go
type Record struct {
    Offset    uint64
    ID        uuid.UUID
    Timestamp time.Time
    Payload   []byte
}

type SegmentInfo struct {
    BaseOffset uint64
    NextOffset uint64
    SizeBytes  int64
    CreatedAt  time.Time
    IsActive   bool
}

type Storage interface {
    Append(ctx context.Context, payload []byte) (offset uint64, err error)
    Read(ctx context.Context, offset uint64) (Record, error)
    ReadBatch(ctx context.Context, start uint64, n int) ([]Record, error)
    LastOffset() uint64
    
    // Low-level consumer offset persistence
    CommitOffset(ctx context.Context, queueName, group string, offset uint64) error
    FetchOffset(ctx context.Context, queueName, group string) (uint64, error)

    // Policy-agnostic maintenance & truncation primitives
    MinOffset() uint64
    Segments() []SegmentInfo
    TruncateBefore(ctx context.Context, offset uint64) (reclaimedBytes int64, err error)
}

```

#### `TruncateBefore(offset)` Execution Flow:

1. Closes file descriptors and unmaps `mmap` regions for closed log segments where `segment.NextOffset <= offset`.
2. Unlinks physical `.log` and `.idx` files from the filesystem.
3. Advances the logical engine floor (`MinOffset`).
4. Rejects subsequent `Read()` calls for offsets lower than `MinOffset()` with `ErrOffsetTruncated`.

---

### 4.2 Driver-Level Segment Metadata & Compaction Logic

`engine.Storage` maintains physical segment state, while higher-level drivers compute semantic deletion eligibility.

#### Log Queue Driver (`internal/logqueue`)



* **TTL Evaluation:** Compares `segment.CreatedAt` from `Storage.Segments()` against the configured log message time-to-live.
* **Consumer Group High-Water Floor:** Queries all active consumer groups for the minimum active offset ($O_{\text{min\_consumer}}$).
* **Compaction Trigger:** Requests truncation up to $\min(\text{TTL\_Offset}, O_{\text{min\_consumer}})$.

#### Task Queue Driver (`internal/taskqueue`)



* **Active Task Tracking:** Maintains an in-memory lookup map of active (un-ACKed) tasks to their creation log offsets.
* **Segment Zero-Count Identification:** A closed segment spanning range `[BaseOffset, NextOffset)` is eligible for removal when no active task references an offset in that range.
* **Checkpoints (`checkpoint.go`):** Periodically writes a snapshot of all un-ACKed tasks to a separate checkpoint file. Once a checkpoint at offset $C$ is durably written, event log entries prior to $C$ for fully ACKed tasks are obsolete.
* **Compaction Trigger:** Calculates the lowest offset required by active tasks or the latest checkpoint ($O_{\text{active\_tasks}}$) and invokes `Storage.TruncateBefore(O_{\text{active\_tasks}})$`.

---

### 4.3 Retention Reconciliation

When a queue is accessed concurrently as both a `TaskQueue` and a `LogQueue`, drivers could issue conflicting truncation requests.

```text
┌─────────────────────────────────────────────────────────────────┐
│                   Retention Coordinator                         │
├─────────────────────────────────────────────────────────────────┤
│ Calculates effective truncation floor:                          │
│                                                                 │
│ TruncateFloor = min( TargetOffset,                              │
│                      min(O_consumer_groups),                    │
│                      min(O_active_tasks) )                      │
└──────────────────────────────┬──────────────────────────────────┘
                               │
                               │ TruncateBefore(TruncateFloor)
                               ▼
┌─────────────────────────────────────────────────────────────────┐
│                       engine.Storage                            │
└─────────────────────────────────────────────────────────────────┘

```

1. **Floor Enforcement:** The effective truncation offset is restricted by the lowest active reader:

$$\text{TruncateOffset}_{\text{effective}} = \min\left(\text{TargetOffset}, \min(O_{\text{consumer\_groups}}), \min(O_{\text{active\_tasks}})\right)$$

2. **Truncation Fail-Safe:** If task compaction attempts to truncate past an active `LogQueue` consumer group, truncation clamps to $O_{\text{consumer\_groups}}$. If strict task-priority deletion is configured in `mikyu.yaml`, any log consumer that falls behind a truncated floor receives `ErrOffsetTruncated` and resets to `Storage.MinOffset()`.

---

### 4.4 Protocol (`internal/protocol`)



Task operation wire opcodes starting at `0x0D`:

* `OpTaskEnqueue = 0x0D`

* `OpTaskDequeue = 0x0E`

* `OpTaskAck     = 0x0F`

* `OpTaskFail    = 0x10`


Existing log opcodes (`0x01`–`0x0C`) and binary framing structure remain unchanged.

---

### 4.5 Access Modes & Entry Point



Access modes (`internal/cli`, `internal/repl`, `internal/daemon`, `internal/tui`) each expose an explicit `Run` entrypoint. Command-line flags are parsed explicitly via `flag.Parse()` inside command runners. `cmd/mikyu/main.go` remains a thin subcommand router.

---

## 5. Migration & Execution Plan



1. **Extract `Storage` Interface & Truncation Primitives**: Add `TruncateBefore`, `MinOffset`, and `Segments` to `internal/engine/storage.go` and implement file unlinking/unmapping in `internal/engine/queue.go`.
2. **Implement `logqueue` Driver**: Wrap `Storage` and integrate consumer group tracking with TTL evaluation.
3. **Implement `taskqueue` Driver & Checkpoints**: Build in-memory task state tracking, periodic snapshot checkpointing, and safe truncation calculation.
4. **Build Retention Coordinator**: Implement truncation floor calculation across active drivers.
5. **Update Public Client & Wire Protocol**: Expose `LogQueue` and `TaskQueue` on `mikyu.Client`, allocate opcodes `0x0D`–`0x10`, and update `typed.go`.
6. **Relocate Access Modes**: Move CLI, REPL, Daemon, and TUI into dedicated `internal/` packages with explicit flag parsing.