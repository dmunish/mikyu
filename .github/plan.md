# Mikyu Revised Architecture Plan

This document lays out the refactored architecture for Mikyu, reflecting the current codebase refactor (split into `internal/client`, `internal/command`, `internal/group`, `internal/retention`, `internal/logqueue`, etc.) while mapping out the execution plan for the upcoming twin-driver design (`internal/taskqueue`), metrics, and interactive access modes.

---

## 1. Layered Architecture Overview

```text
┌─────────────────────────────────────────────────────────────────────────┐
│  Access Modes (CLI, REPL, TUI) – internal/cli, internal/repl, etc.      │
├─────────────────────────────────────────────────────────────────────────┤
│  Shared Command Helpers (internal/command)                              │
│  - List, Create, Delete, Produce, Read, Tail, Stats, Compact, Shutdown  │
├─────────────────────────────────────────────────────────────────────────┤
│  Public Client API (github.com/dmunish/mikyu)                           │
│  - Client, LogQueue, TaskQueue, Group, Message, Task, etc.              │
├─────────────────────────────────────────────────────────────────────────┤
│  Client Drivers (internal/client)                                       │
│  - embeddedClient (in-process manager wrapper)                          │
│  - remoteClient (connection-pooled TCP/IPC wire frame exchanges)        │
├─────────────────────────────────────────────────────────────────────────┤
│  Consumption Drivers & Coordination                                     │
│  - LogDriver (internal/logqueue) & Shared Consumer Group (internal/group)│
│  - Retention Coordinator (internal/retention)                           │
│  - TaskDriver (planned: internal/taskqueue)                             │
├─────────────────────────────────────────────────────────────────────────┤
│  Core Engine (internal/engine)                                          │
│  - Storage Interface, WAL Flusher, Segments, Index, Offsets, Compact    │
├─────────────────────────────────────────────────────────────────────────┤
│  Infrastructure & Protocol (internal/protocol, internal/server, ipc)   │
│  - Length-prefixed binary frames, TCP/IPC listeners, TLS 1.3            │
└─────────────────────────────────────────────────────────────────────────┘

```

The public root package `mikyu` serves as the thin API contract, delegating implementation to `internal/client`. Internal packages are strictly layered: client drivers depend on `internal/engine` or `internal/protocol`, high-level consumption behaviors are modularized into `internal/logqueue` and `internal/taskqueue`, and shared command execution logic is centralized in `internal/command`.

---

## 2. Revised Directory Tree

```text
github.com/dmunish/mikyu/
├── go.mod
├── go.sum
├── README.md
│
├── mikyu.go                  # Public types & re-exports: Client, LogQueue, TaskQueue, Group, Message
├── options.go                # Public configuration option wrappers delegating to internal/client
├── typed.go                  # Generic Typed[T] wrapper & Codec[T] interface
│
├── internal/
│   ├── client/               # Client driver implementations
│   │   ├── embedded.go       # In-process embedded client wrapping engine.Manager
│   │   ├── options.go        # Client & Queue functional options & defaults
│   │   └── remote.go         # Connection-pooled TCP/IPC client using wire protocol
│   │
│   ├── command/              # Shared stateless command runners for CLI/REPL
│   │   ├── command.go        # List, Create, Delete, Produce, Read, Tail, Stats, Compact, Shutdown
│   │   └── command_test.go
│   │
│   ├── config/               # Declarative YAML config & environment variable manager
│   │   └── config.go         # goccy/go-yaml parsing, auto-bootstrapping, env overrides
│   │
│   ├── engine/               # Low-level storage engine & WAL
│   │   ├── storage.go        # Storage interface (Append, Read, TruncateBefore, MinOffset, Segments)
│   │   ├── manager.go        # Multi-queue engine manager & lifecycle
│   │   ├── queue.go          # Concrete log storage, zero-copy read paths, rolling logic
│   │   ├── wal.go            # WAL flusher with dynamic dual-threshold batching & sync.Cond
│   │   ├── segment.go        # Active file I/O, mmap handling, unmap/unlink helpers
│   │   ├── index.go          # Fixed-width 16-byte index structures
│   │   ├── offsets.go        # Compacted consumer group offset store
│   │   ├── compact.go        # Engine compaction & floor adjustment (MinOffset, TruncateBefore)
│   │   ├── encode.go         # Record framing, CRC32, raw UUID v7 handling
│   │   ├── cache.go          # In-memory ring cache for recent messages (fast tail reads)
│   │   ├── sync_*.go         # OS-specific disk sync flushes (F_FULLFSYNC, fdatasync, Sync)
│   │   └── encrypt_*.go      # Kernel-level directory encryption (fscrypt, EFS)
│   │
│   ├── logqueue/             # Log consumption driver
│   │   ├── api.go            # ClientLogQueue interface
│   │   ├── logqueue.go       # LogQueue struct implementing streaming, tailing, groups
│   │   ├── group.go          # Consumer group wrapper around group.Base
│   │   └── retention.go      # Log retention evaluation & safe floor calculation
│   │
│   ├── taskqueue/            # Planned Task driver (visibility, retries, DLQ)
│   │   ├── taskqueue.go      # TaskQueue struct implementing task-side API
│   │   ├── task.go           # Task type and options
│   │   ├── scheduler.go      # In-memory scheduling & retry logic
│   │   ├── checkpoint.go     # Task state snapshot checkpointing
│   │   └── compaction.go     # Safe truncation offset calculation for active tasks
│   │
│   ├── group/                # Shared consumer group state
│   │   └── group.go          # Base struct with thread-safe high-watermark tracking
│   │
│   ├── retention/            # Cross-driver retention arbitration
│   │   └── coordinator.go   # Coordinator arbitrating truncation floors across drivers
│   │
│   ├── protocol/             # Wire protocol definition
│   │   ├── opcodes.go        # Opcodes (0x01-0x0C log, 0x0D-0x10 task, 0x11 truncate), error codes, URI parser
│   │   ├── frames.go         # Length-prefixed binary frame reader & pooled writer
│   │   └── bytes.go          # Bounds-checked binary helpers
│   │
│   ├── server/               # Network server (TCP/IPC/TLS)
│   │   ├── server.go         # Listener loop, connection workers, graceful stop
│   │   ├── handlers.go       # Opcode handlers mapping requests to engine/drivers
│   │   └── tls.go            # TLS 1.3 setup & self-signed cert generator
│   │
│   ├── ipc/                  # Local IPC transports
│   │   └── ipc.go            # Unix domain sockets & Windows named pipes
│   │
│   ├── daemon/               # Background daemon process manager
│   │   ├── daemon.go         # Run(), EnsureDaemonRunning(), IsDaemonAlive(), SendShutdownSignal()
│   │   └── detach_*.go       # OS-specific background process detachment
│   │
│   ├── cli/                  # Stateless CLI operator
│   │   └── cli.go            # CLI subcommand router invoking internal/command
│   │
│   ├── repl/                 # Interactive REPL shell
│   │   ├── repl.go           # Interactive command loop invoking internal/command
│   │   ├── style.go          # Lipgloss ASCII banner & table styling
│   │   └── vt_*.go           # Terminal ANSI setup
│   │
│   ├── tui/                  # Future interactive TUI dashboard
│   │   └── tui.go            # (planned)
│   │
│   ├── metrics/              # Future metrics registry & collectors
│   │   └── metrics.go        # (planned)
│   │
│   └── logstore/             # Future crash-safe logging store
│       └── logstore.go       # (planned)
│
└── cmd/
    └── mikyu/
        └── main.go           # Single-binary entry point & subcommand router

```

---

## 3. Public API (`package mikyu`)

The root `mikyu` package exports domain models and delegates execution to `internal/client`:

```go
package mikyu

import (
    "context"
    "io"
    "iter"
    "time"

    "github.com/dmunish/mikyu/internal/client"
    "github.com/dmunish/mikyu/internal/engine"
    "github.com/dmunish/mikyu/internal/logqueue"
)

// Re-exported engine types
type Message = engine.Message
type SystemStats = engine.EngineStats
type QueueStats = engine.QueueStats

// Re-exported driver interfaces
type Group = logqueue.Group
type LogQueue = logqueue.ClientLogQueue
type TaskQueue = client.TaskQueue

type Task struct {
    ID          string
    Payload     []byte
    Attempts    uint32
    MaxAttempts uint32
    Visibility  time.Time
    ScheduledAt time.Time
}

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

func Open(dir string, opts ...ClientOption) (Client, error) {
    return client.NewEmbedded(dir, opts...)
}

func Dial(target string, opts ...ClientOption) (Client, error) {
    return client.NewRemote(target, opts...)
}

```

---

## 4. Internal Architecture & Storage Contracts

### 4.1 Storage Engine Interface (`internal/engine`)

The engine provides policy-agnostic storage and truncation primitives. It executes physical segment deletion when instructed by drivers or the retention coordinator.

```go
type Storage interface {
    Append(ctx context.Context, payload []byte) (offset uint64, err error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (Message, error)
    ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error)
    LastOffset() uint64

    // Consumer offset persistence
    CommitOffset(ctx context.Context, queueName, group string, offset uint64) error
    FetchOffset(ctx context.Context, queueName, group string) (uint64, error)

    // Maintenance & Truncation
    MinOffset() uint64
    Segments() []SegmentInfo
    TruncateBefore(ctx context.Context, offset uint64) (reclaimedBytes int64, err error)
}

```

#### `TruncateBefore(offset)` Execution Flow:

1. Identifies closed segments where `segment.NextOffset <= offset`.
2. Unmaps `mmap` regions and closes active file descriptors for matched segments.
3. Removes physical `.log` and `.idx` files from disk.
4. Advances logical engine `MinOffset`.
5. Evicts stale records from the ring cache.

---

### 4.2 Consumption Drivers & Compaction Logic

#### Log Queue Driver (`internal/logqueue`)

* Evaluates `segment.CreatedAt` from `Storage.Segments()` against configured TTL.
* Queries active consumer group high-watermarks for $O_{\text{min\_consumer}}$.
* Computes truncation target: $\min(\text{TTL\_Offset}, O_{\text{min\_consumer}})$.

#### Task Queue Driver (`internal/taskqueue` - Planned)

* Tracks active, un-ACKed tasks and their original log offsets in memory.
* Snapshot Checkpoints (`checkpoint.go`): Writes periodic snapshots of active task states.
* Computes safe truncation offset ($O_{\text{active\_tasks}}$) from un-ACKed tasks and the latest checkpoint.

---

### 4.3 Retention Coordination (`internal/retention`)

The retention coordinator prevents data loss when a queue is used by multiple driver semantics:

```text
┌─────────────────────────────────────────────────────────────────┐
│              Retention Coordinator (coordinator.go)            │
├─────────────────────────────────────────────────────────────────┤
│ Effective Truncation Floor:                                     │
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

---

### 4.4 Wire Protocol & Access Layer

* **Protocol Opcodes (`internal/protocol`)**:
* Log Operations: `0x01`–`0x0C`
* Planned Task Operations: `OpTaskEnqueue = 0x0D`, `OpTaskDequeue = 0x0E`, `OpTaskAck = 0x0F`, `OpTaskFail = 0x10`
* System Operations: `OpTruncate = 0x11`, `OpShutdown = 0x12`


* **Shared Command Logic (`internal/command`)**: Both `internal/cli` and `internal/repl` execute operations via `command.go`, guaranteeing consistent execution, payload parsing (`@file`, `-`, raw string), and terminal formatting.

---

## 5. Migration Status & Roadmap Execution Plan

### Phase 1: Client & Engine Refactoring

* [x] Split client implementations into `internal/client` (`embedded.go`, `remote.go`, `options.go`).
* [x] Implement policy-agnostic storage compaction (`compact.go`) with `TruncateBefore`, `MinOffset`, and `Segments`.
* [x] Isolate consumer group tracking base into `internal/group`.

### Phase 2: Driver & Coordination Layer

* [x] Isolate log driver interface and operations in `internal/logqueue`.
* [x] Build `internal/retention` coordinator for arbitrating truncation floors.
* [x] Centralize user command execution in `internal/command`.

### Phase 3: Task Driver Implementation (Next Milestone)

* [ ] Create `internal/taskqueue` driver package.
* [ ] Implement in-memory task scheduler with visibility timeout and retry mechanisms.
* [ ] Build periodic state checkpointing (`checkpoint.go`) and task compaction floor calculation.
* [ ] Allocate wire opcodes `0x0D`–`0x10` in `internal/protocol` and add handlers to `internal/server`.
* [ ] Wire `Client.TaskQueue(name)` in `internal/client/embedded.go` and `internal/client/remote.go`.

### Phase 4: Observability & TUI

* [ ] Build `internal/metrics` registry and collectors.
* [ ] Build `internal/tui` dashboard using Lipgloss/Bubbletea.