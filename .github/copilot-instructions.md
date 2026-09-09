We have analyzed the current codebase to produce this updated specification. The document below is the definitive source of truth for **mikyu's** architecture, behavior, and implementation details.

---

# Mikyu Technical Specification & Codebase Architecture

## 1. Project Overview

**mikyu** is a high-performance, log-structured message queue built from scratch in Go. It provides a unified API surface operating either as an in-process embedded log engine or as a standalone network/IPC daemon. Its dual‑use nature spans **log‑oriented queues** (append‑only streams with consumer groups) and **task‑oriented queues** (at‑least‑once delivery with visibility leases, retries, and a durable dead‑letter queue). Both use the same underlying storage engine.

### Core Architectural Guarantees & Technical Principles

- **Durable‑Before‑Ack Semantics:** Every `Produce()` or `ProduceBatch()` invocation blocks until the payload byte slice and index metadata are durably synced to disk via the Write‑Ahead Log (`wal`) flusher. This guarantees that once an offset is returned, the message is recoverable after a crash.
- **Optional Crash‑Loss Window:** A `Fsync` configuration flag allows operators to disable physical disk sync on every produce operation. When `Fsync: false`, the WAL flusher only performs `WriteAt` syscalls without `syncData`, trading a small crash‑loss window for significantly lower latency. The async fast‑path writes records directly to the segment file and advances `publishedOffset` before data is on disk, allowing readers to immediately access messages. Fsync=false benchmarks show **~3.5x lower write latency (25.9 µs vs 292.6 µs for 64 B records)**, while Fsync=true provides strict durability at ~300 µs per produce. Data is still written to the OS page cache in both modes; only the final flush to physical storage differs.
- **Zero‑Copy Reads:** Closed historical log segments are memory‑mapped (`mmap`) into the process address space. Read requests on historical offsets return direct sub‑slices of mapped memory buffers without heap allocations or system call overhead. Active segments are read via `ReadAt` syscalls on the open file descriptor. An in‑memory ring cache of the most recent 16,384 messages provides an additional fast path for tail reads at **~100 ns/op** with zero allocations.
- **Dynamic Dual‑Threshold WAL Flushing:** The WAL flusher triggers a disk sync (`syncData`) when either pending record count reaches `BatchSize` or elapsed time exceeds `MaxInterval`. Additionally, a **micro‑linger** mechanism (`MinInterval`) briefly waits to group concurrent producers into a single batch, balancing single‑thread latency with high‑concurrency parallel throughput. The flusher runs in a dedicated goroutine, swapping buffer pointers to minimize lock hold time; producers never block on disk I/O directly and wait on a `sync.Cond` for the flusher to publish their sequence.
- **Raw 16‑Byte UUID v7 Encoding:** Messages are encoded using raw 16‑byte Go standard library `uuid.UUID` arrays (`uuid.NewV7()`), avoiding string formatting allocations and reducing log file overhead by 20 bytes per record.
- **Go Range‑Over‑Func Integration:** Streaming and live tailing are exposed using native `iter.Seq2[Message, error]` iterators (Go 1.23+), providing lazy evaluation and clean cancellation via `context.Context`.
- **Transparent Kernel Directory Encryption:** OS‑level directory encryption policies (`fscrypt` ioctl on Linux, NTFS EFS on Windows) are auto‑bootstrapped on queue directories, maintaining zero‑copy `mmap` performance while encrypting storage at rest. Unsupported OSes (e.g., macOS) log a warning and continue.
- **TLS 1.3 Transport Encryption:** Network communication over TCP enforces TLS 1.3 with optional auto‑generated self‑signed certificates for zero‑config local security, while local IPC (unix/pipe sockets) bypasses TLS overhead entirely.
- **Declarative Configuration & Auto‑Bootstrapping:** The daemon parses `mikyu.yaml` via `goccy/go-yaml`, auto‑bootstraps commented default configuration files when missing, and reconciles declared queues on launch.
- **Unified Single‑Binary Execution:** A single `mikyu` executable acts as a background server daemon, stateless CLI operator and an interactive REPL shell with Lipgloss styling.
- **Non‑Blocking Background Auto‑Spawning:** Executing `mikyu` directly (or double‑clicking the binary) probes the local IPC socket, auto‑spawns a detached background daemon if offline, and immediately attaches an interactive REPL prompt.

### Concurrent Design Characteristics

- **Continuous log tailing**: `Tail()` iterators handle the edge case of hitting the log head by treating `offset < last` as the bound and retrying transient reads (e.g., `ErrOffsetNotFound`, `ErrCorruptRecord`, `io.EOF`) with a short sleep. This keeps streams open indefinitely even during in‑flight WAL writes.
- **Reader safety**: A `publishedOffset` high‑water mark is updated only after WAL disk sync completes (or immediately in async mode). Readers check against this value, preventing `io.EOF` errors during concurrent writes.
- **WAL wake‑token handling**: Wake channel draining occurs inside `flush()` under lock, preventing producer hangs from adaptive flushing.
- **Manager lock contention**: `Stats()` uses `RLock()` and in‑memory size counters, providing O(1) metrics retrieval.
- **Duplicate client handles**: Double‑checked locking in `LogQueue()` and `TaskQueue()` guarantees single‑instance caching across both embedded and remote clients.
- **Zero‑allocation encoding**: `encodeRecord` writes directly into pre‑sized slices, achieving ~887 ns/op encoding latency.
- **Double‑buffered WAL flusher**: Steady‑state allocations are eliminated (0 allocs/op, 3 B/op).
- **Protocol frame efficiency**: `WriteFrame` retains expanded buffers, `ReadFrame` streams payloads directly, achieving a 90.7% latency reduction for 64 KB frames (38.5 µs → 3.5 µs) and zero extra heap allocations.
- **Index RAM reduction**: The engine uses `O(log S)` segment binary search and fixed‑width file arithmetic instead of an in‑memory index slice. Heap footprint scales with segment count rather than message count.
- **Offset log compaction**: Appends are compacted every 5,000 commits via atomic rename, bounding the offset file size to active consumer groups.
- **Windows mmap robustness**: `enableMMap()` skips zero‑byte `.idx` files, preventing initialization errors on Windows.
- **Retention coordination**: The retention subsystem integrates with consumption drivers (log groups and task queues) to compute safe truncation floors automatically, ensuring log segments are not deleted while still needed by active consumers or task drivers.

---

## 2. Repository Tree

```text
github.com/dmunish/mikyu/
├── go.mod
├── go.sum
├── README.md
│
├── mikyu.go            # Top-level domain models, type aliases, and public Client interface
├── options.go          # Re-exported functional configuration options from internal/client
├── typed.go            # Generic Typed[T] queue wrapper & Codec[T] interface
│
├── internal/
│   ├── client/         # Client driver implementations (embedded & remote)
│   │   ├── embedded.go       # In-process embedded client
│   │   ├── options.go        # Client/Queue configuration options & defaults
│   │   ├── remote.go         # Connection-pooled TCP/IPC network client
│   │   └── embedded_retention_test.go
│   │
│   ├── command/        # Stateless command helpers used by CLI/REPL
│   │   ├── command.go       # List, Create, Delete, Produce, Read, Tail, Stats, Compact, Shutdown
│   │   ├── command_test.go
│   │   ├── flags.go         # Flag extraction helpers
│   │   ├── task.go          # Task command helpers
│   │   └── task_test.go
│   │
│   ├── config/         # Declarative YAML & environment variable configuration manager
│   │   └── config.go       # Default YAML template, goccy/go-yaml parsing, env overrides
│   │
│   ├── engine/                 # Storage engine core (WAL, segments, index, offsets)
│   │   ├── manager.go          # Multi-queue engine manager & lifecycle state
│   │   ├── queue.go            # Per-queue log append, zero-copy read paths, and rolling logic
│   │   ├── wal.go              # Async/sync batch WAL flusher with sync.Cond
│   │   ├── segment.go          # Active file I/O & read-only mmap segment handling
│   │   ├── index.go            # In-memory and raw on-disk index entry structures
│   │   ├── offsets.go          # Single-file manager-level consumer group offset store
│   │   ├── encode.go           # CRC32 record framing, decoding, and raw UUID v7 handling
│   │   ├── cache.go            # In-memory ring cache for recent messages (fast tail reads)
│   │   ├── compact.go          # Segment metadata & truncation (MinOffset, Segments, TruncateBefore)
│   │   ├── storage.go          # Storage interface definition
│   │   ├── sync_darwin.go      # macOS F_FULLFSYNC drive cache flusher
│   │   ├── sync_fdatasync.go   # POSIX unix.Fdatasync implementation
│   │   ├── sync_windows.go     # Windows file.Sync implementation
│   │   ├── encrypt_linux.go    # Linux fscrypt kernel ioctl directory encryption
│   │   ├── encrypt_windows.go  # Windows NTFS EFS directory encryption
│   │   └── encrypt_other.go    # Fallback directory encryption for unsupported OSes
│   │
│   ├── logqueue/       # High-level log-oriented queue wrapper & consumer group tracking
│   │   ├── api.go            # ClientLogQueue interface for upper layers
│   │   ├── logqueue.go       # LogQueue struct implementing streaming, tailing, group semantics
│   │   ├── group.go          # Group implementation wrapping group.Base
│   │   └── retention.go      # Log retention policy & safe truncation floor calculation
│   │
│   ├── taskqueue/      # Task-oriented queue implementation with at-least-once delivery
│   │   ├── taskqueue.go      # Main TaskQueue implementation
│   │   ├── task.go           # Task struct, options, errors, wire helpers
│   │   ├── scheduler.go      # Heap-based schedule for delayed tasks
│   │   ├── checkpoint.go     # Durable checkpoint encoding/decoding
│   │   ├── compaction.go     # TruncateFloor integration for retention
│   │   └── id.go             # Random task ID generation
│   │
│   ├── group/          # Shared consumer group high-watermark tracking
│   │   └── group.go         # Base struct with Mark/Commit/Close lifecycle
│   │
│   ├── protocol/       # Binary wire format parser & opcode definitions
│   │   ├── opcodes.go      # Wire opcodes, error codes, and target URI parser
│   │   ├── frames.go       # Length-prefixed binary frame reader and pooled writer
│   │   ├── bytes.go        # Bounds-checked binary Reader & Buffer helpers
│   │   └── task.go         # Task encoding/decoding for wire protocol
│   │
│   ├── server/         # Protocol frame dispatcher & TCP server
│   │   ├── server.go       # Listener loop, worker goroutines, graceful Stop()
│   │   ├── handlers.go     # Wire opcode handler routines mapping to internal/engine
│   │   ├── task_handlers.go # Task-specific handlers (in handlers.go actually)
│   │   └── tls.go          # TLS 1.3 helper & self-signed ECDSA cert generator
│   │
│   ├── ipc/            # Specialized local IPC listeners and dialers
│   │   └── ipc.go          # Unix Domain Socket and Windows Named Pipe helpers
│   │
│   ├── daemon/         # Background daemon lifecycle management & socket probing
│   │   ├── daemon.go        # Daemon Run(), EnsureDaemonRunning(), IsDaemonAlive(), SendShutdownSignal()
│   │   ├── detach_unix.go   # POSIX process detachment (//go:build !windows)
│   │   └── detach_windows.go # Windows process detachment (//go:build windows)
│   │
│   ├── retention/      # Retention coordination across queues and drivers
│   │   └── coordinator.go   # Coordinator that arbitrates safe truncation based on driver floors
│   │
│   ├── repl/           # Interactive REPL session shell & command loop
│   │   ├── repl.go         # REPL session struct, command dispatch, payload resolution
│   │   ├── style.go        # Lipgloss-styled ASCII banner & command table
│   │   ├── task.go         # REPL task commands
│   │   ├── vt_other.go     # Virtual terminal setup stub (//go:build !windows)
│   │   └── vt_windows.go   # Windows ANSI escape sequence enabler (//go:build windows)
│   │
│   └── cli/            # Stateless one-shot CLI handler
│       ├── cli.go         # CLI subcommand router, payload resolution, confirmation prompts
│       └── task.go        # CLI task commands
│
└── cmd/
    └── mikyu/            # Unified single-binary entrypoint
        └── main.go           # Subcommand router (daemon, shell, cli, default)
```

---

## 3. Module Structure & Component Interaction

```text
                                  ┌───────────────────────────────────┐
                                  │        github.com/dmunish/mikyu   │
                                  │     (Public API Contracts & SDK)  │
                                  └─────────────────┬─────────────────┘
                                                    │
                            ┌───────────────────────┴───────────────────────┐
                            ▼                                               ▼
               [Embedded Execution Path]                       [Remote Connection Path]
                  internal/client/embedded.go                   internal/client/remote.go
                            │                                               │
                            │ (Direct in-process call)                      │ (Binary frame exchange)
                            ▼                                               ▼
                 ┌─────────────────────┐                         ┌─────────────────────┐
                 │   internal/engine   │                         │  internal/protocol  │
                 │ (Storage Engine)    │                         └──────────┬──────────┘
                 └──────────┬──────────┘                                    │
                            │                                               ▼
                            ▼                                    ┌─────────────────────┐
                 ┌─────────────────────┐                        │   internal/server   │
                 │ internal/logqueue   │                        │   internal/ipc      │
                 │ (LogQueue wrapper)  │                        └─────────────────────┘
                 │ internal/taskqueue  │                                    ▲
                 │ (TaskQueue driver)  │                                    │ (IPC / TLS TCP)
                 └─────────────────────┘                        ┌─────────┴─────────┐
                                                                 │  internal/daemon  │
                                                                 │  internal/repl    │
                                                                 │  internal/cli     │
                                                                 │  internal/command │
                                                                 │  cmd/mikyu        │
                                                                 └───────────────────┘
```

**Layered Architecture:**

The design follows a clean separation of concerns, enabling maximum reuse across deployment modes:

1. **Storage Engine (`internal/engine`)**: Low‑level log storage, WAL, segments, indexes, and offsets. This layer is completely agnostic of transport or presentation.
2. **Logical Queue Abstractions**:
   - **Log‑oriented (`internal/logqueue`)**: Provides high‑level `LogQueue` operations (Stream, Tail, Groups, Retention) on top of the `Storage` interface. The `api.go` file exports the `ClientLogQueue` interface.
   - **Task‑oriented (`internal/taskqueue`)**: Provides a full task driver with at‑least‑once delivery, visibility leases, retry logic, checkpoint recovery, and a durable dead‑letter queue, all built on `engine.Storage`.
3. **Retention Coordination (`internal/retention`)**: A coordinator that calculates safe truncation floors from multiple drivers (log groups, task queues) and instructs the engine to physically remove obsolete segments.
4. **Consumer Group Abstraction (`internal/group`)**: A concurrency‑safe `Base` struct for high‑watermark tracking, shared by embedded and remote group implementations.
5. **Deployment Runners**:
   - **Embedded (in‑process)**: `mikyu.Open()` delegates to `client.NewEmbedded()`, which creates an `engine.Manager` directly, wrapping queues with `logqueue` and `taskqueue` drivers.
   - **Daemon (network)**: `daemon.Run()` starts a `server.Server` which exposes the engine over IPC/TCP.
6. **Access Layers**:
   - **SDK**: `mikyu.Dial()` returns a `Client` that communicates with the daemon via the wire protocol.
   - **Command Helpers (`internal/command`)**: Stateless functions used by both CLI and REPL for consistent user‑facing operations.
   - **CLI/REPL**: Uses the SDK and command helpers to perform user‑facing operations.

**Data Flow & Lifecycle:**

1. **Embedded mode:** `mikyu.Open(dir, opts...)` calls `client.NewEmbedded()`, which creates an `engine.Manager` directly in‑process. Client calls delegate to `engine.Queue` via `logqueue.LogQueue` and `taskqueue.TaskQueue` wrappers without network/IPC overhead.
2. **Remote mode:** `mikyu.Dial(target, opts...)` calls `client.NewRemote()`, which initializes a `remoteClient` managing a thread‑safe connection pool (`chan *clientConn`). Each client operation acquires a connection, writes length‑prefixed protocol frames, awaits the response frame, and returns the connection to the pool.
3. **Daemon mode:** `daemon.Run(configPath)` loads `mikyu.yaml` configuration, instantiates `engine.Manager`, creates `server.Server`, and listens on target endpoints (TCP, Unix domain socket, or Windows named pipe). The daemon also reconciles declared queues on startup.

---

## 4. File‑by‑File Technical Specification

### Root Package (`package mikyu`)

#### `mikyu.go`

Defines public domain models and primary client/queue interfaces, re‑exporting internal types.

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
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// Re-export core engine types as type aliases.
type Message = engine.Message
type SystemStats = engine.EngineStats
type QueueStats = engine.QueueStats

// Group tracks consumer offsets for a log queue.
type Group = logqueue.Group

// Task represents a unit of work owned by a TaskQueue. MaxAttempts is the maximum
// number of deliveries. DeliveryID identifies the current delivery lease.
type Task = taskqueue.Task

type TaskOption = taskqueue.TaskOption
type FailOption = taskqueue.FailOption

func WithVisibilityTimeout(timeout time.Duration) TaskOption {
	return taskqueue.WithVisibilityTimeout(timeout)
}
func WithMaxAttempts(n uint32) TaskOption           { return taskqueue.WithMaxAttempts(n) }
func WithScheduleAt(at time.Time) TaskOption        { return taskqueue.WithScheduleAt(at) }
func WithRetryAfter(delay time.Duration) FailOption { return taskqueue.WithRetryAfter(delay) }

// LogQueue provides log-oriented operations (streaming, tailing, groups).
type LogQueue = logqueue.ClientLogQueue

// TaskQueue provides at-least-once delivery, visibility leases, retries, stale-delivery
// protection, checkpoint recovery, and a durable per-queue dead-letter queue.
type TaskQueue = client.TaskQueue

// Client is the top-level interface for interacting with a Mikyu server.
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

// Open creates an in-process Client.
func Open(dir string, opts ...ClientOption) (Client, error) {
	return client.NewEmbedded(dir, opts...)
}

// Dial creates a lazy remote Client for target.
func Dial(target string, opts ...ClientOption) (Client, error) {
	return client.NewRemote(target, opts...)
}

// Error variables re-exported from the engine/client/taskqueue layers.
var (
	ErrTaskNotAvailable   = taskqueue.ErrTaskNotAvailable
	ErrTaskNotFound       = taskqueue.ErrTaskNotFound
	ErrTaskNotInflight    = taskqueue.ErrTaskNotInflight
	ErrTaskStale          = taskqueue.ErrTaskStale
	ErrTaskClosed         = taskqueue.ErrTaskClosed
	ErrInvalidTask        = taskqueue.ErrInvalidTask
	ErrCheckpointCorrupt  = taskqueue.ErrCheckpointCorrupt
	ErrDeadLetterNotFound = taskqueue.ErrDeadLetterNotFound
	ErrDeadLetterStale    = taskqueue.ErrDeadLetterStale

	ErrQueueExists      = engine.ErrQueueExists
	ErrQueueNotFound    = engine.ErrQueueNotFound
	ErrOffsetNotFound   = engine.ErrOffsetNotFound
	ErrOffsetTruncated  = engine.ErrOffsetTruncated
	ErrMessageTooLarge  = engine.ErrMessageTooLarge
	ErrQueueClosed      = engine.ErrQueueClosed
	ErrCorruptRecord    = engine.ErrCorruptRecord
	ErrTaskNotSupported = client.ErrTaskNotSupported
)

type _ = iter.Seq2[Message, error]
```

**Implementation Details:**

- `LogQueue` is a type alias for `logqueue.ClientLogQueue`, the interface exported by the `logqueue` package.
- `TaskQueue` is a type alias for `client.TaskQueue`, which is an alias for `taskqueue.ClientTaskQueue`.
- `Task` is an alias for `taskqueue.Task`, exposing fields `ID`, `Payload`, `Attempts`, `MaxAttempts`, `Visibility`, `ScheduledAt`, `DeliveryID`.
- `Open` and `Dial` delegate to the `internal/client` package's `NewEmbedded` and `NewRemote` constructors.
- Error variables are re‑exported from `engine`, `client`, and `taskqueue` layers.

#### `options.go`

Re‑exports configuration options from `internal/client` for backwards compatibility.

```go
package mikyu

import (
	"crypto/tls"
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

// ClientConfig is re-exported from the internal client package.
type ClientConfig = client.Config

// ClientOption configures a client.
type ClientOption = client.Option

// QueueConfig is re-exported from the internal client package.
type QueueConfig = client.QueueConfig

// QueueOption configures a queue.
type QueueOption = client.QueueOption

func DefaultClientConfig() ClientConfig {
	return client.DefaultClientConfig()
}

func WithBatchSize(n int) ClientOption {
	return client.WithBatchSize(n)
}
func WithMaxInterval(d time.Duration) ClientOption {
	return client.WithMaxInterval(d)
}
func WithFlushInterval(d time.Duration) ClientOption {
	return client.WithFlushInterval(d)
}
func WithMinInterval(d time.Duration) ClientOption {
	return client.WithMinInterval(d)
}
func WithMaxConns(n int) ClientOption {
	return client.WithMaxConns(n)
}
func WithClientDirectoryEncryption(enabled bool) ClientOption {
	return client.WithClientDirectoryEncryption(enabled)
}
func WithClientTLS(tlsConfig *tls.Config) ClientOption {
	return client.WithClientTLS(tlsConfig)
}
func WithFsync(enabled bool) ClientOption {
	return client.WithFsync(enabled)
}

func WithQueueBatchSize(n int) QueueOption {
	return client.WithQueueBatchSize(n)
}
func WithQueueMaxInterval(d time.Duration) QueueOption {
	return client.WithQueueMaxInterval(d)
}
func WithQueueMinInterval(d time.Duration) QueueOption {
	return client.WithQueueMinInterval(d)
}
func WithMaxSegmentBytes(bytes int64) QueueOption {
	return client.WithMaxSegmentBytes(bytes)
}
func WithMaxMessageBytes(bytes int64) QueueOption {
	return client.WithMaxMessageBytes(bytes)
}
func WithMaxSegmentAge(age time.Duration) QueueOption {
	return client.WithMaxSegmentAge(age)
}
func WithDirectoryEncryption(enabled bool) QueueOption {
	return client.WithDirectoryEncryption(enabled)
}
func WithQueueFsync(enabled bool) QueueOption {
	return client.WithQueueFsync(enabled)
}
```

#### `typed.go`

Provides generic object serialization helpers over byte queues.

```go
package mikyu

import "context"

// Codec provides serialization for a generic type.
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

// Typed is a generic wrapper around a LogQueue that encodes/decodes values.
type Typed[T any] struct {
	queue LogQueue
	codec Codec[T]
}

// NewTyped creates a Typed wrapper.
func NewTyped[T any](q LogQueue, codec Codec[T]) *Typed[T] {
	return &Typed[T]{queue: q, codec: codec}
}

// Produce marshals and produces a value.
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return 0, err
	}
	return t.queue.Produce(ctx, payload)
}

// Read reads and unmarshals a value at offset.
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error) {
	var zero T
	msg, err := t.queue.Read(ctx, offset)
	if err != nil {
		return zero, err
	}
	err = t.codec.Unmarshal(msg.Payload, &zero)
	return zero, err
}
```

### Internal Client Module (`package client`)

#### `options.go`

Defines configuration structs and functional options for both client and queue.

```go
package client

import (
	"crypto/tls"
	"time"
)

// Config controls client-wide defaults and remote connection behavior.
type Config struct {
	BatchSize   int
	MaxInterval time.Duration
	MinInterval time.Duration

	MaxConns int

	DefaultMaxSegmentBytes int64
	DefaultMaxMessageBytes int64
	DefaultMaxSegmentAge   time.Duration
	Encrypted              bool
	Fsync                  bool
	TLSConfig              *tls.Config
}

// DefaultClientConfig returns the same defaults used by the original public API.
func DefaultClientConfig() Config {
	return Config{
		BatchSize:              256,
		MaxInterval:            10 * time.Millisecond,
		MinInterval:            30 * time.Microsecond,
		MaxConns:               10,
		DefaultMaxSegmentBytes: 64 * 1024 * 1024,
		DefaultMaxMessageBytes: 1 * 1024 * 1024,
		DefaultMaxSegmentAge:   1 * time.Hour,
		Encrypted:              false,
		Fsync:                  true,
	}
}

// Option configures a client.
type Option func(*Config)

func WithBatchSize(n int) Option { ... }
func WithMaxInterval(d time.Duration) Option { ... }
func WithFlushInterval(d time.Duration) Option { ... }
func WithMinInterval(d time.Duration) Option { ... }
func WithMaxConns(n int) Option { ... }
func WithClientDirectoryEncryption(enabled bool) Option { ... }
func WithClientTLS(tlsConfig *tls.Config) Option { ... }
func WithFsync(enabled bool) Option { ... }

// QueueConfig controls per-queue engine defaults.
type QueueConfig struct {
	BatchSize       int
	MaxInterval     time.Duration
	MinInterval     time.Duration
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
	Encrypted       bool
	Fsync           bool
}

// QueueOption configures a queue.
type QueueOption func(*QueueConfig)

func WithQueueBatchSize(n int) QueueOption { ... }
func WithQueueMaxInterval(d time.Duration) QueueOption { ... }
func WithQueueMinInterval(d time.Duration) QueueOption { ... }
func WithMaxSegmentBytes(bytes int64) QueueOption { ... }
func WithMaxMessageBytes(bytes int64) QueueOption { ... }
func WithMaxSegmentAge(age time.Duration) QueueOption { ... }
func WithDirectoryEncryption(enabled bool) QueueOption { ... }
func WithQueueFsync(enabled bool) QueueOption { ... }
```

**Implementation Details:**

- All option functions validate input parameters, ignoring non‑positive values for integer/duration limits.
- `WithMinInterval` accepts `>= 0` to support explicitly disabling the micro‑linger (0 or negative).

#### `embedded.go`

Directly wraps `internal/engine.Manager` for in‑process log and task operations.

```go
package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/retention"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// TaskQueue is the client-facing task driver interface.
type TaskQueue = taskqueue.ClientTaskQueue

// ErrTaskNotSupported is retained for source compatibility; task queues are now supported.
var ErrTaskNotSupported = errors.New("task queues are not supported by this client")

// embeddedClient implements the client using an in-process engine.Manager.
type embeddedClient struct {
	mgr    *engine.Manager
	queues map[string]*logqueue.LogQueue
	tasks  map[string]TaskQueue
	mu     sync.RWMutex
	cfg    Config
	closed bool

	retention       *retention.Coordinator
	retentionCancel context.CancelFunc
	retentionWG     sync.WaitGroup
}
```

**Implementation Details:**

- `NewEmbedded`: Applies `Option`s to `DefaultClientConfig()`, creates an `engine.Config` from the client config, and calls `engine.OpenManager(dir, engineCfg)`. Returns an `*embeddedClient` with a `retention.Coordinator`.
- `CreateQueue`: Creates an `engine.Queue` via `mgr.CreateQueue(name, engineCfg)`, wraps it in a `logqueue.NewLogQueue(name, storage)`, and stores it in the `queues` map. Also registers the log queue as a retention driver.
- `LogQueue`: Uses double‑checked locking to return a single `*logqueue.LogQueue` instance.
- `TaskQueue`: Uses double‑checked locking to return a single `*taskqueue.TaskQueue` instance. If not present, it materializes the task driver on the existing storage.
- `DeleteQueue`: Closes any active task queue, removes cached handles, unregisters from retention, and deletes the physical queue.
- `TruncateQueue`: Materializes the log queue and the task queue, then calls `retention.ReconcileQueueAt(ctx, name, minOffset)`.
- `Stats`: Calls `mgr.Stats()` and returns the engine stats.
- `Close`: Cancels retention loop, closes all task queues, closes manager.

#### `remote.go`

Implements client and queue interfaces using connection‑pooled IPC/TCP wire frame exchanges.

```go
package client

import (
	"bufio"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"iter"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/group"
	"github.com/dmunish/mikyu/internal/ipc"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

type remoteClient struct {
	target   string
	network  string
	addr     string
	cfg      Config
	pool     chan *clientConn
	maxConns int
	connSem  chan struct{}
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}
```

**Implementation Details:**

- `NewRemote`: Parses `target` using `protocol.ParseTarget`. Allocates a buffered channel `pool` with capacity `cfg.MaxConns` and a semaphore `connSem`.
- `acquire(ctx)`: Acquires a slot on `connSem`, then tries to get a connection from `pool`; if empty, dials a new one. Respects context cancellation.
- `dialConn(ctx)`: Dials IPC target via `ipc.DialContext` if network is `unix` or `pipe`. For TCP, checks if `TLSConfig` is non‑nil; if so, uses `tls.Dialer`, otherwise standard `net.Dialer`.
- `release(ch)`: Returns `ch` to `pool`. If `closed.Load()` is true or `pool` is full, closes underlying `ch.conn`.
- `Close()`: Swaps `closed` atomic boolean to true and drains/closes all idle connections in `pool`.
- `exchange(ctx, op, body)`: Acquires connection, sets write/read timeout deadline from `ctx`, calls `protocol.WriteFrame` and `protocol.ReadFrame`. If response opcode is `OpError`, decodes wire error via `decodeProtocolError` and returns corresponding Go error.
- `remoteLogQueue`: Implements `logqueue.ClientLogQueue` via `LogQueue` interface. `Produce` writes `OpProduce` frame, `Read` writes `OpRead` frame, `LastOffset` writes `OpTail` frame, `Stream`/`Tail` use `iter.Seq2` with retry logic.
- `remoteTaskQueue`: Implements `taskqueue.ClientTaskQueue` via `TaskQueue` interface. `Enqueue` writes `OpTaskEnqueue` frame, `Dequeue` writes `OpTaskDequeue`, `Ack`/`Fail`/`Extend` write corresponding opcodes, `DeadLetters` writes `OpTaskDeadLetters`, etc. By‑ID variants use the appropriate opcodes.
- `remoteGroup`: Uses `group.Base` for concurrency‑safe high‑water tracking; `Commit` writes an `OpCommitOffset` binary frame to the connection.

### Internal Command Module (`package command`)

#### `command.go`

Provides stateless helper functions used by both CLI and REPL.

```go
package command

import (
	"context"
	"io"
	"os"
	"strings"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"golang.org/x/term"
)

func GetTerminalWidth() int { ... }
func FormatSingleLinePayload(payload []byte, availableWidth int) string { ... }
func ResolvePayload(input string) ([]byte, error) { ... }

func List(ctx context.Context, client mikyu.Client) ([]string, error) { ... }
func Create(ctx context.Context, client mikyu.Client, name string) error { ... }
func Delete(ctx context.Context, client mikyu.Client, name string, confirmFn func() (bool, error)) (executed bool, err error) { ... }
func Produce(ctx context.Context, client mikyu.Client, name string, input string) (offset uint64, payloadLen int, err error) { ... }
func Read(ctx context.Context, client mikyu.Client, name string, offset uint64) (mikyu.Message, error) { ... }
func Tail(ctx context.Context, client mikyu.Client, name string, startOffset uint64, onMessage func(msg mikyu.Message) error) error { ... }
func Stats(ctx context.Context, client mikyu.Client) (mikyu.SystemStats, error) { ... }
func Compact(ctx context.Context, client mikyu.Client, name string, minOffset uint64, confirmFn func() (bool, error)) (reclaimed int64, executed bool, err error) { ... }
func Shutdown(target string, confirmFn func() (bool, error)) (executed bool, err error) { ... }
```

**Implementation Details:**

- `GetTerminalWidth`: Checks stdout, stderr, stdin descriptors for interactive terminal and returns width or -1.
- `FormatSingleLinePayload`: Escapes control characters, strips non‑printable characters, truncates to fit available width using `lipgloss.Width`.
- `ResolvePayload`: Handles `-` for stdin, `@file` for file reads, otherwise literal string.
- `Delete`/`Compact`/`Shutdown`: Accept an optional `confirmFn` for interactive confirmation before executing destructive operations.
- `Shutdown`: Delegates to `daemon.SendShutdownSignal`.

#### `task.go`

Adds task‑oriented command helpers:

```go
package command

import (
	"context"
	"errors"
	"time"

	"github.com/dmunish/mikyu"
)

type PublishOptions struct {
	VisibilityTimeout time.Duration
	MaxAttempts       uint32
	ScheduleAt        time.Time
}

func Publish(ctx context.Context, client mikyu.Client, queue string, input string, opts PublishOptions) (mikyu.Task, int, error) { ... }
func Pop(ctx context.Context, client mikyu.Client, queue string, timeout time.Duration) (mikyu.Task, error) { ... }
func TaskAck(ctx context.Context, client mikyu.Client, queue string, taskID string) error { ... }
func TaskNack(ctx context.Context, client mikyu.Client, queue string, taskID string, retryAfter time.Duration, explicitRetry bool) error { ... }
func TaskExtend(ctx context.Context, client mikyu.Client, queue string, taskID string, timeout time.Duration) error { ... }
func TaskDeadLetters(ctx context.Context, client mikyu.Client, queue string) ([]mikyu.Task, error) { ... }
func TaskAckDeadLetter(ctx context.Context, client mikyu.Client, queue string, taskID string) error { ... }
```

**Implementation Details:**

- `Publish`: Resolves payload, calls `client.TaskQueue(queue).Enqueue()` with provided options.
- `Pop`: Waits for a task with given timeout; treats timeout < 0 as indefinite, 0 as 25ms.
- `TaskAck`/`TaskNack`/`TaskExtend`: Use the corresponding by‑ID methods for convenience (stateless admin operations).
- `TaskDeadLetters`: Lists dead‑letter tasks.
- `TaskAckDeadLetter`: Acknowledges (removes) a dead‑letter task.

### Internal Log Queue Module (`package logqueue`)

#### `api.go`

Defines the `ClientLogQueue` interface exposed to upper layers.

```go
package logqueue

import (
	"context"
	"iter"

	"github.com/dmunish/mikyu/internal/engine"
)

// ClientLogQueue is the queue-driver interface exposed through the public
// client API. The concrete LogQueue remains an implementation detail of this
// package while the interface lets upper layers avoid importing engine details.
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

var _ ClientLogQueue = (*LogQueue)(nil)
```

#### `logqueue.go`

Provides a high‑level wrapper around `engine.Storage` for log‑oriented semantics and consumer group management.

```go
package logqueue

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

type LogQueue struct {
	name    string
	storage engine.Storage
	policy  LogRetentionPolicy
	mu      sync.RWMutex
	groups  map[string]*queueGroup
}

func NewLogQueue(name string, storage engine.Storage) *LogQueue { ... }
func (q *LogQueue) Name() string { ... }
func (q *LogQueue) Produce(ctx context.Context, payload []byte) (uint64, error) { ... }
func (q *LogQueue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) { ... }
func (q *LogQueue) Read(ctx context.Context, offset uint64) (engine.Message, error) { ... }
func (q *LogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error) { ... }
func (q *LogQueue) LastOffset(ctx context.Context) (uint64, error) { ... }
func (q *LogQueue) MinOffset(ctx context.Context) (uint64, error) { ... }
func (q *LogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] { ... }
func (q *LogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] { ... }
func (q *LogQueue) Group(name string) Group { ... }
func (q *LogQueue) CommitOffset(ctx context.Context, group string, offset uint64) error { ... }
func (q *LogQueue) FetchOffset(ctx context.Context, group string) (uint64, error) { ... }
func (q *LogQueue) SetRetentionPolicy(policy LogRetentionPolicy) { ... }
func (q *LogQueue) RetentionPolicy() LogRetentionPolicy { ... }
func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, error) { ... }
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) { ... }
func (q *LogQueue) Close() error { ... }
```

**Implementation Details:**

- `Stream`: Returns an iterator that repeatedly calls `storage.Read` until `ErrOffsetNotFound` or context cancellation.
- `Tail`: Returns an iterator that polls `storage.LastOffset()`; when `offset < last`, reads and yields messages; otherwise sleeps for 5ms. On transient errors, retries after a 5ms sleep.
- `Group`: Returns or creates a `queueGroup` instance for the given name. The `queueGroup` wraps `group.Base` to provide high‑water tracking and commits to storage.
- `TruncateFloor`: Computes the safe truncation floor based on consumer group high‑watermarks and optional TTL policy.
- `Truncate`: If `minOffset == 0`, uses `TruncateFloor()`; otherwise calls `storage.TruncateBefore()`.
- `Close`: Returns nil (no cleanup needed; actual storage is closed by the manager).

#### `group.go`

Provides the `Group` interface and implementation wrapping `group.Base`.

```go
package logqueue

import (
	"context"
	"io"

	"github.com/dmunish/mikyu/internal/group"
)

// Group defines the consumer group offset tracking interface.
type Group interface {
	io.Closer
	Name() string
	Mark(offset uint64)
	Commit(ctx context.Context) error
	FetchOffset(ctx context.Context) (uint64, error)
}

type queueGroup struct {
	group.Base
	q *LogQueue
}

func newQueueGroup(q *LogQueue, name string) *queueGroup { ... }
func (g *queueGroup) Name() string { ... }
func (g *queueGroup) FetchOffset(ctx context.Context) (uint64, error) { ... }
```

**Implementation Details:**

- `queueGroup` embeds `group.Base`, which provides thread‑safe `Mark`, `Commit`, and `Close` methods.
- `newQueueGroup` sets `Base.Name` to the group name and `Base.CommitFn` to delegate to `q.CommitOffset`.

#### `retention.go`

Defines log retention policy and safe truncation floor calculation.

```go
package logqueue

import (
	"context"
	"time"
)

// LogRetentionPolicy configures log driver retention parameters.
type LogRetentionPolicy struct {
	TTL time.Duration
}

func (q *LogQueue) SetRetentionPolicy(policy LogRetentionPolicy) { ... }
func (q *LogQueue) RetentionPolicy() LogRetentionPolicy { ... }
func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, error) { ... }
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error) { ... }
```

### Internal Task Queue Module (`package taskqueue`)

This package implements a full task‑oriented queue on top of `engine.Storage`.

#### `taskqueue.go`

Main implementation.

```go
package taskqueue

import (
	"bytes"
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// TaskQueue implements at-least-once task processing over engine.Storage.
// State transitions are serialized under mu; task writes and checkpoint writes
// share the same critical section so a checkpoint cannot overtake this driver.
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

var _ ClientTaskQueue = (*TaskQueue)(nil)
```

**Implementation Details:**

- `NewTaskQueue`: Opens the queue, loads the latest valid checkpoint, refreshes from storage, and starts a background checkpoint loop.
- `Enqueue`: Appends a task record to storage, updates in‑memory active map, and pushes to schedule.
- `Dequeue`: Waits for a due task; handles visibility timeout, retries, dead‑lettering on final attempt.
- `Ack`/`Fail`/`Extend`: Validate the delivery lease (task ID, Attempts, DeliveryID) and update state accordingly.
- `AckID`/`FailID`/`ExtendID`: Convenience variants that resolve the current lease by task ID.
- `DeadLetters`: Returns a snapshot of the durable dead‑letter queue.
- `AckDeadLetter`: Removes a dead‑letter task after verifying its identity.
- `Close`: Stops checkpoint loop, flushes a final checkpoint.
- `checkpointLocked`: Writes a new checkpoint record (start, chunks, commit) to storage.
- `loadCheckpointLocked`: Scans back for the last valid checkpoint and restores state.
- `refreshLocked`: Reads new records appended since `scanNext` and updates active tasks, dead letters, etc.
- `TruncateFloor`: Returns the minimum safe truncation offset based on the checkpoint anchor and any uncheckpointed task offsets.

#### `task.go`

Defines `Task` struct, options, errors, and wire helpers.

```go
package taskqueue

import (
	"context"
	"errors"
	"time"
)

const (
	defaultVisibilityTimeout  = 30 * time.Second
	defaultMaxAttempts        = 5
	defaultRetryDelay         = time.Second
	defaultMaxRetryDelay      = 5 * time.Minute
	defaultCheckpointInterval = 5 * time.Second
	checkpointEvery           = 128
	checkpointChunkBytes      = 64 * 1024
)

var (
	ErrTaskNotAvailable   = errors.New("no task is currently available")
	ErrTaskNotFound       = errors.New("task not found")
	ErrTaskNotInflight    = errors.New("task is not currently leased")
	ErrTaskStale          = errors.New("task lease is stale")
	ErrTaskClosed         = errors.New("task queue is closed")
	ErrInvalidTask        = errors.New("invalid task record")
	ErrCheckpointCorrupt  = errors.New("task checkpoint is corrupt")
	ErrDeadLetterNotFound = errors.New("dead letter task not found")
	ErrDeadLetterStale    = errors.New("dead letter task is stale")
)

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

func (t Task) OriginalOffset() uint64 { return t.offset }

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

	// By-ID convenience APIs
	AckID(ctx context.Context, taskID string) error
	FailID(ctx context.Context, taskID string, opts ...FailOption) error
	ExtendID(ctx context.Context, taskID string, timeout time.Duration) error
	AckDeadLetterID(ctx context.Context, taskID string) error
}

type TaskOption func(*taskOptions)
type FailOption func(*failOptions)

func ResolveTaskOptions(opts ...TaskOption) (visibilityTimeout time.Duration, maxAttempts uint32, scheduledAt time.Time) { ... }
func ResolveFailOptions(task Task, opts ...FailOption) time.Duration { ... }
func WithVisibilityTimeout(timeout time.Duration) TaskOption { ... }
func WithMaxAttempts(n uint32) TaskOption { ... }
func WithScheduleAt(at time.Time) TaskOption { ... }
func WithRetryAfter(delay time.Duration) FailOption { ... }
```

#### `scheduler.go`

Implements a priority queue for task scheduling.

#### `checkpoint.go`

Implements durable checkpointing: encoding/decoding of task state, dead‑letter queue, and chunked snapshot.

#### `compaction.go`

Implements `TruncateFloor` for the task driver, used by retention coordinator.

### Internal Group Module (`package group`)

#### `group.go`

Provides the concurrency‑safe base for consumer group high‑watermark tracking.

```go
package group

import (
	"context"
	"sync"
)

// CommitFunc persists a group's current high-watermark.
type CommitFunc func(ctx context.Context, offset uint64) error

// Base contains the concurrency-safe high-watermark and lifecycle state shared
// by embedded and remote consumer groups.
type Base struct {
	Name      string
	Mu        sync.Mutex
	HighWater uint64
	Closed    bool
	CommitFn  CommitFunc
}

func (g *Base) Mark(offset uint64) { ... }
func (g *Base) Commit(ctx context.Context) error { ... }
func (g *Base) Close() error { ... }
```

**Implementation Details:**

- `Mark`: Updates the internal high‑water offset to `offset+1` if `offset >= HighWater`.
- `Commit`: Locks `Mu`, reads `HighWater`, unlocks, and calls `CommitFn`.
- `Close`: Idempotent; commits the final high‑watermark before returning.

### Internal Storage Engine (`package engine`)

#### `storage.go`

Defines the policy‑agnostic storage interface implemented by `Queue`.

```go
package engine

import "context"

// Storage defines the policy‑agnostic storage operations for a log queue.
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

	// Maintenance & truncation primitives
	MinOffset() uint64
	Segments() []SegmentInfo
	TruncateBefore(ctx context.Context, offset uint64) (reclaimedBytes int64, err error)
}
```

#### `queue.go`

Implements the `Storage` interface with log‑structured append, zero‑copy reads, segment rolling, WAL integration, and safe truncation.

**Key Implementation Details (updated):**

- `AppendBatch`: Handles both async (Fsync=false) and durable (Fsync=true) paths. The async path writes records directly to the segment file via `asyncChan` and advances `publishedOffset` immediately.
- `Read`: Checks the ring cache first, then locates the segment via binary search. Uses `readFromSegment` for both mmap and `ReadAt` paths.
- `readFromSegment`: Helper that reads from either mmap (closed segments) or `ReadAt` (active segments) and decodes the record.
- `roll`: Rotates the active segment based on size or age, adjusts `nextPreallocBytes` dynamically, truncates the old segment to its written size, and enables mmap for zero‑copy reads.
- `loadSegments`: Scans the queue directory for log files, sorts them by base offset, and opens each via `openSegment`. Calls `recover()` on each segment to rebuild valid index entries.
- `TruncateBefore`: Physically deletes closed segments whose next offset <= target, updates minOffset, and evicts ring cache entries.

#### `wal.go`

Implements Write‑Ahead Log flushing using dynamic dual‑threshold batching, micro‑linger grouping, and platform‑optimal disk sync system calls. The WAL supports two modes controlled by the `Fsync` flag.

**Key Implementation Details (updated):**

- `asyncChan`: A buffered channel (`asyncChanBufferCap = 65536`) used for the Fsync=false fast path. Producers write entries to this channel and the flusher drains them, writes to the segment file, and updates the index.
- `syncLoop`: Monitors `asyncChan`, `ticker.C`, `wake`, and `shutdown` channels. Handles both async and sync paths.
- `drainAndFlushAsync`: Groups up to `BatchSize` async entries, writes them to the segment file, updates indices, and advances `publishedOffset`.
- `pushBatch`: Supports batch produce operations for the durable path.
- `flush`: Swaps staged buffers, writes data, syncs if `Fsync` is enabled, and broadcasts to waiting producers.

#### `segment.go`

Handles active file I/O and read‑only mmap segment handling.

#### `encode.go`

Provides CRC32 record framing, decoding, and raw UUID v7 handling.

#### `cache.go`

Implements the in‑memory ring cache for recent messages.

#### `compact.go`

Implements segment metadata and truncation (`MinOffset`, `Segments`, `TruncateBefore`).

#### `offsets.go`

Implements the manager‑level consumer group offset store.

### Internal Protocol Module (`package protocol`)

#### `opcodes.go`

Defines wire opcodes, error codes, and target URI parsing.

**Key Implementation Details (updated):**

- Added task opcodes: `OpTaskEnqueue`, `OpTaskDequeue`, `OpTaskAck`, `OpTaskFail`, `OpTaskExtend`, `OpTaskDeadLetters`, `OpTaskDeadLetterAck`, and ByID variants.
- `ParseTarget`: Supports `unix://`, `pipe://`, `tcp://` schemes, or bare `host:port` (treated as TCP).

#### `frames.go` and `bytes.go`

(Unchanged.)

#### `task.go`

Provides task encoding/decoding for wire protocol.

```go
package protocol

import (
	"fmt"
	"time"

	"github.com/dmunish/mikyu/internal/taskqueue"
)

func EncodeTask(b *Buffer, task taskqueue.Task) { ... }
func DecodeTask(r *Reader) (taskqueue.Task, error) { ... }
func EncodeTaskList(b *Buffer, tasks []taskqueue.Task) { ... }
func DecodeTaskList(r *Reader) ([]taskqueue.Task, error) { ... }
func EncodeTaskID(b *Buffer, queue, taskID string) { ... }
func DecodeTaskID(r *Reader) (queue string, taskID string, err error) { ... }
```

### Internal Server Module (`package server`)

#### `server.go`

**Key Implementation Details (updated):**

- Added `getOrCreateLogQueue` and `getOrCreateTaskQueue` methods with double‑checked locking to prevent duplicate queue wrappers under concurrent requests.
- `serveConn` uses `bufio.Reader` for frame decoding and delegates to `dispatch`.

#### `handlers.go`

**Key Implementation Details (updated):**

- All handlers call `s.getOrCreateLogQueue(string(name))` or `s.getOrCreateTaskQueue(string(name))` instead of directly accessing `s.mgr`.
- Added handlers for task operations: `handleTaskEnqueue`, `handleTaskDequeue`, `handleTaskAck`, `handleTaskFail`, `handleTaskExtend`, `handleTaskDeadLetters`, `handleTaskDeadLetterAck`, and ByID variants.
- `handleTruncate` now materializes the task driver before invoking retention reconciliation.
- `handleShutdown` signals the server to stop gracefully.

#### `tls.go`

(Unchanged.)

### Internal IPC Module (`package ipc`)

(Unchanged.)

### Internal Daemon Module (`package daemon`)

#### `daemon.go`

**Key Implementation Details (updated):**

- `Run(configPath)`: Loads config, creates `engine.Manager`, reconciles declared queues, starts `server.Server`, and waits for signals or server shutdown.
- `EnsureDaemonRunning(target)`: Checks if daemon is alive; if not, spawns a detached background daemon and waits up to 3 seconds for it to come online.
- `IsDaemonAlive(target)`: Attempts to connect to target with a 150ms timeout.
- `SendShutdownSignal(target)`: Sends an `OpShutdown` frame to the daemon.

### Internal Retention Module (`package retention`)

#### `coordinator.go`

**Key Implementation Details (updated):**

- `DriverProvider` interface defines `TruncateFloor(ctx) (uint64, error)`.
- `Coordinator` tracks `QueueTarget` per queue, with a list of registered drivers.
- `RegisterDriver` adds a driver (log queue or task queue) to a queue's target.
- `ReconcileQueueAt` computes the minimum safe floor across all drivers and invokes `storage.TruncateBefore()`, respecting any caller‑requested offset.

### Internal REPL Module (`package repl`)

#### `repl.go`

**Key Implementation Details (updated):**

- Uses `internal/command` package for all operations (List, Create, Delete, Produce, Read, Tail, Stats, Compact, Shutdown, Publish, Pop, Ack, Nack, Extend, DLQ, DLQ‑Ack).
- `handleShutdown` delegates to `command.Shutdown`.
- `confirm` uses `huh.Confirm` form styled with Lipgloss.

#### `style.go`

(Unchanged.)

#### `task.go`

Adds REPL task commands.

### Internal CLI Module (`package cli`)

#### `cli.go`

**Key Implementation Details (updated):**

- Uses `internal/command` package for all operations.
- `RunCLI(target, args)`: Ensures daemon is running, dials the client, then dispatches based on the first argument.
- Supports subcommands: `list`, `create`, `delete`, `produce`, `read`, `tail`, `stats`, `compact`, `shutdown`, `publish`, `pop`, `ack`, `nack`, `extend`, `dlq`, `dlq-ack`.
- `confirmCLI`: Prompts for yes/no on a terminal; if non‑interactive, requires `-y`/`--yes` flag.

#### `task.go`

Adds CLI task command handling and JSON output for non‑interactive mode.

### Entrypoint (`cmd/mikyu/main.go`)

**Key Implementation Details (updated):**

- No arguments: Runs REPL with default target.
- `daemon` / `server`: Runs daemon with optional config path.
- `shell` / `repl`: Runs REPL with default target.
- `cli`: Runs CLI with arguments after `cli`.
- Default: Runs CLI with all arguments.

---

## 5. Key Performance Characteristics

- **Write path (durable)**: ~292.6 µs/op (with fsync), ~25.9 µs/op (without fsync) for 64 B payloads.
- **Read path (tail)**: ~100 ns/op via ring cache, zero allocations.
- **Encoding**: ~887 ns/op with zero allocation.
- **Protocol frames**: 90.7% latency reduction for 64 KB frames (38.5 µs → 3.5 µs) due to streaming reads and pooled writes.
- **Index memory**: O(log S) segments, no per‑message in‑memory index.
- **Task throughput**: Benchmarks show high concurrent enqueue/dequeue/ack rates; checkpoint writes are batched and amortized.

---

## 6. Concurrency Model

- The engine uses a `sync.RWMutex` per queue for segment and offset management.
- The WAL flusher runs in a dedicated goroutine, using double‑buffered swap and `sync.Cond` to coordinate producers.
- Reads are lock‑free on mmap regions (after initial mapping) and use `ReadAt` for active segments.
- The ring cache is a read/write lock‑protected structure with atomic slot reads.
- The remote client uses a channel‑based connection pool with context‑aware acquire/release.
- The retention coordinator runs a background loop, protected by an `RWMutex` on its target map.
- The `group.Base` uses a mutex for high‑watermark updates, shared across embedded and remote implementations.
- The task queue serializes state transitions with a mutex and uses a heap for scheduling; checkpoint writes are performed under the same lock to maintain consistency.

---

## 7. Error Handling

- All public methods return `context.Context` errors when canceled.
- Engine errors are re‑exported as package‑level variables (`ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`, `ErrOffsetTruncated`, `ErrMessageTooLarge`, `ErrCorruptRecord`, `ErrQueueClosed`).
- Task errors are re‑exported (`ErrTaskNotAvailable`, `ErrTaskNotFound`, `ErrTaskNotInflight`, `ErrTaskStale`, `ErrTaskClosed`, `ErrInvalidTask`, `ErrCheckpointCorrupt`, `ErrDeadLetterNotFound`, `ErrDeadLetterStale`).
- Protocol errors are encoded with specific error codes and decoded on the client side into the correct Go error.
- The REPL and CLI render errors in a styled red format.
- `internal/command` functions propagate errors for consistent handling across UI layers.

---

## 8. Security & Encryption

- Directory encryption uses OS‑native mechanisms: `fscrypt` on Linux, NTFS EFS on Windows, fallback warning on other OSes.
- TLS 1.3 is enforced for TCP connections when configured; local IPC bypasses TLS.
- Self‑signed certificates can be auto‑generated for zero‑config local TLS.

---

## 9. Configuration & Environment

- Configuration is loaded from `mikyu.yaml`; environment variables override YAML values.
- Default config is auto‑bootstrapped if missing.
- Configurable options include WAL batching, flush intervals, segment sizes, and Fsync mode.
- Per‑queue overrides are supported in the YAML.

---

## 10. Deployment Modes

1. **Embedded**: `mikyu.Open(dir, opts...)` – in‑process, no network.
2. **Remote Client**: `mikyu.Dial(target, opts...)` – connects to daemon via IPC/TCP.
3. **Daemon**: `daemon.Run(configPath)` – background server with auto‑spawn support.

---

## 11. Log Record Format

Each record is stored as:

```
[4B Body Length][4B CRC32][2B ID Length][16B Raw UUID v7][8B Unix Nanoseconds][Payload]
```

- CRC32 covers all bytes after the length field.
- UUID v7 is stored raw (no string formatting).
- Payload size is limited by `MaxMessageBytes`.

---

## 12. Storage Layout

```
<data_dir>/
├── manager-offsets.dat      # Consumer group offset store (compacted)
├── <queue_name>/
│   ├── <base_offset>.log    # Log segment data
│   └── <base_offset>.idx    # Fixed-width index entries (16 bytes each)
```

- Segment base offsets are zero‑padded 20‑digit decimals.
- Log files are pre‑allocated to `MaxSegmentBytes` for the active segment; closed segments are trimmed to actual write size.
- Index files are rebuilt after crash recovery if needed.

---

## 13. Maintenance & Truncation

- `TruncateBefore` removes closed segments whose `NextOffset` is <= the target offset.
- The retention coordinator automatically triggers truncation based on TTL, consumer group floors, and task driver floors.
- Offset file compaction occurs every 5,000 commits.
- Ring cache eviction happens after truncation to avoid stale reads.

---

## 14. Task Queue Semantics

- **Enqueue**: Appends a task record and schedules it for delivery.
- **Dequeue**: Claims the next due task, increments `Attempts`, sets a visibility deadline, and returns the task with a unique `DeliveryID`.
- **Ack**: Acknowledges successful processing; validates `DeliveryID` and lease.
- **Fail**: Releases the task with an optional retry delay; if `Attempts >= MaxAttempts`, moves the task to the dead‑letter queue.
- **Extend**: Renews the visibility lease without incrementing attempts.
- **By‑ID operations**: Convenience variants for stateless admin tools.
- **Dead‑letter**: Tasks that exceed `MaxAttempts` or whose lease expires on the final attempt are moved to a durable dead‑letter queue. They can be listed and acknowledged.
- **Checkpointing**: Periodic durable snapshots of active tasks and dead‑letters, enabling recovery after crashes.

---

## 15. Retention Coordination

The retention coordinator arbitrates truncation across all drivers registered for a queue:

- **Log driver**: Safe floor = min consumer group high‑watermark (or TTL‑based floor).
- **Task driver**: Safe floor = checkpoint offset, plus any uncheckpointed active/dead‑letter task offsets.
- The coordinator computes the **minimum** of all driver floors and calls `storage.TruncateBefore()` with that value, ensuring no driver loses data it still needs.

---

## 16. Architectural Layering & Dual‑Use Nature

- **Layer 1 – Engine**: `internal/engine` provides a self‑contained storage engine supporting log‑structured append, WAL, segments, mmap reads, and consumer‑group offsets. It is oblivious to higher‑level semantics.
- **Layer 2 – Drivers**:
  - `internal/logqueue` implements log‑oriented semantics (streaming, tailing, consumer groups).
  - `internal/taskqueue` implements task‑oriented semantics (at‑least‑once delivery, retries, dead letters).
  - Both share the same `Storage` interface and can coexist on the same underlying queue (for example, a queue can be used both as a log and as a task source).
- **Layer 3 – Retention**: `internal/retention` coordinates safe truncation across drivers.
- **Layer 4 – Client**:
  - `internal/client/embedded` provides in‑process access.
  - `internal/client/remote` provides network access via the wire protocol.
- **Layer 5 – Access**:
  - `internal/command` offers stateless helpers.
  - `internal/cli` and `internal/repl` provide user‑facing interfaces.
- **Layer 6 – Deployment**:
  - `internal/daemon` runs the server, auto‑spawns, and manages lifecycle.
  - `cmd/mikyu` is the single entry point.

This layered design allows the same storage engine to serve both log queues and task queues, with embedded and remote deployment modes, all behind a consistent public API.

---

## 17. Appendix: Public API Summary

- `mikyu.Open(dir, opts...)` – in‑process client.
- `mikyu.Dial(target, opts...)` – remote client.
- `Client` interface: `CreateQueue`, `LogQueue`, `TaskQueue`, `ListQueues`, `DeleteQueue`, `TruncateQueue`, `Stats`, `Close`.
- `LogQueue` interface: `Produce`, `ProduceBatch`, `Read`, `ReadBatch`, `LastOffset`, `MinOffset`, `Truncate`, `Stream`, `Tail`, `Group`.
- `TaskQueue` interface: `Enqueue`, `Dequeue`, `Ack`, `Fail`, `Extend`, `DeadLetters`, `AckDeadLetter`, `AckID`, `FailID`, `ExtendID`, `AckDeadLetterID`.
- Config options for batching, intervals, segment sizes, encryption, fsync, TLS, etc.