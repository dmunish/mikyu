# Mikyu Technical Specification & Codebase Architecture

## 1. Project Overview

**mikyu** is a high-performance, log-structured message queue built from scratch in Go. It provides a unified API surface operating either as an in-process embedded log engine or as a standalone network/IPC daemon.

### Core Architectural Guarantees & Technical Principles

- **Durable-Before-Ack Semantics:** Every `Produce()` or `ProduceBatch()` invocation blocks until the payload byte slice and index metadata are durably synced to disk via the Write-Ahead Log (`wal`) flusher. This guarantees that once an offset is returned, the message is recoverable after a crash.
- **Optional Crash-Loss Window:** A `Fsync` configuration flag allows operators to disable physical disk sync on every produce operation. When `Fsync: false`, the WAL flusher only performs `WriteAt` syscalls without `syncData`, trading a small crash-loss window for significantly lower latency. The async fast-path writes records directly to the segment file and advances `publishedOffset` before data is on disk, allowing readers to immediately access messages. Fsync=false benchmarks show **~3.5x lower write latency (25.9 µs vs 292.6 µs for 64 B records)**, while Fsync=true provides strict durability at ~300 µs per produce. Data is still written to the OS page cache in both modes; only the final flush to physical storage differs.
- **Zero-Copy Reads:** Closed historical log segments are memory-mapped (`mmap`) into the process address space. Read requests on historical offsets return direct sub-slices of mapped memory buffers without heap allocations or system call overhead. Active segments are read via `ReadAt` syscalls on the open file descriptor. An in-memory ring cache of the most recent 16,384 messages provides an additional fast path for tail reads at **~100 ns/op** with zero allocations.
- **Dynamic Dual-Threshold WAL Flushing:** The WAL flusher triggers a disk sync (`syncData`) when either pending record count reaches `BatchSize` or elapsed time exceeds `MaxInterval`. Additionally, a **micro-linger** mechanism (`MinInterval`) briefly waits to group concurrent producers into a single batch, balancing single-thread latency with high-concurrency parallel throughput. The flusher runs in a dedicated goroutine, swapping buffer pointers to minimize lock hold time; producers never block on disk I/O directly and wait on a `sync.Cond` for the flusher to publish their sequence.
- **Raw 16-Byte UUID v7 Encoding:** Messages are encoded using raw 16-byte Go standard library `uuid.UUID` arrays (`uuid.NewV7()`), avoiding string formatting allocations and reducing log file overhead by 20 bytes per record.
- **Go Range-Over-Func Integration:** Streaming and live tailing are exposed using native `iter.Seq2[Message, error]` iterators (Go 1.23+), providing lazy evaluation and clean cancellation via `context.Context`.
- **Transparent Kernel Directory Encryption:** OS-level directory encryption policies (`fscrypt` ioctl on Linux, NTFS EFS on Windows) are auto-bootstrapped on queue directories, maintaining zero-copy `mmap` performance while encrypting storage at rest. Unsupported OSes (e.g., macOS) log a warning and continue.
- **TLS 1.3 Transport Encryption:** Network communication over TCP enforces TLS 1.3 with optional auto-generated self-signed certificates for zero-config local security, while local IPC (unix/pipe sockets) bypasses TLS overhead entirely.
- **Declarative Configuration & Auto-Bootstrapping:** The daemon parses `mikyu.yaml` via `goccy/go-yaml`, auto-bootstraps commented default configuration files when missing, and reconciles declared queues on launch.
- **Unified Single-Binary Execution:** A single `mikyu` executable acts as a background server daemon, stateless CLI operator and an interactive REPL shell with Lipgloss styling.
- **Non-Blocking Background Auto-Spawning:** Executing `mikyu` directly (or double-clicking the binary) probes the local IPC socket, auto-spawns a detached background daemon if offline, and immediately attaches an interactive REPL prompt.

### Concurrent Design Characteristics

- **Continuous log tailing**: `Tail()` iterators handle the edge case of hitting the log head by treating `offset < last` as the bound and retrying transient reads (e.g., `ErrOffsetNotFound`, `ErrCorruptRecord`, `io.EOF`) with a short sleep. This keeps streams open indefinitely even during in-flight WAL writes.
- **Reader safety**: A `publishedOffset` high-water mark is updated only after WAL disk sync completes (or immediately in async mode). Readers check against this value, preventing `io.EOF` errors during concurrent writes.
- **WAL wake-token handling**: Wake channel draining occurs inside `flush()` under lock, preventing producer hangs from adaptive flushing.
- **Manager lock contention**: `Stats()` uses `RLock()` and in-memory size counters, providing O(1) metrics retrieval.
- **Duplicate client handles**: Double-checked locking in `LogQueue()` guarantees single-instance caching across both embedded and remote clients.
- **Zero-allocation encoding**: `encodeRecord` writes directly into pre-sized slices, achieving ~887 ns/op encoding latency.
- **Double-buffered WAL flusher**: Steady-state allocations are eliminated (0 allocs/op, 3 B/op).
- **Protocol frame efficiency**: `WriteFrame` retains expanded buffers, `ReadFrame` streams payloads directly, achieving a 90.7% latency reduction for 64 KB frames (38.5 µs → 3.5 µs) and zero extra heap allocations.
- **Index RAM reduction**: The engine uses `O(log S)` segment binary search and fixed-width file arithmetic instead of an in-memory index slice. Heap footprint scales with segment count rather than message count.
- **Offset log compaction**: Appends are compacted every 5,000 commits via atomic rename, bounding the offset file size to active consumer groups.
- **Windows mmap robustness**: `enableMMap()` skips zero-byte `.idx` files, preventing initialization errors on Windows.
- **Retention coordination**: The retention subsystem integrates with consumption drivers to compute safe truncation floors automatically, ensuring log segments are not deleted while still needed by active consumers.

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
│   │   └── remote.go         # Connection-pooled TCP/IPC network client
│   │
│   ├── command/        # Stateless command helpers used by CLI/REPL
│   │   ├── command.go       # List, Create, Delete, Produce, Read, Tail, Stats, Compact, Shutdown
│   │   └── command_test.go
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
│   ├── group/          # Shared consumer group high-watermark tracking
│   │   └── group.go         # Base struct with Mark/Commit/Close lifecycle
│   │
│   ├── protocol/       # Binary wire format parser & opcode definitions
│   │   ├── opcodes.go      # Wire opcodes, error codes, and target URI parser
│   │   ├── frames.go       # Length-prefixed binary frame reader and pooled writer
│   │   └── bytes.go        # Bounds-checked binary Reader & Buffer helpers
│   │
│   ├── server/         # Protocol frame dispatcher & TCP server
│   │   ├── server.go       # Listener loop, worker goroutines, and graceful Stop()
│   │   ├── handlers.go     # Wire opcode handler routines mapping to internal/engine
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
│   │   ├── vt_other.go     # Virtual terminal setup stub (//go:build !windows)
│   │   └── vt_windows.go   # Windows ANSI escape sequence enabler (//go:build windows)
│   │
│   └── cli/            # Stateless one-shot CLI handler
│       └── cli.go         # CLI subcommand router, payload resolution, confirmation prompts
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
                 └─────────────────────┘                                    ▲
                                                                           │ (IPC / TLS TCP)
                                                                 ┌─────────┴─────────┐
                                                                 │  internal/daemon  │
                                                                 │  internal/repl    │
                                                                 │  internal/cli     │
                                                                 │  internal/command │
                                                                 │  cmd/mikyu        │
                                                                 └───────────────────┘
```

**Layered Architecture:**

The design follows a clean separation of concerns, enabling maximum reuse across deployment modes:

1. **Storage Engine (`internal/engine`)**: Low-level log storage, WAL, segments, indexes, and offsets. This layer is completely agnostic of transport or presentation.
2. **Logical Queue Abstraction (`internal/logqueue`)**: Provides high-level `LogQueue` operations (Stream, Tail, Groups, Retention) on top of the `Storage` interface. The `api.go` file exports the `ClientLogQueue` interface for upper layers.
3. **Retention Coordination (`internal/retention`)**: Provides a coordinator that calculates safe truncation floors from multiple drivers and instructs the engine to physically remove obsolete segments.
4. **Consumer Group Abstraction (`internal/group`)**: Provides a concurrency-safe `Base` struct for high-watermark tracking, shared by embedded and remote group implementations.
5. **Deployment Runners**:
   - **Embedded (in-process)**: `mikyu.Open()` delegates to `client.NewEmbedded()`, which creates an `engine.Manager` directly, wrapping queues with `logqueue`.
   - **Daemon (network)**: `daemon.Run()` starts a `server.Server` which exposes the engine over IPC/TCP.
6. **Access Layers**:
   - **SDK**: `mikyu.Dial()` returns a `Client` that communicates with the daemon via the wire protocol.
   - **Command Helpers (`internal/command`)**: Stateless functions used by both CLI and REPL for consistent user-facing operations.
   - **CLI/REPL**: Uses the SDK and command helpers to perform user-facing operations.

**Data Flow & Lifecycle:**

1. **Embedded mode:** `mikyu.Open(dir, opts...)` calls `client.NewEmbedded()`, which creates an `engine.Manager` directly in-process. Client calls delegate to `engine.Queue` via `logqueue.LogQueue` wrappers without network/IPC overhead.
2. **Remote mode:** `mikyu.Dial(target, opts...)` calls `client.NewRemote()`, which initializes a `remoteClient` managing a thread-safe connection pool (`chan *clientConn`). Each client operation acquires a connection, writes length-prefixed protocol frames, awaits the response frame, and returns the connection to the pool.
3. **Daemon mode:** `daemon.Run(configPath)` loads `mikyu.yaml` configuration, instantiates `engine.Manager`, creates `server.Server`, and listens on target endpoints (TCP, Unix domain socket, or Windows named pipe). The daemon also reconciles declared queues on startup.

---

## 4. File-by-File Technical Specification

### Root Package (`package mikyu`)

#### `mikyu.go`

Defines public domain models and primary client/queue interfaces, re-exporting internal types.

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

// Re-export core engine types as type aliases.
type Message = engine.Message
type SystemStats = engine.EngineStats
type QueueStats = engine.QueueStats

// Group tracks consumer offsets for a log queue.
type Group = logqueue.Group

// Task represents a unit of work (future).
type Task struct {
	ID          string
	Payload     []byte
	Attempts    uint32
	MaxAttempts uint32
	Visibility  time.Time
	ScheduledAt time.Time
}

// LogQueue provides log-oriented operations (streaming, tailing, groups).
type LogQueue = logqueue.ClientLogQueue

// TaskQueue is a placeholder for future task semantics.
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

// Error variables re-exported from the engine/client layers.
var (
	ErrQueueExists      = engine.ErrQueueExists
	ErrQueueNotFound    = engine.ErrQueueNotFound
	ErrOffsetNotFound   = engine.ErrOffsetNotFound
	ErrOffsetTruncated  = engine.ErrOffsetTruncated
	ErrMessageTooLarge  = engine.ErrMessageTooLarge
	ErrCorruptRecord    = engine.ErrCorruptRecord
	ErrTaskNotSupported = client.ErrTaskNotSupported
)

type _ = iter.Seq2[Message, error]
```

**Implementation Details:**

- `LogQueue` is now a type alias for `logqueue.ClientLogQueue`, which is the interface exported by the `logqueue` package. This prevents upper layers from importing engine details directly.
- `Open` and `Dial` delegate to the `internal/client` package's `NewEmbedded` and `NewRemote` constructors.
- Error variables are re-exported from both `engine` and `client` layers.

#### `options.go`

Re-exports configuration options from `internal/client` for backwards compatibility.

```go
package mikyu

import (
	"crypto/tls"
	"time"

	"github.com/dmunish/mikyu/internal/client"
)

// ClientConfig is re-exported from the internal client package for backwards compatibility.
type ClientConfig = client.Config

// ClientOption configures a client.
type ClientOption = client.Option

// QueueConfig is re-exported from the internal client package for backwards compatibility.
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

// WithFlushInterval is an alias for WithMaxInterval.
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

**Implementation Details:**

- All types and functions are thin wrappers delegating to the `internal/client` package.
- The `DefaultClientConfig()` returns `client.DefaultClientConfig()` with same defaults: `BatchSize: 256`, `MaxInterval: 10ms`, `MinInterval: 30us`, `MaxConns: 10`, `DefaultMaxSegmentBytes: 64 MiB`, `DefaultMaxMessageBytes: 1 MiB`, `DefaultMaxSegmentAge: 1h`, `Encrypted: false`, `Fsync: true`.

#### `typed.go`

Provides generic object serialization helpers over byte queues. (Unchanged.)

---

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
	MaxConns    int

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

- All option functions validate input parameters, ignoring non-positive values for integer/duration limits.
- `WithMinInterval` accepts `>= 0` to support explicitly disabling the micro-linger (0 or negative).

#### `embedded.go`

Directly wraps `internal/engine.Manager` for in-process log operations.

```go
package client

import (
	"context"
	"errors"
	"io"
	"sync"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
)

// TaskQueue is a placeholder for future task semantics.
type TaskQueue interface {
	io.Closer
	Name() string
}

var ErrTaskNotSupported = errors.New("task queues are not yet supported")

type embeddedClient struct {
	mgr    *engine.Manager
	queues map[string]*logqueue.LogQueue
	tasks  map[string]TaskQueue // stub, unused
	mu     sync.RWMutex
	cfg    Config
	closed bool
}

func NewEmbedded(dir string, opts ...Option) (*embeddedClient, error) { ... }
```

**Implementation Details:**

- `NewEmbedded`: Applies `Option`s to `DefaultClientConfig()`, creates an `engine.Config` from the client config, and calls `engine.OpenManager(dir, engineCfg)`. Returns an `*embeddedClient`.
- `CreateQueue`: Creates an `engine.Queue` via `mgr.CreateQueue(name, engineCfg)`, wraps it in a `logqueue.NewLogQueue(name, storage)`, and stores it in the `queues` map.
- `LogQueue`: Uses double-checked locking. First acquires `c.mu.RLock()` to check the map, and if not found, acquires `c.mu.Lock()` and re-verifies before creating a new wrapper.
- `getOrCreateLogQueue`: Returns an existing `*logqueue.LogQueue` or creates a new wrapper around an existing `engine.Storage` retrieved from the manager.
- `TaskQueue`: Returns `ErrTaskNotSupported` (stub).
- `TruncateQueue`: Retrieves the queue from the manager and calls `q.Truncate(ctx, minOffset)`.
- `Stats`: Calls `mgr.Stats()` and converts `engine.EngineStats` to `SystemStats`.
- `Close`: Sets `closed` flag and calls `mgr.Close()`.

#### `remote.go`

Implements client and queue interfaces using connection-pooled IPC/TCP wire frame exchanges.

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
)

type remoteClient struct {
	target   string
	network  string
	addr     string
	cfg      Config
	pool     chan *clientConn
	maxConns int
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func NewRemote(target string, opts ...Option) (*remoteClient, error) { ... }
```

**Implementation Details:**

- `NewRemote`: Parses `target` using `protocol.ParseTarget`. Allocates a buffered channel `pool` with capacity `cfg.MaxConns`.
- `acquire(ctx)`: Selects a `*clientConn` from `pool`. If pool is empty, executes `dialConn(ctx)`. Respects context timeout/cancellation.
- `dialConn(ctx)`: Dials IPC target via `ipc.DialContext` if network is `unix` or `pipe`. For TCP, checks if `TLSConfig` is non-nil; if so, uses `tls.Dialer`, otherwise standard `net.Dialer`.
- `release(ch)`: Returns `ch` to `pool`. If `closed.Load()` is true or `pool` is full, closes underlying `ch.conn`.
- `Close()`: Swaps `closed` atomic boolean to true and drains/closes all idle connections in `pool`.
- `exchange(ctx, op, body)`: Acquires connection, sets write/read timeout deadline from `ctx`, calls `protocol.WriteFrame` and `protocol.ReadFrame`. If response opcode is `OpError`, decodes wire error via `decodeProtocolError` and returns corresponding Go error.
- `remoteLogQueue`: Implements `LogQueue` via `ClientLogQueue` interface. `Produce` writes `OpProduce` frame, `Read` writes `OpRead` frame, `LastOffset` writes `OpTail` frame, `Stream`/`Tail` use `iter.Seq2` with retry logic.
- `remoteGroup`: Uses `group.Base` for concurrency-safe high-water tracking; `Commit` writes an `OpCommitOffset` binary frame to the connection.

---

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
- `FormatSingleLinePayload`: Escapes control characters, strips non-printable characters, truncates to fit available width using `lipgloss.Width`.
- `ResolvePayload`: Handles `-` for stdin, `@file` for file reads, otherwise literal string.
- `Delete`/`Compact`/`Shutdown`: Accept an optional `confirmFn` for interactive confirmation before executing destructive operations.
- `Shutdown`: Delegates to `daemon.SendShutdownSignal`.

---

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

**Implementation Details:**

- This interface is the stable contract for `LogQueue` used by `mikyu.Client`. It adds `Truncate` and `MinOffset` methods beyond the original `LogQueue` interface, exposing retention/compaction operations.
- The `LogQueue` struct in `logqueue.go` implements this interface.

#### `logqueue.go`

Provides a high-level wrapper around `engine.Storage` for log-oriented semantics and consumer group management.

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

var ErrNotSupported = errors.New("feature not supported")

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
func (q *LogQueue) Close() error { ... }
```

**Implementation Details:**

- `Stream`: Returns an iterator that repeatedly calls `storage.Read` until `ErrOffsetNotFound` or context cancellation.
- `Tail`: Returns an iterator that polls `storage.LastOffset()`; when `offset < last`, reads and yields messages; otherwise sleeps for 5ms. On transient errors, retries after a 5ms sleep.
- `Group`: Returns or creates a `queueGroup` instance for the given name. The `queueGroup` wraps `group.Base` to provide high-water tracking and commits to storage.
- `CommitOffset`/`FetchOffset`: Directly forward to the storage layer for durable group offsets.
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

- `queueGroup` embeds `group.Base`, which provides thread-safe `Mark`, `Commit`, and `Close` methods.
- `newQueueGroup` sets `Base.Name` to the group name and `Base.CommitFn` to delegate to `q.CommitOffset`.

#### `retention.go`

Defines log retention policy and safe truncation floor calculation. (Unchanged.)

---

### Internal Group Module (`package group`)

#### `group.go`

Provides the concurrency-safe base for consumer group high-watermark tracking.

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

- `Mark`: Updates the internal high-water offset to `offset+1` if `offset >= HighWater`.
- `Commit`: Locks `Mu`, reads `HighWater`, unlocks, and calls `CommitFn`.
- `Close`: Idempotent; commits the final high-watermark before returning.

---

### Internal Storage Engine (`package engine`)

#### `storage.go`

Defines the policy-agnostic storage interface implemented by `Queue`. (Unchanged.)

#### `queue.go`

Implements the `Storage` interface with log-structured append, zero-copy reads, segment rolling, WAL integration, and safe truncation.

**Key Implementation Details (updated):**

- `AppendBatch`: Handles both async (Fsync=false) and durable (Fsync=true) paths. The async path writes records directly to the segment file via an asynchronous channel and advances `publishedOffset` immediately.
- `Read`: Checks the ring cache first, then locates the segment via binary search. Uses `readFromSegment` for both mmap and `ReadAt` paths.
- `readFromSegment`: New helper method that reads from either mmap (closed segments) or `ReadAt` (active segments) and decodes the record.
- `roll`: Rotates the active segment based on size or age, adjusts `nextPreallocBytes` dynamically, truncates the old segment to its written size, and enables mmap for zero-copy reads.
- `loadSegments`: Scans the queue directory for log files, sorts them by base offset, and opens each via `openSegment`. Calls `recover()` on each segment to rebuild valid index entries.

#### `wal.go`

Implements Write-Ahead Log flushing using dynamic dual-threshold batching, micro-linger grouping, and platform-optimal disk sync system calls. The WAL supports two modes controlled by the `Fsync` flag.

**Key Implementation Details (updated):**

- `asyncChan`: A buffered channel (`asyncChanBufferCap = 65536`) used for the Fsync=false fast path. Producers write entries to this channel and the flusher drains them, writes to the segment file, and updates the index.
- `syncLoop`: Monitors `asyncChan`, `ticker.C`, `wake`, and `shutdown` channels. Handles both async and sync paths.
- `drainAndFlushAsync`: Groups up to `BatchSize` async entries, writes them to the segment file, updates indices, and advances `publishedOffset`.
- `pushBatch`: Supports batch produce operations for the durable path.
- `flush`: Swaps staged buffers, writes data, syncs if `Fsync` is enabled, and broadcasts to waiting producers.

---

### Internal Protocol Module (`package protocol`)

#### `opcodes.go`

Defines wire opcodes, error codes, and target URI parsing.

**Key Implementation Details (updated):**

- Added `OpTruncate byte = 0x11` opcode.
- `ParseTarget`: Supports `unix://`, `pipe://`, `tcp://` schemes, or bare `host:port` (treated as TCP). Returns network and address components.

#### `frames.go` and `bytes.go`

(Unchanged.)

---

### Internal Server Module (`package server`)

#### `server.go`

**Key Implementation Details (updated):**

- Added `getOrCreateLogQueue` method with double-checked locking to prevent duplicate queue wrappers under concurrent requests.
- `serveConn` uses `bufio.Reader` for frame decoding and delegates to `dispatch`.

#### `handlers.go`

**Key Implementation Details (updated):**

- All handlers call `s.getOrCreateLogQueue(string(name))` instead of directly accessing `s.mgr`.
- `handleTruncate`: Parses queue name and min offset, calls `q.Truncate(ctx, minOffset)`, returns reclaimed bytes count.
- `handleShutdown`: Signals the server to stop gracefully.

---

### Internal IPC Module (`package ipc`)

(Unchanged.)

---

### Internal Daemon Module (`package daemon`)

#### `daemon.go`

**Key Implementation Details (updated):**

- `Run(configPath)`: Loads config, creates `engine.Manager`, reconciles declared queues, starts `server.Server`, and waits for signals or server shutdown.
- `EnsureDaemonRunning(target)`: Checks if daemon is alive; if not, spawns a detached background daemon and waits up to 3 seconds for it to come online.
- `IsDaemonAlive(target)`: Attempts to connect to target with a 150ms timeout.
- `SendShutdownSignal(target)`: Sends an `OpShutdown` frame to the daemon.

---

### Internal Retention Module (`package retention`)

#### `coordinator.go`

(Unchanged.)

---

### Internal REPL Module (`package repl`)

#### `repl.go`

**Key Implementation Details (updated):**

- Uses `internal/command` package for all operations (List, Create, Delete, Produce, Read, Tail, Stats, Compact, Shutdown).
- `handleShutdown` delegates to `command.Shutdown`.
- `confirm` uses `huh.Confirm` form styled with Lipgloss.

#### `style.go`

(Unchanged.)

---

### Internal CLI Module (`package cli`)

#### `cli.go`

**Key Implementation Details (updated):**

- Uses `internal/command` package for all operations.
- `RunCLI(target, args)`: Ensures daemon is running, dials the client, then dispatches based on the first argument.
- Supports subcommands: `list`, `create`, `delete`, `produce`, `read`, `tail`, `stats`, `compact`, `shutdown`.
- `confirmCLI`: Prompts for yes/no on a terminal; if non-interactive, requires `-y`/`--yes` flag.

---

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
- **Index memory**: O(log S) segments, no per-message in-memory index.

---

## 6. Concurrency Model

- The engine uses a `sync.RWMutex` per queue for segment and offset management.
- The WAL flusher runs in a dedicated goroutine, using double-buffered swap and `sync.Cond` to coordinate producers.
- Reads are lock-free on mmap regions (after initial mapping) and use `ReadAt` for active segments.
- The ring cache is a read/write lock-protected structure with atomic slot reads.
- The remote client uses a channel-based connection pool with context-aware acquire/release.
- The retention coordinator runs a background loop, protected by an `RWMutex` on its target map.
- The `group.Base` uses a mutex for high-watermark updates, shared across embedded and remote implementations.

---

## 7. Error Handling

- All public methods return `context.Context` errors when canceled.
- Engine errors are re-exported as package-level variables (`ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`, `ErrOffsetTruncated`, `ErrMessageTooLarge`, `ErrCorruptRecord`).
- Protocol errors are encoded with specific error codes and decoded on the client side into the correct Go error.
- The REPL and CLI render errors in a styled red format.
- `internal/command` functions propagate errors for consistent handling across UI layers.

---

## 8. Security & Encryption

- Directory encryption uses OS-native mechanisms: `fscrypt` on Linux, NTFS EFS on Windows, fallback warning on other OSes.
- TLS 1.3 is enforced for TCP connections when configured; local IPC bypasses TLS.
- Self-signed certificates can be auto-generated for zero-config local TLS.

---

## 9. Configuration & Environment

- Configuration is loaded from `mikyu.yaml`; environment variables override YAML values.
- Default config is auto-bootstrapped if missing.
- Configurable options include WAL batching, flush intervals, segment sizes, and Fsync mode.

---

## 10. Deployment Modes

1. **Embedded**: `mikyu.Open(dir, opts...)` – in-process, no network.
2. **Remote Client**: `mikyu.Dial(target, opts...)` – connects to daemon via IPC/TCP.
3. **Daemon**: `daemon.Run(configPath)` – background server with auto-spawn support.

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

- Segment base offsets are zero-padded 20-digit decimals.
- Log files are pre-allocated to `MaxSegmentBytes` for the active segment; closed segments are trimmed to actual write size.
- Index files are rebuilt after crash recovery if needed.

---

## 13. Maintenance & Truncation

- `TruncateBefore` removes closed segments whose `NextOffset` is <= the target offset.
- The retention coordinator can automatically trigger truncation based on TTL or consumer group floors.
- Offset file compaction occurs every 5,000 commits.
- Ring cache eviction happens after truncation to avoid stale reads.