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
- **Unified Single-Binary Execution:** A single `mikyu` executable acts as a background server daemon, stateless CLI operator, interactive REPL shell with Lipgloss styling, and visual TUI dashboard.
- **Non-Blocking Background Auto-Spawning:** Executing `mikyu` directly (or double-clicking the binary) probes the local IPC socket, auto-spawns a detached background daemon if offline, and immediately attaches an interactive REPL prompt.

### Concurrent Design Characteristics

- **Continuous log tailing**: `Tail()` iterators handle the edge case of hitting the log head by treating `offset < last` as the bound and retrying transient reads (e.g., `ErrOffsetNotFound`, `ErrCorruptRecord`, `io.EOF`) with a short sleep. This keeps streams open indefinitely even during in-flight WAL writes.
- **Reader safety**: A `publishedOffset` high-water mark is updated only after WAL disk sync completes (or immediately in async mode). Readers check against this value, preventing `io.EOF` errors during concurrent writes.
- **WAL wake-token handling**: Wake channel draining occurs inside `flush()` under lock, preventing producer hangs from adaptive flushing.
- **Manager lock contention**: `Stats()` uses `RLock()` and in-memory size counters, providing O(1) metrics retrieval.
- **Duplicate client handles**: Double-checked locking in `LogQueue()` guarantees single-instance caching.
- **Zero-allocation encoding**: `encodeRecord` writes directly into pre-sized slices, achieving ~887 ns/op encoding latency.
- **Double-buffered WAL flusher**: Steady-state allocations are eliminated (0 allocs/op, 3 B/op).
- **Protocol frame efficiency**: `WriteFrame` retains expanded buffers, `ReadFrame` streams payloads directly, achieving a 90.7% latency reduction for 64 KB frames (38.5 µs → 3.5 µs) and zero extra heap allocations.
- **Index RAM reduction**: The engine uses `O(log S)` segment binary search and fixed-width file arithmetic instead of an in-memory index slice. Heap footprint scales with segment count rather than message count.
- **Offset log compaction**: Appends are compacted every 5,000 commits via atomic rename, bounding the offset file size to active consumer groups.
- **Windows mmap robustness**: `enableMMap()` skips zero-byte `.idx` files, preventing initialization errors on Windows.

---

## 2. Repository Tree

```text
github.com/dmunish/mikyu/
├── go.mod
├── go.sum
├── README.md
│
├── mikyu.go            # Top-level domain models & interfaces: Client, LogQueue, TaskQueue, Message, SystemStats
├── options.go          # Functional configuration options for Client and Queue
├── typed.go            # Generic Typed[T] queue wrapper & Codec[T] interface
├── embedded.go         # In-process embedded driver implementing Client, LogQueue, TaskQueue
├── remote.go           # Connection-pooled TCP/IPC network driver implementing Client, LogQueue, TaskQueue
│
├── internal/
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
│   │   ├── storage.go          # Storage interface definition
│   │   ├── sync_darwin.go      # macOS F_FULLFSYNC drive cache flusher
│   │   ├── sync_fdatasync.go   # POSIX unix.Fdatasync implementation
│   │   ├── sync_windows.go     # Windows file.Sync implementation
│   │   ├── encrypt_linux.go    # Linux fscrypt kernel ioctl directory encryption
│   │   ├── encrypt_windows.go  # Windows NTFS EFS directory encryption
│   │   └── encrypt_other.go    # Fallback directory encryption for unsupported OSes
│   │
│   ├── logqueue/       # High-level log-oriented queue wrapper & consumer group tracking
│   │   ├── logqueue.go       # LogQueue struct implementing streaming, tailing, group semantics
│   │   └── group.go          # Group interface & implementation for offset tracking
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
                  embeddedClient struct                           remoteClient struct
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
                                                                 │  cmd/mikyu        │
                                                                 └───────────────────┘
```

**Layered Architecture:**

The design follows a clean separation of concerns, enabling maximum reuse across deployment modes:

1. **Storage Engine (`internal/engine`)**: Low-level log storage, WAL, segments, indexes, and offsets. This layer is completely agnostic of transport or presentation.
2. **Logical Queue Abstraction (`internal/logqueue`)**: Provides high-level `LogQueue` operations (Stream, Tail, Groups) on top of the `Storage` interface. This allows the same logic to be used by both embedded and remote backends.
3. **Deployment Runners**:
   - **Embedded (in-process)**: `mikyu.Open()` creates an `engine.Manager` directly, wrapping queues with `logqueue`.
   - **Daemon (network)**: `daemon.Run()` starts a `server.Server` which exposes the engine over IPC/TCP.
4. **Access Layers**:
   - **SDK**: `mikyu.Dial()` returns a `Client` that communicates with the daemon via the wire protocol.
   - **CLI/REPL**: Uses the SDK to perform user-facing operations, ensuring consistent behavior regardless of deployment mode.

**Data Flow & Lifecycle:**

1. **Embedded mode:** `mikyu.Open(dir, opts...)` creates an `engine.Manager` directly in-process. Client calls delegate to `engine.Queue` via `logqueue.LogQueue` wrappers without network/IPC overhead.
2. **Remote mode:** `mikyu.Dial(target, opts...)` initializes a `remoteClient` managing a thread-safe connection pool (`chan *clientConn`). Each client operation acquires a connection, writes length-prefixed protocol frames, awaits the response frame, and returns the connection to the pool.
3. **Daemon mode:** `daemon.Run(configPath)` loads `mikyu.yaml` configuration, instantiates `engine.Manager`, creates `server.Server`, and listens on target endpoints (TCP, Unix domain socket, or Windows named pipe). The daemon also reconciles declared queues on startup.

---

## 4. File-by-File Technical Specification

### Root Package (`package mikyu`)

#### `mikyu.go`

Defines public domain models and primary client/queue interfaces.

```go
package mikyu

import (
	"context"
	"errors"
	"io"
	"iter"
	"time"

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
type LogQueue interface {
	Name() string
	Produce(ctx context.Context, payload []byte) (uint64, error)
	ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
	Read(ctx context.Context, offset uint64) (Message, error)
	ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error)
	LastOffset(ctx context.Context) (uint64, error)
	Stream(ctx context.Context, startOffset uint64) iter.Seq2[Message, error]
	Tail(ctx context.Context, startOffset uint64) iter.Seq2[Message, error]
	Group(name string) Group
}

// TaskQueue is a placeholder for future task semantics.
type TaskQueue interface {
	io.Closer
	Name() string
}

// Client is the top-level interface for interacting with a Mikyu server.
type Client interface {
	io.Closer
	CreateQueue(ctx context.Context, name string, opts ...QueueOption) error
	LogQueue(name string) (LogQueue, error)
	TaskQueue(name string) (TaskQueue, error)
	ListQueues(ctx context.Context) ([]string, error)
	DeleteQueue(ctx context.Context, name string) error
	Stats(ctx context.Context) (SystemStats, error)
}

// Error variables re-exported from the engine.
var (
	ErrQueueExists      = errors.New("queue with this name already exists")
	ErrQueueNotFound    = errors.New("queue not found")
	ErrOffsetNotFound   = errors.New("offset not found")
	ErrMessageTooLarge  = errors.New("message payload exceeds maximum allowed size")
	ErrTaskNotSupported = errors.New("task queues are not yet supported")
)

type _ = iter.Seq2[Message, error]
```

**Implementation Details:**

- `Stream`: Returns an `iter.Seq2[Message, error]` iterator. Loops from `startOffset`, calling `Read()`. Terminates when `Read()` returns `ErrOffsetNotFound` or when context is canceled.
- `Tail`: Returns an `iter.Seq2[Message, error]` iterator. Polls `LastOffset()` continuously; if `curr >= last`, sleeps for 5 milliseconds before re-checking. Yields records as they become available. The bound check is `offset < last` (not `<=`) and transient read errors (`ErrOffsetNotFound`, `ErrCorruptRecord`, `io.EOF`) are retried after a short sleep to handle in-flight WAL writes without terminating the stream.
- `Group`: Tracks consumer high-water marks. `Mark` updates an internal `highWater` variable if `offset >= highWater`; the `highWater` is set to `offset + 1` (i.e., the next offset to be read). `Commit` persists `highWater` to the engine offset store. `Close` automatically invokes `Commit(context.Background())`.

#### `options.go`

Configures client connection pools and per-queue limits via functional options.

```go
type ClientConfig struct {
	BatchSize   int
	MaxInterval time.Duration // Maximum wait time before forcing WAL fsync
	MinInterval time.Duration // Micro-linger window to group concurrent produces
	MaxConns    int
	Fsync       bool          // If true, syncs to disk before ack. If false, accepts crash-loss window for performance

	DefaultMaxSegmentBytes int64
	DefaultMaxMessageBytes int64
	DefaultMaxSegmentAge   time.Duration
	Encrypted              bool
	TLSConfig              *tls.Config
}

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

func DefaultClientConfig() ClientConfig
func WithBatchSize(n int) ClientOption
func WithMaxInterval(d time.Duration) ClientOption
func WithMinInterval(d time.Duration) ClientOption
func WithFlushInterval(d time.Duration) ClientOption // Alias for WithMaxInterval
func WithMaxConns(n int) ClientOption
func WithClientDirectoryEncryption(enabled bool) ClientOption
func WithClientTLS(tlsConfig *tls.Config) ClientOption
func WithClientFsync(enabled bool) ClientOption

// Queue options
func WithQueueBatchSize(n int) QueueOption
func WithQueueMaxInterval(d time.Duration) QueueOption
func WithQueueMinInterval(d time.Duration) QueueOption
func WithMaxSegmentBytes(bytes int64) QueueOption
func WithMaxMessageBytes(bytes int64) QueueOption
func WithMaxSegmentAge(age time.Duration) QueueOption
func WithDirectoryEncryption(enabled bool) QueueOption
func WithQueueFsync(enabled bool) QueueOption
```

**Implementation Details:**

- `DefaultClientConfig`: Defaults to `BatchSize: 256`, `MaxInterval: 10ms`, `MinInterval: 30us`, `MaxConns: 10`, `DefaultMaxSegmentBytes: 64 MiB`, `DefaultMaxMessageBytes: 1 MiB`, `DefaultMaxSegmentAge: 1h`, `Encrypted: false`, `Fsync: true`.
- Option functions validate input parameters, ignoring non-positive values for integer/duration limits.

#### `typed.go`

Provides generic object serialization helpers over byte queues.

```go
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

type Typed[T any] struct {
	queue LogQueue
	codec Codec[T]
}

func NewTyped[T any](q LogQueue, codec Codec[T]) *Typed[T]
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error)
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error)
```

**Implementation Details:**

- `Produce`: Encodes `val` into bytes using `codec.Marshal` and delegates directly to `queue.Produce`.
- `Read`: Calls `queue.Read`, unmarshals `msg.Payload` into a zero-initialized value `T` using `codec.Unmarshal`, and returns it.

#### `embedded.go`

Directly wraps `internal/engine.Manager` for in-process log operations, using `internal/logqueue` for high-level queue semantics.

```go
type embeddedClient struct {
	mgr    *engine.Manager
	queues map[string]*logqueue.LogQueue
	tasks  map[string]TaskQueue // stub, unused
	mu     sync.RWMutex
	cfg    ClientConfig
	closed bool
}

func Open(dir string, opts ...ClientOption) (Client, error)
```

**Implementation Details:**

- `Open`: Applies `ClientOption`s to `DefaultClientConfig()`, creates an `engine.Config`, and calls `engine.OpenManager(dir, engineCfg)`. Returns an `*embeddedClient`.
- `CreateQueue`: Creates an `engine.Queue` via `mgr.CreateQueue(name, engineCfg)`, then wraps it in a `logqueue.NewLogQueue(name, storage)` and stores it in the `queues` map.
- `LogQueue`: Uses double-checked locking to avoid duplicate instances under concurrent calls. First acquires `c.mu.RLock()` to check the map, and if not found, acquires `c.mu.Lock()` and re-verifies before creating a new wrapper. Returns an existing `*logqueue.LogQueue` or creates a new wrapper around an existing `engine.Storage` retrieved from the manager.
- `TaskQueue`: Returns `ErrTaskNotSupported` (stub).
- `embeddedClient`: Implements `Client`. Methods validate `ctx.Err()` prior to execution and forward calls to `mgr`.
- `Stats`: Calls `mgr.Stats()` and converts `engine.EngineStats` to `SystemStats`.

#### `remote.go`

Implements client and queue interfaces using connection-pooled IPC/TCP wire frame exchanges.

```go
type remoteClient struct {
	target   string
	network  string
	addr     string
	cfg      ClientConfig
	pool     chan *clientConn
	maxConns int
	mu       sync.Mutex
	closed   atomic.Bool
}

type clientConn struct {
	conn net.Conn
	r    *bufio.Reader
}

func Dial(target string, opts ...ClientOption) (Client, error)
```

**Implementation Details:**

- `Dial`: Parses `target` using `protocol.ParseTarget`. Allocates a buffered channel `pool` with capacity `cfg.MaxConns`.
- `acquire(ctx)`: Selects a `*clientConn` from `pool`. If pool is empty, executes `dialConn(ctx)`. Respects context timeout/cancellation.
- `dialConn(ctx)`: Dials IPC target via `ipc.Dial(target)` if network is `unix` or `pipe`. For TCP, checks if `TLSConfig` is non-nil or URI starts with `tls://`/`tcps://`; if so, uses `tls.Dialer`, otherwise standard `net.Dialer`.
- `release(ch)`: Returns `ch` to `pool`. If `closed.Load()` is true or `pool` is full, closes underlying `ch.conn`.
- `Close()`: Swaps `closed` atomic boolean to true and drains/closes all idle connections in `pool`.
- `exchange(ctx, op, body)`: Acquires connection, sets write/read timeout deadline from `ctx`, calls `protocol.WriteFrame` and `protocol.ReadFrame`. If response opcode is `OpError`, decodes wire error via `decodeProtocolError` and returns corresponding Go error (`ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`).
- `remoteLogQueue`: Implements `LogQueue`. `Produce` writes `OpProduce` frame, `Read` writes `OpRead` frame, `LastOffset` writes `OpTail` frame, `Stream`/`Tail` use `iter.Seq2` with `q.Read` and `q.LastOffset`. `Tail` uses the same retry logic as the embedded version.
- `remoteGroup`: Uses `sync.Mutex` for thread-safe high-water offset tracking; `Commit` writes an `OpCommitOffset` binary frame to the connection.

---

### Internal Declarative Config Module (`package config`)

#### `internal/config/config.go`

Parses YAML configurations using `goccy/go-yaml`, resolves config paths, auto-bootstraps missing config files, and applies environment variable overrides.

```go
const defaultConfigYAML = `# Mikyu Daemon Configuration

server:
  target: "tcp://127.0.0.1:7420" # Bind target (tcp://, unix://, or pipe://)
  data_dir: "./data"             # Path for WAL log segments and offset store

engine:
  batch_size: 256                # Max pending messages before triggering WAL fsync
  max_interval: "10ms"           # Max wait time before forcing WAL fsync
  min_interval: "30us"           # Micro-linger window to group concurrent produces
  max_segment_bytes: 67108864    # 64 MiB: Max size per log segment before rolling
  max_message_bytes: 1048576     # 1 MiB: Max payload size per produce call
  max_segment_age: "1h"          # Max time before rolling inactive segment
  encrypted: false               # Enable OS transparent directory encryption
  fsync: true                    # Enable durable-before-ack semantics; set false for crash-loss window

queues: []                       # Declarative pre-created queues on startup
`

type Config struct {
	Server ServerConfig  `yaml:"server"`
	Engine EngineConfig  `yaml:"engine"`
	Queues []QueueConfig `yaml:"queues"`
}

type ServerConfig struct {
	Target      string `yaml:"target"`
	DataDir     string `yaml:"data_dir"`
	TLSEnabled  bool   `yaml:"tls_enabled"`
	TLSCertFile string `yaml:"tls_cert_file"`
	TLSKeyFile  string `yaml:"tls_key_file"`
}

type EngineConfig struct {
	BatchSize       int           `yaml:"batch_size"`
	MaxInterval     time.Duration `yaml:"max_interval"`
	MinInterval     time.Duration `yaml:"min_interval"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes"`
	MaxMessageBytes int64         `yaml:"max_message_bytes"`
	MaxSegmentAge   time.Duration `yaml:"max_segment_age"`
	Encrypted       bool          `yaml:"encrypted"`
	Fsync           bool          `yaml:"fsync"`
}

type QueueConfig struct {
	Name            string        `yaml:"name"`
	BatchSize       int           `yaml:"batch_size,omitempty"`
	MaxInterval     time.Duration `yaml:"max_interval,omitempty"`
	MinInterval     time.Duration `yaml:"min_interval,omitempty"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes,omitempty"`
	MaxMessageBytes int64         `yaml:"max_message_bytes,omitempty"`
	MaxSegmentAge   time.Duration `yaml:"max_segment_age,omitempty"`
	Encrypted       *bool         `yaml:"encrypted,omitempty"`
	Fsync           *bool         `yaml:"fsync,omitempty"`
}

func DefaultConfig() Config
func Load(flagPath string) (*Config, string, error)
func resolveOrBootstrapPath(flagPath string) (path string, bootstrapped bool, err error)
func applyEnvOverrides(cfg *Config)
func ToEngineConfig(e EngineConfig) engine.Config
func ToQueueEngineConfig(defaults EngineConfig, q QueueConfig) engine.Config
```

**Implementation Details:**

- `defaultConfigYAML`: Hardcoded string template containing default server target (`tcp://127.0.0.1:7420`), data directory (`./data`), engine defaults, and documented inline comments.
- `Load(flagPath)`: Calls `resolveOrBootstrapPath`. If `flagPath` is empty, checks `./mikyu.yaml`, then `$XDG_CONFIG_HOME/mikyu/mikyu.yaml` (or system user config path). If no file exists, creates `mikyu.yaml` with `defaultConfigYAML` at user config path or `./mikyu.yaml`. Unmarshals YAML data into `Config` struct and executes `applyEnvOverrides(&cfg)`.
- `applyEnvOverrides`: Overrides configuration fields if environment variables are present (`MIKYU_SERVER_TARGET`, `MIKYU_SERVER_DATA_DIR`, `MIKYU_ENGINE_BATCH_SIZE`, `MIKYU_ENGINE_MAX_INTERVAL`, `MIKYU_ENGINE_MIN_INTERVAL`, `MIKYU_ENGINE_MAX_SEGMENT_BYTES`, `MIKYU_ENGINE_MAX_MESSAGE_BYTES`, `MIKYU_ENGINE_MAX_SEGMENT_AGE`, `MIKYU_ENGINE_ENCRYPTED`, `MIKYU_ENGINE_FSYNC`). Also supports a legacy `MIKYU_ENGINE_FLUSH_INTERVAL` as a fallback for `max_interval`.

---

### Internal Storage Engine (`package engine`)

#### `storage.go`

Defines the policy-agnostic storage interface implemented by `Queue`.

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
}
```

**Implementation Details:**

- `Storage` is the minimal interface required by `logqueue.LogQueue` to provide log-oriented operations. It abstracts the engine `Queue` implementation, allowing the `logqueue` package to be reusable across embedded and remote backends.

#### `index.go`

Defines structures for in-memory and persisted segment indices.

```go
type indexEntry struct {
	seg *segment
	pos int64
	len int64
}

type rawIndexEntry struct {
	pos int64
	len int64
}
```

**Implementation Details:**

- `indexEntry`: O(1) in-memory lookup record mapping a logical offset to a target segment, file byte offset (`pos`), and total record length (`len`). This struct is no longer used in `Queue`; it remains available for potential future use but the engine now uses binary search over segments.
- `rawIndexEntry`: 16-byte on-disk structure stored in `.idx` segment files (8 bytes big-endian `pos`, 8 bytes big-endian `len`).

#### `offsets.go`

Provides durable consumer group offset tracking in a unified manager-level file.

```go
const managerOffsetFileName = "manager-offsets.dat"

type OffsetStore struct {
	mu                  sync.RWMutex
	dir                 string
	file                *os.File
	w                   *bufio.Writer
	offsets             map[string]uint64
	appendsSinceCompact int
}

func openOffsetStore(dir string) (*OffsetStore, error)
func (s *OffsetStore) Commit(queueName, group string, offset uint64) error
func (s *OffsetStore) Fetch(queueName, group string) uint64
func (s *OffsetStore) Close() error
```

**Implementation Details:**

- `openOffsetStore`: Opens `manager-offsets.dat` with `O_RDWR|O_CREATE` permissions. Calls `recover()` to populate the in-memory `offsets` map.
- `recover()`: Iteratively reads `[2-byte Key Length][Key Bytes ("queue:group")][8-byte Uint64 Offset]`. If an `io.EOF` or partial record is encountered, stops reading and retains valid state.
- `Commit`: Takes write lock (`mu.Lock`), updates `offsets["queueName:group"]`, appends entry to `bufio.Writer`, flushes writer, and calls `file.Sync()` to ensure immediate disk durability. **Compaction:** After every 5,000 appends, `compactLocked()` rewrites the entire offset file to a temporary file, fsyncs, atomically renames, and reopens it. This bounds the file size to `O(active groups)` and keeps recovery fast.
- `Fetch`: Takes read lock (`mu.RLock`), returns offset value from map (0 if not present).

#### `cache.go`

Provides a fixed-capacity ring buffer for the most recent messages, enabling ~100 ns/op tail reads with zero heap allocations.

```go
type ringCache struct {
	mu    sync.RWMutex
	slots []Message
	mask  uint64
}

func newRingCache(capacity uint64) *ringCache
func (r *ringCache) Put(msg Message)
func (r *ringCache) Get(offset uint64) (Message, bool)
```

**Implementation Details:**

- `newRingCache`: Allocates a slice of `capacity` `Message` values. The `mask` is `capacity - 1` (capacity must be a power of two).
- `Put`: Stores a message at the slot indexed by `msg.Offset & mask` under write lock.
- `Get`: Reads the slot under read lock and verifies the offset matches and the timestamp is non-zero before returning a hit.
- The default capacity is `defaultRingCacheCapacity = 16384`.

#### `segment.go`

Manages log files (`.log`), index files (`.idx`), pre-allocation, zero-copy `mmap` regions, and index recovery.

```go
type segment struct {
	baseOff   uint64
	logFile   *os.File
	idxFile   *os.File
	pos       int64
	createdAt time.Time
	mmapData  mmap.MMap
	idxMMap   mmap.MMap // Memory-mapped index region for closed historical segment
}

func openSegment(dir string, baseOff uint64, preallocSize int64) (*segment, error)
func (s *segment) enableMMap() error
func (s *segment) writeIndex(pos, len int64) error
func (s *segment) recover() ([]rawIndexEntry, error)
func (s *segment) rebuild() ([]rawIndexEntry, error)
func (s *segment) Close() error
```

**Implementation Details:**

- `openSegment`: Opens `%020d.log` and `%020d.idx` files. If log file size is zero, pre-allocates `preallocSize` bytes (dynamic based on the queue's `nextPreallocBytes` field) using `logFile.Truncate(preallocSize)`.
- `enableMMap`: Maps log file into memory using `mmap.Map(logFile, mmap.RDONLY, 0)`. Called when a segment is closed after rolling to enable zero-copy read paths. Before mapping the index file, checks `idxFile.Stat().Size() > 0` to avoid Windows `CreateFileMapping` errors on zero-byte files. If size is zero, skips mapping `idxMMap`.
- `writeIndex`: Encodes `pos` and `len` into 16 big-endian bytes and writes to `idxFile`.
- `recover()`: Performs fast-path verification by reading the final 16-byte record in `idxFile`. If `lastPos + lastLen == logFileSize`, reads the index file in bulk. Otherwise, falls back to `rebuild()`.
- `rebuild()`: Truncates `idxFile` to zero. Scans `logFile` sequentially from `pos = 0`, reading 4-byte record length prefixes and verifying CRC checksums via `decodeRecord`. Reconstructs valid `rawIndexEntry` slices, writes repaired index entries to `idxFile`, truncates `logFile` to the last valid position, and syncs `idxFile`.
- `Close()`: Unmaps `mmapData` and `idxMMap` if non-nil, then closes `logFile` and `idxFile`.

#### `wal.go` & OS-Specific Sync Helpers

Implements Write-Ahead Log flushing using dynamic dual-threshold batching, micro-linger grouping, and platform-optimal disk sync system calls. The WAL supports two modes controlled by the `Fsync` flag:

- **Durable mode (`Fsync: true`):** Standard blocking path. Producers append to the WAL buffer, then wait on `sync.Cond` for the flusher to publish their sequence. The flusher performs `WriteAt` followed by `syncData` on the underlying file. This guarantees that once an offset is returned, the record is durable.
- **Async mode (`Fsync: false`):** The producers write records directly to the segment file via an asynchronous channel (`asyncChan`) and immediately advance `publishedOffset`. The WAL's background goroutine drains the channel, writes the records, and updates the index. No `syncData` is performed, trading a small crash-loss window for significantly lower latency.

The flusher runs in a dedicated goroutine and uses double-buffered swap to minimize lock hold time. Wake tokens are drained inside `flush()` to avoid producer hangs. The implementation uses `sync.Cond` to block producers until their pending records are published.

For OS-specific sync, the following files provide platform-optimal implementations:

- `sync_darwin.go`: Uses `unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)` for maximum durability on macOS.
- `sync_fdatasync.go`: Uses `unix.Fdatasync(fd)` on Linux and other POSIX systems.
- `sync_windows.go`: Uses `file.Sync()` (FlushFileBuffers) on Windows.

#### `manager.go`

Manages multiple queues within a single data directory, handling lifecycle and aggregate statistics.

```go
type Manager struct {
	mu      sync.RWMutex
	dir     string
	cfg     Config
	queues  map[string]*Queue
	offsets *OffsetStore
}

func OpenManager(dir string, cfg Config) (*Manager, error)
func (m *Manager) CreateQueue(name string, opts ...Config) (Storage, error)
func (m *Manager) GetQueue(name string) (Storage, error)
func (m *Manager) ListQueues() []string
func (m *Manager) DeleteQueue(name string) error
func (m *Manager) Stats() EngineStats
func (m *Manager) Close() error
```

**Implementation Details:**

- `OpenManager`: Creates the data directory if not present, initializes the `OffsetStore`, scans for existing queue subdirectories, and opens each one via `openQueue`. If `cfg.Encrypted` is true, it bootstraps directory encryption.
- `CreateQueue`: Checks for existence, optionally applies per-queue overrides, and opens a new `Queue` via `openQueue`.
- `GetQueue`: Returns the `Storage` for an existing queue.
- `ListQueues`: Returns the names of all active queues.
- `DeleteQueue`: Closes the queue and removes its directory.
- `Stats`: Aggregates per-queue statistics (message count, segment count, size, last offset) using in-memory counters under `RLock`.
- `Close`: Closes all queues and the offset store.

#### `queue.go`

Implements the `Storage` interface with log-structured append, zero-copy reads, segment rolling, and WAL integration.

```go
type Queue struct {
	name              string
	dir               string
	cfg               Config
	active            *segment
	segments          []*segment
	nextOffset        uint64
	publishedOffset   uint64
	w                 *wal
	offsets           *OffsetStore
	cache             *ringCache
	nextPreallocBytes int64
	mu                sync.RWMutex
}

func openQueue(parentDir, name string, cfg Config, store *OffsetStore) (*Queue, error)
func (q *Queue) Append(ctx context.Context, payload []byte) (uint64, error)
func (q *Queue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error)
func (q *Queue) ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error)
func (q *Queue) LastOffset() uint64
func (q *Queue) CommitOffset(ctx context.Context, group string, offset uint64) error
func (q *Queue) FetchOffset(ctx context.Context, group string) (uint64, error)
func (q *Queue) Close() error
```

**Implementation Details:**

- `openQueue`: Creates the queue directory, clamps initial preallocation to `MaxSegmentBytes`, loads existing segments via `loadSegments`, and initializes the WAL.
- `AppendBatch`: Encodes each payload with `encodeRecord`, checks size limits, and either follows the async fast path (Fsync=false) or the synchronous durable path (Fsync=true). In async mode, messages are placed in the ring cache and `publishedOffset` is advanced immediately, then queued for background write. In durable mode, producers push to the WAL and wait for the flusher to publish.
- `Read`: Checks the ring cache first; otherwise locates the segment via binary search and reads from the segment (mmap for closed segments, `ReadAt` for active).
- `ReadBatch`: Reads a contiguous range of offsets, preferring cache hits and falling back to segment reads.
- `roll()`: Rotates the active segment based on size or age, adjusts `nextPreallocBytes` dynamically (doubling or halving based on fill duration), truncates the old segment to its written size, and enables mmap for zero-copy reads.
- `CommitOffset` / `FetchOffset`: Delegate to the `OffsetStore`.

#### `encode.go`

Provides CRC32 record framing, decoding, and raw UUID v7 encoding.

```go
func encodeRecord(id uuid.UUID, ts time.Time, payload []byte) []byte
func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error)
```

**Implementation Details:**

- `encodeRecord`: Constructs a record of the form `[4B Body Length][4B CRC32][2B ID Length][16B Raw UUID][8B Unix Nanoseconds][Payload]`. CRC32 is computed over the bytes following the length field.
- `decodeRecord`: Validates the CRC32 checksum, extracts the UUID and timestamp, and returns the payload. Returns `ErrCorruptRecord` on any mismatch or truncation.