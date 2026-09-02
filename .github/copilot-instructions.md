# Mikyu Technical Specification & Codebase Architecture

## 1. Project Overview

**mikyu** is a high-performance, log-structured message queue built from scratch in Go. It provides a unified API surface operating either as an in-process embedded log engine or as a standalone network/IPC daemon.

### Core Architectural Guarantees & Technical Principles

* **Durable-Before-Ack Semantics:** Every `Produce()` or `ProduceBatch()` invocation blocks until the payload byte slice and index metadata are durably synced to disk via the Write-Ahead Log (`wal`) flusher. This guarantees that once an offset is returned, the message is recoverable after a crash.
* **Zero-Copy Reads:** Closed historical log segments are memory-mapped (`mmap`) into the process address space. Read requests on historical offsets return direct sub-slices of mapped memory buffers without heap allocations or system call overhead. Active segments are read via `ReadAt` syscalls on the open file descriptor.
* **Dynamic Dual-Threshold WAL Flushing:** The WAL flusher triggers a disk `Sync()` when either pending record count reaches `BatchSize` or elapsed time exceeds `Interval`, balancing single-thread latency with high-concurrency parallel throughput. The flusher runs in a dedicated goroutine, so producers never block on disk I/O themselves; they wait for the flusher to publish their sequence.
* **Raw 16-Byte UUID v7 Encoding:** Messages are encoded using raw 16-byte Go 1.27 standard library `uuid.UUID` arrays (`uuid.NewV7()`), avoiding string formatting allocations and reducing log file overhead by 20 bytes per record.
* **Go Range-Over-Func Integration:** Streaming and live tailing are exposed using native `iter.Seq2[Message, error]` iterators (Go 1.23+), providing lazy evaluation and clean cancellation via `context.Context`.
* **Transparent Kernel Directory Encryption:** OS-level directory encryption policies (`fscrypt` ioctl on Linux, NTFS EFS on Windows) are auto-bootstrapped on queue directories, maintaining zero-copy `mmap` performance while encrypting storage at rest. Unsupported OSes (e.g., macOS) log a warning and continue.
* **TLS 1.3 Transport Encryption:** Network communication over TCP enforces TLS 1.3 with optional auto-generated self-signed certificates for zero-config local security, while local IPC (unix/pipe sockets) bypasses TLS overhead entirely.
* **Declarative Configuration & Auto-Bootstrapping:** The daemon parses `mikyu.yaml` via `goccy/go-yaml`, auto-bootstraps commented default configuration files when missing, and reconciles declared queues on launch.
* **Unified Single-Binary Execution:** A single `mikyu` executable acts as a background server daemon, stateless CLI operator, interactive REPL shell with Lipgloss styling, and visual TUI dashboard.
* **Non-Blocking Background Auto-Spawning:** Executing `mikyu` directly (or double-clicking the binary) probes the local IPC socket, auto-spawns a detached background daemon if offline, and immediately attaches an interactive REPL prompt.

---

## 2. Repository Tree

```text
github.com/dmunish/mikyu/
├── go.mod
├── go.sum
├── README.md
│
├── mikyu.go            # Top-level interfaces: Client, Queue, Group, Message, SystemStats
├── options.go          # Functional options for Client and Queue configuration
├── typed.go            # Generic Typed[T] wrapper & Codec[T] interface
├── embedded.go         # In-process embedded driver implementing Client/Queue/Group
├── remote.go           # Network/IPC connection-pooled driver
│
├── internal/
│   ├── config/         # Declarative YAML & env configuration manager
│   │   └── config.go       # Auto-bootstrapping, goccy/go-yaml parsing, env overrides
│   │
│   ├── engine/         # Storage engine core (WAL, segments, index, offsets)
│   │   ├── manager.go      # Multi-queue manager & lifecycle state
│   │   ├── queue.go        # Per-queue append log, read paths, and rolling logic
│   │   ├── wal.go          # Async/sync batch WAL flusher with sync.Cond
│   │   ├── segment.go      # Active file I/O & read-only mmap segment handling
│   │   ├── index.go        # In-memory index entry structures
│   │   ├── offsets.go      # Single-file manager-level consumer offset store
│   │   ├── encode.go       # CRC32 record encoding, decoding, and raw UUID v7 handling
│   │   ├── encrypt_linux.go# Linux fscrypt kernel ioctl directory encryption
│   │   ├── encrypt_windows.go# Windows NTFS EFS directory encryption
│   │   └── encrypt_other.go# Fallback directory encryption for unsupported OSes
│   │
│   ├── protocol/       # Binary wire format parser & opcode definitions
│   │   ├── opcodes.go      # Wire opcodes (including OpShutdown), error codes, target parsing
│   │   ├── frames.go       # Length-prefixed binary frame reader and pooled writer
│   │   └── bytes.go        # Bounds-checked binary Reader & Buffer helpers
│   │
│   ├── server/         # Protocol frame dispatcher & TCP server
│   │   ├── server.go       # Listener loop, worker routines, and graceful Stop()
│   │   ├── handlers.go     # Mapping wire OpCodes -> internal/engine calls
│   │   └── tls.go          # TLS 1.3 config helper & self-signed cert auto-generation
│   │
│   └── ipc/            # Specialized local IPC listeners and dialers
│       └── ipc.go          # Unix Domain Socket and Windows Named Pipe helpers
│
└── cmd/
    └── mikyu/          # Unified single-binary entrypoint
        ├── main.go         # Subcommand router (daemon, shell, tui, CLI)
        ├── daemon.go       # Socket prober & background process spawner
        ├── detach_unix.go  # POSIX process detachment (//go:build !windows)
        ├── detach_windows.go# Windows process detachment (//go:build windows)
        ├── vt_other.go     # Virtual terminal setup (//go:build !windows)
        ├── vt_windows.go   # Windows ANSI escape sequence enabling (//go:build windows)
        ├── repl.go         # Interactive REPL session shell & command loop
        ├── banner.go       # Lipgloss-styled ASCII banner & command table
        └── cli.go          # Stateless one-shot CLI handler (@file, stdin support)
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
                 └─────────────────────┘                                    │
                            ▲                                               ▼
                            │                                    ┌─────────────────────┐
                            │ (Server Dispatch)                  │   internal/server   │
                            └────────────────────────────────────┤   internal/ipc      │
                                                                 └─────────────────────┘
                                                                            ▲
                                                                            │ (IPC / TLS TCP)
                                                                 ┌──────────┴──────────┐
                                                                 │      cmd/mikyu      │
                                                                 │  (Unified Executable)│
                                                                 └─────────────────────┘
```

**Data Flow & Lifecycle:**

1. **Embedded mode:** `mikyu.Open()` creates an `engine.Manager` directly. All `Client` methods are synchronous wrappers over the engine. No serialization overhead.
2. **Remote mode:** `mikyu.Dial()` creates a `remoteClient` with a connection pool. Each operation serializes arguments into a binary frame, writes to a pooled connection, reads the response frame, and deserializes. The pool is a buffered channel of `*clientConn` with `MaxConns` capacity.
3. **Daemon mode:** `cmd/mikyu` loads config, creates an `engine.Manager`, creates a `server.Server`, and listens on a target (TCP or unix socket). Incoming connections are handled in goroutines, each dispatching frames to the engine.

---

## 4. File-by-File Technical Specification

### Root Package (`package mikyu`)

#### `mikyu.go`

Defines public domain models and core SDK interface contracts.

```go
type Message struct {
	Offset    uint64    // Logical offset in the queue (monotonically increasing)
	ID        string    // UUID v7 string representation of the record ID
	Timestamp time.Time // Creation timestamp (stored as nanoseconds in the log)
	Payload   []byte    // Raw user payload
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
}
```

**Interfaces:**

```go
type Client interface {
	io.Closer
	CreateQueue(ctx context.Context, name string, opts ...QueueOption) (Queue, error)
	Queue(name string) (Queue, error)
	ListQueues(ctx context.Context) ([]string, error)
	DeleteQueue(ctx context.Context, name string) error
	Stats(ctx context.Context) (SystemStats, error)
}

type Queue interface {
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

type Group interface {
	io.Closer
	Name() string
	Mark(offset uint64)
	Commit(ctx context.Context) error
	FetchOffset(ctx context.Context) (uint64, error)
}
```

**Implementation Notes:**
- `Client` and `Queue` interfaces are the single source of truth for the SDK. All implementations (embedded and remote) must strictly adhere to these contracts.
- `iter.Seq2[Message, error]` is used for streaming to allow lazy evaluation and early termination by the caller. The iterator functions must handle context cancellation and return `ctx.Err()` when done.
- `Group` is designed for consumer offset tracking. `Mark` is a non-blocking update to a local high-water mark; `Commit` persists it.

#### `options.go`

Configures client connection pools and queue properties via functional options.

```go
type ClientConfig struct {
	BatchSize              int
	Interval               time.Duration
	MaxConns               int
	DefaultMaxSegmentBytes int64
	DefaultMaxMessageBytes int64
	DefaultMaxSegmentAge   time.Duration
	Encrypted              bool
	TLSConfig              *tls.Config
}

type QueueConfig struct {
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
	Encrypted       bool
}
```

**Functional Options Pattern:**
- `ClientOption` is `func(*ClientConfig)` and `QueueOption` is `func(*QueueConfig)`.
- All `With*` functions are pure and return an `Option` closure. They validate their input and silently ignore invalid values (e.g., negative sizes).
- `DefaultClientConfig()` and `DefaultQueueConfig()` provide sensible defaults that are applied before options are processed.

**Implementation Notes:**
- Options are applied in order, so later options override earlier ones.
- This pattern allows the config to grow without breaking existing API calls.
- Use `crypto/tls` for TLS configuration; it is optional.

#### `typed.go`

Provides generic type marshaling wrappers over raw byte queues.

```go
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

type Typed[T any] struct { queue Queue; codec Codec[T] }

func NewTyped[T any](q Queue, codec Codec[T]) *Typed[T]
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error)
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error)
```

**Implementation Notes:**
- `Typed` wraps a raw `Queue` and applies user-defined marshaling/unmarshaling logic.
- This layer is stateless and does not add any locking or concurrency semantics; it simply delegates to the underlying queue.

#### `embedded.go`

Implements `Client`, `Queue`, and `Group` by wrapping `internal/engine` directly.

```go
func Open(dir string, opts ...ClientOption) (Client, error)
```

**Implementation Notes:**
- `Open` creates an `engine.Manager` using the provided `ClientConfig`. It passes `BatchSize`, `Interval`, `MaxSegmentBytes`, `MaxMessageBytes`, and `MaxSegmentAge` to the engine.
- `embeddedClient` holds a reference to the `*engine.Manager`. It has no locks of its own; all thread safety is provided by the engine manager.
- `embeddedQueue` wraps an `*engine.Queue`. It exposes the same methods directly.
- `embeddedGroup` maintains a local `highWater` offset guarded by `sync.Mutex`. `Mark` updates it, `Commit` calls `CommitOffset` on the engine queue.
- All `ctx` parameters are checked at the beginning of each method to respect cancellation.

#### `remote.go`

Implements `Client`, `Queue`, and `Group` over pooled TCP/IPC socket connections.

```go
func Dial(target string, opts ...ClientOption) (Client, error)
```

**Implementation Notes:**
- `remoteClient` maintains a **bounded channel** (`chan *clientConn`) as a connection pool. This is the idiomatic Go way to limit resource usage.
- `acquire(ctx)` tries to fetch a connection from the pool; if the pool is empty, it dials a new one. It respects `ctx` cancellation.
- `release(ch)` puts the connection back into the pool. If the pool is full, it closes the connection. If the client is closed, it also closes.
- `exchange(ctx, op, body)` is the core operation: acquires a connection, sets deadlines (from `ctx`), writes the frame, reads the response, and releases the connection.
- `remoteQueue` and `remoteGroup` are thin wrappers that encode/decode protocol frames.
- **Locking:** `remoteClient` has an unused `mu sync.Mutex` field, reserved for future state (e.g., connection count limits). Currently, the pool channel inherently manages concurrency.
- `remoteGroup` uses a `sync.Mutex` to guard its local `highWater` offset during `Mark` and `Commit`.

---

### Internal Declarative Config Module (`package config`)

#### `internal/config/config.go`

Handles `mikyu.yaml` unmarshaling using `goccy/go-yaml`, directory resolution hierarchies, auto-bootstrapping, and environment variable overrides.

```go
type Config struct {
	Server ServerConfig  `yaml:"server"`
	Engine EngineConfig  `yaml:"engine"`
	Queues []QueueConfig `yaml:"queues"`
}
```

**Implementation Notes:**
- `Load(flagPath)` resolves the config path in the following order:
  1. Explicit `flagPath` (if provided and exists).
  2. `./mikyu.yaml` in the current directory.
  3. `$XDG_CONFIG_HOME/mikyu/mikyu.yaml` (or `os.UserConfigDir()/mikyu/mikyu.yaml`).
  4. If none exist, bootstrap a default config file at the user config path (or current directory as a fallback).
- `applyEnvOverrides(&cfg)` reads environment variables prefixed with `MIKYU_` (e.g., `MIKYU_SERVER_TARGET`). It uses `strconv` and `time.ParseDuration` for numeric and duration fields.
- `ToEngineConfig` and `ToQueueEngineConfig` convert the declarative config into `engine.Config`, applying per-queue overrides for properties like `max_segment_bytes`.
- **Thread Safety:** This module is only used at startup (in the daemon), so it is single-threaded. It uses no shared mutable state.

---

### Internal Storage Engine (`package engine`)

#### `index.go`

Defines the in-memory index entry and the on-disk raw index entry format.

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

**Implementation Notes:**
- `indexEntry` maps a logical offset to a specific segment and its byte position and length within that segment.
- The `rawIndexEntry` is the binary format persisted in segment `.idx` files: two 8-byte big-endian integers (position and length).
- The index is held in memory (a `[]indexEntry` slice per queue), allowing O(1) lookup by offset.

#### `offsets.go`

Persists consumer group high-water marks across all queues into a single binary file (`manager-offsets.dat`).

```go
type OffsetStore struct {
	mu      sync.RWMutex
	file    *os.File
	w       *bufio.Writer
	offsets map[string]uint64
}

func openOffsetStore(dir string) (*OffsetStore, error)
func (s *OffsetStore) Commit(queueName, group string, offset uint64) error
func (s *OffsetStore) Fetch(queueName, group string) uint64
func (s *OffsetStore) Close() error
```

**Implementation Notes:**
- `openOffsetStore` opens the file with `O_RDWR|O_CREATE`, creates a `bufio.Writer`, and calls `recover()` to load existing offsets.
- `recover()` reads the file sequentially: [2-byte key length][key bytes][8-byte offset]. On a truncated tail (partial record), it stops loading and treats it as the end of valid data.
- `Commit()` takes a **write lock** (`Lock`), updates the in-memory map, writes the key and offset to the buffered writer, flushes, and syncs to disk (`file.Sync()`). This ensures durability.
- `Fetch()` takes a **read lock** (`RLock`) and returns the offset, defaulting to 0 if absent.
- **Concurrency:** `Commit` and `Fetch` are fully thread-safe. Because `file.Sync()` is expensive, `Commit` is intentionally synchronous to guarantee durability before return.

#### `segment.go`

Manages append-only active log files and closed read-only `mmap` regions.

```go
type segment struct {
	baseOff   uint64
	logFile   *os.File
	idxFile   *os.File
	pos       int64
	createdAt time.Time
	mmapData  mmap.MMap
}

func openSegment(dir string, baseOff uint64) (*segment, error)
func (s *segment) enableMMap() error
func (s *segment) writeIndex(pos, len int64) error
func (s *segment) recover() ([]rawIndexEntry, error)
func (s *segment) rebuild() ([]rawIndexEntry, error)
func (s *segment) Close() error
```

**Implementation Notes:**
- `openSegment` creates (or opens) the `.log` and `.idx` files for a given base offset.
- `enableMMap` maps the entire log file into memory using `mmap.Map` with `mmap.RDONLY`. This is only called on closed segments to enable zero-copy reads.
- `writeIndex` appends a 16-byte entry (pos, len) to the `.idx` file.
- `recover` reads the `.idx` file and validates it against the `.log` file size. If the last index entry's `pos + len` equals the log size, the index is trusted and loaded in bulk. Otherwise, it falls back to `rebuild`.
- `rebuild` scans the `.log` file sequentially, attempting to decode each record (using `decodeRecord` which checks CRC32). If it hits a torn write or corrupt record, it truncates the log and index at that point and returns the recovered entries.
- **Concurrency:** `segment` is not designed to be thread-safe on its own. All access to a segment's `pos` and `mmapData` is guarded by the owning `Queue`'s `mu`. `enableMMap` is safe to call multiple times (no-op if already mapped).

#### `wal.go`

Executes write-ahead log flushing with dynamic dual-threshold batching.

```go
type wal struct {
	cfg       Config
	file      *os.File
	mu        sync.Mutex
	cond      *sync.Cond
	buf       []byte
	pending   []pendingEntry
	pushed    int64
	published int64
	err       error
	wake      chan struct{}
	shutdown  chan struct{}
	done      chan struct{}
}
```

**Implementation Notes:**
- `newWAL` initializes the WAL with a copy of `Config` (with defaults applied), a `published` offset of `startOffset`, and a buffered `wake` channel.
- `start()` launches the `syncLoop` goroutine.
- `syncLoop` runs a `for` loop with a `select` on `ticker.C`, `wake`, and `shutdown`. On `shutdown`, it performs a final `flush()` and exits.
- `push(rec, pos)` and `pushBatch(recs, positions)` acquire the lock, append records and pending entries, update `pushed`, and return a sequence number and a `wake` flag (true when the buffer is empty or the batch threshold is reached).
- `wait(seq)` acquires the lock and blocks on `cond` until `published > seq` or `err` is set. This is the critical "durable-before-ack" mechanism.
- `flush()` is the core I/O routine. It **locks**, swaps the `buf` and `pending` with nil (to minimize lock hold time during I/O), **unlocks**, performs `WriteAt` and `Sync`, then **relocks** to update `published` and `err`, and broadcasts on `cond`.
- `stop()` closes the `shutdown` channel, waits for `done`, and returns any persistent error.
- **Concurrency:** The mutex is held only for in-memory operations. Disk I/O happens outside the lock. This allows producers to continue adding records while the flusher is writing to disk. The `wake` channel prevents busy-waiting.

#### `queue.go`

Implements per-queue record appends, offset indexing, zero-copy reads, and segment rolling.

```go
type Config struct {
	BatchSize       int
	Interval        time.Duration
	MaxSegmentBytes int64
	MaxMessageBytes int64
	MaxSegmentAge   time.Duration
	Encrypted       bool
}

type Queue struct {
	name       string
	dir        string
	cfg        Config
	active     *segment
	segments   []*segment
	index      []indexEntry
	nextOffset uint64
	w          *wal
	offsets    *OffsetStore
	mu         sync.RWMutex
}
```

**Implementation Notes:**
- `openQueue` creates the directory, optionally enables encryption, loads existing segments via `loadSegments`, and starts the WAL.
- `loadSegments` scans for `.log` files, extracts base offsets, sorts them, opens each segment, recovers its index, and loads entries. The last segment is set as `active`. It then creates a new `wal` for the active segment and starts it.
- `roll()` stops the current WAL, enables `mmap` on the old active segment, opens a new segment with the next offset, and starts a new WAL.
- **`Produce(ctx, payload)`:**
  1. Check `ctx.Err()`.
  2. Validate payload size against `MaxMessageBytes`.
  3. Acquire `q.mu.Lock()`.
  4. Check if the active segment is full (based on `MaxSegmentBytes`) or too old (`MaxSegmentAge`); if so, call `roll()`.
  5. Append the encoded record to the WAL buffer via `w.push(rec, pos)`, update `active.pos`, and increment `nextOffset`.
  6. Release `q.mu.Unlock()`.
  7. If `wake` is true, signal `w.wake`.
  8. Block on `w.wait(seq)` until the record is durably synced.
  9. Acquire `q.mu.Lock()` again to write the index entry.
  10. Release lock and return the offset.
- **`ProduceBatch(ctx, payloads)`:**
  - Similar to `Produce` but iterates over all payloads in one lock hold, pushing them all to the WAL at once. Returns a slice of offsets.
- **`Read(ctx, offset)`:**
  1. Check `ctx.Err()`.
  2. Acquire `q.mu.RLock()` to get the index entry.
  3. Release `q.mu.RUnlock()`.
  4. If the segment is `mmap`ed, return a sub-slice of the mapped data (zero copy). Otherwise, allocate a buffer and `ReadAt` from the log file.
  5. Decode the record and return a `Message`.
- **`ReadBatch`:** Calls `Read` in a loop until `count` messages are read or an error occurs.
- **`Close`:** Stops the WAL and closes all segments. It should be called when no other operations are in flight (e.g., before deleting the queue or shutting down the manager).
- **Concurrency & Locking Summary:**
  - `q.mu` protects `segments`, `index`, `nextOffset`, and `active`.
  - `Produce` uses a two-phase lock: first to reserve and write to WAL, then to update the index. This ensures the index is only updated after the record is durably persisted.
  - `Read` uses a short read lock to lookup the index entry, then reads from `mmap`/file without holding the lock. This is safe because segments are immutable once rolled.
  - `LastOffset()` acquires a read lock and returns `nextOffset`.

#### `encode.go`

Encodes/decodes records using raw 16-byte `uuid.UUID` values and calculates IEEE CRC32 checksums.

* **Binary Record Format:**
  `[4B Length][4B CRC32 Checksum][2B ID Len (16)][16B Raw UUID v7][8B Timestamp Nano][M-B Payload]`

**Implementation Notes:**
- `encodeRecord(id, ts, payload)` constructs the byte slice. It uses a `sync.Pool` of `*bytes.Buffer` to avoid allocations on the hot path. The buffer is reset and reused.
- The `Length` field is the body length (everything except the length field itself). The CRC is computed over the body (from ID length to payload).
- `decodeRecord(buf)` takes the body (excluding the 4-byte length field), checks the CRC, extracts the raw UUID, timestamp (nanoseconds), and payload.
- This module has no locks; it is pure and safe for concurrent use.

#### `encrypt_linux.go`, `encrypt_windows.go`, `encrypt_other.go`

Provides OS-specific transparent kernel directory encryption.

**Implementation Notes:**
- `EnsureDirectoryEncrypted(dirPath) error`:
  - **Linux:** Uses `unix.Syscall(FS_IOC_SET_ENCRYPTION_POLICY)` with an `fscryptPolicy` struct. If the filesystem does not support it (EOPNOTSUPP), it returns nil (best-effort).
  - **Windows:** Calls `EncryptFileW` from `advapi32.dll` to apply EFS attributes.
  - **Other (macOS/BSD):** Logs a warning and returns nil. This is a no-op fallback.
- **Concurrency:** This is a startup operation. It is called during `OpenManager` or `openQueue` before any other concurrent access. It is not thread-safe but is not meant to be called concurrently.

#### `manager.go`

Handles multi-queue engine state, queue deletion, and system storage metrics.

```go
type Manager struct {
	mu      sync.RWMutex
	dir     string
	cfg     Config
	queues  map[string]*Queue
	offsets *OffsetStore
}
```

**Implementation Notes:**
- `OpenManager(dir, cfg)` creates the directory, optionally encrypts it, opens the `OffsetStore`, scans the directory for subdirectories (each representing a queue), and opens each queue. It returns an error if any queue fails to open.
- `CreateQueue(name, opts)`:
  1. Acquire `m.mu.Lock()`.
  2. Check if `name` exists in the map or as a directory on disk. If so, return `ErrQueueExists`.
  3. Call `openQueue` with the provided config (or manager defaults).
  4. Add the queue to the map.
  5. Release lock and return the queue.
- `GetQueue(name)`:
  1. Acquire `m.mu.RLock()`.
  2. Look up the queue in the map. If not found, return `ErrQueueNotFound`.
- `DeleteQueue(name)`:
  1. Acquire `m.mu.Lock()`.
  2. Get the queue from the map.
  3. Call `q.Close()` to stop its WAL and close segments.
  4. Delete the queue from the map.
  5. Remove the queue directory from disk (`os.RemoveAll`).
- `Stats()`:
  1. Acquire `m.mu.RLock()`.
  2. For each queue, acquire the queue's own `RWMutex` to read `len(index)` and segment info.
  3. Aggregate and return.
- `Close()`:
  1. Acquire `m.mu.Lock()`.
  2. Close all queues.
  3. Close the `OffsetStore`.
- **Concurrency:** All public methods are thread-safe. The manager's lock is separate from the queues' locks to prevent deadlocks (e.g., `DeleteQueue` holds the manager lock while calling `q.Close()`, which acquires the queue lock).

---

### Internal Protocol Module (`package protocol`)

#### `opcodes.go`

Defines wire opcodes, error formats, and target URI parsing.

* **Opcodes:** `OpCreateQueue` (`0x01`), `OpProduce` (`0x02`), `OpRead` (`0x03`), `OpTail` (`0x04`), `OpCommitOffset` (`0x05`), `OpFetchOffset` (`0x06`), `OpProduceBatch` (`0x07`), `OpReadBatch` (`0x08`), `OpListQueues` (`0x09`), `OpDeleteQueue` (`0x0A`), `OpStats` (`0x0B`), `OpShutdown` (`0x0C`), `OpError` (`0xFF`).
* **Error codes:** `ErrCodeUnknown` (`0x01`), `ErrCodeQueueNotFound` (`0x02`), `ErrCodeQueueExists` (`0x03`), `ErrCodeOffsetNotFound` (`0x04`).

**Implementation Notes:**
- `ParseTarget(target)` parses URIs like `tcp://host:port`, `unix:///path`, `pipe:///path`. If no scheme is present, it assumes `tcp://`.
- `EncodeError` and `DecodeError` format error responses as `[1-byte error code][message bytes]`.

#### `frames.go`

Manages 5-byte length-prefixed frame encoding/decoding with zero-allocation `sync.Pool` buffers.

```go
func ReadFrame(r *bufio.Reader) (op byte, payload []byte, err error)
func WriteFrame(w io.Writer, op byte, payload []byte) error
```

**Implementation Notes:**
- Frame layout: `[4-byte big-endian length][1-byte opcode][payload]`. The length includes the opcode and payload but not the length field itself.
- `WriteFrame` uses a `sync.Pool` of `*[]byte` to avoid allocation. It writes directly to the underlying `io.Writer`.
- `ReadFrame` reads the length prefix, then allocates a buffer for the frame body. This allocation is unavoidable for incoming frames.

#### `bytes.go`

Provides bounds-checked binary reader and writer buffer abstractions.

**Implementation Notes:**
- `Reader` wraps a byte slice and provides sequential reads with bounds checking. It returns `ErrTruncatedBody` if the request exceeds the remaining bytes.
- `Buffer` is a zero-allocation builder for constructing frames. It uses `append` internally.

---

### Internal Server & IPC Modules (`package server` / `package ipc`)

#### `internal/server/server.go`

Accepts connections and handles graceful shutdown when receiving `OpShutdown` or signal events.

```go
type Server struct {
	mgr       *engine.Manager
	listener  net.Listener
	tlsConfig *tls.Config
	mu        sync.Mutex
	stopCh    chan struct{}
}
```

**Implementation Notes:**
- `New(mgr, opts...)` creates a server. `WithTLS` sets the TLS config.
- `ListenAndServe(target)`:
  1. Parses the target.
  2. Creates a listener using `ipc.Listen` for unix/pipe or `net.Listen` for tcp.
  3. If TCP and TLS is configured, wraps the listener with `tls.NewListener`.
  4. Stores the listener, calls `Serve`.
- `Serve(ln)`:
  - Accepts connections in a loop. For each connection, spawns `go s.serveConn(conn)`.
  - If the listener is closed and `stopCh` is closed, it returns `nil` (graceful stop). Otherwise, it returns the error.
- `Stop()`:
  - Closes `stopCh` once and closes the listener. Idempotent via `select` on `stopCh`.
- `serveConn(conn)`:
  - Creates a `bufio.Reader`.
  - Loops: `ReadFrame`, `dispatch`, `WriteFrame`.
  - If `dispatch` returns `shouldShutdown=true`, it triggers `mgr.Close()` and `Stop()` in a goroutine and returns.
- **Concurrency:** `Server.mu` protects `listener` and `stopCh`. Each connection is handled independently.

#### `internal/server/handlers.go`

Dispatches binary frames to storage engine calls and triggers `OpShutdown` stops.

**Implementation Notes:**
- `dispatch(ctx, op, body)` is a pure dispatch function. It maps each opcode to a `handle*` method.
- Each `handle*` method:
  1. Parses the body with `protocol.Reader`.
  2. Retrieves the queue from the manager (if required).
  3. Calls the appropriate engine method.
  4. Encodes the response with `protocol.Buffer`.
  5. Returns `(opcode, responseBody)`.
- Errors are encoded with `protocol.EncodeError` and returned with `OpError`.
- **Note:** `handleTail` only returns `LastOffset`. Actual tail streaming is done client-side by repeatedly calling `Read`.

#### `internal/server/tls.go`

Generates TLS 1.3 server configurations with auto-generated self-signed certificate fallback.

**Implementation Notes:**
- `DefaultTLSConfig(certFile, keyFile)` loads the provided certificate and key, or generates a self-signed ECDSA P256 certificate valid for 365 days for `localhost` and `127.0.0.1`.
- The returned `tls.Config` enforces `MinVersion: tls.VersionTLS13`.

#### `internal/ipc/ipc.go`

Manages Unix Domain Socket and Windows Named Pipe listeners and dialers with stale socket unlinking.

**Implementation Notes:**
- `Listen(target)` parses the target. If it's `unix` or `pipe`, it calls `listenUnix` or `listenPipe`.
- `listenUnix(path)`:
  - Creates the parent directory if needed.
  - Removes a stale socket file (if present).
  - Calls `net.Listen("unix", path)`.
  - Chmods the socket to `0600` for security.
- `listenPipe` on Windows maps to `net.Listen("unix", \\.\pipe\...)`, otherwise falls back to `listenUnix`.
- `Dial` and `DialContext` connect to the target.
- `Listener.Close()` closes the listener and removes the socket file from disk.

---

### Executable Application (`cmd/mikyu`)

#### `cmd/mikyu/main.go`

Unified subcommand router and entrypoint for bare execution, server daemon, REPL, TUI, and CLI commands.

**Implementation Notes:**
- `main()` parses global flags (`-target`, `-config`).
- If `os.Args` is empty (bare execution), it calls `RunREPL`, which auto-spawns the daemon.
- For `daemon`/`server` subcommand, it calls `runDaemonServer`.
- `runDaemonServer`:
  1. Loads config.
  2. Creates `engine.Manager`.
  3. Reconciles queues from `cfg.Queues` (creates them if they don't exist).
  4. Creates a `server.Server`.
  5. Handles OS signals (SIGINT/SIGTERM) by closing the manager and stopping the server.
  6. Calls `srv.ListenAndServe`.

#### `cmd/mikyu/repl.go`

Interactive REPL prompt session handling command evaluation.

**Implementation Notes:**
- `RunREPL(target)`:
  1. Calls `EnsureDaemonRunning`.
  2. Dials the daemon.
  3. Prints banner.
  4. Reads input via `bufio.Scanner`.
  5. Parses the line with `parseREPLArgs` (respects double quotes).
  6. Dispatches commands: `list`, `create`, `delete`, `produce`, `read`, `tail`, `stats`, `shutdown`, `help`, `exit`.
- `parseREPLArgs` is a custom parser that handles quoted strings. It is covered by unit tests.
- `confirm(prompt)` uses `huh` to create a themed confirmation prompt.
- `sendShutdownSignal(target)` dials the daemon directly and writes an `OpShutdown` frame.

#### `cmd/mikyu/cli.go`

One-shot stateless CLI subcommand executor.

**Implementation Notes:**
- `RunCLI(target, args)` handles a single command.
- It ensures the daemon is running, dials, and executes the command.
- `resolvePayload(input)` supports raw strings, `@file` (read file), and `-` (read stdin).
- `confirmCLI` checks for `-y`/`--yes` flags or prompts on TTY.

#### `cmd/mikyu/daemon.go`

Probes socket readiness and auto-spawns background daemon instances.

**Implementation Notes:**
- `IsDaemonAlive(target)` attempts a TCP/Unix dial with a 150ms timeout. If successful, the daemon is alive.
- `EnsureDaemonRunning(target)`:
  1. Checks if the daemon is alive.
  2. If not, gets the executable path.
  3. Spawns a detached process (`exec.Command(execPath, "daemon")`) with `setDetachedProcessAttr`.
  4. Polls `IsDaemonAlive` every 50ms until it responds or a 3-second timeout elapses.

#### `cmd/mikyu/banner.go`

Renders the Lipgloss-styled ASCII banner, command reference table, and connection status.

**Implementation Notes:**
- `PrintREPLBanner(target)` uses `lipgloss` to render a gradient logo, a command reference table, and connection status. It is purely for aesthetics.

#### `cmd/mikyu/detach_unix.go` & `detach_windows.go`

Platform-specific process detachment using Go build tags.

**Implementation Notes:**
- **Unix:** Sets `cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}` to detach from the terminal.
- **Windows:** Sets `cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000}` (CREATE_NO_WINDOW).

---

## 5. Cross-Cutting Concerns & Best Practices

This section is a guide for implementing any new feature or modifying existing code. It should be treated as an internal style guide and should be followed strictly.

### 5.1 Concurrency & Locking Model

- **Never hold a lock across I/O.** The `wal.flush()` and `queue.Produce()` methods exemplify this: they lock only for in-memory state changes, release the lock, perform I/O, then re-lock. Always follow this pattern.
- **Use `sync.RWMutex` for read-heavy structures.** The `Manager` and `Queue` use `RWMutex` because reads are much more frequent than writes.
- **Ensure goroutine lifecycles.** Every goroutine you spawn must have a clear exit. Use `context.Context` for cancellation. The WAL flusher uses a `shutdown` channel.
- **Default to unbuffered channels.** Only use buffered channels when there is a measured need. The connection pool uses a buffered channel with `MaxConns` capacity; this is justified.
- **Always include `ctx.Done()` in `select` statements.** This prevents goroutine leaks.
- **Only the sender closes a channel.** In the WAL, the `shutdown` channel is closed by `stop()`, which is the owner.
- **Use `sync.Once` for one-time initialization.** Good candidates are lazy connection pools or expensive resource creation.

### 5.2 Performance & Allocation Optimization

- **Profile before optimizing.** Use `go test -bench`, `pprof`, and `fgprof` to identify actual bottlenecks. Avoid premature optimization.
- **Reduce allocations on hot paths.** Use `sync.Pool` for `bytes.Buffer` (in `encode.go`) and frame buffers (in `frames.go`). Reuse slices with `append(s[:0], ...)`.
- **Use zero-copy reads.** Historical segments are `mmap`ed. Never copy data from `mmap` unless necessary.
- **Avoid repeated map lookups.** Use `for k, v := range map` instead of `map[k]` inside the loop.
- **Use `slices` and `maps` packages.** These are Go 1.21+ and provide efficient operations. (e.g., `slices.Sort`, `slices.Contains`).
- **Compile regexp once.** There are no regexes in the codebase, but if you add any, they MUST be at package level.
- **Limit everything.** Connection pools have `MaxConns`. Buffers have size limits. Never create unbounded goroutines.

### 5.3 Error Handling & Validation

- **Validate at boundaries.** Validate input in handlers (CLI, REPL, protocol) before passing to domain logic. Don't re-validate internally.
- **Use sentinel errors.** `engine.ErrQueueExists`, `engine.ErrOffsetNotFound`, etc., are sentinel errors. Check with `errors.Is`.
- **Wrap errors with context.** Use `fmt.Errorf("...: %w", err)` to preserve the error chain.
- **Panic only for bugs.** Nil pointer dereferences or violated invariants may panic. Expected failures (missing file, invalid input) must return errors.
- **Make illegal states unrepresentable.** Use custom types (e.g., `Message`, `QueueStats`) to ensure data integrity.

### 5.4 Design Patterns & Code Style

- **Use functional options for constructors.** This is the preferred pattern for `Open`, `Dial`, `NewServer`, etc. It allows the config to grow without breaking API.
- **Use interfaces for decoupling.** `Client`, `Queue`, and `Group` define the public API. Implementations are behind these interfaces.
- **Avoid `init()`.** Do not use `init()` for resource initialization (e.g., database connections). Use explicit constructors (`OpenManager`, `New`, etc.).
- **Start enums at 1 or use `Unknown` at 0.** This prevents the zero value from being a valid enum member. Currently, engine config uses default values; if you introduce a status enum, follow this rule.
- **Use `any` instead of `interface{}`.** Go 1.18+.
- **Use early returns.** Keep the happy path flat and handle errors first.
- **Use `strings.Builder` for concatenation.** This is not currently used, but it is a best practice for building strings in loops.

### 5.5 Resource Management & Lifecycle

- **`defer Close()` immediately after opening.** Apply this to files, connections, and any resource that implements `Close`.
- **Use `runtime.AddCleanup` over `runtime.SetFinalizer`.** (Go 1.24+). Avoid finalizers if possible; use explicit `Close` methods.
- **Graceful shutdown.** Use `signal.NotifyContext` for daemon shutdown, and `context.WithTimeout` for draining. The daemon currently uses `signal.Notify` with a channel; consider migrating to `signal.NotifyContext` for cleaner code.
- **Resource pools.** The connection pool uses a buffered channel. This is the correct pattern. When the pool is full, discard the resource.

### 5.6 Testing & Verification

- **Use `go.uber.org/goleak` in `TestMain`.** The project already does this; it catches goroutine leaks.
- **Use `go test -race ./...`** in CI to catch data races.
- **Use benchmarks for hot paths.** The WAL and queue operations should have benchmarks to track performance.
- **Use `testing/synctest` for concurrency tests** (Go 1.25+). This is the modern way to test concurrent code deterministically.
- **Use `b.Loop()` in benchmarks** (Go 1.24+).
- **Use `t.Context()` for test contexts** (Go 1.24+).

### 5.7 Adding New Features: A Checklist

When adding a new feature (e.g., a new opcode, a new queue feature), follow these steps:

1. **Design the API:** Add the method to the appropriate interface (`Client`, `Queue`, `Group`). This ensures both embedded and remote modes are supported.
2. **Add a protocol opcode** if needed.
3. **Implement the engine logic** in `internal/engine`.
4. **Implement the remote encoding/decoding** in `remote.go`.
5. **Implement the server handler** in `internal/server/handlers.go`.
6. **Add CLI/REPL commands** if user-facing.
7. **Add tests** for the new feature in both embedded and remote modes.
8. **Document** the new feature in this spec.

### 5.8 Code Examples & Patterns

#### Example: Adding a new `Ping` command

1. Add `OpPing = 0x0D` to `opcodes.go`.
2. Implement `handlePing` in `handlers.go` that returns the current time or a status string.
3. Add `Ping(ctx) (time.Time, error)` to `Client` interface.
4. Implement `Ping` in `embedded.go` and `remote.go`.
5. Add `ping` command to `repl.go` and `cli.go`.

#### Example: Adding a new queue feature (e.g., TTL)

1. Add a `TTL time.Duration` field to `engine.Config`.
2. Add a `WithTTL` option to `options.go`.
3. Implement TTL logic in `queue.go` (e.g., during `Read`, check timestamp and skip expired messages).
4. Expose TTL through the config file (`config.go`).
5. Add a `mikyu.yaml` field for TTL.
