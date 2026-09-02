# Mikyu Technical Specification & Codebase Architecture

## 1. Project Overview

**mikyu** is a high-performance, log-structured message queue built from scratch in Go. It provides a unified API surface operating either as an in-process embedded log engine or as a standalone network/IPC daemon.

### Core Architectural Guarantees & Technical Principles

* **Durable-Before-Ack Semantics:** Every `Produce()` or `ProduceBatch()` invocation blocks until the payload byte slice and index metadata are durably synced to disk via the Write-Ahead Log (`wal`) flusher. This guarantees that once an offset is returned, the message is recoverable after a crash.
* **Zero-Copy Reads:** Closed historical log segments are memory-mapped (`mmap`) into the process address space. Read requests on historical offsets return direct sub-slices of mapped memory buffers without heap allocations or system call overhead. Active segments are read via `ReadAt` syscalls on the open file descriptor.
* **Dynamic Dual-Threshold WAL Flushing:** The WAL flusher triggers a disk sync (`syncData`) when either pending record count reaches `BatchSize` or elapsed time exceeds `Interval`, balancing single-thread latency with high-concurrency parallel throughput. The flusher runs in a dedicated goroutine, swapping buffer pointers to minimize lock hold time; producers never block on disk I/O directly and wait on a `sync.Cond` for the flusher to publish their sequence.
* **Raw 16-Byte UUID v7 Encoding:** Messages are encoded using raw 16-byte Go standard library `uuid.UUID` arrays (`uuid.NewV7()`), avoiding string formatting allocations and reducing log file overhead by 20 bytes per record.
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
├── mikyu.go            # Top-level domain models & interfaces: Client, Queue, Group, Message, SystemStats
├── options.go          # Functional configuration options for Client and Queue
├── typed.go            # Generic Typed[T] queue wrapper & Codec[T] interface
├── embedded.go         # In-process embedded driver implementing Client, Queue, Group
├── remote.go           # Connection-pooled TCP/IPC network driver implementing Client, Queue, Group
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
│   │   ├── sync_darwin.go      # macOS F_FULLFSYNC drive cache flusher
│   │   ├── sync_fdatasync.go   # POSIX unix.Fdatasync implementation
│   │   ├── sync_windows.go     # Windows file.Sync implementation
│   │   ├── encrypt_linux.go    # Linux fscrypt kernel ioctl directory encryption
│   │   ├── encrypt_windows.go  # Windows NTFS EFS directory encryption
│   │   └── encrypt_other.go    # Fallback directory encryption for unsupported OSes
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
│   └── ipc/            # Specialized local IPC listeners and dialers
│       └── ipc.go          # Unix Domain Socket and Windows Named Pipe helpers
│
└── cmd/
    └── mikyu/            # Unified single-binary entrypoint
        ├── main.go           # Subcommand router (daemon, shell, tui, CLI)
        ├── daemon.go         # Socket prober & background process spawner
        ├── repl.go           # Interactive REPL session shell & command loop
        ├── cli.go            # Stateless one-shot CLI handler (@file, stdin support)
        ├── banner.go         # Lipgloss-styled ASCII banner & command table
        ├── detach_unix.go    # POSIX process detachment (//go:build !windows)
        ├── detach_windows.go # Windows process detachment (//go:build windows)
        ├── vt_other.go       # Virtual terminal setup stub (//go:build !windows)
        └── vt_windows.go     # Windows ANSI escape sequence enabler (//go:build windows)

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

1. **Embedded mode:** `mikyu.Open(dir, opts...)` creates an `engine.Manager` directly in-process. Client calls delegate to engine queues without network/IPC overhead.
2. **Remote mode:** `mikyu.Dial(target, opts...)` initializes a `remoteClient` managing a thread-safe connection pool (`chan *clientConn`). Each client operation acquires a connection, writes length-prefixed protocol frames, awaits the response frame, and returns the connection to the pool.
3. **Daemon mode:** `cmd/mikyu` loads `mikyu.yaml` configuration, instantiates `engine.Manager`, creates `server.Server`, and listens on target endpoints (TCP, Unix domain socket, or Windows named pipe).

---

## 4. File-by-File Technical Specification

### Root Package (`package mikyu`)

#### `mikyu.go`

Defines public domain models and primary client/queue interfaces.

```go
type Message struct {
	Offset    uint64    // Monotonically increasing logical record offset
	ID        string    // UUID v7 string representation
	Timestamp time.Time // Creation timestamp (nanosecond accuracy)
	Payload   []byte    // Raw user payload slice
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

**Implementation Details:**

* `Stream`: Returns an `iter.Seq2[Message, error]` iterator. Loops from `startOffset`, calling `Read()`. Terminates when `Read()` returns `ErrOffsetNotFound` or when context is canceled.
* `Tail`: Returns an `iter.Seq2[Message, error]` iterator. Polls `LastOffset()` continuously; if `curr >= last`, sleeps for 5 milliseconds before re-checking. Yields records as they become available.
* `Group`: Tracks consumer high-water marks. `Mark` updates an internal `highWater` variable if `offset >= highWater`. `Commit` persists `highWater` to the engine offset store. `Close` automatically invokes `Commit(context.Background())`.

#### `options.go`

Configures client connection pools and per-queue limits via functional options.

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

func DefaultClientConfig() ClientConfig
func WithBatchSize(n int) ClientOption
func WithFlushInterval(d time.Duration) ClientOption
func WithMaxConns(n int) ClientOption
func WithClientDirectoryEncryption(enabled bool) ClientOption
func WithClientTLS(tlsConfig *tls.Config) ClientOption
func WithMaxSegmentBytes(bytes int64) QueueOption
func WithMaxMessageBytes(bytes int64) QueueOption
func WithMaxSegmentAge(age time.Duration) QueueOption
func WithDirectoryEncryption(enabled bool) QueueOption

```

**Implementation Details:**

* `DefaultClientConfig`: Defaults to `BatchSize: 256`, `Interval: 10ms`, `MaxConns: 10`, `DefaultMaxSegmentBytes: 64 MiB`, `DefaultMaxMessageBytes: 1 MiB`, `DefaultMaxSegmentAge: 1h`, `Encrypted: false`.
* Option functions validate input parameters, ignoring non-positive values for integer/duration limits.

#### `typed.go`

Provides generic object serialization helpers over byte queues.

```go
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

type Typed[T any] struct {
	queue Queue
	codec Codec[T]
}

func NewTyped[T any](q Queue, codec Codec[T]) *Typed[T]
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error)
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error)

```

**Implementation Details:**

* `Produce`: Encodes `val` into bytes using `codec.Marshal` and delegates directly to `queue.Produce`.
* `Read`: Calls `queue.Read`, unmarshals `msg.Payload` into a zero-initialized value `T` using `codec.Unmarshal`, and returns it.

#### `embedded.go`

Directly wraps `internal/engine.Manager` for in-process log operations. Re-exports core error variables (`ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`, `ErrMessageTooLarge`).

```go
func Open(dir string, opts ...ClientOption) (Client, error)

```

**Implementation Details:**

* `Open`: Applies `ClientOption`s to `DefaultClientConfig()`, creates an `engine.Config`, and calls `engine.OpenManager(dir, engineCfg)`. Returns an `*embeddedClient`.
* `embeddedClient`: Implements `Client`. Methods validate `ctx.Err()` prior to execution and forward calls to `mgr`.
* `embeddedQueue`: Wraps `*engine.Queue`. Methods delegate directly to engine queue primitives.
* `embeddedGroup`: Wraps `*engine.Queue` and tracks high-water offset guarded by `sync.Mutex`. `Commit` invokes `q.CommitOffset(ctx, groupName, highWater)`.

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

func Dial(target string, opts ...ClientOption) (Client, error)

```

**Implementation Details:**

* `Dial`: Parses `target` using `protocol.ParseTarget`. Allocates a buffered channel `pool` with capacity `cfg.MaxConns`.
* `acquire(ctx)`: Selects a `*clientConn` from `pool`. If pool is empty, executes `dialConn(ctx)`. Respects context timeout/cancellation.
* `dialConn(ctx)`: Dials IPC target via `ipc.Dial(target)` if network is `unix` or `pipe`. For TCP, checks if `TLSConfig` is non-nil or URI starts with `tls://`/`tcps://`; if so, uses `tls.Dialer`, otherwise standard `net.Dialer`.
* `release(ch)`: Returns `ch` to `pool`. If `closed.Load()` is true or `pool` is full, closes underlying `ch.conn`.
* `Close()`: Swaps `closed` atomic boolean to true and drains/closes all idle connections in `pool`.
* `exchange(ctx, op, body)`: Acquires connection, sets write/read timeout deadline from `ctx`, calls `protocol.WriteFrame` and `protocol.ReadFrame`. If response opcode is `OpError`, decodes wire error via `decodeProtocolError` and returns corresponding Go error (`ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`).
* `remoteGroup`: Uses `sync.Mutex` for thread-safe high-water offset tracking; `Commit` writes an `OpCommitOffset` binary frame to the connection.

---

### Internal Declarative Config Module (`package config`)

#### `internal/config/config.go`

Parses YAML configurations using `goccy/go-yaml`, resolves config paths, auto-bootstraps missing config files, and applies environment variable overrides.

```go
type Config struct {
	Server ServerConfig  `yaml:"server"`
	Engine EngineConfig  `yaml:"engine"`
	Queues []QueueConfig `yaml:"queues"`
}

func DefaultConfig() Config
func Load(flagPath string) (*Config, string, error)
func ToEngineConfig(e EngineConfig) engine.Config
func ToQueueEngineConfig(defaults EngineConfig, q QueueConfig) engine.Config

```

**Implementation Details:**

* `defaultConfigYAML`: Hardcoded string template containing default server target (`tcp://127.0.0.1:7420`), data directory (`./data`), engine defaults, and documented inline comments.
* `Load(flagPath)`: Calls `resolveOrBootstrapPath`. If `flagPath` is empty, checks `./mikyu.yaml`, then `$XDG_CONFIG_HOME/mikyu/mikyu.yaml` (or system user config path). If no file exists, creates `mikyu.yaml` with `defaultConfigYAML` at user config path or `./mikyu.yaml`. Unmarshals YAML data into `Config` struct and executes `applyEnvOverrides(&cfg)`.
* `applyEnvOverrides`: Overrides configuration fields if environment variables are present (`MIKYU_SERVER_TARGET`, `MIKYU_SERVER_DATA_DIR`, `MIKYU_ENGINE_BATCH_SIZE`, `MIKYU_ENGINE_FLUSH_INTERVAL`, `MIKYU_ENGINE_MAX_SEGMENT_BYTES`, `MIKYU_ENGINE_MAX_MESSAGE_BYTES`, `MIKYU_ENGINE_MAX_SEGMENT_AGE`, `MIKYU_ENGINE_ENCRYPTED`).

---

### Internal Storage Engine (`package engine`)

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

* `indexEntry`: O(1) in-memory lookup record mapping a logical offset to a target segment, file byte offset (`pos`), and total record length (`len`).
* `rawIndexEntry`: 16-byte on-disk structure stored in `.idx` segment files (8 bytes big-endian `pos`, 8 bytes big-endian `len`).

#### `offsets.go`

Provides durable consumer group offset tracking in a unified manager-level file (`manager-offsets.dat`).

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

**Implementation Details:**

* `openOffsetStore`: Opens `manager-offsets.dat` with `O_RDWR|O_CREATE` permissions. Calls `recover()` to populate the in-memory `offsets` map.
* `recover()`: Iteratively reads `[2-byte Key Length][Key Bytes ("queue:group")][8-byte Uint64 Offset]`. If an `io.EOF` or partial record is encountered, stops reading and retains valid state.
* `Commit`: Takes write lock (`mu.Lock`), updates `offsets["queueName:group"]`, appends entry to `bufio.Writer`, flushes writer, and calls `file.Sync()` to ensure immediate disk durability.
* `Fetch`: Takes read lock (`mu.RLock`), returns offset value from map (0 if not present).

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
}

func openSegment(dir string, baseOff uint64) (*segment, error)
func (s *segment) enableMMap() error
func (s *segment) writeIndex(pos, len int64) error
func (s *segment) recover() ([]rawIndexEntry, error)
func (s *segment) rebuild() ([]rawIndexEntry, error)
func (s *segment) Close() error

```

**Implementation Details:**

* `openSegment`: Opens `%020d.log` and `%020d.idx` files. If log file size is zero, pre-allocates 64 MiB (`logFile.Truncate(64 * 1024 * 1024)`).
* `enableMMap`: Maps log file into memory using `mmap.Map(logFile, mmap.RDONLY, 0)`. Called when a segment is closed after rolling to enable zero-copy read paths.
* `writeIndex`: Encodes `pos` and `len` into 16 big-endian bytes and writes to `idxFile`.
* `recover()`: Performs fast-path verification by reading the final 16-byte record in `idxFile`. If `lastPos + lastLen == logFileSize`, reads the index file in bulk. Otherwise, falls back to `rebuild()`.
* `rebuild()`: Truncates `idxFile` to zero. Scans `logFile` sequentially from `pos = 0`, reading 4-byte record length prefixes and verifying CRC checksums via `decodeRecord`. Reconstructs valid `rawIndexEntry` slices, writes repaired index entries to `idxFile`, truncates `logFile` to the last valid position, and syncs `idxFile`.
* `Close()`: Unmaps `mmapData` if non-nil, then closes `logFile` and `idxFile`.

#### `wal.go` & OS-Specific Sync Helpers

Implements Write-Ahead Log flushing using dynamic dual-threshold batching and platform-optimal disk sync system calls.

```go
type pendingEntry struct {
	pos int64
	len int64
}

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

func newWAL(f *os.File, cfg Config, startOffset int64) *wal
func (w *wal) start()
func (w *wal) syncLoop()
func (w *wal) push(rec []byte, pos int64) (seq int64, wake bool, err error)
func (w *wal) pushBatch(recs [][]byte, positions []int64) (seq int64, wake bool, err error)
func (w *wal) wait(seq int64) error
func (w *wal) flush()
func (w *wal) stop() error

```

**Implementation Details:**

* `newWAL`: Allocates `wal` with pre-allocated buffer capacity (`BatchSize * 1024`), initializes `sync.Cond` linked to `mu`, and creates channels.
* `syncLoop`: Goroutine loop selecting on `ticker.C` (`cfg.Interval`), `wake`, and `shutdown`. Triggers `flush()` on receive.
* `push`: Appends record bytes to `buf` and `pendingEntry{pos, len}` to `pending`. Sets `wake = len(pending) == 1 || len(pending) >= BatchSize`. Returns sequence number `pushed` incremented by 1.
* `pushBatch`: Appends multiple records to `buf` and `pending`. Sets `wake` if batch thresholds are reached.
* `wait(seq)`: Holds `mu`, calls `cond.Wait()` in a loop while `published <= seq && err == nil`. Returns persistent write error if present.
* `flush()`: Locks `mu`. If `buf` is empty or `err` is set, unlocks and returns. Otherwise, **swaps `buf` and `pending` to local variables** (`w.buf = nil`, `w.pending = nil`) and **unlocks `mu**`. Performs `w.file.WriteAt(data, entries[0].pos)` followed by `syncData(w.file)`. Relocks `mu`: if write failed, sets `w.err`; otherwise advances `published += len(entries)`. Signals waiting caller goroutines via `cond.Broadcast()`.
* **Platform Sync Helpers:**
* `sync_darwin.go`: Calls `unix.FcntlInt(f.Fd(), unix.F_FULLFSYNC, 0)`; falls back to `f.Sync()` on failure.
* `sync_fdatasync.go` (Linux/BSD): Executes `unix.Fdatasync(int(f.Fd()))` to flush data blocks without updating directory inode metadata overhead.
* `sync_windows.go`: Executes `f.Sync()`.



#### `queue.go`

Implements per-queue engine mechanics, rolling logic, batch produce, zero-copy reads, and index updates.

```go
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

func openQueue(parentDir, name string, cfg Config, store *OffsetStore) (*Queue, error)
func (q *Queue) Produce(ctx context.Context, payload []byte) (uint64, error)
func (q *Queue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
func (q *Queue) Read(ctx context.Context, offset uint64) (Message, error)
func (q *Queue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error)
func (q *Queue) LastOffset() uint64
func (q *Queue) CommitOffset(ctx context.Context, group string, offset uint64) error
func (q *Queue) FetchOffset(ctx context.Context, group string) (uint64, error)
func (q *Queue) Close() error

```

**Implementation Details:**

* `openQueue`: Invokes `cfg.withDefaults()`, creates queue directory, applies directory encryption if configured, calls `loadSegments()`.
* `loadSegments()`: Identifies `.log` files in directory, extracts numerical base offsets (`%020d.log`), sorts base offsets. Opens each segment via `openSegment`, recovers index entries. Calls `enableMMap()` on all historical closed segments. Sets active segment to the latest segment, initializes WAL at `startOffset = len(q.index)`, and starts WAL flusher goroutine.
* `roll()`: Stops active WAL, enables `mmap` on active segment, opens a new segment starting at `nextOffset`, appends it to `segments`, initializes a new WAL, and starts it.
* `Produce`:
1. Validates payload length against `MaxMessageBytes` (returns `ErrMessageTooLarge` if exceeded).
2. Encodes record with raw UUID v7 and nanosecond timestamp.
3. Acquires `q.mu.Lock()`. Checks segment size limits (`MaxSegmentBytes`) and segment age (`MaxSegmentAge`); triggers `roll()` if thresholds are exceeded.
4. Calls `q.w.push(rec, pos)`, updates `q.active.pos += recLen`, increments `q.nextOffset++`, unlocks `q.mu`.
5. Signals `q.w.wake` if required, then calls `q.w.wait(seq)` to block until durable disk sync completes.
6. Re-acquires `q.mu.Lock()`, writes 16-byte index entry to segment via `writeIndex`, updates `q.index` slice at offset position, unlocks `q.mu`, and returns offset.


* `ProduceBatch`: Performs batched record encoding, checks limits, executes `w.pushBatch`, waits for sync, updates index entries, and returns allocated offset slice.
* `Read`:
1. Acquires `q.mu.RLock()`. Validates offset within `q.index` bounds (returns `ErrOffsetNotFound` if invalid).
2. Retrieves `indexEntry`. Checks if memory-mapped data (`entry.seg.mmapData`) spans the offset region.
3. **Zero-Copy Path:** If `mmapData` is available, extracts byte slice slice `entry.seg.mmapData[pos : pos+len]` directly without memory allocation or syscalls.
4. **Fallback Path:** If active segment (unmapped), allocates buffer and calls `entry.seg.logFile.ReadAt(rec, pos)`.
5. Releases `q.mu.RUnlock()`. Decodes record via `decodeRecord(rec[4:])` and returns `Message`.


* `ReadBatch`: Takes read lock, clamps `count` to available messages, snapshots `indexEntry` slice, releases lock, and decodes records outside lock hold.

#### `encode.go`

Handles CRC32 record encoding, decoding, and raw UUID v7 conversions.

* **Binary Record Framing:**
`[4B Body Length][4B CRC32 Checksum][2B ID Len (16)][16B Raw UUID v7][8B Timestamp Nano][M-B Payload]`

```go
func encodeRecord(id uuid.UUID, ts time.Time, payload []byte) []byte
func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error)

```

**Implementation Details:**

* `encodeRecord`: Uses `encPool` (`sync.Pool` of `*bytes.Buffer`) to construct raw binary frames. Calculates body length `bodyLen = 4 (CRC) + 2 (ID Len) + 16 (UUID) + 8 (TS) + len(payload)`. Writes fields big-endian, calculates IEEE CRC32 checksum over body bytes (`buf[8:]`), stores CRC at bytes `4:8`, and returns a copy of record bytes.
* `decodeRecord`: Expects body slice (excluding 4-byte frame length). Checks payload length >= 14 bytes. Computes IEEE CRC32 checksum over `buf[4:]` and verifies against `buf[0:4]` (returns `ErrCorruptRecord` on mismatch). Converts 16-byte slice `buf[6:22]` directly to `uuid.UUID`, decodes uint64 nanosecond timestamp, and extracts payload sub-slice.

#### `encrypt_linux.go`, `encrypt_windows.go`, `encrypt_other.go`

OS-specific kernel directory transparent encryption handlers.

**Implementation Details:**

* `EnsureDirectoryEncrypted(dirPath)`:
* **Linux (`encrypt_linux.go`):** Opens directory file descriptor, constructs `fscryptPolicy` struct, and executes `unix.Syscall(unix.SYS_IOCTL, f.Fd(), FS_IOC_SET_ENCRYPTION_POLICY, ...)` using AES-256-XTS contents and AES-256-CTS filenames encryption modes. Silently ignores `ENOTTY`, `EOPNOTSUPP`, and `EEXIST` error codes for unsupported filesystems.
* **Windows (`encrypt_windows.go`):** Invokes `EncryptFileW` via DLL procedure call on `advapi32.dll` to apply NTFS Encrypting File System (EFS) directory attributes.
* **Other (`encrypt_other.go`):** macOS/BSD fallback. Logs a warning that host OS kernel lacks native transparent directory policies and returns nil.



#### `manager.go`

Manages multi-queue lifecycle, directory scanning, concurrent operations, and aggregated engine metrics.

```go
type Manager struct {
	mu      sync.RWMutex
	dir     string
	cfg     Config
	queues  map[string]*Queue
	offsets *OffsetStore
}

func OpenManager(dir string, cfg Config) (*Manager, error)
func (m *Manager) CreateQueue(name string, opts ...Config) (*Queue, error)
func (m *Manager) GetQueue(name string) (*Queue, error)
func (m *Manager) ListQueues() []string
func (m *Manager) DeleteQueue(name string) error
func (m *Manager) Stats() EngineStats
func (m *Manager) Close() error

```

**Implementation Details:**

* `OpenManager`: Creates root data directory, applies directory encryption if configured, opens manager `OffsetStore`, scans subdirectories, and calls `openQueue` to restore existing queues. Returns `*Manager`.
* `CreateQueue`: Takes write lock (`m.mu.Lock()`). Checks if queue exists in map or on disk (returns `ErrQueueExists` if present). Opens new queue via `openQueue`, stores in `m.queues`, and returns `*Queue`.
* `GetQueue`: Takes read lock (`m.mu.RLock()`), looks up queue by name, returns `ErrQueueNotFound` if absent.
* `DeleteQueue`: Takes write lock, retrieves queue, calls `q.Close()` to flush WAL and close file handles, removes queue from map, and deletes queue directory recursively via `os.RemoveAll`.
* `Stats`: Takes read lock, iterates through active queues, acquires per-queue read locks to sum segment sizes and index counts, and returns `EngineStats`.
* `Close`: Takes write lock, invokes `Close()` on all active queues, and closes manager `OffsetStore`.

---

### Internal Protocol Module (`package protocol`)

#### `opcodes.go`

Defines wire protocol opcodes, error codes, error formatting, and target URI parsing.

```go
const (
	OpCreateQueue  byte = 0x01
	OpProduce      byte = 0x02
	OpRead         byte = 0x03
	OpTail         byte = 0x04
	OpCommitOffset byte = 0x05
	OpFetchOffset  byte = 0x06
	OpProduceBatch byte = 0x07
	OpReadBatch    byte = 0x08
	OpListQueues   byte = 0x09
	OpDeleteQueue  byte = 0x0A
	OpStats        byte = 0x0B
	OpShutdown     byte = 0x0C
	OpError        byte = 0xFF
)

const (
	ErrCodeUnknown        byte = 0x01
	ErrCodeQueueNotFound  byte = 0x02
	ErrCodeQueueExists    byte = 0x03
	ErrCodeOffsetNotFound byte = 0x04
)

func EncodeError(code byte, msg string) []byte
func DecodeError(body []byte) (code byte, msg string)
func ParseTarget(target string) (network string, addr string, err error)

```

**Implementation Details:**

* `EncodeError`: Formats wire error bytes: `[1-byte Error Code][Message Bytes]`.
* `DecodeError`: Reads error code byte and converts remainder of slice to error message string.
* `ParseTarget`: Converts connection target strings (`"tcp://127.0.0.1:7420"`, `"unix:///tmp/mikyu.sock"`, `"pipe://./pipe/mikyu"`, or bare `"host:port"`) into network protocol name (`"tcp"`, `"unix"`, or `"pipe"`) and address string.

#### `frames.go`

Implements length-prefixed binary frame readers and pooled frame writers.

* **Wire Frame Layout:**
`[4B Big-Endian Length][1B Opcode][Payload]` (Length value = `1 + len(payload)`).

```go
func ReadFrame(r *bufio.Reader) (op byte, payload []byte, err error)
func WriteFrame(w io.Writer, op byte, payload []byte) error

```

**Implementation Details:**

* `ReadFrame`: Reads 4-byte length prefix from `bufio.Reader`. Allocates buffer of size `length`, reads full frame payload, and splits into 1-byte opcode (`buf[0]`) and payload slice (`buf[1:]`).
* `WriteFrame`: Calculates `total = 1 + len(payload)`. Fetches a `*[]byte` buffer pointer from `framePool` (`sync.Pool`), encodes big-endian length prefix, sets opcode at index 4, copies payload bytes starting at index 5, and writes to `io.Writer`. Returns buffer to pool if capacity matches.

#### `bytes.go`

Provides bounds-checked sequential byte decoding and buffer writing primitives.

```go
type Reader struct { b []byte; i int }
func NewReader(b []byte) *Reader
func (r *Reader) ReadU32() (uint32, error)
func (r *Reader) ReadU64() (uint64, error)
func (r *Reader) ReadBytes() ([]byte, error)

type Buffer struct { b []byte }
func (buf *Buffer) WriteU32(v uint32)
func (buf *Buffer) WriteU64(v uint64)
func (buf *Buffer) WriteBytes(data []byte)
func (buf *Buffer) Bytes() []byte

```

**Implementation Details:**

* `Reader`: Maintains internal slice offset `i`. `ReadBytes` reads a 4-byte big-endian Uint32 length prefix followed by that exact number of bytes. Returns `ErrTruncatedBody` if remaining slice bounds are exceeded.
* `Buffer`: Constructs wire payloads zero-allocation where possible using binary byte encoding and `append`.

---

### Internal Server & IPC Modules (`package server` / `package ipc`)

#### `internal/server/server.go`

Manages server listener loops, connection worker goroutines, TLS configuration, and graceful daemon shutdowns.

```go
type Server struct {
	mgr       *engine.Manager
	listener  net.Listener
	tlsConfig *tls.Config
	mu        sync.Mutex
	stopCh    chan struct{}
}

func New(mgr *engine.Manager, opts ...ServerOption) *Server
func WithTLS(tlsConfig *tls.Config) ServerOption
func (s *Server) ListenAndServe(target string) error
func (s *Server) Serve(ln net.Listener) error
func (s *Server) Stop()

```

**Implementation Details:**

* `ListenAndServe`: Resolves target via `protocol.ParseTarget`. Invokes `ipc.Listen` for `unix`/`pipe` targets or `net.Listen` for `tcp`. Wraps TCP listener with `tls.NewListener` if `tlsConfig` is set. Stores listener in `s.listener` and calls `Serve`.
* `Serve`: Enters connection acceptance loop. For each accepted `net.Conn`, spawns worker goroutine `go s.serveConn(conn)`.
* `Stop`: Idempotently closes `stopCh` channel and closes active `listener`.
* `serveConn(conn)`: Allocates `bufio.Reader` over connection. Loop reads frames via `protocol.ReadFrame`, dispatches to `s.dispatch`, and writes response frame via `protocol.WriteFrame`. If dispatch returns `shouldShutdown = true`, spawns background routine to execute `mgr.Close()` and `s.Stop()`.

#### `internal/server/handlers.go`

Dispatches protocol opcodes to corresponding storage engine operations.

```go
func (s *Server) dispatch(ctx context.Context, op byte, body []byte) (respOp byte, respBody []byte, shouldShutdown bool)

```

**Implementation Details:**

* `dispatch`: Evaluates opcode byte:
* `OpCreateQueue`: Reads queue name string, calls `mgr.CreateQueue`.
* `OpProduce`: Reads queue name and payload bytes, calls `q.Produce`, returns Uint64 offset response.
* `OpProduceBatch`: Reads queue name, payload count, and payloads slice, calls `q.ProduceBatch`, returns Uint32 count and array of Uint64 offsets.
* `OpRead`: Reads queue name and Uint64 offset, calls `q.Read`, encodes offset, string ID, Uint64 timestamp (nanoseconds), and payload bytes.
* `OpReadBatch`: Reads queue name, Uint64 startOffset, and Uint32 count, calls `q.ReadBatch`, encodes batch response.
* `OpTail`: Reads queue name, calls `q.LastOffset()`, returns Uint64 offset.
* `OpCommitOffset`: Reads queue name, group name, Uint64 offset, calls `q.CommitOffset`.
* `OpFetchOffset`: Reads queue name, group name, calls `q.FetchOffset`, returns Uint64 offset.
* `OpListQueues`: Calls `mgr.ListQueues`, encodes Uint32 count and string names.
* `OpDeleteQueue`: Reads queue name string, calls `mgr.DeleteQueue`.
* `OpStats`: Calls `mgr.Stats`, encodes Uint32 active queues and Uint64 total bytes.
* `OpShutdown`: Returns `(protocol.OpShutdown, nil, true)` to signal server shutdown routine.
* Default: Encodes `ErrCodeUnknown` via `protocol.EncodeError` and returns `OpError`.



#### `internal/server/tls.go`

Configures server TLS settings and handles self-signed certificate auto-generation.

```go
func DefaultTLSConfig(certFile, keyFile string) (*tls.Config, error)

```

**Implementation Details:**

* `DefaultTLSConfig`: If `certFile` and `keyFile` paths are supplied, loads keypair via `tls.LoadX509KeyPair`. If empty, invokes `generateSelfSignedCert()`.
* `generateSelfSignedCert`: Generates ECDSA P-256 private key via `ecdsa.GenerateKey(elliptic.P256(), rand.Reader)`. Creates self-signed X.509 certificate template with 128-bit random serial number, valid for 365 days for Subject CommonName `"localhost"`, DNS `"localhost"`, and IP addresses `127.0.0.1` and `::1`. Returns `tls.Certificate`.
* Sets `MinVersion: tls.VersionTLS13` to enforce TLS 1.3 protocol.

#### `internal/ipc/ipc.go`

Provides specialized listeners and dialers for local Unix Domain Sockets and Windows Named Pipes with stale socket cleanup.

```go
type Listener struct {
	net.Listener
	network string
	path    string
}

func Listen(target string) (net.Listener, error)
func Dial(target string) (net.Conn, error)
func DialContext(ctx context.Context, target string) (net.Conn, error)
func (l *Listener) Close() error

```

**Implementation Details:**

* `Listen`: Parses target via `protocol.ParseTarget`. Routes to `listenUnix` or `listenPipe`.
* `listenUnix`: Ensures parent directories exist (`os.MkdirAll`). If socket file exists at `path`, unlinks it (`os.Remove(path)`). Binds socket via `net.Listen("unix", path)`, chmods socket file permissions to `0600` for owner security, and returns `*Listener`.
* `listenPipe`: On Windows, formats path prefix `\\.\pipe\` and binds socket. On POSIX systems, falls back to `listenUnix`.
* `Close()`: Closes underlying net listener and unlinks Unix domain socket file from filesystem (`os.Remove(l.path)`).

---

### Executable Application (`cmd/mikyu`)

#### `cmd/mikyu/main.go`

Single-binary CLI entrypoint routing commands between background server daemon, REPL shell, TUI dashboard, and CLI executor.

**Implementation Details:**

* `main()`: Parses `-target` and `-config` global flags.
* **Bare Execution (`len(os.Args) == 1`):** Calls `RunREPL(*targetFlag)`. Auto-spawns daemon if offline.
* **Subcommand `daemon`/`server`:** Calls `runDaemonServer(*configFlag)`.
* **Subcommand `shell`/`repl`:** Calls `RunREPL(*targetFlag)`.
* **Subcommand `tui`/`dashboard`:** Prints TUI connection notice.
* **Default Fallback:** Routes remaining commands directly to `RunCLI(*targetFlag, os.Args[1:])`.
* `runDaemonServer`: Loads config via `config.Load`, opens `engine.Manager`, reconciles declared queues in `cfg.Queues` (calling `mgr.CreateQueue`), starts `server.Server`, listens for `SIGINT`/`SIGTERM` OS signal notifications, closes manager, stops server, and exits cleanly.

#### `cmd/mikyu/repl.go`

Interactive REPL shell with auto-daemon spawning, Lipgloss banner rendering, and command evaluation loop.

```go
func RunREPL(target string)

```

**Implementation Details:**

* `RunREPL`: Executes `EnsureDaemonRunning(target)`. Dials client via `mikyu.Dial(target)`. Clears terminal screen, renders header banner via `PrintREPLBanner(target)`. Reads input lines using `bufio.Scanner`.
* `parseREPLArgs(line)`: Custom string parser that splits arguments by space while preserving double-quoted string literals (`"hello world"`).
* `resolvePayload(input)`: Converts argument string to bytes: reads from stdin if input is `"-"`, reads file contents from disk if input starts with `"@"` (`@file.json`), or converts raw string literal to byte slice.
* `confirm(prompt)`: Displays an interactive confirmation prompt using `charm.land/huh/v2` form controls with custom Lipgloss button styles.
* `sendShutdownSignal(target)`: Dials daemon directly, writes an `OpShutdown` wire frame, and closes connection.

#### `cmd/mikyu/cli.go`

Stateless one-shot subcommand handler.

```go
func RunCLI(target string, args []string)

```

**Implementation Details:**

* `RunCLI`: Calls `EnsureDaemonRunning(target)`, connects via `mikyu.Dial(target)`, and evaluates `args[0]`:
* `list`: Calls `client.ListQueues` and prints queue list.
* `create`: Reads queue name, calls `client.CreateQueue`.
* `delete`: Requires confirmation prompt via `confirmCLI` unless `-y`/`--yes`/`-f`/`--force` flag is present. Calls `client.DeleteQueue`.
* `produce`: Resolves payload via `resolvePayload` (supports raw text, `@file`, stdin `-`), calls `q.Produce`.
* `read`: Parses Uint64 offset argument, calls `q.Read`, prints formatted message string.
* `tail`: Parses optional startOffset argument, streams live records using `q.Tail`.
* `stats`: Displays active queue count and storage footprint bytes.
* `shutdown`: Prompts confirmation, calls `sendShutdownSignal(target)`.


* `confirmCLI`: Checks arguments for `-y`/`--yes`/`-f`/`--force`. If absent, checks if `os.Stdin` is a TTY char device (`isTerminal`); if TTY, prompts `[y/N]` answer on stdin; otherwise fails with TTY error requirement.

#### `cmd/mikyu/daemon.go`

Daemon socket probing and non-blocking background process spawning.

```go
func IsDaemonAlive(target string) bool
func EnsureDaemonRunning(target string) error

```

**Implementation Details:**

* `IsDaemonAlive`: Attempts `net.DialTimeout(network, addr, 150ms)`. Returns `true` if connection succeeds, `false` otherwise.
* `EnsureDaemonRunning`: Returns immediately if `IsDaemonAlive(target)` is true. If false, resolves `os.Executable()`, creates process `exec.Command(execPath, "daemon")`, sets platform process detachment attributes via `setDetachedProcessAttr(cmd)`, and executes `cmd.Start()`. Polls `IsDaemonAlive` every 50ms for up to 3 seconds until daemon responds.

#### `cmd/mikyu/banner.go`

Renders Lipgloss-styled ASCII banner logo, command reference table, and status.

```go
func PrintREPLBanner(target string)

```

**Implementation Details:**

* `PrintREPLBanner`: Renders ASCII logo with multi-color vertical fire gradient using `charm.land/lipgloss/v2` (`#FFB200` to `#6F1FEF`). Renders command reference table, quickstart command hint (`produce events "hello world"`), and daemon connection target footer.

#### `cmd/mikyu/detach_unix.go` & `detach_windows.go`

Platform-specific background process detachment build tag routines.

**Implementation Details:**

* **Unix (`detach_unix.go` - `//go:build !windows`):** Sets `cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}` to create a new session group detached from the controlling terminal.
* **Windows (`detach_windows.go` - `//go:build windows`):** Sets `cmd.SysProcAttr = &syscall.SysProcAttr{CreationFlags: 0x08000000}` (`CREATE_NO_WINDOW`) to prevent spawning a console window.

#### `cmd/mikyu/vt_other.go` & `vt_windows.go`

Platform-specific virtual terminal setup routines.

**Implementation Details:**

* **Windows (`vt_windows.go` - `//go:build windows`):** Executed in package `init()`. Retrieves stdout and stderr console handles (`windows.STD_OUTPUT_HANDLE`, `windows.STD_ERROR_HANDLE`) and updates console mode flags using `ENABLE_VIRTUAL_TERMINAL_PROCESSING | ENABLE_PROCESSED_OUTPUT` to enable native ANSI escape code sequence parsing.
* **Other (`vt_other.go` - `//go:build !windows`):** Empty `init()` stub for POSIX systems.