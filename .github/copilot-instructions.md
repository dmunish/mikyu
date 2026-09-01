# Mikyu Technical Specification & Codebase Architecture

## 1. Project Overview

**mikyu** is a high-performance, log-structured message queue built from scratch in Go. It provides a unified API surface operating either as an in-process embedded log engine or as a standalone network/IPC daemon.

### Core Architectural Guarantees & Technical Principles

* **Durable-Before-Ack Semantics:** Every `Produce()` or `ProduceBatch()` invocation blocks until the payload byte slice and index metadata are durably synced to disk via the Write-Ahead Log (`wal`) flusher.
* **Zero-Copy Reads:** Closed historical log segments are memory-mapped (`mmap`) into the process address space. Read requests on historical offsets return direct sub-slices of mapped memory buffers without heap allocations or system call overhead.
* **Dynamic Dual-Threshold WAL Flushing:** The WAL flusher triggers a disk `Sync()` when either pending record count reaches `BatchSize` or elapsed time exceeds `Interval`, balancing single-thread latency with high-concurrency parallel throughput.
* **Raw 16-Byte UUID v7 Encoding:** Messages are encoded using raw 16-byte Go 1.27 standard library `uuid.UUID` arrays (`uuid.NewV7()`), avoiding string formatting allocations and reducing log file overhead by 20 bytes per record.
* **Go Range-Over-Func Integration:** Streaming and live tailing are exposed using native `iter.Seq2[Message, error]` iterators.
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
├── mikyu.go            # Top-level interfaces: Client, Queue, Group, Message
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
│   │   └── handlers.go     # Mapping wire OpCodes -> internal/engine calls
│   │
│   └── ipc/            # Specialized local IPC listeners
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
                                                                            │ (IPC / TCP)
                                                                 ┌──────────┴──────────┐
                                                                 │      cmd/mikyu      │
                                                                 │  (Unified Executable)│
                                                                 └─────────────────────┘

```

1. **`[github.com/dmunish/mikyu](https://github.com/dmunish/mikyu)`**: Root package exposing unified `Client`, `Queue`, and `Group` interfaces instantiated via `mikyu.Open()` (embedded) or `mikyu.Dial()` (remote TCP/IPC socket).
2. **`internal/config`**: Reads and writes `mikyu.yaml` via `goccy/go-yaml`, handles resolution hierarchies, applies environment variable overrides, and converts settings to engine configurations.
3. **`internal/engine`**: Storage engine handling WAL flushing, zero-copy `mmap` slices, segment rolling, index recovery, transparent directory encryption, and single-file offset persistence.
4. **`internal/protocol`**: Defines 5-byte length-prefixed frame layout (`[4B Length][1B Opcode][Payload]`) and protocol opcodes including `OpShutdown`.
5. **`internal/server` & `internal/ipc**`: Binds network/IPC listeners and dispatches binary wire frames to the underlying engine manager.
6. **`cmd/mikyu`**: Single binary combining server daemon, detached auto-spawner, Lipgloss REPL prompt, and stateless CLI handler.

---

## 4. File-by-File Technical Specification

### Root Package (`package mikyu`)

#### `mikyu.go`

Defines public domain models and core SDK interface contracts.

```go
type Message struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Payload   []byte
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
func WithMaxSegmentBytes(bytes int64) QueueOption
func WithMaxMessageBytes(bytes int64) QueueOption
func WithMaxSegmentAge(age time.Duration) QueueOption
func WithDirectoryEncryption(enabled bool) QueueOption

```

#### `typed.go`

Provides generic type marshaling wrappers over raw byte queues.

```go
type Codec[T any] struct {
	Marshal   func(T) ([]byte, error)
	Unmarshal func([]byte, *T) error
}

type Typed[T any] struct { /* private fields */ }

func NewTyped[T any](q Queue, codec Codec[T]) *Typed[T]
func (t *Typed[T]) Produce(ctx context.Context, val T) (uint64, error)
func (t *Typed[T]) Read(ctx context.Context, offset uint64) (T, error)

```

#### `embedded.go`

Implements `Client`, `Queue`, and `Group` by wrapping `internal/engine` directly.

```go
func Open(dir string, opts ...ClientOption) (Client, error)

```

#### `remote.go`

Implements `Client`, `Queue`, and `Group` over pooled TCP/IPC socket connections.

```go
func Dial(target string, opts ...ClientOption) (Client, error)

```

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

type ServerConfig struct {
	Target  string `yaml:"target"`
	DataDir string `yaml:"data_dir"`
}

type EngineConfig struct {
	BatchSize       int           `yaml:"batch_size"`
	FlushInterval   time.Duration `yaml:"flush_interval"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes"`
	MaxMessageBytes int64         `yaml:"max_message_bytes"`
	MaxSegmentAge   time.Duration `yaml:"max_segment_age"`
	Encrypted       bool          `yaml:"encrypted"`
}

type QueueConfig struct {
	Name            string        `yaml:"name"`
	BatchSize       int           `yaml:"batch_size,omitempty"`
	FlushInterval   time.Duration `yaml:"flush_interval,omitempty"`
	MaxSegmentBytes int64         `yaml:"max_segment_bytes,omitempty"`
	MaxMessageBytes int64         `yaml:"max_message_bytes,omitempty"`
	MaxSegmentAge   time.Duration `yaml:"max_segment_age,omitempty"`
	Encrypted       *bool         `yaml:"encrypted,omitempty"`
}

func DefaultConfig() Config
func Load(flagPath string) (*Config, string, error)
func ToEngineConfig(e EngineConfig) engine.Config
func ToQueueEngineConfig(defaults EngineConfig, q QueueConfig) engine.Config

```

---

### Internal Storage Engine (`package engine`)

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

func newWAL(f *os.File, cfg Config, startOffset int64) *wal
func (w *wal) start()
func (w *wal) push(rec []byte, pos int64) (seq int64, wake bool, err error)
func (w *wal) pushBatch(recs [][]byte, positions []int64) (seq int64, wake bool, err error)
func (w *wal) wait(seq int64) error
func (w *wal) flush()
func (w *wal) stop() error

```

#### `encode.go`

Encodes/decodes records using raw 16-byte `uuid.UUID` values and calculates IEEE CRC32 checksums.

* **Binary Record Format:**
`[4B Length][4B CRC32 Checksum][2B ID Len (16)][16B Raw UUID v7][8B Timestamp Nano][M-B Payload]`

```go
func encodeRecord(id uuid.UUID, ts time.Time, payload []byte) []byte
func decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error)

```

#### `encrypt_linux.go` / `encrypt_windows.go` / `encrypt_other.go`

Provides OS-specific transparent kernel directory encryption (`fscrypt` ioctl on Linux, NTFS EFS on Windows, no-op fallback on macOS/Darwin).

```go
func EnsureDirectoryEncrypted(dirPath string) error

```

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

func OpenManager(dir string, cfg Config) (*Manager, error)
func (m *Manager) CreateQueue(name string, opts ...Config) (*Queue, error)
func (m *Manager) GetQueue(name string) (*Queue, error)
func (m *Manager) ListQueues() []string
func (m *Manager) DeleteQueue(name string) error
func (m *Manager) Stats() EngineStats
func (m *Manager) Close() error

```

---

### Internal Protocol Module (`package protocol`)

#### `opcodes.go`

Defines wire opcodes, error formats, and target URI parsing.

* **Opcodes:** `OpCreateQueue` (`0x01`), `OpProduce` (`0x02`), `OpRead` (`0x03`), `OpTail` (`0x04`), `OpCommitOffset` (`0x05`), `OpFetchOffset` (`0x06`), `OpProduceBatch` (`0x07`), `OpReadBatch` (`0x08`), `OpListQueues` (`0x09`), `OpDeleteQueue` (`0x0A`), `OpStats` (`0x0B`), `OpShutdown` (`0x0C`), `OpError` (`0xFF`).

```go
func EncodeError(code byte, msg string) []byte
func DecodeError(body []byte) (code byte, msg string)
func ParseTarget(target string) (network string, addr string, err error)

```

#### `frames.go`

Manages 5-byte length-prefixed frame encoding/decoding with zero-allocation `sync.Pool` buffers.

```go
func ReadFrame(r *bufio.Reader) (op byte, payload []byte, err error)
func WriteFrame(w io.Writer, op byte, payload []byte) error

```

#### `bytes.go`

Provides bounds-checked binary reader and writer buffer abstractions.

```go
type Reader struct { /* private fields */ }
func NewReader(b []byte) *Reader
func (r *Reader) ReadU32() (uint32, error)
func (r *Reader) ReadU64() (uint64, error)
func (r *Reader) ReadBytes() ([]byte, error)

type Buffer struct { /* private fields */ }
func (buf *Buffer) WriteU32(v uint32)
func (buf *Buffer) WriteU64(v uint64)
func (buf *Buffer) WriteBytes(data []byte)
func (buf *Buffer) Bytes() []byte

```

---

### Internal Server & IPC Modules (`package server` / `package ipc`)

#### `internal/server/server.go`

Accepts connections and handles graceful shutdown when receiving `OpShutdown` or signal events.

```go
type Server struct {
	mgr      *engine.Manager
	listener net.Listener
	mu       sync.Mutex
	stopCh   chan struct{}
}

func New(mgr *engine.Manager) *Server
func (s *Server) ListenAndServe(target string) error
func (s *Server) Serve(ln net.Listener) error
func (s *Server) Stop()

```

#### `internal/server/handlers.go`

Dispatches binary frames to storage engine calls and triggers `OpShutdown` stops.

```go
func (s *Server) dispatch(ctx context.Context, op byte, body []byte) (byte, []byte, bool)

```

#### `internal/ipc/ipc.go`

Manages Unix Domain Socket and Windows Named Pipe listeners with stale socket unlinking.

```go
type Listener struct {
	net.Listener
	network string
	path    string
}

func Listen(target string) (net.Listener, error)
func (l *Listener) Close() error

```

---

### Executable Application (`cmd/mikyu`)

#### `cmd/mikyu/main.go`

Unified subcommand router and entrypoint for bare execution, server daemon, REPL, TUI, and CLI commands.

```go
func main()
func runDaemonServer(configPath string)
func printUsage()

```

#### `cmd/mikyu/daemon.go`

Probes socket readiness and auto-spawns background daemon instances.

```go
func IsDaemonAlive(target string) bool
func EnsureDaemonRunning(target string) error

```

#### `cmd/mikyu/detach_unix.go` & `detach_windows.go`

Platform-specific process detachment using Go build tags (`//go:build !windows` vs `//go:build windows`).

```go
func setDetachedProcessAttr(cmd *exec.Cmd)

```

#### `cmd/mikyu/vt_other.go` & `vt_windows.go`

Platform-specific Virtual Terminal (ANSI) enabling for colored output.

```go
// vt_other.go: empty init()
// vt_windows.go: enables ENABLE_VIRTUAL_TERMINAL_PROCESSING on stdout/stderr
func init()

```

#### `cmd/mikyu/banner.go`

Renders the Lipgloss-styled ASCII banner, command reference table, and connection status.

```go
func PrintREPLBanner(target string)

```

#### `cmd/mikyu/repl.go`

Interactive REPL prompt session handling command evaluation with `scanner.Err()` checks.

```go
func RunREPL(target string)
func parseREPLArgs(line string) []string
func sendShutdownSignal(target string) error

```

#### `cmd/mikyu/cli.go`

One-shot stateless CLI subcommand executor supporting raw payloads, `@file_path` file reading, and `-` stdin streaming.

```go
func RunCLI(target string, args []string)
func resolvePayload(input string) ([]byte, error)

```