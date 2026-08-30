# mikyu

This file is the single source of truth for new agents. It consolidates the
repository structure, what each file does, and the contracts you must respect
when editing. Do **not** go re-exploring the repo from scratch — read this
first. The narrative/roadmap context lives in `.notes/plan.md`; this file is the
structural map. 

Module path: `github.com/dmunish/mikyu`
Go version: `1.27.0`
Runtime deps: `github.com/edsrzf/mmap-go` (segment mmap), `golang.org/x/sys` (indirect).
`uuid` is used via the standard library.

Build/test:
- `go build ./...` — clean (verified).
- `go vet ./...` — clean (verified).
- `go test ./...` — TDD throughout; white-box (`package queue`) + black-box (`package queue_test` / `package mikyu_test`).

## Tool Use Rules
- When reading `SKILL.md` files or workspace context files, NEVER rely on partial/truncated initial views (lines 1–100). Use large ranges (1-500) by default.
- If a file exceeds the default tool view limit, issue sequential tool calls until the entire file content is loaded into memory before taking action.

# Go Language Updates
`.github/modern-go` contains new features of Go from release 1.23, 
up to the most recent 1.27. Read that at the start every time to get a fresher.

## Architectural invariants (load-bearing — do not break)

1. **Embeddable-first.** The core engine in `queue/` has zero network code.
   The TCP layer (`internal/server`) and client SDK (`mikyu.go`) sit *across*
   the boundary and are optional. Anything embedders need must live in `queue/`.
2. **Durable-before-ack.** `Queue.Produce` blocks until the record is `fsync`ed
   to disk. This is the documented crash-semantics contract — do not change it
   to fire-and-forget without an explicit, deliberate decision.
3. **Three independent axes** (never conflate):
   - reading: destructive vs. non-destructive (`Read` is non-destructive here)
   - offset tracking: logical stream position (client-facing)
   - deletion: retention policy (not yet implemented — Phase 6+)
4. **Offset vs. index terminology:** "offset" = logical stream position
   (client-facing, Kafka/Pugs convention); "index" = internal `indexEntry`
   (offset → byte position + length) lookup structure. Never conflate.
5. **No premature abstraction.** Concrete-first for early phases; justify any
   new interface by the problem in front of you, not "it might be useful."

## Package / file map

### root — `package mikyu` (public TCP client SDK)
`github.com/dmunish/mikyu/mikyu.go`
Mirrors the server wire protocol as a client. Deliberate API bridge across the
`internal/` boundary, not redundant code.

- `type Client struct { conn net.Conn; r *bufio.Reader }` — one TCP connection.
- `Dial(addr string) (*Client, error)` — opens a TCP connection to a `mikyud` server.
- `(*Client) Close() error` — closes the connection.
- `(*Client) CreateQueue(ctx, name string) error` — sends `OpCreateQueue`.
- `(*Client) Produce(ctx, queue string, payload []byte) (uint64, error)` —
  sends `OpProduce`, returns the assigned offset.
- `(*Client) Read(ctx, queue string, offset uint64) (Message, error)` —
  sends `OpRead`, reconstructs a `Message`.
- `(*Client) LastOffset(ctx, queue string) (uint64, error)` — sends `OpTail`.
- `(*Client) Queue(name string) Engine` — returns a `remoteEngine` bound to the named queue, implementing the unified `Engine` interface.
- `(*Client) CommitOffset(ctx, queue, group string, offset uint64) error` — sends `OpCommitOffset`.
- `(*Client) FetchOffset(ctx, queue, group string) (uint64, error)` — sends `OpFetchOffset`.
- `(*Client) exchange(ctx, op byte, body []byte) ([]byte, error)` — frames a
  request, applies `ctx` deadline to the socket, reads the response frame.
  Returns `ctx.Err()` if the context fired; maps `OpError` via `decodeError`.
- `decodeError(body []byte) error` — translates protocol error codes to the
  exported `ErrQueueExists` / `ErrQueueNotFound` / `ErrOffsetNotFound`.
- Exported aliases: `ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`
  (re-exported from `queue`), `type Message = queue.Message` (alias).

### `queue/` — `package queue` (core engine, embeddable)
The real heart of mikyu. Log-structured: WAL + bounded segments + mmap reads.

**`queue/manager.go`** — multi-queue registry and lifecycle.
- `type Manager struct { mu sync.RWMutex; dir string; queues map[string]*Queue }`
- `Open(dir string) (*Manager, error)` — creates the data dir if needed and
  restores every subdirectory as a `Queue` (replay on open).
- `(*Manager) CreateQueue(name string, opts ...Config) (*Queue, error)` —
  takes `mu.Lock()`; rejects duplicates with `ErrQueueExists`.
- `(*Manager) Queue(name string) (*Queue, error)` — takes `mu.RLock()` /
  `RUnlock()` (matched — verified, the historical Phase-1 bug is fixed).
- `(*Manager) Close() error` — closes every queue, returns first error.

**`queue/queue.go`** — per-queue state, segment loading/rotation, lifecycle.
- `type Queue struct { name, dir string; cfg Config; active *segment; segments []*segment; index []indexEntry; w *wal; mu sync.Mutex }`
- `type Config struct { BatchSize int; Interval time.Duration; MaxSegmentBytes int64; MaxMessageBytes int64; MaxSegmentAge time.Duration }`
  — all tunables, applied per-queue at creation time.
- `Config.withDefaults() Config` — fills zero values (BatchSize 256,
  Interval 10ms, MaxSegmentBytes 64MiB, MaxMessageBytes 1MiB, MaxSegmentAge 1h).
- `type indexEntry struct { seg *segment; pos, len int64 }` — offset→byte mapping.
- `openQueue(parentDir, name string, cfg Config) (*Queue, error)` — creates the
  queue dir, loads segments.
- `(*Queue) loadSegments() error` — sorts `*.log` bases, `openSegment` + `recover`
  each, `enableMMap` for historical (non-active) segments, rebuilds `index`.
- `(*Queue) roll() error` — stops the WAL, `enableMMap`s the sealed segment,
  opens a new segment at `len(index)` base offset, restarts the WAL.
- `(*Queue) Close() error` — stops WAL, closes all segments; WAL error wins.

**`queue/ops.go`** — the message API.
- `type Message struct { Offset uint64; Timestamp time.Time; ID string; Payload []byte }`
- `(*Queue) Produce(ctx, payload []byte) (uint64, error)` — rejects
  `> MaxMessageBytes` with `ErrMessageTooLarge`; **pre-check-before-write** rolls
  if `active.pos + recLen > MaxSegmentBytes` OR age `> MaxSegmentAge`; pushes to
  WAL, blocks on `w.wait(seq)`, then appends to `index`. Returns the assigned offset.
- `(*Queue) Read(ctx, offset uint64) (Message, error)` — `ErrOffsetNotFound` if
  out of range; **zero-copy** slices `mmapData` for rolled segments, falls back
  to `ReadAt` for the active segment; `decodeRecord` the body.
- `(*Queue) LastOffset() uint64` — `len(index)` under `mu`.

**`queue/encode.go`** — on-disk record framing.
- `encodeRecord(id string, ts time.Time, payload []byte) []byte` — layout:
  `uint32 totalLen | uint32 crc32(body) | uint16 idLen | id | uint64 tsNano | payload`.
  CRC covers everything after the length prefix.
- `decodeRecord(buf []byte) (id string, ts time.Time, payload []byte, err error)` —
  verifies CRC, validates bounds; returns `ErrCorruptRecord` on mismatch/truncation.

**`queue/segment.go`** — segment files (`.log` + `.idx`).
- `type segment struct { baseOff uint64; logFile, idxFile *os.File; pos int64; createdAt time.Time; mmapData mmap.MMap }`
- `openSegment(dir string, baseOff uint64) (*segment, error)` — opens
  `%020d.log` / `%020d.idx` (RDWR, create).
- `(*segment) enableMMap() error` — read-only mmap of the log file (fast-path reads).
- `(*segment) writeIndex(pos, len int64) error` — appends a 16-byte (pos,len) entry.
- `(*segment) recover() ([]rawIndexEntry, error)` — **fast path**: if `.idx` is
  consistent with `.log` size, load it directly; **fallback**: `rebuild()` scans
  the log, verifying each CRC, truncating any torn-write tail.
- `(*segment) rebuild() ([]rawIndexEntry, error)` — rescans log, rewrites `.idx`,
  truncates the file at the last good record.
- `(*segment) Close() error` — unmaps (guards double-unmap), closes both files.

**`queue/wal.go`** — batched, durable fsync writer.
- `type wal struct { cfg Config; file *os.File; mu sync.Mutex; cond *sync.Cond; buf []byte; pending []pendingEntry; published int64; err error; wake, shutdown, done chan }`
- `type pendingEntry struct { pos, len int64 }` — per-segment write pointer.
- `newWAL(f *os.File, cfg Config, startOffset int64) *wal` — seeds `published`
  from replayed offset so sequence numbers are contiguous across restarts.
- `(*wal) start()` — launches `syncLoop` goroutine.
- `(*wal) syncLoop()` — **dynamic dual-threshold**: wakes on ticker (`Interval`)
  OR `wake` channel (when a record is staged and nothing is pending, or batch
  full); flushes; on `shutdown` flushes and returns.
- `(*wal) push(rec []byte, pos int64) (seq int64, wake bool, err error)` — stages
  bytes + index metadata; returns the durable-sequence to wait on; `wake` true
  when this is the first staged record or batch size reached.
- `(*wal) wait(seq int64) error` — blocks on `cond` until `published > seq` or
  sticky `err` set. This is what makes `Produce` durable-before-ack.
- `(*wal) flush()` — swaps buffers (minimal lock hold), `WriteAt` + `Sync` off-lock,
  then advances `published` and broadcasts.
- `(*wal) stop() error` — signals shutdown, waits for `done`, returns sticky err.

**`queue/` errors:** `ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`,
`ErrCorruptRecord`, `ErrMessageTooLarge`.

### `internal/protocol/` — `package protocol` (wire format)
Shared by server and client. Keep it dependency-free and stable.

**`internal/protocol/protocol.go`**
- Frame: `uint32 length | uint8 opcode | payload`. Opcodes: `OpCreateQueue 0x01`,
  `OpProduce 0x02`, `OpRead 0x03`, `OpTail 0x04`, `OpError 0xFF`. Error codes:
  `ErrCodeUnknown/QueueExists(0x01)/QueueNotFound(0x02)/OffsetNotFound(0x03)`.
- `ReadFrame(r *bufio.Reader) (op byte, payload []byte, err error)`
- `WriteFrame(w io.Writer, op byte, payload []byte) error`
- `EncodeError(code byte, msg string) []byte` / `DecodeError(body []byte) (code byte, msg string)`

**`internal/protocol/bytes.go`** — typed reader/writer for payloads.
- `type Reader struct { b []byte; i int }` — `NewReader`, `ReadU32`, `ReadU64`,
  `ReadBytes` (length-prefixed); returns `ErrTruncatedBody` on underflow.
- `type Buffer struct { b []byte }` — `WriteU32`, `WriteU64`, `WriteBytes`
  (length-prefixed), `Bytes`.

### `internal/server/` — `package server` (TCP dispatch)
**`internal/server/server.go`**
- `type Server struct { mgr *queue.Manager }`
- `New(mgr *queue.Manager) *Server`
- `(*Server) Start(addr string) error` — listen + `Serve`.
- `(*Server) Serve(ln net.Listener) error` — accept loop, one goroutine per conn.
- `(*Server) serve(conn net.Conn)` — frame read → `dispatch` → frame write loop.
- `(*Server) dispatch(ctx, op, body) (byte, []byte)` — routes opcodes to ops.

**`internal/server/ops.go`** — per-op handlers.
- `createQueue(body)` — `mgr.CreateQueue`, maps `ErrQueueExists`.
- `produce(ctx, body)` — looks up queue, `Produce`, returns offset.
- `read(ctx, body)` — looks up queue, `Read`, serializes `Message` (offset, id, tsNano, payload).
- `tail(body)` — returns `LastOffset`.

### `cmd/mikyud/` — `package main` (server binary)
`cmd/mikyud/main.go` — entrypoint. `queue.Open("data")`, `server.New(mgr)`,
listens on `:7420`, handles SIGINT/SIGTERM for graceful shutdown, closes the
manager on exit. Flags noted in `plan.md` (`-port`/`-data`) are the intended
shape; current code uses hardcoded `:7420` / `"data"`.

### `dashboard/` — Modern Svelte5 app (scaffold for now)
Svelte 5 + Vite + TypeScript, Bun package manager. `src/App.svelte`, `src/lib/Counter.svelte`,
`src/main.ts`, `src/app.css`. Intended to consume a future SSE metrics API
(Phase 4/5). Treat as a separate frontend project; leave it alone unless the
task is explicitly the dashboard.

### Tests (what each validates)
- `queue/queue_test.go` (`package queue_test`): manager create/get, disk restore,
  produce+read, WAL replay preserves messages, torn-write tail truncation.
- `queue/bench_test.go`: `BenchmarkProduceSerial/Parallel`, `BenchmarkConsumeSequential/Random`.
- `queue/encode_test.go`: record round-trip (incl. empty/edge cases), corrupt-CRC, truncated.
- `queue/segment_test.go`: `TestSegment_RecoverAndRebuild`.
- `internal/protocol/protocol_test.go`: buffer/reader round-trip, truncation, frame round-trip, length prefix, error codec.
- `internal/server/server_test.go`: dispatch happy-path + error codes + duplicate create (uses in-memory `net.Pipe`).
- `mikyu_test.go` (`package mikyu_test`): full client↔server E2E over a real
  ephemeral port — lifecycle, error mapping, context deadline, multi-queue isolation.

## Things to know before editing
- **Windows caveat:** open files are locked; tests must `Close()` WALs before
  `t.TempDir()` cleanup (already handled in helpers).
- **mmap:** only historical/rolled segments are mmapped; the active segment is
  written via `ReadAt`/write path. Don't mmap the active segment.
- **Sequence numbers** in the WAL are contiguous across restarts because
  `newWAL` seeds `published` from `len(index)`; keep that invariant if you touch replay.
- **`go.mod` only lists `mmap-go` + indirect `golang.org/x/sys`**; `uuid` resolves
  via stdlib. If you add a dep, run `go mod tidy`.

### `engine.go` — `package mikyu` (Engine interface + adapters + SDK extensions)
- `Engine` interface: `Produce`, `ProduceBatch`, `Read`, `ReadBatch`, `LastOffset`, `CommitOffset`, `FetchOffset` — unified contract for embedded (`queue.Queue`) and remote (`Client`).
- `LastOffsetAdapter`: embeds `*queue.Queue`, overrides `LastOffset(ctx)` to add context awareness.
- `remoteEngine`: binds `Client` + queue name; implements `Engine` over TCP.
- `Client.Queue(name) Engine`: returns `remoteEngine` bound to named queue.
- `Stream` / `Tail`: `iter.Seq2[Message, error]` iterators with active `ctx.Err()` cancellation.
- `ConsumerGroup`: `JoinGroup`, `Mark`, `Commit`, `FetchOffset`, `Close` (actor goroutine + synchronous final commit).
- `Codec[T]` / `Typed[T]`: generic wrapper; value receiver (`Typed[T]`); `NewTyped`.
- `Message` alias (`queue.Message`) exported.
- `Engine` interface (`engine.go`): `Produce`, `ProduceBatch`, `Read`, `ReadBatch`, `LastOffset`, `CommitOffset`, `FetchOffset` — unified contract for embedded (`queue.Queue` via `LastOffsetAdapter`) and remote (`Client` via `remoteEngine`).
- `LastOffsetAdapter`: embeds `*queue.Queue`, overrides `LastOffset(ctx)` to add context awareness.
- `remoteEngine`: binds `Client` + queue name; implements `Engine` over TCP.

### `queue/offets.go` — manager-level offset persistence (`package queue`)
- `OffsetStore`: single `manager-offsets.dat` file for all queues (not 1 per queue).
- `OpenManagerOffsetStore`, `Commit(queueName, group, offset)`, `Fetch(queueName, group)`, `Close`.
- `bufio.Writer` + `Flush()` before `Sync()` (Batch 4 performance fix).

### `queue/offsets_test.go` — white-box tests (`package queue`)
- `TestOffsetStore_CommitAndFetch`: verifies manager-level store writes and reads.

### `engine_test.go` — SDK contract tests (`package mikyu`)
- `TestEngineContract`: compile-time assertion `var _ Engine = LastOffsetAdapter{}`.

### New modules added (post-Phase-6):
- `engine.go`: `Engine`, adapters, `ConsumerGroup`, `Typed[T]`, iterators.
- `queue/offets.go`: manager-level `OffsetStore` (replaces deleted per-queue `offsets.go`).
- `queue/offsets_test.go`: manager store tests.
- `engine_test.go`: SDK contract verification.
