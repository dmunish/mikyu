# Mikyu — Architecture & Implementation Specification

## 1. Project Overview

**mikyu** is a high‑performance, log‑structured message queue written from scratch in Go. It presents **two distinct delivery semantics over one physical storage engine**:

- **Log queues** — append‑only streams with consumer‑group offset tracking, replay, continuous tail, and truncation. Used for event logs, CDC pipelines, and replayable streams.
- **Task queues** — at‑least‑once delivery with visibility leases, exponential backoff, scheduling, stale‑lease protection, a durable per‑queue dead‑letter queue, and chunked checkpoint‑based crash recovery. Used for work pools and job processing.

Both drivers sit on the same `engine.Storage` interface and can share a single physical queue directory: a log producer and a task enqueuer may write to the same segments, and each driver **ignores records it does not own** (log records lack the task magic header; task records are foreign to log readers).

The project ships as a **single binary** with four roles:

| Role | Entry | Description |
|---|---|---|
| Embedded engine | `mikyu.Open(dir, opts...)` | In‑process `engine.Manager`, no network, ref‑counted per data directory. |
| Remote client | `mikyu.Dial(target, opts...)` | Pooled TCP / Unix / Named‑Pipe wire client. |
| Daemon | `mikyu daemon` | Long‑lived server process over TCP, Unix socket, or Windows named pipe. |
| CLI / REPL | `mikyu <cmd>`, `mikyu shell` | Stateless one‑shot CLI and a styled interactive REPL. |

### 1.1 Core Guarantees

- **Durable‑before‑ack (default, `Fsync: true`).** `Append`/`AppendBatch` block until the WAL flusher has `syncData`'d the record bytes and index entry. The returned offset is crash‑safe.
- **Optional crash‑loss window (`Fsync: false`).** The record is published to the ring cache and `publishedOffset` immediately; the flusher writes via `WriteAt` without `syncData`. Trades a small crash window for ~3.5× lower latency. Bytes still land in the OS page cache.
- **Zero‑copy reads.** Closed segments are `mmap`'d read‑only. `Read` on a closed segment returns a sub‑slice of mapped memory with no heap allocation.
- **Ring cache.** The last 16,384 messages are cached in a lock‑protected ring for ~100 ns/op tail reads.
- **Long‑poll tailing.** `Fetch` blocks on the queue's `notify` channel for up to its `maxWait`, waking on every `AppendBatch` broadcast. `Stream` reuses the same primitive with `maxWait=0` for a non‑blocking drain; `Tail` re‑arms a 30 s window per iteration, so an idle stream is one request per window instead of a 5 ms polling loop.
- **Chunked, self‑describing checkpoints.** Task state is snapshotted into `recordCPStart` / `recordCPChunk` / `recordCPCommit` records; the newest valid commit wins on recovery, and corrupt or incomplete checkpoints are skipped in favor of earlier ones.
- **Single source of truth for configuration.** Every queue and session parameter lives in `spec.Table` with a canonical key, path, kind, layer, mutability, env name, flag name, and option name. All layers (SDK, CLI, env, YAML, wire, storage) are driven from that one table, and the table itself is a published artifact: `mikyu.Schema()` returns it locally, `OpSchema` serves it over the wire.
- **Live telemetry under one surface.** `internal/metrics` is the single observation point: a pull‑based `Collector` over a `Registry` of atomic instruments, materializing `Snapshot`s for the wire (`OpMetrics`), the CLI/REPL (`metrics`), and the daemon's Prometheus/JSON/SSE HTTP endpoints. There is no separate `Info`/`Stats` path.
- **Enforced architecture.** `.go-arch-lint.yml` defines the package‑level dependency graph; `internal/audit` gates legacy names and comment style. CI fails on a violation.
- **Authentication is a handshake, not a per‑op concern.** The first frame on every connection is `OpAuth`; the server verifies it once, then dispatches. `none`, `token`, and `mtls` are the only supported modes, and an unauthenticated non‑loopback daemon refuses to start.

### 1.2 Concurrency & Performance Snapshot

| Path | Result |
|---|---|
| Append (durable, `Fsync: true`) | ≈ 292.6 µs/op (64 B payloads) |
| Append (async, `Fsync: false`) | ≈ 25.9 µs/op (≈ 3.5× faster) |
| Ring‑cache `Read` | ≈ 100 ns/op, 0 allocs |
| `encodeRecord` | ≈ 887 ns/op, 0 allocs |
| WAL steady‑state flusher | 0 allocs/op, 3 B/op (double‑buffered swap) |
| 64 KB protocol frame | 38.5 µs → 3.5 µs (pooled writer) |
| Index RAM | `O(log S)` per read; footprint scales with segment count, not message count |
| `OpAuth` handshake | one extra frame per connection; amortized to zero by the pooled client |

---

## 2. Architectural Principles

1. **Layer purity.** `spec` and `engine` are bottoms. `spec` imports nothing internal. `engine` imports only `spec` (and `metrics` for its scopes). `protocol` is pure DTOs over `bytes`/`frames`. `metrics` is a bottom‑adjacent utility imported by `engine`, `server`, `node`, `daemon`, and the drivers. `node` never touches `protocol`/`server`/`client`. `client` and `server` never import each other. `ui/*` reaches the world only through `mikyu`, `client`, `command`, `daemon`, `spec`. This graph is enforced.
2. **One storage engine, two policy drivers.** `engine.Storage` is the only contract drivers see. `logqueue` and `taskqueue` implement different semantics over identical bytes.
3. **One configuration table.** `spec.Table` is the canonical parameter list. Every other surface is generated from it, and the table itself is published — `mikyu.Schema()` locally, `OpSchema` over the wire.
4. **One observation surface.** `internal/metrics` is the only telemetry contract. Every instrument flows through a `metrics.Scope`; every snapshot comes from a `metrics.Collector`. `Info` and `Stats` do not exist.
5. **The registry is a config artifact.** Per‑queue specs live in `queues.json`. The filesystem is the source of truth for existence; missing directories purge entries, orphan directories are adopted.
6. **The data directory is a lock.** `node.Acquire` takes an exclusive `flock` on `<dir>/mikyu.lock`. Two processes on the same dir fail fast with `ErrLocked`.
7. **Path escape is a compile‑time concern.** Every file operation in `engine` uses `os.Root` scoped to the data directory.
8. **Refcount over teardown.** Embedded clients share one `node.Node` per absolute data directory. `Close` decrements; the last one releases the manager, drivers, lock, collector, and retention loop.
9. **Errors are stable sentinels.** Wire codes map 1:1 to Go sentinels. Messages are human text, never contracts.
10. **Auth is one handshake per connection.** Credentials are declared once in `OpAuth` and verified once in `authMode.Verify`; the rest of the protocol is credential‑free. The verifier lives in `internal/server/auth.go`, not in `dispatch.go`, so a future auth mode is one file.

---

## 3. Layered Architecture & The Dual‑Driver Nature

```
┌─────────────────────────────────────────────────────────────────────────────┐
│  Layer 6 — Access & Deployment                                              │
│    internal/ui/cli     internal/ui/repl     internal/ui/command             │
│    internal/daemon     cmd/mikyu                                            │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 5 — Client & Process Core                                            │
│    internal/client (embedded | remote)     internal/node (registry, pool)   │
│    internal/config (yaml + env)                                             │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 4 — Drivers                                                          │
│    internal/queue/logqueue      internal/queue/taskqueue                    │
│    internal/queue/group         internal/queue/retention                    │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 3 — Transport                                                        │
│    internal/transport/protocol      internal/transport/ipc                  │
│    internal/server (dispatch + auth)                                        │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 2 — Configuration & Schema                                           │
│    internal/spec (Spec, Session, Table, Value, Duration, codec, validate)   │
└─────────────────────────────┬───────────────────────────────────────────────┘
                              │
┌─────────────────────────────▼───────────────────────────────────────────────┐
│  Layer 1 — Storage Engine          internal/metrics (instruments, registry) │
│    internal/engine.Manager → engine.Queue                                   │
│      segments (active + mmap), WAL (sync + async), raw 16‑byte index,       │
│      ring cache (16,384), manager‑scoped OffsetStore (compacting)           │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 3.1 The Two Drivers

Both drivers take an `engine.Storage` handle. A **single physical queue** can be registered with both drivers simultaneously; each driver writes its own record shape and ignores records it does not recognize.

| Aspect | `logqueue.LogQueue` | `taskqueue.TaskQueue` |
|---|---|---|
| Record shape | Raw `encodeRecord` from `engine` | `taskRecordMagic` + kind byte + task payload |
| Foreign records | Yielded as opaque `engine.Message` | Ignored in `refreshLocked` (unknown kind) |
| Consumer state | `group.Base` (HighWater) in memory | `active` map + `scheduleHeap` + `dlq` |
| Durability anchor | Consumer group commit offsets | Chunked checkpoints (`recordCPCommit`) |
| Retention floor | `min(HighWater)` across groups | `min(checkpointOffset, active offsets, uncheckpointed DLQ)` |
| Primary API | `Append`, `Read`, `Fetch`, `Stream`, `Tail`, `Group` | `Enqueue`, `Dequeue`, `Ack`, `Nack`, `Extend`, `DeadLetters` |
| Metrics scope | `appends`, `reads`, `bytes_in`, `bytes_out`, `segment_rolls`, `group_commits`, `append_duration`, `read_duration`, `wal_flushes`, `wal_flush_duration`, `wal_pending` | `enqueues`, `dequeues`, `acks`, `nacks`, `extends`, `dlq_acks`, `enqueue_duration`, `dequeue_wait` |

### 3.2 Retention Arbitration Between Drivers

```
logqueue.TruncateFloor   ──┐
                           ├── retention.Coordinator.ReconcileQueueAt(ctx, name, requested)
taskqueue.TruncateFloor  ──┘            │
                                        ▼
                            min(floors)  clamped to ≤ requested
                                        │
                            if safeFloor == 0 → no-op
                                        │
                            engine.Storage.TruncateBefore(safeFloor)
```

The coordinator holds a `Policy{Age, Bytes}` per queue. A policy shrink (`SetPolicy` reports it) triggers an immediate out‑of‑band reconcile, bounded by a 5‑minute context. Driver floors **cap** truncation but never mandate it: without a policy or an explicit `requested` floor, nothing is deleted.

### 3.3 The Auth Boundary

Authentication sits between the transport and dispatch. It is deliberately not a driver concern: the task and log drivers never see a credential, and the wire server never sees a stored token—only the `authMode.Verify` contract:

```
client dial ──► OpAuth(method, credential) ──► server serveConn
                                                    │
                                                    ▼
                                          authMode.Verify(method, cred, tlsState)
                                                    │
                                        ┌───────────┴───────────┐
                                        ▼                       ▼
                                    OpAuth ack               OpError(0x09)
                                        │
                                        ▼
                                     dispatch
```

The client presents an mTLS claim implicitly when its `tls.Config` carries a cert, a token claim when the session config carries a token or token file, and `none` otherwise. `internal/server/tls.go` is the only place that turns a session into a `tls.Config` that can satisfy an `mtls` verifier.

---

## 4. The Configuration Singularity — `internal/spec`

Every configurable value in mikyu is one row of `spec.Table`:

```go
type Param struct {
    Key        string   // canonical flat key: identity of env, flag, option, patch
    Path       string   // nested location, e.g. "engine.batch_size"
    Kind       Kind     // int | uint32 | duration | bytes | bool | string | timestamp
    Layer      Layer    // call | spec | session
    Scope      Scope    // client | server | both (session layer only)
    Mutability Mutability // mutable | create-only | not-applicable
    EnvName    string   // MIKYU_<KEY>
    FlagName   string   // <key-with-dashes>
    OptionName string   // WithPascalCase
    Apply        func(*Spec, Value)
    ApplySession func(*Session, Value)
}
```

Everything downstream is driven from the table:

- **SDK options** (`WithBatchSize`, `WithSegmentSize`, `WithMetricsEnabled`, …) are `OptionName` string‑matched in `internal/spec/parity_test.go` and `mikyu/surface_test.go`.
- **CLI flags** (`--batch-size`, `--segment-size`, `--metrics-listen`) come from `FlagName`.
- **Environment variables** (`MIKYU_BATCH_SIZE`, `MIKYU_METRICS_LISTEN`) come from `EnvName`.
- **YAML keys** (`defaults.engine.batch_size`, `server.metrics_listen`) come from `Path`.
- **Wire patches** are the JSON encoding of a sparse `Spec`.
- **`queues.json`** stores the same sparse `Spec` shape.
- **`update` flags** are filtered to `spec.Updatable()`; attempting a create‑only key returns `spec.ErrImmutableField` naming the key.

### 4.1 Layers

| Layer | Where it can be set | Persistence |
|---|---|---|
| `LayerCall` | Per call (`WithVisibility`, `WithRetryAfter`, `WithMaxAttempts`, `WithScheduledAt`) | Never persisted. Not a `Spec` field. No env var. |
| `LayerSpec` | Per queue (`CreateQueue`/`UpdateQueue` options, `defaults:` block in YAML, `MIKYU_*` env) | `queues.json` and wire JSON. Has `Spec` fields. |
| `LayerSession` | Per process / session (`--dir`, `--target`, `MIKYU_TARGET`, `--metrics-listen`, `--auth-method`, etc.) | `mikyu.yaml` `server:` block, or env. Never reaches the wire queue spec. |

### 4.2 Effective Spec Resolution

```
effective = stored.Spec.Merge(session_defaults).Merge(spec.Builtin())
```

- `stored.Spec` is the per‑queue sparse spec from `queues.json`.
- `session_defaults` is the process's `WithSessionSpec` (or the YAML `defaults:` block, or env overrides).
- `spec.Builtin()` is the total built‑in layer.

`DescribeQueue` returns **all three spec layers**: the stored spec, the effective spec, and the session defaults it inherits. Live storage stats no longer live on `DescribeQueue`; they are observed through `Metrics`. Tests assert that embedded, remote, CLI, and YAML defaults surfaces all produce identical effective JSON.

### 4.3 Built‑ins (One Source of Truth)

| Key | Value |
|---|---|
| `engine.batch_size` | `256` |
| `engine.max_interval` | `10ms` |
| `engine.min_interval` | `30µs` |
| `engine.segment_size` | `64 MiB` |
| `engine.segment_age` | `1h` |
| `engine.max_message_bytes` | `1 MiB` |
| `engine.encrypted` | `false` |
| `engine.fsync` | `true` |
| `retention.age` / `retention.bytes` | `0` (unlimited) |
| `task.default_visibility` | `30s` |
| `task.default_max_attempts` | `5` |
| `server.target` | `tcp://127.0.0.1:7420` |
| `server.data_dir` | `./data` |
| `server.max_conns` | `10` |
| `server.auth_method` | `none` |
| `server.auth_token_file` | `""` |
| `server.auth_token` | `""` |
| `server.tls_client_ca_file` | `""` |
| `server.metrics_enabled` | `true` |
| `server.metrics_interval` | `1s` |
| `server.metrics_history` | `300` |
| `server.metrics_listen` | `""` (HTTP endpoint off) |
| `server.metrics_per_queue_prometheus` | `false` |
| `server.metrics_histograms` | `true` |

`Spec.Builtin()` and `Spec.BuiltinSession()` are total: every `LayerSpec`/`LayerSession` field is set. `spec.Spec.Effective` is therefore always a fully resolved spec.

### 4.4 Sparse Encoding

All `Spec` fields are pointers. `EncodeSpec` omits unset fields (`{"engine":{"fsync":true}}`), and `DecodeSpec` uses `DisallowUnknownFields` + `MaxSpecPayload = 1 MiB` + trailing‑data rejection. Same schema for JSON (wire) and YAML (config), enforced by `TestSpecTagsIdentical`.

### 4.5 Schema Publication

The table is a first‑class SDK artifact. Three surfaces publish it:

- **SDK.** `mikyu.Schema()` returns a fresh copy of the table; `mikyu.Lookup(key)` finds one row by canonical key. `mikyu.Param`, `mikyu.Kind`, `mikyu.Layer`, `mikyu.Scope`, and `mikyu.Session` are aliases for the corresponding `internal/spec` types, and every `Kind*`, `Layer*`, and `Scope*` constant is re‑exported. `mikyu.EncodeSpec`/`DecodeSpec` (JSON) and `mikyu.EncodeSpecYAML`/`DecodeSpecYAML` (YAML) round‑trip a sparse `Spec` with the same rejection rules as the wire codec. `mikyu.ValidateCreate`, `mikyu.ValidateUpdate`, and `mikyu.ValidateSession` are the exported validators; `mikyu.Builtin()` and `mikyu.BuiltinSession()` return the total built‑in layers.
- **Wire.** `OpSchema` (0x08) returns `u16 specVersion bytes(json []Param)`, the same envelope as `OpDescribeQueue` and `OpMetrics`. `Client.Schema(ctx)` is the cross‑mode façade: embedded returns a copy of `spec.Table`, remote round‑trips `OpSchema`. `TestSchemaOverWire` requires the two to agree field for field.
- **Codec.** The JSON and YAML encoders share the same struct tags; `TestJSONIsValidYAML` proves anything the wire sends is readable by the YAML config path.

The net effect: a client can discover what a server accepts, decode a stored spec, and validate a patch without linking `internal/spec`.

### 4.6 Session Validation

`spec.ValidateSession` is the only place session‑level invariants are checked. Auth is one of them:

- `max_conns` must be ≥ 1.
- `metrics_interval` must be positive when metrics are enabled; `metrics_history` ≥ 1.
- `metrics_listen`, when set, must be a `host:port`.
- `auth_method` must be `""`, `none`, `none-insecure`, `token`, or `mtls`.
- `token` mode requires `auth_token` or `auth_token_file`; a token shorter than 16 bytes is rejected.
- `mtls` requires `tls_enabled`.

`node.Acquire` and `config.Load` both call it, so a session that fails validation never reaches the wire server.

---

## 5. Repository Layout

```
github.com/dmunish/mikyu/
├── .go-arch-lint.yml               # Enforced package graph
├── mikyu.go                        # Public SDK facade
├── options.go                      # Re-exported option constructors
├── spec.go                         # Schema table, codec, validators
├── typed.go                        # Typed[T] log wrapper + Codec[T]
├── mikyu_test.go / surface_test.go # SDK surface contract
├── schema_test.go                  # Schema surface contract
│
├── cmd/mikyu/main.go               # Single-binary entrypoint
│
└── internal/
    ├── audit/                      # Source-code gates (no legacy names, comment style)
    │
    ├── spec/                       # THE config schema
    │   ├── spec.go                 # Spec, EngineSpec, RetentionSpec, TaskSpec, Session
    │   ├── table.go                # Table, Param, Kind, Layer, Mutability, Value
    │   ├── codec.go                # Duration, Encode/DecodeSpec, MaxSpecPayload
    │   ├── env.go                  # ApplyEnv / ApplyEnvSession
    │   ├── errors.go               # ErrInvalidConfig, ErrImmutableField, ...
    │   └── validate.go             # ValidateCreate / ValidateUpdate / ValidateSession
    │
    ├── config/                     # mikyu.yaml + env loading
    │   └── config.go               # Load, DefaultConfig, legacy-key errors
    │
    ├── engine/                     # Storage engine (see §6)
    │   ├── storage.go  manager.go  queue.go  wal.go  segment.go
    │   ├── index.go  encode.go  cache.go  offsets.go  compact.go
    │   ├── read.go  append.go  fetch.go  offset_api.go  message.go  config.go
    │   ├── metrics.go              # Per-queue instruments + Manager.Collect
    │   ├── sync_{darwin,fdatasync,windows}.go
    │   └── encrypt_{linux,windows,other}.go
    │
    ├── metrics/                    # THE telemetry model (see §7)
    │   ├── doc.go                  # Package one-liner
    │   ├── instruments.go          # Counter, Gauge, Histogram
    │   ├── registry.go             # Registry, Scope
    │   ├── snapshot.go             # Snapshot, QueueView, Rates, TotalsView
    │   ├── history.go              # RingBuffer[T]
    │   ├── source.go               # Source / SourceFunc
    │   ├── collector.go            # Collector (tick, subscribe, merge scopes)
    │   └── prometheus.go           # Prometheus text exposition
    │
    ├── enginetest/memstorage.go    # In-memory engine.Storage for synctest bubbles
    │
    ├── queue/
    │   ├── group/group.go          # Base: HighWater, Mark, Commit, Close
    │   ├── logqueue/               # Log driver (api.go, logqueue.go, group.go,
    │   │                            # retention.go, stream.go)
    │   ├── taskqueue/              # Task driver (taskqueue.go, task.go, enqueue.go,
    │   │                            # dequeue.go, ack.go, nack.go, extend.go, dlq.go,
    │   │                            # lease.go, scheduler.go, checkpoint.go,
    │   │                            # compaction.go, metrics.go, id.go)
    │   └── retention/coordinator.go # Policy, QueueTarget, Coordinator
    │
    ├── transport/
    │   ├── protocol/               # frames.go, bytes.go, opcodes.go, errors.go,
    │   │                            # auth.go, task.go, log.go, doc.go
    │   └── ipc/ipc.go              # Unix socket / named pipe listener & dialer
    │
    ├── node/                       # Process-wide shared local node
    │   ├── node.go                 # Node: manager + drivers + retention + registry
    │   ├── metrics.go              # Collector wiring, preregistration, MetricsConfig
    │   ├── runtime_source.go       # RuntimeSource (goroutines, heap, CPU, RSS)
    │   ├── runtime_source_{unix,windows}.go
    │   ├── pool.go                 # Acquire / refcount / ErrSessionDefaultsMismatch
    │   ├── registry.go             # queues.json load/save/reconcile
    │   ├── lock.go  lock_unix.go  lock_windows.go
    │
    ├── server/                     # Wire server
    │   ├── server.go  dispatch.go  errors.go
    │   ├── auth.go                 # authMode: NewAuthMode, Verify
    │   ├── tls.go                  # DefaultTLSConfig (client CA aware)
    │   ├── metrics.go              # Per-op counters + histograms
    │   └── handlers_admin.go  handlers_log.go  handlers_task.go
    │
    ├── client/                     # Embedded and remote clients
    │   ├── embedded.go  embedded_queues.go  options.go  errors.go  types.go
    │   ├── remote.go  remote_logqueue.go  remote_taskqueue.go  remote_group.go
    │
    ├── daemon/                     # Daemon lifecycle
    │   ├── daemon.go  detach_unix.go  detach_windows.go
    │   └── metrics_http.go         # /metrics, /api/metrics, /api/history, /api/events
    │
    └── ui/
        ├── command/                # Shared verb + flag helpers (command.go, task.go,
        │                            # flags.go, verbs.go, auth.go)
        ├── cli/                    # One-shot CLI (cli.go, task.go)
        └── repl/                   # Interactive REPL (repl.go, task.go, style.go,
                                     # vt_{other,windows}.go)
```

The package graph is enforced by `.go-arch-lint.yml`; the allowed dependency edges are:

```
spec, engine, protocol, group, enginetest, metrics  → bottoms
ipc                                                 → protocol
logqueue                                            → engine, group
taskqueue, retention                                → engine
node                                                → spec, engine, metrics, logqueue, taskqueue, retention
client, server                                      → spec, engine, metrics, node, {log,task}queue, ipc, protocol
config                                              → spec
daemon                                              → spec, config, engine, metrics, node, server, protocol
mikyu                                               → spec, client, engine, metrics, node, logqueue, taskqueue
command                                             → mikyu, client, daemon, spec
cli, repl                                           → mikyu, client, daemon, command, spec
cmd                                                 → mikyu, daemon, cli, repl
```

`server` owns the auth verifier; `client` owns the credential resolution. Neither imports the other.

---

## 6. Storage Engine — `internal/engine`

### 6.1 `Storage` Interface

```go
type Storage interface {
    Name() string
    Append(ctx context.Context, payload []byte) (uint64, error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
    Read(ctx context.Context, offset uint64) (Message, error)
    ReadBatch(ctx context.Context, start uint64, n int) ([]Message, error)
    Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Message, error)
    LastOffset() uint64
    CommitOffset(ctx context.Context, group string, offset uint64) error
    Committed(ctx context.Context, group string) (uint64, error)
    MaxAppendPayloadBytes() int64
    MinOffset() uint64
    Segments() []SegmentInfo
    TruncateBefore(ctx context.Context, offset uint64) (int64, error)
    FloorOlderThan(ctx context.Context, olderThan time.Time) (uint64, error)
    FloorForSize(ctx context.Context, maxBytes int64) (uint64, error)
}
```

`Fetch` is the streaming primitive both `logqueue.Stream` and `logqueue.Tail` build on; it is the only read method on `Storage` that blocks. It is not the only observation point: `Manager.Collect` fills a `metrics.Snapshot` from the same `Queue` state under the same lock (§6.11).

### 6.2 `Config` (per queue)

```go
type Config struct {
    BatchSize       int
    MaxInterval     time.Duration
    MinInterval     time.Duration
    SegmentSize     int64
    MaxMessageBytes int64
    SegmentAge      time.Duration
    Encrypted       bool
    Fsync           bool
}
```

`withDefaults()` fills zeros: `BatchSize=256`, `MaxInterval=10ms`, `MinInterval=30µs` (negative clamps to 0 = "no linger"), `SegmentSize=64 MiB`, `MaxMessageBytes=1 MiB`, `SegmentAge=1h`. `ApplyConfig` merges mutable fields into a live queue and calls `wal.UpdateConfig`. Create‑only fields (`SegmentSize`, `MaxMessageBytes`, `Encrypted`, `Fsync`) are silently ignored on the live path.

### 6.3 `Manager`

Owns a data directory, an `*os.Root` scoped to it, one shared `*OffsetStore`, a `map[string]*Queue`, and an optional `*metrics.Registry`.

- `OpenManager(dir, cfg, perQueue, regs...)` — creates the dir, opens the offset store, walks subdirectories, opens each queue with its per‑queue config override and per‑queue scope.
- `CreateQueue(name, opts...)` — respects the per‑queue `Config` if given; errors with `ErrQueueExists` otherwise.
- `GetQueue`, `ListQueues` (sorted), `DeleteQueue` (close + `root.RemoveAll`).
- `Collect(ctx, *metrics.Snapshot)` — walks every live queue under `RLock` and fills the pull‑based gauges (§6.11).

### 6.4 `Queue`

```go
type Queue struct {
    name              string
    dir               string
    cfg               Config
    active            *segment
    segments          []*segment
    nextOffset        uint64   // next logical offset to assign
    minOffset         uint64   // lowest readable offset on disk
    publishedOffset   uint64   // high-water mark visible to readers
    w                 *wal
    offsets           *OffsetStore
    cache             *ringCache
    nextPreallocBytes int64
    closed            bool
    notify            chan struct{}
    m                 *queueMetrics
    scope             *metrics.Scope
    mu                sync.RWMutex
}
```

`openQueue` creates the queue directory **through `os.Root` scoped to the parent**, so a hostile name cannot escape the data dir. All file I/O in `segment` uses the same mechanism.

#### 6.4.1 Append & AppendBatch

`Append` delegates to `AppendBatch`. The batch path stages per‑segment sub‑batches so a WAL write never straddles a roll:

1. Validate `len(payload) <= MaxMessageBytes`; encode with `uuid.NewV7()` and a shared timestamp; reject records larger than `SegmentSize` with `ErrRecordTooLarge`.
2. Stage into a `stagedBatch{w, seg, recs, positions, recLens, offsets, ids, totalLen, startIdx}`.
3. On roll (size or age), commit the staged group through either:
   - **`commitSync`** (`Fsync: true`): `wal.pushBatch` publishes sequence numbers; the caller unlocks and waits on `wal.wait(seq)` (or delivers the wake token), then `roll()`s and starts a new group.
   - **`commitAsync`** (`Fsync: false`): clone payloads into the ring cache, publish records on `wal.asyncChan`, advance `publishedOffset`, then `roll()`.
4. Final staged group is committed after the loop.

#### 6.4.2 Read & ReadBatch

`Read`:

1. `q.cache.Get(offset)` — hit returns the shallow message.
2. `q.locateSegment(offset)` — `slices.BinarySearchFunc` over `q.segments` base offsets (O(log S)). Handles `offset < minOffset` (`ErrOffsetTruncated`), `offset ≥ publishedOffset` (`ErrOffsetNotFound`).
3. `q.readFromSegment(seg, offset)` — reads from `seg.mmapData` when mapped, else `seg.logFile.ReadAt`; `decodeRecord` validates CRC and length.

`ReadBatch` walks offsets in order, reusing the current segment pointer between consecutive offsets and short‑circuiting to `locateSegment` only on boundary crossings.

#### 6.4.3 Fetch

`Fetch` is the blocking, notify‑driven read primitive that `logqueue.Stream` and `logqueue.Tail` sit on. It is a single loop over the queue's `notify` channel:

1. Snapshot `closed`, `minOffset`, `publishedOffset`, and the current `notify` channel under `RLock`.
2. `closed` → `ErrQueueClosed`; `offset < minOffset` → `ErrOffsetTruncated`.
3. `offset < publishedOffset` → delegate to `ReadBatch(ctx, offset, maxCount)` and return.
4. No data and `maxWait <= 0` → `ErrNoData`.
5. Otherwise arm a timer for the remaining budget and block on `{ctx.Done(), notify, timer.C}`. A notify wake means an `AppendBatch` reached the publish point and the loop re‑checks; a timer fire returns `ErrNoData`; ctx cancellation returns `ctx.Err()`.

`notify` is broadcast by `broadcastLocked()` — called by both the sync and async `AppendBatch` paths after `publishedOffset` advances — so a parked reader wakes the instant a new record is visible. `Fetch` never touches `segments` directly; it hands off to `ReadBatch` once data is present, which keeps the mmap/read logic in one place.

#### 6.4.4 Segment Rolling & Preallocation

`nextPreallocBytes` starts at `min(8 MiB, SegmentSize)` and adapts inside `roll()`:

- Segment filled in under `fastFillThreshold` (10 s) → double, capped at `min(256 MiB, SegmentSize)`.
- Segment filled after `slowFillThreshold` (5 m) → halve, floored at the 8 MiB min.
- The active segment is truncated to its written `pos` before `enableMMap`.

This keeps small queues small and large queues fast.

#### 6.4.5 Close & ApplyConfig

`Close` stops the WAL, closes every segment, marks `closed`. `ApplyConfig` updates `BatchSize`, `MaxInterval`, `MinInterval`, `SegmentAge` on the live queue and pushes the flusher config via `wal.UpdateConfig`.

### 6.5 WAL — `wal.go`

```go
type wal struct {
    cfg  Config
    file *os.File

    mu          sync.Mutex
    cond        *sync.Cond
    buf         []byte
    pending     []pendingEntry
    swapBuf     []byte
    swapPending []pendingEntry
    pushed      int64
    published   int64
    err         error

    asyncChan     chan asyncEntry
    wake          chan struct{}
    configChanged chan struct{}
    shutdown      chan struct{}
    done          chan struct{}
    stopOnce      sync.Once

    m *walMetrics
}
```

**Sync path.** `push`/`pushBatch` stage bytes + pending entries into `buf`/`pending` under lock; the flusher (`syncLoop`) selects over `asyncChan`, `ticker.C` (`MaxInterval`), `wake`, `configChanged`, `shutdown`. `adaptiveFlush` sleeps `MinInterval` before calling `flush()` when the current batch is below `BatchSize`. `flush` swaps `buf ↔ swapBuf` and `pending ↔ swapPending`, drains the wake token under lock, calls `file.WriteAt`, and (if `Fsync`) `syncData`; then broadcasts on `cond` and reclaims the swap buffers. Producers `wait(seq)` on `cond`.

**Async path.** `queue.AppendBatch` writes records onto `wal.asyncChan` and clones into the ring cache. `drainAndFlushAsync(first)` groups up to `BatchSize` entries, writes each record at its recorded `pos`, writes the corresponding `.idx` entry, and advances `published` contiguously.

**Wake‑token correctness.** The `wake` channel has buffer 1 and is drained inside `flush()` under lock, preventing producer hangs when `adaptiveFlush` is mid‑linger.

**Metrics.** The WAL holds a `walMetrics{flushes, flushDur, pending}` cached from its scope. `flush` and `drainAndFlushAsync` increment `flushes` and observe `flushDur`; `push`/`pushBatch` set `pending` to the current batch length; `flush` also resets it. The steady state costs one atomic store per flush.

**Platform `syncData`:**

| OS | Call |
|---|---|
| Darwin | `unix.FcntlInt(fd, unix.F_FULLFSYNC, 0)` (drive cache) |
| Linux/BSD | `unix.Fdatasync(fd)` |
| Windows | `file.Sync()` |

### 6.6 Segment — `segment.go`

Fields: `.log` file handle, `.idx` file handle, `pos`, `preallocSize`, `createdAt`, `mmapData`, `idxMMap`.

- `openSegment(dir, baseOff, preallocSize)` preallocates the `.log` file if empty.
- `enableMMap()` maps both files read‑only **only if `Stat().Size() > 0`** (Windows robustness).
- `recover()` tries `tryFastRecovery` (validates last index entry, copies raw entries); on failure, `rebuild()` re‑walks the `.log` file, validates each CRC‑checked record, rewrites `.idx`, truncates torn tails.
- `readIndexEntry(offset)` reads 16 bytes from `idxMMap` (closed) or `idxFile.ReadAt`.
- `lastTimestamp()` returns the timestamp of the newest record (used by `FloorOlderThan`).
- `destroy()` unmaps, closes, and unlinks both files through an `os.Root`.
- `Close()` truncates the active `.log` to `pos`, unmaps, closes.

### 6.7 Record Format — `encode.go`

```
[4B Body Length][4B CRC32][2B ID Length (=16)][16B Raw UUID v7][8B Unix Nanoseconds][Payload]
```

CRC‑32/IEEE covers the body (`rec[8:]`). `minRecordSize = 34`. `decodeRecord` returns `(id string, ts time.Time, payload []byte, err error)`.

### 6.8 Ring Cache — `cache.go`

`slots [16384]Message`, indexed by `offset & (capacity-1)` under `sync.RWMutex`. `EvictBefore(minOffset)` zeros slots below the new floor. `Get(offset)` validates `msg.Offset == offset` and non‑zero timestamp to defend against stale slots.

### 6.9 Offset Store — `offsets.go`

One `manager-offsets.dat` file persists `(queue:group) → uint64` as `[2B keyLen][key][8B offset]` tuples. `recover()` streams valid records and stops on torn tails. `Commit` writes, flushes, `Sync`s, and every `compactAppendsThreshold = 5000` appends triggers `compactLocked`, which rewrites the file atomically via `os.Rename` from `manager-offsets.dat.tmp`.

### 6.10 Retention Primitives — `compact.go`

- `MinOffset()`, `Segments()` return snapshots.
- `FloorOlderThan(ctx, olderThan)` walks **closed** segments oldest → newest; a segment qualifies only if its last record is strictly before `olderThan`. The active segment is never included.
- `FloorForSize(ctx, maxBytes)` drops whole oldest closed segments while total size exceeds `maxBytes`.
- `TruncateBefore(ctx, targetOffset)` unlinks closed segments with `nextOffset <= targetOffset`; stops at the first straddling segment; updates `minOffset`; calls `cache.EvictBefore`.

### 6.11 Queue Metrics — `metrics.go`

Every `Queue` holds a `queueMetrics` and a `*metrics.Scope` (both optional — a manager without a registry yields nil‑safe instruments that no‑op):

```go
type queueMetrics struct {
    appends      *metrics.Counter
    reads        *metrics.Counter
    bytesIn      *metrics.Counter
    bytesOut     *metrics.Counter
    segmentRolls *metrics.Counter
    groupCommits *metrics.Counter
    appendDur    *metrics.Histogram
    readDur      *metrics.Histogram
}

type walMetrics struct {
    flushes  *metrics.Counter
    flushDur *metrics.Histogram
    pending  *metrics.Gauge
}
```

`recordAppend` / `recordRead` update the counters and histograms on the hot path with one atomic op per instrument. `roll()` bumps `segmentRolls`. `CommitOffset` bumps `groupCommits`.

**Pull‑based gauges** are produced by `Manager.Collect`:

```go
func (m *Manager) Collect(_ context.Context, dst *metrics.Snapshot) {
    m.mu.RLock()
    defer m.mu.RUnlock()
    for _, q := range m.queues {
        q.mu.RLock()
        dst.Queues = append(dst.Queues, metrics.QueueView{
            Name: q.name, Kind: "shared",
            Gauges: map[string]int64{
                GaugeMessages:   int64(q.publishedOffset),
                GaugeSegments:   int64(len(q.segments)),
                GaugeSizeBytes:  q.onDiskSizeLocked(),
                GaugeLastOffset: int64(q.nextOffset),
                GaugeMinOffset:  int64(q.minOffset),
            },
        })
        q.mu.RUnlock()
    }
}
```

Gauge names: `messages`, `segments`, `size_bytes`, `last_offset`, `min_offset`. The `Metrics` source is registered on the node's collector (see §11), so every snapshot the wire, CLI, or HTTP endpoints see is materialized on demand from the live queues.

### 6.12 Kernel Directory Encryption — `encrypt_*.go`

| OS | Mechanism |
|---|---|
| Linux | `SYS_IOCTL` with `FS_IOC_SET_ENCRYPTION_POLICY`; `AES_256_XTS` contents, `AES_256_CTS` filenames. Tolerates `ENOTTY`, `EOPNOTSUPP`, `EEXIST`. |
| Windows | `EncryptFileW` from `advapi32.dll` (NTFS EFS). |
| Other (macOS/BSD) | Logs a warning and returns `nil`. |

---

## 7. Telemetry — `internal/metrics`

`internal/metrics` is the single source of truth for live Mikyu telemetry. It is a bottom‑adjacent package: `engine`, `server`, `node`, `daemon`, and both queue drivers import it, and it imports nothing from mikyu except `spec` (for the `Spec` fields inside a queue view).

### 7.1 Instruments — `instruments.go`

| Type | Semantics |
|---|---|
| `Counter` | Monotonically increasing `atomic.Int64`; `Add`, `Inc`, `Load`. |
| `Gauge` | Direction‑agnostic `atomic.Int64`; `Set`, `Add`, `Load`. |
| `Histogram` | Fixed upper bounds, one `atomic.Uint64` bucket each, plus `sum` (CAS'd `math.Float64bits`) and `total`. `Observe`, `Snapshot`. |

All three are **nil‑safe**: a nil receiver no‑ops, so a queue built without a metrics registry pays exactly one branch per instrument call.

`HistogramSnapshot` is an immutable copy: `Bounds []float64`, `Counts []uint64`, `Sum float64`, `Total uint64`.

### 7.2 Registry & Scope — `registry.go`

```go
type Registry struct {
    mu     sync.RWMutex
    cfg    Config
    scopes map[string]*Scope
}

type Scope struct {
    name       string
    mu         sync.Mutex
    counters   map[string]*Counter
    gauges     map[string]*Gauge
    histograms map[string]*Histogram
}
```

`Registry.Scope(name)` returns the named scope, creating it on first use (double‑checked). `Registry.Scopes()` yields every scope sorted by name. `Scope.Counter(name)`, `Gauge(name)`, and `Histogram(name, bounds)` are the only way to obtain instruments, and each caches its instance so identity is stable across calls. `Scope.read()` copies every reading for the collector — the only place a scope is read.

Scopes are named for the queue they instrument (`q.name`), plus two reserved names: `"server"` for the wire server's per‑op counters (preregistered by the node, §11) and `"shared"` for engine queues that a snapshot merged without a source‑supplied kind.

### 7.3 Snapshot Model — `snapshot.go`

```go
type Snapshot struct {
    Disabled  bool        `json:"disabled,omitempty"`
    Timestamp time.Time   `json:"timestamp"`
    Runtime   RuntimeView `json:"runtime"`
    Process   ProcessView `json:"process"`
    Storage   StorageView `json:"storage"`
    Queues    []QueueView `json:"queues"`
    Totals    TotalsView  `json:"totals"`
}
```

- `RuntimeView` — `goroutines`, `heap_alloc`, `heap_inuse`, `gc_count`, `uptime_seconds`.
- `ProcessView` — `cpu_percent`, `rss_bytes`.
- `StorageView` — `data_dir`, `segments`, `size_bytes` (aggregate).
- `QueueView` — `name`, `kind`, `spec`, `effective`, `counters`, `gauges`, `histograms`, `groups`, `rates`.
- `GroupView` — `name`, `committed`, `high_water`, `lag`.
- `Rates` — `appends_per_sec`, `reads_per_sec`, `enqueues_per_sec`, `dequeues_per_sec`, `bytes_in_per_sec`, `bytes_out_per_sec`.
- `TotalsView` — `queues`, `messages`, `bytes`.

`Snapshot.IterQueues()` yields queues in name order; `Snapshot.Queue(name)` does the O(n) lookup.

### 7.4 Sources — `source.go`

```go
type Source interface {
    Collect(ctx context.Context, dst *Snapshot)
}

type SourceFunc func(context.Context, *Snapshot)
```

A source is a pull‑based producer of snapshot fields. The `*engine.Manager` implements `Source` (§6.11); the node also registers a `RuntimeSource` (§11).

### 7.5 Collector — `collector.go`

```go
type Config struct {
    Interval   time.Duration // ticker cadence; <= 0 → 1s
    History    int           // ring depth; < 1 → 1
    Histograms bool          // include histogram snapshots
    Disabled   bool          // skip the ticker and return closed subscriptions
}

type Collector struct {
    reg     *Registry
    cfg     Config
    sources []Source

    latest  atomic.Pointer[Snapshot]
    history *RingBuffer[Snapshot]

    mu       sync.Mutex
    subs     map[int]*subscriber
    nextSub  int
    started  bool
    stopped  bool
    stopOnce sync.Once
    stop     chan struct{}
    wg       sync.WaitGroup
}
```

- `New(reg, cfg, sources...)` — the collector's interval defaults to 1s, its history to 1.
- `Start(ctx)` — idempotent; a disabled collector is a no‑op.
- `Stop()` — idempotent and safe before `Start`; closes every subscriber channel.
- `Snapshot(ctx)` — builds a fresh snapshot, records it as `latest`, and returns it. Never touches history or notifies subscribers.
- `History()` — buffered tick snapshots, oldest first.
- `Subscribe()` — a channel of at most one buffered tick snapshot, plus a cancel func. A slow subscriber drops extra frames (logged once at `Debug`).
- `build(ctx)` — runs every source, merges scope readings (`mergeScopes`), computes rates against the previous snapshot (`computeRates`), sorts queues by name, and totals them (`totalize`).
- `mergeScopes` — a source may already have appended a queue view for a name (the engine does); its scopes are merged *into* that view rather than replacing it, so a source's gauges survive.
- `computeRates` — derives `Rates` for each queue from the previous snapshot's counters and the elapsed wall time.

### 7.6 History — `history.go`

`RingBuffer[T]` is a fixed‑capacity, concurrency‑safe FIFO used for the collector's snapshot history. `Push` evicts the oldest when full; `Snapshot` copies the buffer oldest‑first; `Latest` returns the newest; `Len` reports occupancy.

### 7.7 Prometheus — `prometheus.go`

```go
type EncodeOptions struct {
    PerQueue   bool
    TopNQueues int   // 0 → all queues
    Histograms bool
}

func Encode(w io.Writer, s Snapshot, opts EncodeOptions) error
```

Emits the Prometheus text exposition format 0.0.4. Global series (`mikyu_goroutines`, `mikyu_heap_alloc_bytes`, `mikyu_heap_inuse_bytes`, `mikyu_gc_count_total`, `mikyu_uptime_seconds`, `mikyu_process_cpu_percent`, `mikyu_process_rss_bytes`, `mikyu_queues`, `mikyu_messages`, `mikyu_bytes`) are always present. With `PerQueue`, each queue is rendered as labeled series (`queue="<name>"`), with `TopNQueues` bounding the cardinality by largest queues first. Known keys are mapped to idiomatic names via `gaugeNames`, `counterNames`, `histogramNames` (e.g. `size_bytes` → `mikyu_queue_bytes`, `appends` → `mikyu_append_total`, `append_duration` → `mikyu_append_duration_seconds`); unknown keys are sanitized into `mikyu_*`. Histograms render as `_bucket` (with `le` labels), `_sum`, and `_count`.

---

## 8. Log Queue Driver — `internal/queue/logqueue`

### 8.1 Interface — `api.go`

```go
type ClientLogQueue interface {
    Name() string
    Append(ctx context.Context, payload []byte) (uint64, error)
    AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
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

The interface deliberately exposes only `engine.Message`, not `engine.Storage`, keeping the engine internal. `LogQueue.Fetch` exists on the concrete type and is used by the server's `OpFetch` handler; it is not part of the client interface because remote callers reach it through `Tail`/`Stream`. `LogQueue` does not expose a `Metrics` method — telemetry is observed through the client's `Metrics`, never per queue handle.

### 8.2 Behavior — `logqueue.go` & `stream.go`

- `LogQueue` holds a `map[string]*queueGroup` for cached consumer groups.
- **`Fetch(ctx, offset, maxCount, maxWait)`** is the streaming primitive. It returns `ErrOffsetTruncated` when `offset < MinOffset()`, returns up to `maxCount` records when `offset < publishedOffset`, blocks on the queue's `notify` channel for up to `maxWait` when the reader has caught up, and returns `ErrNoData` when the wait elapses with nothing new. `LogQueue.Fetch` is a thin delegate to `storage.Fetch`.
- **`Stream(ctx, startOffset)`** drains every existing record from `startOffset` and returns on `ErrNoData`. It passes `maxWait=0`, which turns `Fetch` into a strictly non‑blocking drain: the first empty iteration ends the iterator. Any other error is yielded once and stops the iterator.
- **`Tail(ctx, startOffset)`** yields existing records, then blocks in `Fetch` for up to `tailWait = 30 s` per iteration; `ErrNoData` simply re‑arms the wait, so an idle tail costs one long‑poll per 30 s window instead of the previous 5 ms polling loop. A `ctx` cancellation is yielded once and stops the iterator. `notify` is broadcast on every `AppendBatch` publish, so a tail wakes the instant new records become visible; the queue stays alive across in‑flight WAL writes because the wait is signal‑driven, not offset‑polled. This is tested by `TestLogQueueTailBugProof`, `TestLogQueueTailContinuous`, and `TestTailBatchDeliversAll` under `testing/synctest`.
- `Group(name)` returns a cached `*queueGroup` (a `group.Base` with `CommitFn` bound to `LogQueue.CommitOffset`).

### 8.3 Retention — `retention.go`

```go
func (q *LogQueue) TruncateFloor(ctx context.Context) (uint64, bool, error)
func (q *LogQueue) Truncate(ctx context.Context, minOffset uint64) (int64, error)
```

`TruncateFloor` returns `min(HighWater across all groups)` and a `constrained` flag. No groups → `(0, false)`. When `Truncate(0)` is called with no explicit offset, the driver substitutes its own floor if constrained; otherwise the call is a no‑op.

---

## 9. Task Queue Driver — `internal/queue/taskqueue`

### 9.1 State — `taskqueue.go`

```go
type TaskQueue struct {
    name    string
    storage engine.Storage

    mu                sync.Mutex
    active            map[string]*taskState     // id → in-flight or scheduled task
    dlq               map[string]Task
    dlqUncheckpointed map[string]struct{}
    schedule          scheduleHeap              // min-heap by (due, seq)
    sequence          uint64                    // monotonic seq for tie-breaking
    scanNext          uint64                    // next offset to scan
    checkpointOffset  uint64                    // last committed checkpoint start
    dirty             uint64                    // ops since last checkpoint

    notify          chan struct{}               // broadcast on every state change
    stopCh          chan struct{}
    wg              sync.WaitGroup
    closing, closed bool

    defaultVisibility  time.Duration
    defaultMaxAttempts uint32

    m *taskMetrics
}
```

Lifecycle: `NewTaskQueue` opens the storage, calls `loadCheckpointLocked`, sets `scanNext = MinOffset()` if zero, calls `refreshLocked` to replay post‑checkpoint records, and starts `checkpointLoop` (5 s ticker). `Close` stops the loop, writes a final checkpoint if dirty, marks closed.

`SetDefaults(Defaults{Visibility, MaxAttempts})` updates the queue's defaults for future enqueues; in‑flight tasks keep their captured values.

### 9.2 Types — `task.go`

```go
type Task struct {
    ID          string
    Payload     []byte
    Attempts    uint32
    MaxAttempts uint32
    Visibility  time.Time
    ScheduledAt time.Time
    DeliveryID  string    // random per delivery; empty until first lease
    Offset      uint64    // engine offset of the task record
}

type TaskRef interface{ taskRef(); taskID() string }
type TaskID string       // bare ID → skip strict lease check
// Task also implements TaskRef → strict check on Offset+Attempts+DeliveryID
```

Defaults: `defaultVisibilityTimeout = 30s`, `defaultMaxAttempts = 5`, `defaultRetryDelay = 1s`, `defaultMaxRetryDelay = 5m`, `defaultCheckpointInterval = 5s`, `checkpointEvery = 128`, `checkpointChunkBytes = 64 KiB`.

Options:

- **`EnqueueOption`** — `WithVisibility(d)`, `WithMaxAttempts(n)`, `WithScheduledAt(t)`. Zero/unset values inherit queue defaults.
- **`NackOption`** — `WithRetryAfter(d)`.

`ResolveNackOptions` computes exponential backoff `defaultRetryDelay * 2^(Attempts-1)` capped at `defaultMaxRetryDelay`.

### 9.3 Enqueue — `enqueue.go`

Resolves options under lock, generates a 32‑char hex ID from `crypto/rand` (`id.go`), appends `encodeTaskRecord(id, maxAttempts, visibilityTimeout, scheduledAt, payload)`, installs a `taskState{task, nextAt: scheduledAt, visibilityTimeout}`, pushes onto the schedule, sets `scanNext = offset + 1`, increments `dirty`, broadcasts, and observes `enqueue_duration`.

### 9.4 Dequeue — `dequeue.go`

```go
func (q *TaskQueue) TryDequeue(ctx context.Context) (Task, error)      // one pass
func (q *TaskQueue) Dequeue(ctx context.Context) (Task, error)         // blocks
```

`Dequeue` loops on `takeDueLocked(now)`:

1. Pop stale entries (version mismatch or missing from `active`).
2. If the due task is in‑flight on its **final** attempt → dead‑letter: append `recordDLQ`, move to `dlq`, mark `dlqUncheckpointed`, broadcast, continue.
3. Otherwise assign a fresh `DeliveryID`, set `inFlight=true`, `Attempts++`, `Visibility = now + visibilityTimeout`, bump version, re‑push onto schedule, broadcast, return.

`Dequeue` parks on `notify` or a timer until `waitUntil`. Context cancellation returns `ctx.Err()`. Successful takes observe `dequeue_wait` with the time between the due moment and the actual delivery.

### 9.5 Lease Resolution — `lease.go`

`resolveLeaseLocked(ref)` is the single source of truth for lease validity:

- `nil` → `ErrInvalidTask`.
- Not in `active` → `ErrTaskNotFound`.
- Not `inFlight` → `ErrTaskNotInflight`.
- Visibility expired → `ErrTaskStale`.
- **Strict mode** (`ref` is `Task`): requires `Offset`, `Attempts`, and non‑empty `DeliveryID` to match exactly; mismatch → `ErrTaskStale`.
- **Loose mode** (`ref` is `TaskID`): skips the strict comparison; the visibility check still applies.

`broadcastLocked` closes the current `notify` channel and allocates a fresh one, waking every parked `Dequeue`.

### 9.6 Ack / Nack / Extend / DLQ

- **`ack.go`** — `resolveLeaseLocked`, delete from `active`, `dirty++`, broadcast, prune schedule, bump `acks`.
- **`nack.go`** — `resolveLeaseLocked`; if `Attempts >= MaxAttempts`, dead‑letter; otherwise reset `inFlight`, clear `DeliveryID`, set `nextAt = now + retryAfter`, bump version, re‑push, broadcast. Bumps `nacks` in both branches.
- **`extend.go`** — only extends `Visibility` forward (never shortens); never increments `Attempts`; bumps `extends`.
- **`dlq.go`** — `DeadLetters` returns an offset‑sorted snapshot; `AckDeadLetter` verifies `Offset`/`Attempts` when `ref` is a `Task` (else `ErrDeadLetterStale`), appends `recordDLQAck`, deletes, bumps `dlq_acks`.

### 9.7 Scheduler — `scheduler.go`

`scheduleHeap` is a binary min‑heap ordered by `(due, seq)`. `pushScheduleLocked` bumps `q.sequence` and pushes. `pruneScheduleLocked` rebuilds the heap when `len(schedule) > len(active)*3 + 1024`.

### 9.8 Checkpointing — `checkpoint.go`

Record kinds (8‑byte magic `MIKYUTQ\x01`, then one kind byte):

| Kind | Name | Payload |
|---|---|---|
| 1 | `recordTask` | id, maxAttempts, visibilityTimeout, scheduledAt, payload |
| 2 | `recordCPStart` | coveredThrough (u64), checkpointVersion (u32 = 2) |
| 3 | `recordCPChunk` | index (u32), data (bytes) |
| 4 | `recordCPCommit` | startOffset (u64), totalChunks (u32), coveredThrough (u64), version (u32) |
| 5 | `recordDLQ` | offset, id, attempts, maxAttempts, scheduledAt, payload |
| 6 | `recordDLQAck` | offset, id, attempts |

**`checkpointLocked`**:

1. `refreshLocked` to include foreign records appended since the last task op.
2. Snapshot `active` (sorted by offset) + `dlq` (sorted by offset) via `encodeSnapshot`.
3. Append `recordCPStart(coveredThrough)`.
4. Split the snapshot into ≤ `min(MaxAppendPayloadBytes() - header, checkpointChunkBytes)` chunks; append `recordCPChunk(i, data)` for each.
5. Append `recordCPCommit(startOffset, totalChunks, coveredThrough)`.
6. Verify `commitOffset == startOffset + totalChunks + 1`; on mismatch return `ErrCheckpointCorrupt`.
7. Clear `dlqUncheckpointed` and reset `dirty`.

If the queue is empty, one empty chunk is written so `totalChunks ≥ 1` always holds.

**`loadCheckpointLocked`** scans backward from `LastOffset()` for the newest valid commit record. It validates: commit bounds, chunk count ≤ `maxCheckpointChunks = 1<<20`, covered bytes ≤ `maxCheckpointBytes = 512 MiB`, chunk indices match, snapshot decodes cleanly. Any failure → try an earlier commit. Success → rebuild `active`, `dlq`, and set `scanNext = off + 1`.

**`refreshLocked`** walks `scanNext` forward. `recordTask`, `recordDLQ`, `recordDLQAck` are decoded; `recordCP*` are ignored; **anything else is treated as a foreign log record and skipped**. This is how log and task drivers share storage safely.

### 9.9 Metrics — `metrics.go`

`taskMetrics` mirrors the engine's per‑queue caching pattern:

```go
type taskMetrics struct {
    enqueues    *metrics.Counter
    dequeues    *metrics.Counter
    acks        *metrics.Counter
    nacks       *metrics.Counter
    extends     *metrics.Counter
    dlqAcks     *metrics.Counter
    enqueueDur  *metrics.Histogram
    dequeueWait *metrics.Histogram
}
```

`NewTaskQueue` accepts an optional `*metrics.Scope` and caches the instruments once. `recordEnqueue` and `recordDequeue` observe the histograms; `Ack`, `Nack`, `Extend`, and `AckDeadLetter` bump their counters inside the same critical section that mutates state, so a counter increment never diverges from a state change.

---

## 10. Cross‑Driver Retention — `internal/queue/retention`

```go
type DriverProvider interface {
    TruncateFloor(ctx context.Context) (floor uint64, constrained bool, err error)
}

type Policy struct { Age time.Duration; Bytes int64 }

type QueueTarget struct {
    Name    string
    Storage engine.Storage
    Drivers []DriverProvider
    Policy  Policy
    mu      sync.Mutex
}

type Coordinator struct {
    mu       sync.RWMutex
    targets  map[string]*QueueTarget
    interval time.Duration
}
```

- `RegisterTarget(*QueueTarget)` — replaces the target.
- `RegisterDriver(name, storage, driver)` — idempotent append to the target's list.
- `UnregisterDriver` / `UnregisterTarget`.
- `SetPolicy(name, Policy) (shrunk bool)` — compares against the previous policy using `isShrink`, which reports `true` when a zero (=unlimited) dimension becomes bounded or a finite bound decreases. `tighter` handles zero as "above every finite bound".
- `ReconcileQueueAt(ctx, name, requested)` — computes `policyFloor` (via `FloorOlderThan` / `FloorForSize`, at least `requested`), then clamps by `min(all driver floors)`. Zero → no‑op. Otherwise `storage.TruncateBefore(safeFloor)`.
- `ReconcileAll(ctx)` — iterates every target, ignoring per‑queue errors.
- `Start(ctx)` — background ticker loop.

The node wires `SetPolicy` to `UpdateQueue`; a shrink triggers an out‑of‑band reconcile bounded by the node's retention context (5 minutes).

---

## 11. Transport — `internal/transport`

### 11.1 Protocol — `internal/transport/protocol`

#### 11.1.1 Frames — `frames.go`

```
+-----------------+---------------+----------------------+
| Length (uint32) | Opcode (u8)   | Payload              |
| 4 bytes         | 1 byte        | (Length - 1) bytes   |
+-----------------+---------------+----------------------+
```

`MaxFrameBytes = 256 MiB`. `WriteFrame` grabs a pooled `[]byte` (32 KiB seed), grows it in‑place while retaining the pointer, writes `[length][op][payload]`, returns the buffer to the pool. `ReadFrame` validates length, allocates exactly `length-1` bytes, reads in one `io.ReadFull`. `ReadFrameInto` supports buffer reuse for the server's per‑connection read loop.

#### 11.1.2 Bytes — `bytes.go`

`Reader` provides bounds‑checked `ReadU16`/`ReadU32`/`ReadU64`/`ReadBytes`; `Buffer` provides symmetric writers. All integers are big‑endian.

#### 11.1.3 Opcodes — `opcodes.go`

Opcodes are **nibble‑grouped** by subsystem:

| Subsystem | Range | Ops |
|---|---|---|
| Admin | `0x0X` | `OpCreateQueue`, `OpDeleteQueue`, `OpListQueues`, `OpUpdateQueue`, `OpDescribeQueue`, `OpTruncate`, `OpMetrics` (0x07), `OpSchema` (0x08), `OpShutdown` (0x0C) |
| Log | `0x1X` | `OpAppend`, `OpAppendBatch`, `OpRead`, `OpReadBatch`, `OpLastOffset`, `OpCommit`, `OpCommitted`, `OpFetch` |
| Task | `0x2X` | `OpEnqueue`, `OpDequeue`, `OpAck`, `OpNack`, `OpExtend`, `OpDeadLetters`, `OpDeadLetterAck` |
| Protocol‑level | — | `OpError` (0xFF), `OpAuth` (0x7E) |

`OpMetrics` occupies `0x07`, the slot that was reserved before metrics landed; `OpSchema` occupies `0x08`, returning the parameter table. `OpAuth` occupies `0x7E`, the protocol‑level handshake slot; it was reserved before auth landed and is now live. The `Info` opcode is retired and never reissued; the frozen table skips it. The table is **frozen**: opcodes are never reused. `TestOpcodeNibbleGrouping` asserts every op lives in its declared nibble and no collisions exist.

`ParseTarget` supports `tcp://`, `unix://`, `pipe://`, and bare `host:port` (treated as TCP).

#### 11.1.4 Errors — `errors.go`

Codes are blocked by subsystem nibble: `0x01–0x0F` admin, `0x11–0x1F` storage, `0x21–0x2F` task, `0x31–0x3F` dead‑letter, `0x41–0x4F` server. `0x09` (`ErrCodeUnauthorized`) is the auth rejection code; the server uses it for both a bad handshake and a missing handshake on an auth‑required server. `OpError` frames carry one code plus a human message; the code is the stable contract.

#### 11.1.5 Auth Frames — `auth.go`

```go
const (
    AuthMethodNone  byte = 0
    AuthMethodToken byte = 1
    AuthMethodMTLS  byte = 2
)

func EncodeAuth(b *Buffer, method byte, credential []byte)
func DecodeAuth(r *Reader) (byte, []byte, error)
```

The `OpAuth` frame is `u8 method` followed by a length‑prefixed `credential` (empty for `none` and `mtls`). It is sent as the first frame on every connection; the server verifies it and replies with an empty `OpAuth` frame, or with `OpError(0x09, msg)` and closes. `DecodeAuth` uses the same bounds‑checked `Reader` as every other codec, so a truncated credential surfaces as `ErrTruncatedBody` and is rejected before dispatch.

#### 11.1.6 Task Wire — `task.go`

`EncodeTask` / `DecodeTask` serialize `Task` including `DeliveryID` and `Offset`. `DecodeTask` rejects malformed payloads (`ErrInvalidTask`), including non‑zero `Visibility` without a `DeliveryID`. `EncodeTaskRef` / `DecodeTaskRef` prefix a **mode byte**:

- `TaskRefModeTask = 0` → full task record (server runs the strict lease check).
- `TaskRefModeTaskID = 1` → bare ID (server skips the strict check).

`EncodeTaskDequeue` clamps negative timeouts to 0 so they decode as "non‑blocking" instead of a 64‑bit millisecond value. `EncodeTaskList` / `DecodeTaskList` handle `DeadLetters` responses with a `10,000,000` cap.

#### 11.1.7 Log Wire — `log.go`

The log wire DTO is `LogRecord{Offset, ID, Timestamp, Payload}`.

- `WriteLogRecord(b, rec)` / `ReadLogRecord(r)` — one record as `u64 offset`, `bytes ID`, `u64 UnixNano timestamp`, `bytes payload`.
- `WriteLogRecordList` / `ReadLogRecordList` — `u32 count` (cap `65536`) followed by that many records. Over‑large counts decode as `ErrTruncatedBody`.
- `EncodeFetch(b, queue, start, maxCount, maxWait)` / `DecodeFetch(r)` — the `OpFetch` request: `bytes(queue) u64 start u32 maxCount u64 maxWait` (`maxWait` as a `time.Duration` in nanoseconds). A negative `maxWait` on the wire decodes to a negative duration, which the server clamps to 0 before dispatch.

`OpFetch` requests carry a `u64` wait in nanoseconds; the server caps it at `maxFetchWait = 5 min` so a hostile client cannot pin a handler indefinitely.

### 11.2 IPC — `internal/transport/ipc`

`Listen(target)` returns a `*Listener` wrapping either a Unix domain socket (with stale file removal and `chmod 0600`) or a Windows named pipe. `Listener.Close()` unlinks the Unix socket file. `DialContext(ctx, target)` maps `pipe://` onto the `unix` network on POSIX (as a Unix socket) and onto `\\.\pipe\…` on Windows.

### 11.3 Server — `internal/server`

```go
type Server struct {
    node      *node.Node
    listener  net.Listener
    tlsConfig *tls.Config
    maxConns  int
    slots     chan struct{}
    mu        sync.Mutex
    stopCh    chan struct{}
    doneCh    chan struct{}
    metrics   *serverMetrics
    auth      *authMode
}

func New(n *node.Node, opts ...ServerOption) *Server
func WithAuth(m *authMode) ServerOption
func WithTLS(tlsConfig *tls.Config) ServerOption
func WithMaxConns(n int) ServerOption
```

- `getOrCreateLogQueue` / `getOrCreateTaskQueue` delegate to the node's stable cached handles.
- `ListenAndServe(target)` picks `ipc.Listen` for `unix`/`pipe` or `net.Listen` for `tcp`, wraps `tcp` with TLS if configured, and calls `Serve`.
- `Serve(ln)` accepts and spawns `serveConn` per connection. Admission control uses `slots` (`maxConns > 0`); a rejected connection receives `OpError(ErrCodeTooManyConnections)` from `rejectConn`, which reads (and discards) one frame to make the client's write complete.
- `serveConn` reads frames on a **single reader goroutine** feeding an unbuffered channel; the main goroutine dispatches each frame and writes the response. This gives one frame of lookahead and natural backpressure, and lets a dead connection cancel a long‑polling handler. `OpShutdown` sets `shouldShutdown`; the goroutine then launches `s.Stop()` and returns.
- `Stop()` closes the stop channel and the listener; safe to call from any goroutine.

`serveConn` begins with a **mandatory handshake read** before dispatch:

1. `ReadFrame` the first frame.
2. If it is `OpAuth`, `DecodeAuth` and call `s.auth.Verify(method, cred, tlsStateOf(conn))`. A rejection replies with `OpError(ErrCodeUnauthorized)` and closes; a success replies with `OpAuth, nil`, and the reader is re-armed for the next frame.
3. Otherwise, if `s.auth.Required()` is set, the connection is refused with `OpError(ErrCodeUnauthorized)` and closed. Anonymous servers accept a first frame that is not `OpAuth` (legacy first‑frame clients still work when the server is anonymous).

`tlsStateOf(conn)` returns the negotiated TLS state (or nil for plain connections) so `authMode.Verify` can require a verified client chain when the mode is `mtls`.

`dispatch.go` routes every opcode and records each one's latency. `handlers_admin.go` covers queue lifecycle, `OpMetrics`, and `OpSchema`; `handlers_log.go` covers append/read/read‑batch/last‑offset/commit/committed/fetch; `handlers_task.go` covers enqueue/dequeue/ack/nack/extend/dead‑letters. Every response is either a success frame reusing the request opcode, or `OpError`.

`handleMetrics` (admin) serves the live `metrics.Snapshot` as `u16 specVersion bytes(json)`, the same envelope as `OpDescribeQueue`, so a future schema bump travels through the same `CheckSpecVersion` gate.

`handleSchema` (admin) serves the JSON‑encoded parameter table as the same `u16 specVersion bytes(json)` envelope, with no queue name: the table is a process‑wide fact.

`handleFetch` clamps the client's requested wait at `maxFetchWait = 5 min` and then calls `LogQueue.Fetch`; it serializes the result as a `LogRecordList` frame. A non‑blocking call (`wait == 0`) that finds no data returns `ErrCodeNoData`, which the client maps back to `engine.ErrNoData`; the long‑polling `Tail` path therefore treats `ErrNoData` as "re‑arm the window" rather than an error.

`handleDequeue` caps server‑side long‑poll at `maxDequeueWait = 5 min` and reports `ErrCodeTaskNotAvailable` on clean context deadline, so clients re‑arm.

#### 11.3.1 Auth — `auth.go`

```go
type authMode struct {
    method byte
    token  []byte
}

func NewAuthMode(sess spec.Session) (*authMode, error)
func (a *authMode) Required() bool
func (a *authMode) Verify(method byte, cred []byte, tlsState *tls.ConnectionState) error
```

`NewAuthMode` resolves a session into a verifier:

| `auth_method` | Verifier | Notes |
|---|---|---|
| `""`, `none`, `none-insecure` | `method: AuthMethodNone` | Accepts any first frame. |
| `token` | `method: AuthMethodToken`, `token: []byte(tok)` | Reads `AuthTokenFile` when `AuthToken` is empty; the token must be ≥ 16 bytes. |
| `mtls` | `method: AuthMethodMTLS` | Requires `tls_enabled` and, at the TLS layer, a verified client chain. |

`Verify` is a constant‑time comparison for token mode; mTLS delegates the actual certificate check to the TLS handshake and asserts `tlsState.VerifiedChains` is non‑empty. Unknown modes fall through to `ErrUnauthorized`. This is the only place a credential is compared; nothing else in `server` (or anywhere else) inspects a token.

`minTokenBytes = 16` is enforced both in `NewAuthMode` and `spec.ValidateSession`, so a short token fails at the process boundary as well as on the wire.

#### 11.3.2 TLS — `tls.go`

```go
func DefaultTLSConfig(certFile, keyFile, clientCAFile string) (*tls.Config, error)
```

Loads a keypair or generates an ECDSA P‑256 self‑signed cert (`localhost`, `127.0.0.1`, `::1`, 1‑year validity), always with `MinVersion: tls.VersionTLS13`. When `clientCAFile` is non‑empty, the file's PEM bundle populates `ClientCAs` and `ClientAuth = tls.RequireAndVerifyClientCert`, which is what makes `AuthMethodMTLS` verifiable at the handshake layer.

#### 11.3.3 Server Metrics — `metrics.go`

The server caches a `serverMetrics` for its own scope, resolved once in `New`:

```go
type serverMetrics struct {
    connections *metrics.Gauge
    rejected    *metrics.Counter
    ops         map[byte]*metrics.Counter
    durations   map[byte]*metrics.Histogram
}
```

`newServerMetrics` creates one counter and one histogram per opcode in `opLabels` (`op_<label>` and `op_<label>_duration`). `recordOp(op, start)` counts and observes each dispatched operation. The node **preregisters** every instrument before the collector ticks (§12.3), so the first tick already has a `"server"` scope to read and the Prometheus endpoint labels it correctly on an idle server.

`connections` is a gauge the server maintains across `serveConn`: +1 on entry, −1 on exit. `rejected` counts connections turned away by the admission cap. `OpAuth` is deliberately not in `opLabels`: the handshake is recorded as part of `serveConn`, not as a dispatched op.

---

## 12. Process Core — `internal/node`

`node.Node` is the shared local core: one data directory, its queue registry, cached drivers, collector, and retention lifecycle.

```go
type Node struct {
    mgr        *engine.Manager
    retention  *retention.Coordinator
    logQueues  map[string]*logqueue.LogQueue
    taskQueues map[string]*taskqueue.TaskQueue
    mu         sync.RWMutex

    retentionCtx    context.Context
    retentionCancel context.CancelFunc
    retentionWG     sync.WaitGroup
    reconcileWG     sync.WaitGroup
    closed          bool

    dir      string
    defaults spec.Spec
    registry *registry
    lock     *dirLock
    openedAt time.Time
    metrics  *metrics.Collector
}
```

- `openNode(dir, defaults, mc)` — `MkdirAll`, acquire the directory lock, load and reconcile `queues.json`, build per‑queue engine configs from the effective spec, open the manager, start the retention loop, and build/start the collector.
- `Acquire(dir, defaults, opts...)` in `pool.go` — ref‑counted process pool keyed by absolute path. Mismatched session defaults → `ErrSessionDefaultsMismatch`. The release func is idempotent via `sync.Once`.
- `CreateQueue(ctx, name, s)` — `spec.ValidateCreate`, refuse duplicates, create the engine queue, persist the spec to `queues.json`, cache the log driver, register both the driver and policy.
- `UpdateQueue(ctx, name, patch)` — `spec.ValidateUpdate`, merge patch over stored, persist, apply mutable engine config, keep cached driver and retention target in sync, push updated defaults into a live task queue, and (if the policy shrank) launch a bounded immediate reconcile.
- `DescribeQueue(ctx, name)` — returns stored, effective, and defaults.
- `Metrics(ctx)` — returns the collector's current live snapshot.
- `Collector()` — returns the collector (used by the CLI/REPL SSE subscription and the daemon HTTP server).
- `LogQueue` / `TaskQueue` — double‑checked locking, cached handles, retention registered on first use. `TaskQueue` passes `n.metrics.Registry().Scope(name)` to the driver.
- `DeleteQueue` — closes the task driver, drops both cached handles, unregisters retention, removes the directory, updates the registry.
- `Truncate` — materializes both drivers, calls `retention.ReconcileQueueAt(ctx, name, minOffset)`.
- `ListQueues`, `TaskDefaults`, `PolicyFor`, `ToEngineConfig` — helpers.

`registry.go` — `queues.json` with `{version, queues}` shape, `DisallowUnknownFields`, atomic temp+rename writes, and `reconcileRegistry` purging entries whose directories are missing and adopting orphan directories on session defaults. Corrupt registry → `ErrRegistryCorrupt` at startup.

`lock.go` + `lock_{unix,windows}.go` — `flock(LOCK_EX|LOCK_NB)` on Unix, `LockFileEx` on Windows, on `<dir>/mikyu.lock`. Failure → `ErrLocked`.

### 12.1 Metrics Config — `metrics.go`

```go
type MetricsConfig struct {
    Enabled    bool
    Interval   time.Duration
    History    int
    Histograms bool
}

type AcquireOption func(*MetricsConfig)

func WithMetrics(cfg MetricsConfig) AcquireOption
func MetricsFromSession(s spec.Session) MetricsConfig
```

`MetricsFromSession` maps the six session parameters onto the config. `Acquire` accepts `WithMetrics` and applies the config only when it opens a **new** node — a second `Acquire` on the same directory reuses the existing collector unchanged.

### 12.2 Collector Wiring

`openNode` builds the registry, the collector, and preregisters the server scope:

```go
mreg := newRegistry(mc)
mgr, err := engine.OpenManager(dir, defaultEngineCfg, perQueue, mreg)
...
n.metrics = newCollector(mreg, mc, NewRuntimeSource(), mgr)
preregisterServerMetrics(mreg)
n.retentionWG.Go(func() { n.retention.Start(ctx) })
n.metrics.Start(ctx)
```

`newRegistry` builds the registry with the config's histogram preference. `newCollector` builds the collector from the config (disabled when metrics are off). The collector's sources are:

1. `NewRuntimeSource()` — fills `Runtime` and `Process`.
2. The `*engine.Manager` — fills every queue view's gauges.

Scope readings (§7.5 `mergeScopes`) supply the counters and histograms from each queue's scope, plus the server's per‑op instruments.

### 12.3 Server Scope Preregistration

`preregisterServerMetrics(reg)` creates every server‑scope counter and histogram before the collector's first tick, so:

- The `"server"` scope is visible to a snapshot even on an idle process.
- Two concurrent calls cannot race instrument creation inside `Scope.Histogram`.

This mirrors the instruments the server's `opLabels` map declares, and it is why `TestMetricsHTTPPrometheusPerQueue` sees `queue="server"` series on a fresh daemon. `OpSchema` is deliberately not in `opLabels`: it is a schema‑discovery op, not a data‑path op, and the server records it through the generic dispatch path. `OpAuth` is handled before dispatch and is therefore also absent from `opLabels`.

### 12.4 Runtime Source — `runtime_source.go`

```go
type RuntimeSource struct { start time.Time }

func NewRuntimeSource() *RuntimeSource
func (s *RuntimeSource) Collect(_ context.Context, dst *metrics.Snapshot)
```

`Collect` fills `Runtime` (goroutines, heap alloc/in‑use, GC count, uptime) from `runtime.ReadMemStats` and `Process` from the platform readers. `readProcessRSS` / `readProcessCPU` live in `runtime_source_{unix,windows}.go`:

- Unix — `getrusage(RUSAGE_SELF)`; `Maxrss` and `Utime+Stime`.
- Windows — `GetProcessMemoryInfo` for the working set; `GetProcessTimes` for kernel+user 100 ns ticks.

`cpuPercent` converts a cumulative CPU‑time reading into a utilization percent against the process's own wall‑clock delta. The first sample establishes the baseline and reports zero; subsequent samples are (Δ CPU / Δ wall) × 100.

---

## 13. Client — `internal/client`

### 13.1 Options — `options.go`

`SessionConfig` carries `Spec`, `Target`, `DataDir`, `MaxConns`, `TLSEnabled`, `TLSCertFile`, `TLSKeyFile`, `AutoStart`, `AuthMethod`, `AuthToken`, `AuthTokenFile`, `TLSClientCaFile`, and the six metrics settings. `WithSessionSpec` replaces the inherited defaults. `TLSConfig()` builds a `tls.Config` from a cert pair, using the client cert as its own root, `MinVersion: tls.VersionTLS13`.

Auth options (each maps directly onto a session parameter):

- `WithAuthMethod(method string)`
- `WithAuthToken(tok string)`
- `WithAuthTokenFile(path string)`
- `WithTLSClientCAFile(path string)`

Metrics options (each maps directly onto a session parameter):

- `WithMetricsEnabled(enabled bool)`
- `WithMetricsInterval(d time.Duration)`
- `WithMetricsHistory(n int)`
- `WithMetricsListen(addr string)`
- `WithMetricsPerQueuePrometheus(enabled bool)`
- `WithMetricsHistograms(enabled bool)`

`WithAutoStart(enabled bool)` makes `Dial` spawn a daemon with the session's settings when the target is not already serving; it is a client‑composition helper, not a table row. It forwards the auth settings through to the spawned daemon's `SpawnOptions` so a one‑call `Dial` to a token‑mode target reaches the right server.

`QueueOption func(*spec.Spec)` — every queue option writes exactly one field, so the spec is sparse by construction. `applyQueueOptions` folds them.

### 13.2 Embedded — `embedded.go`, `embedded_queues.go`

`embeddedClient` holds `*node.Node` and the release func from `node.Acquire`. Every method delegates to the node. `Metrics(ctx)` returns the node's snapshot. `Schema(ctx)` returns a fresh copy of `spec.Table`. `SubscribeSnapshots()` returns the node collector's subscription so the CLI/REPL `metrics --watch` can stream without polling. `Close` releases the reference.

`NewEmbedded` rejects `WithTarget` with `ErrModeMismatch`; auth options are accepted but only `TLSClientCaFile`, `AuthMethod`, `AuthToken`, and `AuthTokenFile` are meaningful in the embedded case (they have no wire peer, so they simply flow into the session defaults and are ignored at this layer).

### 13.3 Remote — `remote.go`, `remote_logqueue.go`, `remote_taskqueue.go`, `remote_group.go`

`remoteClient` holds a pooled `chan *clientConn` and a `connSem` counting semaphore sized to `MaxConns`:

- `acquire(ctx)` — take a semaphore slot, then pop from the pool or dial fresh.
- `dialConn` — `ipc.DialContext` for `unix`/`pipe`; `tls.Dialer` or `net.Dialer` for `tcp` with a 5 s timeout. **Immediately after the socket is up**, it calls `authCredential()` and sends the `OpAuth` handshake via `writeAuth`, reading back either an empty `OpAuth` frame or an `OpError`, which is decoded through the same `decodeProtocolError` the rest of the client uses.
- `authCredential()` — resolves the method and credential the client presents: a token file wins over an inline token, which wins over the implicit mTLS claim (`c.tls != nil`), which wins over `none`.
- `release(ch)` — return to pool unless closed or full.
- `Close` — flip `closed`, drain the pool.
- `exchange(ctx, op, body)` — acquire, set deadline from `ctx`, run a cancellation watcher that forces the socket deadline to a poisoned time on `ctx.Done()`, write frame, read frame, decode `OpError`. On cancellation or error, `discard` the connection (never return a connection with an unread response to the pool). A `ErrCodeTooManyConnections` reply always discards, because the server closed the connection after refusing.

`Metrics(ctx)` decodes the `OpMetrics` response: `u16 specVersion` gated through `spec.CheckSpecVersion`, then a length‑prefixed JSON `metrics.Snapshot`, capped at `MaxSpecPayload`. Embedded and remote clients produce identical JSON (modulo timestamp and machine‑local values), verified by `TestEmbeddedMetricsMatchesRemote`.

`Schema(ctx)` decodes the `OpSchema` response with the same `CheckSpecVersion` gate, yielding the server's `[]spec.Param` as the embedded surface does; `TestSchemaOverWire` requires the two to agree field for field.

`remoteLogQueue` implements `ClientLogQueue`. Its internal `fetch(ctx, start, maxCount, maxWait)` is the mirrored `OpFetch` primitive:

- The server budget is `wait = min(maxWait, ctx deadline remaining)`.
- The call deadline is `wait + pollMargin` (`5 s`), so a stalled server cannot hold a socket past the poll window.
- `Stream` calls `fetch(ctx, curr, streamBatch=1024, 0)` — the server's non‑blocking path — and returns on `engine.ErrNoData`.
- `Tail` calls `fetch(ctx, curr, streamBatch, longPoll=30 s)`; `engine.ErrNoData` is treated as "poll elapsed, re‑arm", so an idle tail spends one request per 30 s window. The whole iterator yields on parent‑context cancellation, restoring the parent error after the call returns.
- `MinOffset` reads the snapshot's `min_offset` gauge via the client's `Metrics` — the only place a remote reader needs the collector.

`remoteTaskQueue` implements `TaskQueue` with a **long‑poll loop** in `Dequeue`:

- Server budget per call: `longPollTimeout = 30s`, capped by `ctx` deadline.
- Client deadline: `wait + longPollMargin` (`5s`).
- On `ErrTaskNotAvailable` (poll elapsed) or client‑side deadline, re‑arm immediately.
- On parent `ctx.Done()`, return the context error.

`remoteGroup` embeds `group.Base` with a `CommitFn` that exchanges `OpCommit`.

`errors.go` `decodeProtocolError` maps each wire code back to the exact Go sentinel, preserving server detail after the sentinel text (so `ErrImmutableField: …segment_size…` still satisfies `errors.Is`). `ErrCodeNoData` maps to `engine.ErrNoData`; `ErrCodeUnauthorized` maps to `protocol.ErrUnauthorized`, which the SDK re‑exports as `mikyu.ErrUnauthorized`.

---

## 14. User Surface — `internal/ui`

### 14.1 Verb Table — `internal/ui/command/verbs.go`

The canonical CLI/REPL verb list:

| Kind | Verbs |
|---|---|
| Log | `list`, `create`, `update`, `describe`, `delete`, `append`, `read`, `tail`, `truncate` |
| Task | `enqueue`, `dequeue`, `ack`, `nack`, `extend`, `dlq`, `dlq-ack` |
| Admin | `metrics`, `shutdown`, `login`, `logout` |

`command.Verbs` is the single source; `cli.handlers` and `repl.replHandlers` are keyed by the same names, and `TestNamingLaw` in `mikyu/surface_test.go` asserts no retired vocabulary (`Produce`, `Publish`, `Pop`, `Stats`, `Info`, `DequeueNow`, `FetchOffset`, `OriginalOffset`, …) leaks into any verb or method.

### 14.2 Shared Command Helpers — `internal/ui/command`

- Log: `List`, `Create`, `Update`, `Describe`, `Delete`, `Append`, `Read`, `Tail`, `Truncate`, `Metrics`, `Shutdown`.
- Task: `Enqueue`, `Dequeue`, `TaskAck`, `TaskNack`, `TaskExtend`, `TaskDeadLetters`, `TaskAckDeadLetter`.
- Auth (`auth.go`): `CredentialsPath`, `LoadToken`, `SaveToken`, `DeleteToken`, `Logout`, `Login`, `LoginWithToken`, `readTokenFrom`.
- `ResolvePayload` — string, `@file`, or `-` (stdin).
- `FormatSingleLinePayload` — escapes CR/LF/TAB, strips control chars, and truncates to a terminal‑width cell count via Lipgloss.
- `TightensRetention(current, patch)` — used to decide whether to prompt before an irreversible update.
- Flags: `SessionFlags`, `QueueFlagSet`, `QueuePatchFlagSet`, `EnqueueFlagSet`, `NackFlagSet`, plus `ParseAnywhere` for flags before/after positionals. All flag sets are driven by `spec.Params(layer)` and `spec.ParamsInScope(layer, scopes...)`.
- `SpecOption(spec.Spec)` — the one bridge that hands a fully‑built spec to the SDK.
- `OpenClient(session)` — embedded if `--dir`, otherwise ensures the daemon is running and dials; before dialing it loads a stored credential for the target when the session has no explicit token.

#### 14.2.1 Credential Store — `auth.go`

`os.UserConfigDir()/mikyu/credentials` holds a YAML map of `target → token`, written atomically at mode `0o600`. `LoadToken`, `SaveToken`, and `DeleteToken` are the only accessors; a missing file reads as empty, and `writeCredentials` fsyncs the temp file before renaming so a crash leaves either the old map or the new one, never a torn file. `LoginWithToken` verifies a token against the target with a one‑shot `Dial` + `ListQueues` before persisting it; a bad token therefore never lands on disk. `Login` reads from `os.Stdin` (masked when a TTY), and `Logout` is a thin alias for `DeleteToken`. The store is the single place a session obtains a token when neither `--auth-token` nor `--auth-token-file` is set.

### 14.3 CLI — `internal/ui/cli`

`App{Stdin, Stdout, Stderr, Confirm}` — testable stream surface. `Run(target, args)` parses global flags, looks up the verb, optionally prints per‑verb help from `spec.Params`, opens the client, and dispatches. `confirm` uses the injected `Confirm` in tests, otherwise prompts on a TTY and requires `-y/--yes` on non‑interactive input. Exit codes come from `cmd/mikyu` (`0` success, `1` general error, `2` usage error via `daemon.ErrUsage`).

`handleLogin` reads a token from the process's stdin (via `command.Login`), verifies it, and stores it; `handleLogout` clears the stored entry. Both accept `--target` (default: the session's target) and are the only verbs that do not need a working client to complete their main effect.

`handleMetrics` accepts `--json`, `--watch`, and `--queue <name>`. `--watch` first tries to subscribe to the embedded node's collector (`SubscribeSnapshots`); when the client is remote it polls `Metrics` on a one‑second ticker. `--json` always emits the full snapshot; the text form prints uptime, data dir, queue count, total bytes, then one line per queue with its gauges.

`task.go` renders tasks as Lipgloss cards on a TTY and as JSON otherwise; `--json` always forces JSON. Task records round‑trip through `command.ParseTaskRef`, so `dequeue --json | ack` works end‑to‑end with the strict lease check preserved.

### 14.4 REPL — `internal/ui/repl`

`replSession` carries the client, target, dir, active queue, last task queue, last task ID, and a `done` flag. `dispatch` maps the first token:

- Session commands: `use`, `help`, `exit`/`quit`.
- Every verb from `command.Verbs`, including `login` and `logout`.
- Retired names (`produce`, `publish`, `pop`, `stats`, `info`) are explicitly unreachable (`TestREPLMatchesCLIVerbs`, `TestREPLNoInfoVerb`).

`resolveQueue` and `queueFor` let `ack`/`nack`/`extend`/`dlq`/`dlq-ack` fall back to the active queue and then to the last dequeued task's queue. `taskRef` lets `ack`/`nack`/`extend` reuse `lastTaskID` when no argument is given.

`handleLogin` reads a token from the REPL's own `bufio.Scanner` (rather than `os.Stdin`) so it composes with the interactive input loop, verifies it against the target, and stores it. `handleLogout` clears the stored entry.

`handleMetrics` supports `--watch`, streaming snapshots every second until interrupted. The summary line is `Uptime: … | Data Dir: …` followed by `Queues: N | Total Storage: M Bytes`; the per‑queue breakdown is intentionally omitted in the interactive surface (the CLI's `metrics` prints it for scripted use).

`style.go` — fire‑gradient ASCII banner, command reference table, task/dead‑letter cards, confirmation prompt theme. `vt_windows.go` enables `ENABLE_VIRTUAL_TERMINAL_PROCESSING` on Windows.

### 14.5 Entrypoint — `cmd/mikyu/main.go`

```go
run(args):
  no args               → repl.RunREPL(defaultTarget, nil)
  "daemon" / "server"   → daemon.Run(args[1:])
  "shell" / "repl"      → repl.RunREPL(defaultTarget, args[1:])
  "cli"                 → cli.RunCLI(defaultTarget, args[1:])
  otherwise             → cli.RunCLI(defaultTarget, args)
```

`exitCode(err)` maps `nil → 0`, `daemon.ErrUsage → 2`, else `1`.

---

## 15. Daemon — `internal/daemon`

```go
func Run(args []string) error
func EnsureDaemonRunning(opts SpawnOptions) error
func IsDaemonAlive(target string) bool
func SendShutdownSignal(target string, sess spec.Session) error
```

- `Run` resolves config (`resolveConfig` layers CLI flags > `MIKYU_*` env > `mikyu.yaml` > compiled defaults, only applying **explicitly set** flags via `fs.Visit`), opens the node, reconciles declared queues, optionally wraps the server in TLS, optionally starts the metrics HTTP server, serves, and blocks on `SIGINT`/`SIGTERM` or the serve result. A bind failure is reported through a buffered channel so deferred cleanups run.
- `EnsureDaemonRunning` probes the target; if dead, spawns a detached copy of `os.Executable()` with `daemon` and waits up to 3 s. The spawn forwards `Target`, `DataDir`, TLS files, `AuthMethod`, `AuthTokenFile`, and `TLSClientCaFile`. It deliberately cannot forward `AuthToken`: the daemon rejects that flag.
- `setDetachedProcessAttr` uses `Setsid: true` on Unix, `CREATE_NO_WINDOW` on Windows.
- `SendShutdownSignal` authenticates with `resolveShutdownCredential` (token file > inline token > mTLS > `none`) and writes an `OpShutdown` frame; an `OpError(ErrCodeUnauthorized)` reply is surfaced as `ErrUnauthorized` so the caller can retry with credentials.

### 15.1 Daemon Flags & Fail‑Closed Defaults

| Flag | Field | Notes |
|---|---|---|
| `--config` | — | Path to `mikyu.yaml`. |
| `--target` | `Server.Target` | Must parse via `protocol.ParseTarget`. |
| `--data-dir` | `Server.DataDir` | Non-empty. |
| `--tls-enabled` | `Server.TLSEnabled` | Boolean, honors explicit `=false`. |
| `--tls-cert-file` | `Server.TLSCertFile` | PEM. |
| `--tls-key-file` | `Server.TLSKeyFile` | PEM. |
| `--tls-client-ca-file` | `Server.TLSClientCaFile` | Required for `mtls`. |
| `--max-conns` | `Server.MaxConns` | > 0. |
| `--auth-method` | `Server.AuthMethod` | `none` \| `none-insecure` \| `token` \| `mtls`. |
| `--auth-token-file` | `Server.AuthTokenFile` | Read at `NewAuthMode`. |
| `--auth-token` | — | **Rejected**: a token on argv leaks via `ps`. The flag is registered so `--help` lists it, but using it returns `ErrUsage`. |

`resolveConfig` enforces the fail‑closed policy on `tcp://` targets whose address is not loopback:

- `auth_method = token` requires `--tls-enabled`.
- `auth_method = mtls` requires `--tls-enabled` and `--tls-client-ca-file`.
- `auth_method = "" | none` is refused with a clear error; the operator must pass `--auth-method token|mtls`, or opt out explicitly with `--auth-method none-insecure`.

Loopback targets are exempt, as are `unix://` and `pipe://` targets (the filesystem is the boundary). `isLoopback` matches `localhost`, `127.0.0.0/8`, and `::1`.

`logEffectiveConfig` records the fully resolved settings (target, data dir, TLS, max connections, queue count, and the JSON defaults block) so a running daemon's log always shows what it actually started with.

### 15.2 Auth Resolution

After flag parsing, `Run` calls `server.NewAuthMode(cfg.Server)`. A `token` mode with a short or missing credential, or an unknown mode, is reported as `auth setup: …` and stops the daemon before the wire listener binds. The `authMode` is passed to the server via `server.WithAuth(mode)`; a server constructed without it falls back to an anonymous verifier (`spec.BuiltinSession()`), so existing tests and embedded hosts keep working.

### 15.3 Metrics HTTP — `metrics_http.go`

```go
type metricsServer struct {
    node       *node.Node
    perQueue   bool
    histograms bool
    srv        *http.Server
}

func startMetricsHTTP(n *node.Node, perQueue, histograms bool, addr string) (*metricsServer, error)
func (ms *metricsServer) Stop(ctx context.Context) error
```

Started only when `server.metrics_listen` is non‑empty. Binds first, then serves; a bind failure is reported to `Run` before the wire server listens, so `Run` can stop the wire server cleanly. `warnIfNotLoopback` logs a warning when the listen address is not `127.0.0.1` / `::1`.

Four fixed routes:

| Route | Response |
|---|---|
| `GET /metrics` | Prometheus text 0.0.4; per‑queue series only if `--metrics-per-queue-prometheus` is set; capped at `topNQueues = 50`; histograms only if `--metrics-histograms` is set. |
| `GET /api/metrics` | A single JSON `metrics.Snapshot` — the same encoding as the wire and CLI. |
| `GET /api/history` | `?limit=N` (positive int) slices the collector's tick history, newest last. |
| `GET /api/events` | Server‑sent events, one `snapshot` event per collector tick, until the client disconnects. |

`Stop` calls `http.Server.Shutdown` with the daemon's 5 s context, so scrapers racing shutdown are still answered. A slow SSE client never blocks the collector (§7.5 `fanout` drops extra frames) and never blocks a concurrent scraper (§15.1 `Stop`). The metrics endpoint is documentation‑secured, not firewall‑secured: it inherits the daemon's loopback‑by‑default posture.

---

## 16. Configuration — `internal/config`

`Config{Server spec.Session, Defaults spec.Spec, Queues map[string]spec.Spec}`.

- `Load(flagPath)` resolves the config file (flag → `mikyu.yaml` next to the executable, bootstrapping a commented template if absent), rejects legacy YAML keys (`max_segment_bytes`, `max_segment_age`) and legacy env names (`MIKYU_ENGINE_*`, `MIKYU_SERVER_*`, `MIKYU_ENGINE_FLUSH_INTERVAL`) with a hard error naming the replacement, parses with `DisallowUnknownField`, and applies `spec.ApplyEnvSession` + `spec.ApplyEnv`.
- `DefaultConfig()` returns built‑in session defaults plus an empty queue map.
- `DefaultConfigYAML` is the bootstrapped template; it carries the same schema as `spec.BuiltinSession()` and `spec.Builtin()`. Auth settings are intentionally absent from the template, so a fresh install runs anonymous on loopback and must be opted into token/mTLS.

The credential store (`internal/ui/command/auth.go`) lives next to the config in `os.UserConfigDir()/mikyu/credentials`; it is not touched by `config.Load`, because tokens are session state, not configuration.

---

## 17. Configuration & Environment Reference

### 17.1 YAML Schema

```yaml
server:
  target: "tcp://127.0.0.1:7420"
  data_dir: "./data"
  tls_enabled: false
  tls_cert_file: ""
  tls_key_file: ""
  tls_client_ca_file: ""       # client CA bundle for mtls
  max_conns: 10
  auth_method: "none"          # none | none-insecure | token | mtls
  auth_token_file: ""          # path to a bearer token (token mode)
  auth_token: ""               # inline bearer token (token mode; prefer the file)
  metrics_enabled: true
  metrics_interval: "1s"
  metrics_history: 300
  metrics_listen: ""           # e.g. "127.0.0.1:9090"; empty leaves HTTP off
  metrics_per_queue_prometheus: false
  metrics_histograms: true

defaults:                      # session defaults inherited by new queues
  engine:
    batch_size: 256
    max_interval: "10ms"
    min_interval: "30us"
    segment_size: 67108864
    segment_age: "1h"
    max_message_bytes: 1048576
    encrypted: false
    fsync: true
  retention:
    age: "0s"
    bytes: 0
  task:
    default_visibility: "30s"
    default_max_attempts: 5

queues: {}                     # declarative pre-created queues (sparse Specs)
```

### 17.2 Env Overrides

Every `LayerSpec` and `LayerSession` param has a `MIKYU_<KEY>` env var. Examples: `MIKYU_TARGET`, `MIKYU_DATA_DIR`, `MIKYU_MAX_CONNS`, `MIKYU_BATCH_SIZE`, `MIKYU_SEGMENT_SIZE`, `MIKYU_FSYNC`, `MIKYU_RETENTION_AGE`, `MIKYU_DEFAULT_VISIBILITY`, `MIKYU_DEFAULT_MAX_ATTEMPTS`, `MIKYU_AUTH_METHOD`, `MIKYU_AUTH_TOKEN`, `MIKYU_AUTH_TOKEN_FILE`, `MIKYU_TLS_CLIENT_CA_FILE`, `MIKYU_METRICS_ENABLED`, `MIKYU_METRICS_INTERVAL`, `MIKYU_METRICS_HISTORY`, `MIKYU_METRICS_LISTEN`, `MIKYU_METRICS_PER_QUEUE_PROMETHEUS`, `MIKYU_METRICS_HISTOGRAMS`. Legacy `MIKYU_ENGINE_*` and `MIKYU_SERVER_*` names are hard errors naming the replacement.

### 17.3 Auth Modes at a Glance

| Mode | Wire | Server check | Client setup |
|---|---|---|---|
| `none` | `AuthMethodNone`, no credential | always accepts | default |
| `none-insecure` | `AuthMethodNone`, no credential | alias for `none`, spelled to be refused loudly on non‑loopback | default |
| `token` | `AuthMethodToken`, `[]byte(token)` | constant‑time compare against `authMode.token` | `--auth-token-file`, `--auth-token`, stored credential, or `WithAuthToken`/`WithAuthTokenFile` |
| `mtls` | `AuthMethodMTLS`, no credential | requires a TLS handshake with a verified client chain | `--tls-enabled` + `--tls-cert-file`/`--tls-key-file`, or `WithTLSEnabled` + `WithTLSCertFile`/`WithTLSKeyFile` |

---

## 18. Wire Protocol Summary

```
Client                                         Server
  │  WriteFrame(conn, OpAuth, method+cred)      │
  ├────────────────────────────────────────────►│  serveConn: DecodeAuth → authMode.Verify
  │  ReadFrame(bufio.Reader)                    │
  │◄────────────────────────────────────────────┤  OpAuth ack, or OpError(0x09) + close
  │                                             │
  │  WriteFrame(conn, Op, body)                 │
  ├────────────────────────────────────────────►│  dispatch → handler
  │                                             │
  │  ReadFrame(bufio.Reader)                    │
  │◄────────────────────────────────────────────┤
  │  OpError → decodeProtocolError              │
```

- **First frame on every connection is `OpAuth`.** The server verifies it once, replies with an empty `OpAuth` on success or `OpError(ErrCodeUnauthorized)` on failure, and then transitions to ordinary dispatch. Anonymous servers accept a non‑`OpAuth` first frame for backwards compatibility; auth‑required servers do not.
- **Admin frames** carry `bytes(name) u16 specVersion bytes(json Spec)`. The server checks `specVersion` **before** touching the JSON, so a future schema is rejected without interpretation (`ErrCodeSpecTooNew`).
- **`OpDescribeQueue`** response is `u16 specVersion bytes(json QueueDescription)`. `QueueDescription` carries three specs (stored, effective, defaults) and no stats.
- **`OpMetrics`** response is `u16 specVersion bytes(json metrics.Snapshot)`, the same envelope as `OpDescribeQueue`.
- **`OpSchema`** response is `u16 specVersion bytes(json []Param)`, the same envelope. There is no `OpInfo`.
- **`OpAuth`** request is `u8 method bytes(credential)`; the response is either an empty `OpAuth` frame or an `OpError(0x09)` frame. Error codes remain the stable contract; the human message after the code is not.