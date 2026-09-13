# Mikyu Reconcile Plan

*A single pass over config, persistence, wire, retention, task defaults, the SDK surface, the CLI/REPL, and naming. One wire format. One spec shape. Every phase ends green.*

---

## 0. Ground rules

1. **Never `git commit`.** Owner commits, one per phase, using the suggested messages in §11.
2. **Never `gofmt -w .`** repo-wide — CRLF checkout. gofmt only files you edited. Verify with `git diff --name-only`.
3. **Toolchain:** go1.27.0 windows/amd64. Verify stdlib signatures against local GOROOT before use. `testing/synctest` is stable.
5. **Every phase gate:** `go build ./... ; go vet ./... ; go test -race ./...` green, plus the phase's specific tests. Never start a phase on a red tree.
6. **synctest rules:** no `T.Run`/`T.Parallel` inside a bubble; every bubble goroutine drains before `synctest.Test` returns; cleanup runs inside the bubble; use `synctest.Sleep`, not `time.Sleep`.
7. **Learned pitfalls:** `testify` compares durations via `.Milliseconds()`; call `t.Context()` before spawning long-lived goroutines; rewrite import paths with word boundaries (`internal/cli` is a prefix of `internal/client`); use `;` not `&&` between PowerShell commands if the shell is old.

### Current state you inherit

Steps 1–8, 10, 13, 14 of the prior refactor are merged. Package tree `internal/{engine,enginetest,node,client,server,daemon,config,queue/*,transport/*,ui/*}` exists. Queue tests use synctest. Error sentinels are package-level. Shim packages are gone. Steps 9 (arch-lint), 11 (docs), 12 (CI), 15 (DoD) remain open; this plan slots before them.

---

## 1. The model

### 1.1 Four layers, strict resolution, one home per parameter

```
per-call        EnqueueOption / NackOption                    (ephemeral)
queue spec      QueueOption → queues.json                     (persistent)
session         flags / env / mikyu.yaml / Open(dir, opts)    (per-process)
built-in        constants in one table
```

Resolution: **call > spec > session > built-in**, field by field. A parameter lives in exactly one layer. No side doors.

Session defaults and queue specs are the **same Go type**: `spec.Spec`. Every layer is a possibly-sparse Spec. One merge function. One `describe` that shows the merge with provenance. One wire JSON codec. One registry entry.

### 1.2 File layout — one place per role

| Role | Path | Written by | Human-edited? |
|---|---|---|---|
| Session config | `mikyu.yaml` (user config dir or `--config`) | bootstrap once | yes |
| Queue registry | `<data-dir>/queues.json` | node only (atomic) | no — inspect via `mikyu describe` |
| Directory lock | `<data-dir>/mikyu.lock` | held while any node is open | no |
| Engine data | `<data-dir>/<queue>/*.log`, `manager-offsets.dat` | engine | no |

Rules:
- **No per-queue sidecar files.** Registry is one sparse JSON file. Queues absent from it run on session defaults; entries whose directory vanished are purged on startup (log warning); directories without entries are adopted (stay sparse).
- **Never write queue facts into `mikyu.yaml`.** That file is human input; mixing machine state destroys hand edits and comments.
- **Engine never reads `queues.json`.** The node owns the file and hands per-queue `engine.Config` down.

### 1.3 Process-level embedded sharing

SQLite allows many handles to one database file in one process. So does Mikyu.

- `<data-dir>/mikyu.lock` is acquired once per **process** on first `Open`.
- Additional `Open` calls in the same process **ref-count** the existing `*node.Node`.
- `Close` decrements; the last `Close` releases the OS lock.
- A **second process** sees `ErrLocked` with a message naming the running server and offering `--target`.
- `Dial` is unaffected; it is a network client.

This is the same idea as `database/sql`’s single `*sql.DB` per `driver.Connector`, generalized to the facade.

### 1.4 Parity invariants (CI-enforced, Phase I)

1. Every parameter is reachable identically from: SDK options, CLI flags, env vars, `mikyu.yaml`, the wire, and `queues.json`. Curated CLI subsets are banned.
2. Naming law: the CLI verb, SDK method, struct field, option name, wire op, and yaml/json key of a concept are the **same word**. Any word not in the vocabulary table is a bug.
3. `create-with-flags → describe → restart → describe` yields byte-identical output. Always. Embedded and remote.

---

## 2. Vocabulary (canonical; deviations are bugs)

| Concept | Canonical | Forms |
|---|---|---|
| write a log record | **Append** | `q.Append`, `AppendBatch`, CLI `append`, `OpAppend` |
| read a log record | **Read** | `q.Read`, `ReadBatch`, CLI `read`, `OpRead` |
| last written log offset | **LastOffset** | `q.LastOffset`, `OpLastOffset` |
| stream log records | **Tail** | `q.Tail` (client composite of Read+LastOffset; no dedicated wire op) |
| write a task | **Enqueue** | `q.Enqueue`, CLI `enqueue`, `OpEnqueue` |
| take a task | **Dequeue** | `q.Dequeue`, `q.TryDequeue`, CLI `dequeue`, `OpDequeue` |
| finish a task OK | **Ack** | `q.Ack(ctx, ref)`, CLI `ack`, `OpAck` |
| report delivery failed | **Nack** | `q.Nack(ctx, ref, opts...)`, CLI `nack`, `OpNack` |
| renew a lease | **Extend** | `q.Extend(ctx, ref, d)`, CLI `extend`, `OpExtend` |
| dead-letter list / remove | **DeadLetters** / **DeadLetterAck** | CLI `dlq` / `dlq-ack`, `OpDeadLetters` / `OpDeadLetterAck` |
| consumer offsets | **Group: Commit / Committed** | `g.Commit(ctx)`, `g.Committed(ctx)` |
| drop old records | **Truncate** | `client.Truncate(ctx, name, minOffset)`, CLI `truncate`, `OpTruncate` |
| whole-system facts | **Info** | `client.Info(ctx)`, CLI `info`, `OpInfo` |
| one queue’s truth | **DescribeQueue** | `client.DescribeQueue(ctx, name)`, CLI `describe`, `OpDescribeQueue` |
| change queue config | **UpdateQueue** | `client.UpdateQueue(ctx, name, opts...)`, CLI `update`, `OpUpdateQueue` |
| open embedded | **Open(dir, opts...)** | facade |
| connect remote | **Dial(target, opts...)** | facade |

**Deleted:** `Produce`, `Publish`, `Pop`, `Stats`, `TruncateQueue`, `FetchOffset` (public), `TaskOption`, `FailOption`, `WithScheduleAt`, `WithVisibilityTimeout`, all `*ID` sibling methods, `OriginalOffset()`, `DequeueNow`.

**Option types:** `Option` (session), `QueueOption` (spec), `EnqueueOption`, `NackOption`. Cross-layer application is a compile error — that is the point.

---

## 3. Shape rules (constitution for types, not values)

Six rules. Every divergence in the current repo violates one of them.

### S1. Collapse sibling methods to one interface-typed method

`Ack`/`AckID`, `Nack`/`NackID`, `Extend`/`ExtendID`, `AckDeadLetter`/`AckDeadLetterID` become one method each taking a `TaskRef`:

```go
// TaskRef identifies a task for Ack/Nack/Extend/DeadLetterAck.
// Task carries full lease identity (strict); TaskID carries only the identifier.
type TaskRef interface {
    taskRef()
    id() string
}

type TaskID string
func (TaskID) taskRef()      {}
func (id TaskID) id() string { return string(id) }

// taskqueue.Task implements TaskRef:
func (t Task) taskRef()      {}
func (t Task) id() string    { return t.ID }
```

```go
Ack(ctx context.Context, ref TaskRef) error
Nack(ctx context.Context, ref TaskRef, opts ...NackOption) error
Extend(ctx context.Context, ref TaskRef, timeout time.Duration) error
AckDeadLetter(ctx context.Context, ref TaskRef) error
```

The wire mode byte derives from the concrete type via a type switch. Stale-lease protection is preserved: passing a stale `Task` still returns `ErrTaskStale`; passing a `TaskID` skips that check by design.

### S2. One struct, dual tags

Every spec type carries both `json` and `yaml` tags with identical key text. `internal/config.EngineConfig`/`QueueConfig` are deleted in Phase G; the config package becomes a thin loader that populates a `spec.Spec`.

Rule is enforced by reflection test in Phase I.

### S3. One name per semantic; suffixes only when meaningful

`TruncateBefore` (primitive, the `Before` carries meaning) → `Truncate` (driver, client). One name at the client. The `Queue` suffix was decorative.

Same for `Info` (was `Stats`, renamed because it now carries uptime/version/dir) and `Committed` (was `FetchOffset`, renamed because the operation means “what did I last commit”).

### S4. `Try` prefix for non-blocking siblings

`DequeueNow` → `TryDequeue`. `Try` unambiguously means “return promptly whether or not the operation succeeds.” `Now` is temporally ambiguous.

### S5. Encapsulate embedding targets

`group.Base` becomes private-fields + methods:

```go
type Base struct {
    name      string
    mu        sync.Mutex
    highWater uint64
    closed    bool
    commitFn  CommitFunc
}
func (g *Base) Name() string
func (g *Base) Mark(offset uint64)
func (g *Base) Commit(ctx context.Context) error
func (g *Base) Committed() uint64
func (g *Base) Close() error
```

Callers stop reaching into `g.Mu`/`g.HighWater`.

### S6. No public aliases of internal types

`mikyu.Info` and `mikyu.QueueDescription` are their own structs with their own fields. `engine.EngineStats` is not re-exported. This is why `database/sql` has `sql.DBStats` rather than exposing a `sqlite3_int64`.

### S7. Constructor prefixes have fixed meaning

| Prefix | Meaning |
|---|---|
| `Open` | acquire a resource at a location; returns a live handle with a lifecycle |
| `Dial` | establish a lazy connection to a remote endpoint |
| `New` | construct from parts; no I/O beyond init; closed by caller |

`New*` are internal constructors used by `Open`/`Dial` and by tests. Document the rule in `doc.go`.

### S8. Error sentinels are namespaced

Public sentinels re-exported by the `mikyu` facade must have unique identity. A reflection test in Phase I walks every `Err*` in the facade and asserts no two are equal. Where a private package needs a same-named sentinel (`protocol.ErrInvalidTask`), it is unexported and translated at the boundary.

---

## 4. Parameter table (single source of truth)

Canonical key = json key = yaml key. Env = `MIKYU_` + SCREAMING_SNAKE of it. CLI flag = kebab-case. SDK option = `With` + PascalCase.

### 4.1 Queue-scoped (appear on `create` and, where mutable, `update`)

| canonical | type | mutable | meaning |
|---|---|---|---|
| `batch_size` | int | yes | max pending appends before WAL flush |
| `max_interval` | duration | yes | max delay forcing a WAL flush |
| `min_interval` | duration | yes | micro-linger grouping concurrent appends |
| `segment_size` | bytes | no | roll threshold (was `max_segment_bytes`) |
| `segment_age` | duration | yes | roll threshold by age (was `max_segment_age`) |
| `max_message_bytes` | bytes | no | producer contract |
| `encrypted` | bool | no | OS directory encryption |
| `fsync` | bool | no | durability class |
| `retention_age` | duration | yes | delete records older than this (capped by floors) |
| `retention_bytes` | bytes | yes | cap on-disk size, drop whole oldest segments |
| `default_visibility` | duration | yes | task-queue default lease |
| `default_max_attempts` | uint32 | yes | task-queue default retry budget |

Retention words mean **delete**. Segment words mean **roll**. Never mix.

### 4.2 Per-call

| canonical | type | option |
|---|---|---|
| `visibility` | duration | `WithVisibility` (EnqueueOption) |
| `max_attempts` | uint32 | `WithMaxAttempts` (EnqueueOption) |
| `scheduled_at` | time | `WithScheduledAt` (EnqueueOption) |
| `retry_after` | duration | `WithRetryAfter` (NackOption) |

Zero-value semantics: unset = inherit (queue default > built-in).

### 4.3 Session-only (never on queue ops, never on the wire)

`target`, `data_dir`, `tls_enabled`, `tls_cert_file`, `tls_key_file`, `max_conns`. Session defaults additionally include all of §4.1 as fallback for new queues.

### 4.4 Daemon-internal — no flag, no env, no yaml, no wire

Reconciler tick (30s), checkpoint interval (5s), ring-cache capacity, segment prealloc bounds, fast/slow-fill thresholds. Implementation, not config.

---

## 5. Persistence format

### 5.1 `<data-dir>/queues.json`

```json
{
  "version": 1,
  "queues": {
    "payments": {
      "engine":    { "fsync": true, "max_message_bytes": 65536 },
      "retention": { "age": "72h", "bytes": 1073741824 },
      "task":      { "default_visibility": "1m", "default_max_attempts": 3 }
    }
  }
}
```

- Sparse: only explicitly-set fields appear (Go pointer fields).
- Durations are strings (`"10ms"`) via `spec.Duration`.
- Written by the node under its mutex: `queues.json.tmp` → `Sync()` → `Rename`.
- **Unreadable/corrupt file = refuse to start.** `ErrRegistryCorrupt` naming the path. Never boot with silent defaults on a corrupt registry.
- A `queues:` map in `mikyu.yaml` seeds creation at startup through `node.CreateQueue` — same code path as a client — then the registry is the truth.

### 5.2 `mikyu.yaml` (same shape, one schema)

```yaml
server:
  target: "tcp://127.0.0.1:7420"
  data_dir: "./data"
  tls_enabled: false
  tls_cert_file: ""
  tls_key_file: ""

defaults:
  engine:
    batch_size: 256
    max_interval: "10ms"
    min_interval: "30us"
    segment_size: 67108864
    max_message_bytes: 1048576
    segment_age: "1h"
    encrypted: false
    fsync: true
  retention:
    age: "0s"
    bytes: 0
  task:
    default_visibility: "30s"
    default_max_attempts: 5

queues:
  payments:
    engine: { fsync: true, max_message_bytes: 65536 }
    retention: { age: "72h", bytes: 1073741824 }
    task: { default_visibility: "1m", default_max_attempts: 3 }
```

`defaults` is a full `spec.Spec`. `queues` is `map[string]spec.Spec` — the same type `queues.json` serializes. Env vars are flat canonical keys (`MIKYU_BATCH_SIZE`, not `MIKYU_ENGINE_BATCH_SIZE`). Old keys hard-error with a one-line migration hint.

---

## 6. The wire protocol (one format, chosen once)

Opcodes are grouped by subsystem nibble (`dispatch = op >> 4`). No version field on the protocol — opcodes are frozen once written, added never reused. A `u16 specVersion` prefixes only spec-bearing admin frames: that is a schema number for the JSON payload, not a protocol version.

```
0x01 CreateQueue    req: bytes(name) u16 specVersion bytes(json Spec)
0x02 DeleteQueue    req: bytes(name)
0x03 ListQueues     req: —                          resp: u32 count + bytes(name)*
0x04 UpdateQueue    req: bytes(name) u16 specVersion bytes(json patch)
0x05 DescribeQueue  req: bytes(name)
                    resp: u16 specVersion bytes(json DescribeResult)
0x06 Truncate       req: bytes(name) u64 minOffset  resp: u64 reclaimed
0x07 Info           req: —                          resp: bytes(json Info)

0x11 Append         req: bytes(queue) bytes(payload)              resp: u64 offset
0x12 AppendBatch    req: bytes(queue) u32 n + bytes*              resp: u32 n + u64*
0x13 Read           req: bytes(queue) u64 offset                  resp: message record
0x14 ReadBatch      req: bytes(queue) u64 start u32 count         resp: u32 n + records
0x15 LastOffset     req: bytes(queue)                             resp: u64
0x16 Commit         req: bytes(queue) bytes(group) u64 offset
0x17 Committed      req: bytes(queue) bytes(group)                resp: u64

0x21 Enqueue        req: bytes(queue) bytes(payload) u64 visibility u32 maxAttempts u64 scheduledAt
                    resp: task record
0x22 Dequeue        req: bytes(queue) u64 timeoutMs               resp: task record
0x23 Ack            req: bytes(queue) u8 mode [mode-specific]
0x24 Nack           req: bytes(queue) u8 mode [mode-specific] u64 retryAfter
0x25 Extend         req: bytes(queue) u8 mode [mode-specific] u64 timeout
0x26 DeadLetters    req: bytes(queue)                             resp: u32 n + task*
0x27 DeadLetterAck  req: bytes(queue) u8 mode [mode-specific]

0x7E (reserved handshake)
0xFF Error          u8 code + bytes(message)
```

Mode byte:
- `0` — full `protocol.Task` record follows.
- `1` — `bytes(taskID)` follows.

The mode is derived from the `TaskRef` concrete type on the client and reconstructed on the server. No `*ByID` op proliferation. Stale-lease strictness is preserved: mode 0 rejects stale deliveries; mode 1 does not.

Error codes re-blocked:
- `0x01–0x0F` admin: Unknown, QueueNotFound, QueueExists, InvalidConfig, ImmutableField, SpecTooNew, SpecTooLarge.
- `0x11–0x1F` storage: OffsetNotFound, OffsetTruncated, MessageTooLarge, QueueClosed.
- `0x21–0x2F` task: NotAvailable, NotFound, NotInflight, Stale, Invalid, CheckpointCorrupt.
- `0x31–0x3F` dead-letter: NotFound, Stale.

JSON payload guards: reject spec/patch/describe/info payloads > 1 MiB (`ErrSpecTooLarge`). Server accepts `specVersion <= CurrentSpecVersion`; else `ErrSpecTooNew`.

`DescribeResult` (json):
```json
{
  "spec": {...}, "effective": {...}, "defaults": {...},
  "stats": {"message_count":0, "segment_count":0, "size_bytes":0,
            "last_offset":0, "min_offset":0}
}
```

`Info` (json):
```json
{"version":"","uptime_ns":0, "data_dir":"",
 "active_queues":0, "total_bytes":0,
 "queues":{"name":{"message_count":0,...}}}
```

---

## 7. Phases

Each phase lists: **goal**, **new/changed files**, **work**, **tests that prove it**, **gate**. Phases run in order; each must leave the tree green.

### Phase A — `internal/spec` (types, table, validation)

**Goal:** every configuration value is a `spec.Param`. One struct, dual tags. Strict validation rejects misconfigurations before they reach storage.

**Files (new):** `internal/spec/spec.go`, `table.go`, `validate.go`, `codec.go`, `errors.go`, `spec_test.go`, `validate_test.go`.

**Work:**
1. `spec.Spec` with `Engine`, `Retention`, `Task` sub-structs, pointer fields, both `json` and `yaml` tags with identical key text.
2. `spec.Duration` wrapping `time.Duration`; JSON/YAML codec accepts `"10ms"` strings and int64 nanos.
3. `Spec.Merge(p Spec) Spec` — pointer-presence overlay.
4. `Spec.Effective(session, builtin Spec) Spec` — pure, no I/O.
5. `spec.Table []Param` covering every §4.1 and §4.2 entry with `Key`, `Kind`, `Layer`, `Mutability`, `EnvName`, `FlagName`, `OptionName`, `Apply func(*Spec, Value)`.
6. `spec.ValidateCreate(s) error`, `spec.ValidateUpdate(s) error`, `spec.IsCreateOnly(key) bool`.
7. Sentinels: `ErrImmutableField` (names field), `ErrSpecTooLarge`, `ErrSpecTooNew`, `ErrInvalidConfig`.

**Tests that prove it:**
- `TestDurationRoundTrip` — JSON and YAML, both string and int64 forms.
- `TestMergePresence` — nil stays nil; set wins.
- `TestTableSelfCheck` — keys unique; every entry has env/flag/option names; every mutable field appears in `update` schema.
- `TestSpecTagsIdentical` — reflection: every field has `json` tag and `yaml` tag with identical key text.
- `TestValidateRejects` — negative/zero values, `max_message_bytes > segment_size`, unknown keys via `DisallowUnknownFields`.
- `TestValidateUpdateRejectsCreateOnly` — every `Mutability == create-only` field rejected by `ValidateUpdate` with `ErrImmutableField` naming the field.

**Gate:** `go build ./... ; go vet ./... ; go test -race ./internal/spec/...`

**Suggested commit:** `spec: typed session/queue spec, params table, JSON+yaml codecs, strict validation`

---

### Phase B — Engine per-queue config

**Goal:** engine loads existing queues with per-queue config; live config updates are possible without reopening.

**Files:** `internal/engine/config.go`, `queue.go`, `manager.go`, `wal.go`, `internal/config/config.go` (call sites only), plus tests.

**Work:**
1. Rename `Config.MaxSegmentBytes` → `SegmentSize`, `Config.MaxSegmentAge` → `SegmentAge`. Update `withDefaults`, `AppendBatch` roll logic, `MaxAppendPayloadBytes`, `roll()`, and `internal/config`'s yaml field names temporarily (Phase G re-roots config).
2. `engine.OpenManager(dir string, cfg Config, perQueue map[string]Config) (*Manager, error)` — third arg may be nil; loads existing queue dirs preferring `perQueue[name]` merged over global via `withDefaults`.
3. `Queue.ApplyConfig(cfg Config)` — write-lock, updates `q.cfg`, calls `wal.UpdateConfig` for mutable fields, leaves `SegmentSize`/`MaxMessageBytes`/`Encrypted`/`Fsync` untouched. Document that only mutable fields can reach here by construction.
4. `wal.UpdateConfig(batchSize int, maxInterval, minInterval time.Duration)` — safely swaps ticker interval and batch threshold under the WAL mutex.

**Tests that prove it:**
- `TestPerQueueConfigAppliesOnLoad` — create two queues with different `MaxMessageBytes`, reopen manager with `perQueue` map, assert each rejects/accepts its boundary.
- `TestApplyConfigLiveEffect` — `ApplyConfig` with a smaller batch size changes WAL flush timing in a bounded test (synctest).
- `TestApplyConfigIgnoresImmutable` — passing a config whose `SegmentSize` differs has no effect (documented contract).
- `TestRenamedFieldsCompileAndWork` — existing engine tests updated.

**Gate:** full tree green.

**Suggested commit:** `engine: per-queue config injection; rename segment_size/segment_age; live ApplyConfig`

---

### Phase C — Node registry, lock, process pool

**Goal:** queue specs survive restart. One process can `Open` the same dir many times. A second process fails fast.

**Files (new):** `internal/node/registry.go`, `lock.go`, `pool.go`, `registry_test.go`, `lock_test.go`, `pool_test.go`.

**Work:**
1. `registry.go`: `type registry struct { Version int; Queues map[string]spec.Spec }`. Load with `DisallowUnknownFields`; save via tmp+fsync+rename. Corrupt → `ErrRegistryCorrupt` wrapping the path. Purge orphan entries (log via `slog`); adopt orphan dirs (leave sparse).
2. `lock.go`: `<data-dir>/mikyu.lock`, exclusive per process. Unix: `syscall.Flock(LOCK_EX|LOCK_NB)`. Windows: `windows.LockFileEx` (new direct dependency, note in commit). Failure → `ErrLocked` with “data directory is in use by a running server — use --target to connect to it”.
3. `pool.go`: package-level `map[string]*nodeEntry` keyed by `filepath.Abs(dir)`. `nodeEntry` holds `*Node` and a refcount. `Acquire(dir, defaults spec.Spec) (*Node, release func(), error)`.
4. `Node.Open(dir string, defaults spec.Spec)` (internal) — acquire lock → load registry → compute per-queue `engine.Config` (registry spec merged over defaults) → `OpenManager`.
5. Node API:
   - `CreateQueue(ctx, name, s spec.Spec) error` — validate create-only vs built-in-only; persist registry; open engine queue.
   - `UpdateQueue(ctx, name, patch spec.Spec) error` — read-modify-write; create-only field set → `ErrImmutableField` (name the field); persist, `Queue.ApplyConfig`, `retention.SetPolicy` (Phase E).
   - `DescribeQueue(ctx, name) (QueueDescription, error)` — `{Spec, Effective, Defaults, Stats}`.
   - `Info(ctx) (Info, error)` — manager stats + `runtime/debug.ReadBuildInfo` version + open-time uptime.
6. `DeleteQueue` updates registry after `mgr.DeleteQueue` succeeds.
7. `daemon.Run` and `client.NewEmbedded` route through `node.Acquire`.

**Tests that prove it:**
- `TestRestartPreservesSpec` — create with non-default spec, `Close`, reopen, describe identical. **Regression test for the bug this phase kills.**
- `TestRegistryCorruptRefusesStartup` — write garbage into `queues.json`, `Open` returns `ErrRegistryCorrupt` naming the path.
- `TestRegistryAtomicUnderKill` — spawn a subprocess that writes then is SIGKILLed; parent reads the registry and finds it either pre- or post-write, never partial.
- `TestLockedDirRejectsSecondProcess` — spawn a subprocess that holds the lock; parent `Open` returns `ErrLocked`.
- `TestProcessPoolSharesNode` — two `Open` calls in-process return clients backed by the same `*Node`; `Close` on one does not release the lock; second `Close` does.
- `TestAdoptOrphanAndPurgeOrphan` — dir without registry entry adopts; registry entry without dir purges and logs.
- `TestDescribeAfterCreateWithFlags` — round-trip spec/effective/defaults/stats.

**Gate:** full tree green; new synctest-based tests use virtual time.

**Suggested commit:** `node: queues.json registry, directory lock, process pool — fixes per-queue config lost across restarts`

---

### Phase D — Transport: the wire protocol

**Goal:** one wire format, chosen once. Nibble-grouped opcodes. `TaskRef` mode byte replaces `*ByID` clones. `specVersion` prefixes spec-bearing frames.

**Files:** `internal/transport/protocol/opcodes.go`, `errors.go` (new), `frames.go` (unchanged), `task.go` (add mode byte helpers), all `internal/server/handlers_*.go`, `internal/server/dispatch.go`, `internal/server/errors.go`, `internal/client/errors.go`, `internal/client/remote_*.go`, plus tests.

**Work:**
1. `opcodes.go`: opcode table per §6 verbatim. Constant `CurrentSpecVersion uint16 = 1`. `MaxSpecPayload = 1 << 20`. Delete `OpProduce`, `OpPublish` (n/a), `OpTail`, `OpFetchOffset`, `OpTask*ByID` family.
2. `errors.go`: re-block error codes per §6. Add `ErrCodeInvalidConfig`, `ErrCodeImmutableField`, `ErrCodeSpecTooNew`, `ErrCodeSpecTooLarge`.
3. `task.go`: add `WriteMode(b *Buffer, mode byte)`; `ReadMode(r *Reader) (byte, error)`. Add `EncodeTaskRef(b *Buffer, ref)` and `DecodeTaskRef(r) (taskID string, task *Task, mode byte, err error)`.
4. Handlers: `handleAppend`, `handleEnqueue`, `handleDequeue`, `handleAck`, `handleNack`, `handleExtend`, `handleDeadLetters`, `handleDeadLetterAck`. Delete `handleProduce`, `handlePublish` (n/a), `handlePop`, all `handleTask*ByID`. `handleCreateQueue`/`handleUpdateQueue`/`handleDescribeQueue` decode `specVersion` and JSON payload.
5. `handleInfo` returns JSON `Info`. Delete binary `OpStats` handler.
6. `server/errors.go`: add mappings for `node.ErrRegistryCorrupt`, `node.ErrLocked`, `spec.ErrImmutableField`, `spec.ErrSpecTooNew`, `spec.ErrSpecTooLarge`, `spec.ErrInvalidConfig`.
7. `client/errors.go`: extend `decodeProtocolError` with the new codes. `ErrProtocol` for unrecognized.
8. Client remote methods rewritten per §6. `remoteTaskQueue.Enqueue` sends **unset zeros** (0 visibility, 0 attempts, 0 scheduledAt) when options are absent, letting the server resolve queue defaults (fixes the remote-default bypass bug). `remoteTaskQueue.Dequeue` sends `timeoutMs`; the server clamps to `maxDequeueWait`.
9. Delete `remote_group.FetchOffset` → `Committed`; wire `OpCommitted`.

**Tests that prove it:**
- `TestOpcodeNibbleGrouping` — table-driven: every opcode's `>> 4` matches its subsystem constant.
- `TestFrameRoundTripAllOps` — every op encodes and decodes.
- `TestSpecVersionTooNew` — client sends `ver = CurrentSpecVersion + 1`; server returns `ErrCodeSpecTooNew`; client surfaces `spec.ErrSpecTooNew`.
- `TestSpecPayloadTooLarge` — 2 MiB spec frame → `ErrSpecTooLarge`.
- `TestAckModeByteStrict` — mode 0 with stale `DeliveryID` → `ErrCodeTaskStale`; mode 0 with fresh → OK.
- `TestAckModeByteTaskID` — mode 1 with a valid ID → OK; mode 1 with an ID that is not leased → `ErrCodeTaskNotInflight`.
- `TestRemoteEnqueueUsesQueueDefaults` — create remote queue with `default_visibility: 1m`, `Enqueue` with no opts, lease deadline is ~1m.
- `TestRemoteDequeueLongPoll` — 200ms budget blocks ~200ms; empty queue returns `ErrTaskNotAvailable`.
- `TestUpdateQueueRejectsImmutableField` — remote `UpdateQueue` with `segment_size` → `ErrImmutableField` naming the field.

**Gate:** full tree green.

**Suggested commit:** `transport: define the wire protocol (nibble opcodes, specVersion on admin frames, TaskRef mode byte)`

---

### Phase E — Retention policy

**Goal:** age and size retention floors that never exceed the drivers’ safe floor.

**Files:** `internal/queue/retention/coordinator.go`, `internal/engine/compact.go`, `storage.go`, `internal/enginetest/memstorage.go`, `internal/node/node.go`, plus tests.

**Work:**
1. `retention.Policy struct { Age time.Duration; Bytes int64 }` (0 = unlimited). Stored on `QueueTarget`.
2. `Coordinator.SetPolicy(name string, p Policy)`.
3. In `ReconcileQueueAt`: `policyFloor = max(ageFloor, sizeFloor)`; `safeFloor = min(policyFloor, driverSafeFloor)`; then truncate. Package doc states loudly: **retention caps deletability; it never mandates it past consumer lag.**
4. `engine.Storage` gains:
   - `FloorOlderThan(ctx, olderThan time.Time) (uint64, error)` — walk segments oldest→newest; a segment qualifies only if the **last record’s timestamp** is `< olderThan`. Stop at the first segment whose last record is `>= olderThan` or is active. (Timestamps are monotonic; last-record is correct and cheaper than first.)
   - `FloorForSize(ctx, maxBytes int64) (uint64, error)` — pure arithmetic over `q.segments` sizes; drop whole oldest segments while Σ > cap; never drop active.
5. `enginetest.MemStorage` implements both with matching semantics.
6. Node: `CreateQueue`/`UpdateQueue` push Policy into the coordinator. The 30s reconciler applies. On **policy shrink**, an async goroutine triggers one immediate `ReconcileQueue(name)` (ctx-bound).

**Tests that prove it:**
- `TestFloorOlderThanSegmentBoundary` — segment with records straddling the cutoff is not dropped.
- `TestFloorOlderThanActiveNeverDropped`.
- `TestFloorForSizeDropsWholeSegments` — never partial.
- `TestFloorForSizeNeverActive`.
- `TestRetentionCappedByLaggingGroup` — produce N, consumer group marks K < N, policy says drop all, assert truncation stops at K. (synctest.)
- `TestRetentionShrinkTriggersImmediateReconcile` — policy set, truncation observed before the 30s tick.
- `TestOffsetTruncatedAfterPolicyTruncation` — readers below the new min get `ErrOffsetTruncated`.

**Gate:** full tree green; `MemStorage` satisfies `engine.Storage` compile-time.

**Suggested commit:** `retention: age/bytes policy floors, capped by driver safe floors`

---

## Phase F — Architecture reconciliation

**Goal:** bring the layering rules, the CI gates, and the config-type ownership in line with the `spec` package that Phase A introduced. No behavior change. No file moves. Green tree in and out.

This phase exists because Phase A introduced `internal/spec` as a new bottom-layer package and Phases B–E already route `node`, `client`, `config`, and the retention coordinator through it, but the original `plan.md` §Layering and the `go-arch-lint` rules from `plan.md` Step 9 do not mention `spec`. If we continue without fixing this, Phases D, G, H, and I of the original reconcile plan will either fail the arch-lint gate or, worse, force us to duplicate state (`protocol.CurrentSpecVersion` alongside `spec.CurrentSpecVersion`) to satisfy stale rules.

**Files:** `.go-arch-lint.yml`, `internal/spec/codec.go`, `internal/transport/protocol/opcodes.go`, `internal/node/node.go`, `internal/queue/taskqueue/taskqueue.go`, `internal/queue/taskqueue/task.go`, plus tests. `ARCHITECTURE.md` is **not** touched here; it lands in Phase J.11.

### F.1 Codify `spec` as a bottom-layer package

Update `.go-arch-lint.yml` only. Add `spec` as a component with `mayDependOn: []`, and add `spec` to `mayDependOn` of `node`, `client`, `server`, `daemon`, `config`, and `ui`. Leave `engine`, `queue/*`, `transport/*` without a `spec` dependency — they must not see spec directly.

Add a one-line package comment to `internal/spec/codec.go` or a new `internal/spec/doc.go`:

```go
// Package spec is the schema, parameter table, and codec for all Mikyu configuration.
package spec
```

That is the whole package doc for `spec`. No `ARCHITECTURE.md` reference, no invariants essay.

### F.2 Comment-style rule

Formalize the style in a short comment at the top of `internal/spec/doc.go` (or, if you prefer a single home, in `.golangci.yml` as a `godot`/`revive` setting). The rule:

> **Every symbol under `internal/**` and `cmd/**` carries at most one line of comment, at the top.** Package docs are one line. Exported symbols in the `mikyu` package are the only place with descriptive godoc.

Concretely, when writing new code in Phases G–J:

- Internal types, functions, methods, and package clauses get a single-sentence comment or none.
- `mikyu.Client`, `mikyu.LogQueue`, `mikyu.TaskQueue`, `mikyu.TaskRef`, `mikyu.Info`, `mikyu.QueueDescription`, and every facade error sentinel get full godoc, because they are the contract an end user reads.
- Comments are not used to explain *what* a function does when the signature already says it. Reserve them for invariants the signature cannot express (locking, ordering, ownership).

### F.3 Single source of truth for spec version and payload cap

`spec.CurrentSpecVersion` and `spec.MaxSpecPayload` are the only definitions. `protocol` must not redefine them. Move the two constants out of `protocol/opcodes.go` if they were added there. The wire's `u16 specVersion` is opaque to `protocol`; the server and client call `spec.CheckSpecVersion` and enforce `spec.MaxSpecPayload` at the boundary.

### F.4 Process-pool session defaults: first `Open` wins, mismatch is an error

`node.Acquire(dir, defaults spec.Spec)` ref-counts one `*Node` per directory. If a second in-process `Open` passes different non-zero session defaults, the second call must fail loudly instead of silently inheriting the first caller's defaults.

```go
// ErrSessionDefaultsMismatch: same dir opened twice with different session defaults.
var ErrSessionDefaultsMismatch = errors.New("process already opened this data directory with different session defaults")
```

Add `spec.Equal(a, b Spec) bool` — pointer-presence-aware comparison over every `Table` entry.

In `node.Acquire`:

```go
if e, ok := poolMap[abs]; ok {
    if !spec.Equal(e.node.defaults, defaults) {
        poolMu.Unlock()
        return nil, nil, fmt.Errorf("%w (dir %s)", ErrSessionDefaultsMismatch, abs)
    }
    e.refs++
    ...
}
```

Re-export from `mikyu`.

### F.5 Task-queue defaults must be updatable live

`Node.UpdateQueue` currently updates the registry, applies engine config, and updates the retention policy, but cached `taskqueue.TaskQueue` values keep their constructor-time defaults. After `UpdateQueue(default_visibility=2m)`, an `Enqueue` with no options still uses the old `1m`.

`taskqueue.TaskQueue` gains:

```go
// SetDefaults changes the fallback visibility and attempts for future enqueues.
func (q *TaskQueue) SetDefaults(visibility time.Duration, maxAttempts uint32)
```

`Node.UpdateQueue` calls it on the cached handle. Future `Enqueue` calls observe the new defaults; already-enqueued tasks keep their own `Visibility`/`MaxAttempts`.

Tests: `TestQueueDefaultUpdateAffectsFutureEnqueues`, `TestQueueDefaultUpdateSurvivesRestart`.

### F.6 Rename sweep is enforced, not aspirational

The grep list from `reconcile.md` §H.7 becomes a test using `go/parser`:

```go
func TestNoLegacyNames(t *testing.T) // scans non-test .go files for the banned identifiers
```

Banned identifiers: `Produce`, `ProduceBatch`, `Publish`, `Pop`, `Stats`, `TruncateQueue`, `FetchOffset`, `TaskOption`, `FailOption`, `WithScheduleAt`, `WithVisibilityTimeout`, `MaxSegmentAge`, `MaxSegmentBytes`, `OriginalOffset`, `DequeueNow`, plus the opcode names `OpProduce`, `OpPublish`, `OpPop`, `OpStats`, `OpTail`, `OpFetchOffset`, and every `OpTask*ByID`.

### F.7 Gate

`go build ./... ; go vet ./... ; go test -race ./...` green. `go-arch-lint check` passes with the updated rules. `TestNoLegacyNames` is present and green. `spec.Equal` and `node.ErrSessionDefaultsMismatch` have their own unit tests.

**Suggested commit:** `reconcile: formalize spec layer, single-source spec version, process-pool default mismatch, live task-queue defaults`

---

## Phase G — `TaskRef`, `EnqueueOption`/`NackOption`, per-queue defaults

**Goal:** collapse `*ID` siblings to one `TaskRef`-typed method each. Per-call options become distinct types (`EnqueueOption`, `NackOption`). Per-queue `default_visibility` / `default_max_attempts` are resolved before validation. Zero `reflect` in hot paths.

**Files:** `internal/queue/taskqueue/task.go`, `enqueue.go`, `dequeue.go`, `ack.go`, `fail.go`, `extend.go`, `dlq.go`, `lease.go`, `scheduler.go`, `checkpoint.go`, `taskqueue.go`, plus tests.

### G.1 `TaskRef` interface and `TaskID`

```go
// TaskRef identifies a task for Ack/Nack/Extend/DeadLetterAck.
type TaskRef interface {
    taskRef()
    taskID() string
}

// TaskID is a bare task identifier (no strict lease check).
type TaskID string

func (TaskID) taskRef()          {}
func (id TaskID) taskID() string { return string(id) }

func (Task) taskRef()          {}
func (t Task) taskID() string  { return t.ID }
```

`leaseRef`, `refFromTask`, `refFromID` are deleted. `resolveLeaseLocked` accepts a `TaskRef` and type-switches on the concrete value to decide strictness:

```go
func (q *TaskQueue) resolveLeaseLocked(ref TaskRef) (*taskState, error) {
    if ref == nil {
        return nil, ErrInvalidTask
    }
    state, ok := q.active[ref.taskID()]
    if !ok {
        return nil, ErrTaskNotFound
    }
    if !state.inFlight {
        return nil, ErrTaskNotInflight
    }
    if state.task.Visibility.IsZero() || !time.Now().Before(state.task.Visibility) {
        return nil, ErrTaskStale
    }
    if t, strict := ref.(Task); strict {
        if state.task.offset != t.offset ||
            state.task.Attempts != t.Attempts ||
            state.task.DeliveryID != t.DeliveryID ||
            t.DeliveryID == "" {
            return nil, ErrTaskStale
        }
    }
    return state, nil
}
```

### G.2 Single method per operation

```go
Ack(ctx context.Context, ref TaskRef) error
Nack(ctx context.Context, ref TaskRef, opts ...NackOption) error
Extend(ctx context.Context, ref TaskRef, timeout time.Duration) error
AckDeadLetter(ctx context.Context, ref TaskRef) error
```

Delete `AckID`, `FailID`, `Fail`, `ExtendID`, `AckDeadLetterID`. Rename `Fail` → `Nack`, `FailOption` → `NackOption`, `ResolveFailOptions` → `ResolveNackOptions`.

### G.3 Per-call option types

```go
type EnqueueOption func(*enqueueOptions)
type NackOption    func(*nackOptions)

func WithVisibility(d time.Duration) EnqueueOption
func WithMaxAttempts(n uint32) EnqueueOption
func WithScheduledAt(at time.Time) EnqueueOption
func WithRetryAfter(d time.Duration) NackOption
```

Delete `TaskOption`, `FailOption`, `WithVisibilityTimeout`, `WithScheduleAt`.

### G.4 Per-queue defaults

`TaskQueue` holds resolved defaults under `q.mu`:

```go
type TaskQueue struct {
    ...
    defaultVisibility  time.Duration
    defaultMaxAttempts uint32
    ...
}

// Defaults carry the queue's task-queue defaults into the driver.
type Defaults struct {
    Visibility  time.Duration
    MaxAttempts uint32
}

func NewTaskQueue(name string, storage engine.Storage, defaults Defaults) (*TaskQueue, error)
```

Zero `Visibility`/`MaxAttempts` fall back to the driver's built-ins (`30s`/`5`). `taskqueue` does not import `spec`.

`Enqueue` resolves first, validates second:

```go
o := enqueueOptions{
    visibility:  q.defaultVisibility,
    maxAttempts: q.defaultMaxAttempts,
    scheduledAt: time.Now(),
}
for _, opt := range opts { opt(&o) }
if o.visibility <= 0 || o.maxAttempts == 0 || o.scheduledAt.IsZero() {
    return Task{}, ErrInvalidTask
}
```

`Node.TaskQueue` reads the queue's effective `spec.Spec`, extracts `DefaultVisibility`/`DefaultMaxAttempts`, passes them into `NewTaskQueue`. `Node.UpdateQueue` calls `SetDefaults` on the cached handle.

### G.5 `Task.Offset` promoted; `OriginalOffset` deleted

`Task.Offset uint64` becomes exported. Every call site (`scheduler.go`, `checkpoint.go`, `dequeue.go`, `dlq.go`, `server/handlers_task.go`, `client/remote_taskqueue.go`) is updated. `OriginalOffset()` is deleted.

### G.6 `DequeueNow` → `TryDequeue`; `Close` removed from `ClientTaskQueue`

`Try` unambiguously means "return promptly whether or not the operation succeeds." `Close` belongs to the node, not the driver handle.

### G.7 Tests

- `TestQueueDefaultsApply` — spec `default_visibility: 1m`, `Enqueue` no opts, lease ~1m.
- `TestPerCallOverridesQueueDefault`.
- `TestDefaultsSurviveNodeRestart`.
- `TestDefaultsUpdateAfterUpdateQueue` — driver side of F.5.
- `TestAckViaTaskRef` — `Task` accepted, stale `Task` returns `ErrTaskStale`; `TaskID` accepted, strict check skipped.
- `TestNackViaTaskRef`, `TestExtendViaTaskRef`, `TestDeadLetterAckViaTaskRef`.
- `TestTryDequeueNonBlocking` — empty queue returns `ErrTaskNotAvailable` promptly.
- `TestNoIDMethods` — reflection over `ClientTaskQueue`: no method ends in `ID`.
- `TestNoCloseOnDriver` — reflection: `ClientTaskQueue` has no `Close`.

### G.8 Gate

Full tree green. `go test -race ./internal/queue/...` green. Allocation counts in `BenchmarkTaskQueueEnqueue` and `BenchmarkTaskQueueDequeueAck` unchanged from `bench/baseline.txt`.

**Suggested commit:** `taskqueue: TaskRef interface, EnqueueOption/NackOption, per-queue defaults, TryDequeue`

---

## Phase H — Client SDK, config re-root, facade

**Goal:** the client API speaks `spec.Spec` in and `QueueDescription`/`Info` out. `internal/config` collapses onto `spec`. The `mikyu` facade re-exports only what a README example needs, with descriptive godoc on every re-export.

**Files:** `internal/client/options.go`, `embedded*.go`, `remote*.go`, `types.go`, `errors.go`, `internal/config/config.go`, `internal/daemon/daemon.go`, `internal/node/node.go`, `mikyu/*.go`, plus tests.

### H.1 Client options are spec-native

```go
type SessionConfig struct {
    Spec     spec.Spec
    Target   string
    DataDir  string
    MaxConns int
    TLS      *tls.Config
}

type Option      func(*SessionConfig)
type QueueOption func(*spec.Spec)
```

`queueSpecFrom` and `queueSpecPatch` are deleted. Every queue option becomes a three-line setter consulting `spec.Table`:

```go
func WithRetentionAge(d time.Duration) QueueOption {
    return func(s *spec.Spec) { s.Retention.Age = new(spec.Duration(d)) }
}
```

Option renames, matching the canonical keys in `reconcile.md` §4.1:

| old | new |
|---|---|
| `WithQueueBatchSize` | `WithBatchSize` |
| `WithQueueMaxInterval` | `WithMaxInterval` |
| `WithQueueMinInterval` | `WithMinInterval` |
| `WithMaxSegmentBytes` | `WithSegmentSize` |
| `WithMaxMessageBytes` | `WithMaxMessageBytes` (unchanged) |
| `WithMaxSegmentAge` | `WithSegmentAge` |
| `WithDirectoryEncryption` | `WithDirectoryEncryption` (unchanged) |
| `WithQueueFsync` | `WithFsync` |
| — | `WithRetentionAge` (new) |
| — | `WithRetentionBytes` (new) |
| — | `WithDefaultVisibility` (new) |
| — | `WithDefaultMaxAttempts` (new) |

The `WithQueue*` prefix is dropped; every option on a `QueueOption` is by definition queue-scoped. `ClientOption` and `QueueOption` can share a name (`WithBatchSize`) because the option type disambiguates the layer.

### H.2 Client interface

```go
type Client interface {
    io.Closer

    CreateQueue(ctx context.Context, name string, opts ...QueueOption) error
    UpdateQueue(ctx context.Context, name string, opts ...QueueOption) error
    DescribeQueue(ctx context.Context, name string) (QueueDescription, error)
    DeleteQueue(ctx context.Context, name string) error
    ListQueues(ctx context.Context) ([]string, error)
    Truncate(ctx context.Context, name string, minOffset uint64) (int64, error)
    Info(ctx context.Context) (Info, error)

    LogQueue(name string) (LogQueue, error)
    TaskQueue(name string) (TaskQueue, error)
}
```

Delete `Stats()`, `TruncateQueue()`, and every `SystemStats` alias. `QueueDescription`, `Info`, `QueueStats` are client-level types with their own fields.

### H.3 Config re-root

Delete `internal/config.EngineConfig` and `QueueConfig`. New shape:

```go
// Config is the loader's view of mikyu.yaml.
type Config struct {
    Server   ServerConfig
    Defaults spec.Spec
    Queues   map[string]spec.Spec
}
```

`ServerConfig` keeps `Target`, `DataDir`, `TLSEnabled`, `TLSCertFile`, `TLSKeyFile`, adds `MaxConns`. `spec.Spec` uses the same struct, same tags, same codec as `queues.json`. One schema, two files.

`applyEnvOverrides` iterates `spec.Params(spec.LayerSession)` and applies `MIKYU_` + SCREAMING_SNAKE. Legacy keys (`MIKYU_ENGINE_*`, `MIKYU_SERVER_*`) hard-error with a one-line migration hint. Missing `defaults:` block falls back to `spec.Builtin()`.

### H.4 Daemon must move with the config

`daemon.Run` currently calls `config.ToSpec` and `config.ToQueueSpec` (deleted in H.3). Update:

```go
cfg, _, err := config.Load(*configPath)
...
n, release, err := node.Acquire(cfg.Server.DataDir, cfg.Defaults)
...
for name, qSpec := range cfg.Queues {
    if err := n.CreateQueue(ctx, name, qSpec); err != nil && !errors.Is(err, engine.ErrQueueExists) {
        return fmt.Errorf("reconcile queue %q: %w", name, err)
    }
}
```

### H.5 Facade — full godoc here, one-liners elsewhere

`mikyu.go` is the **only** file in the repo where descriptive godoc is written. Every re-export carries a real doc comment. Example shape:

```go
// Client is a handle to a Mikyu server, embedded or remote.
type Client interface { ... }

// Info describes the server's identity and aggregate storage state.
type Info = client.Info

// QueueDescription is the full truth about one queue: its stored spec,
// its effective spec, its session defaults, and its current stats.
type QueueDescription = client.QueueDescription

// TaskRef identifies a task for Ack/Nack/Extend/DeadLetterAck. Pass a Task
// for the strict stale-lease check; pass a TaskID to skip it.
type TaskRef = taskqueue.TaskRef

// TaskID is a bare task identifier.
type TaskID = taskqueue.TaskID

// WithVisibility sets how long a dequeued task stays leased.
func WithVisibility(d time.Duration) EnqueueOption { return taskqueue.WithVisibility(d) }

// WithMaxAttempts sets the maximum number of deliveries before dead-lettering.
func WithMaxAttempts(n uint32) EnqueueOption { return taskqueue.WithMaxAttempts(n) }

// WithScheduledAt delays the first delivery until the supplied time.
func WithScheduledAt(at time.Time) EnqueueOption { return taskqueue.WithScheduledAt(at) }

// WithRetryAfter overrides the retry delay for a nacked task.
func WithRetryAfter(d time.Duration) NackOption { return taskqueue.WithRetryAfter(d) }

// WithRetentionAge deletes records older than d, capped by consumer lag.
func WithRetentionAge(d time.Duration) QueueOption { return client.WithRetentionAge(d) }

// WithRetentionBytes caps on-disk size, dropping whole oldest segments.
func WithRetentionBytes(b int64) QueueOption { return client.WithRetentionBytes(b) }

// WithDefaultVisibility sets the queue's fallback lease for optionless enqueues.
func WithDefaultVisibility(d time.Duration) QueueOption { return client.WithDefaultVisibility(d) }

// WithDefaultMaxAttempts sets the queue's fallback retry budget.
func WithDefaultMaxAttempts(n uint32) QueueOption { return client.WithDefaultMaxAttempts(n) }
```

Delete `SystemStats`, `TaskOption`, `FailOption`, `WithScheduleAt`, `WithVisibilityTimeout`, and any `Stats()`-shaped method.

Re-export `ErrImmutableField`, `ErrInvalidConfig`, `ErrLocked`, `ErrRegistryCorrupt`, `ErrSessionDefaultsMismatch`, `ErrSpecTooNew`, `ErrSpecTooLarge`.

### H.6 Tests

- `TestEmbeddedUpdateQueueImmutable` — `errors.Is(err, mikyu.ErrImmutableField)`, message names the field.
- `TestRemoteUpdateQueueImmutable` — same over the wire.
- `TestDescribeAfterCreateWithFlags` — embedded and remote produce byte-identical `DescribeResult` JSON.
- `TestInfoShape` — embedded and remote round-trip.
- `TestConfigOldKeyHardError` — `max_segment_bytes` in `mikyu.yaml` fails with a migration hint.
- `TestConfigEnvOverridesFromTable` — every `spec.Params(LayerSession)` entry has a working env override.
- `TestOpenDefaultsMismatch` — second `Open(dir, opts...)` with different session defaults returns `ErrSessionDefaultsMismatch`.
- `TestOpenDefaultsMatchSucceeds` — second `Open` with identical defaults shares the node.

### H.7 Gate

Full tree green. `go-arch-lint check` green. `TestNoLegacyNames` green.

**Suggested commit:** `client: UpdateQueue/DescribeQueue/Info; spec-based options; config re-root onto spec.Spec`

---

## Phase I — CLI and REPL

**Goal:** every CLI verb matches the SDK vocabulary. Every `LayerSpec` and `LayerCall` parameter is reachable from the appropriate verb. `--dir` runs embedded. `--json` on `info`/`describe` is the machine contract.

**Files:** `internal/ui/cli/*.go`, `internal/ui/repl/*.go`, `internal/ui/command/*.go`, `cmd/mikyu/*.go`, plus tests.

### I.1 `--dir` mode

Both CLI and REPL gain a `--dir <path>` mode:

- `--dir` set → `mikyu.Open(dir, sessionOptsFromFlagsEnv())`
- `--dir` unset → existing daemon-discovery path via `--target` / env / `mikyu.yaml`

Same flag set for both paths.

### I.2 Verb reconciliation

| old | new | notes |
|---|---|---|
| `produce` | `append` | log write |
| `read` | `read` | unchanged |
| `tail` | `tail` | unchanged |
| `publish` | `enqueue` | task write |
| `pop` | `dequeue` | task take |
| `ack` | `ack` | unchanged |
| `nack` | `nack` | unchanged |
| `extend` | `extend` | unchanged |
| `dlq` | `dlq` | unchanged |
| `dlq-ack` | `dlq-ack` | unchanged |
| `stats` | `info` | renamed; now includes version/uptime/dir |
| — | `describe <queue>` | new |
| — | `update <queue> <flags>` | new |

Every mutating task verb takes a `TaskRef`-shaped argument: a full task record (from `dequeue --json`) or a bare ID (admin mode).

### I.3 Flag registration from `spec.Table`

No hand-listed flags. Both `create` and `update` iterate `spec.Params(spec.LayerSpec)`; `enqueue` and `nack` iterate `spec.Params(spec.LayerCall)` and switch on `p.Key`. A small `flag.Value` adapter wraps `spec.ParseValue`.

### I.4 `--json`

`info --json` and `describe --json` emit `json.MarshalIndent(mikyu.Info{})` / `mikyu.QueueDescription{}`. This is the machine contract; the styled terminal output is presentation only.

### I.5 Retention shrink requires confirmation

`update --retention-age` / `--retention-bytes` that tightens the bound prompts for confirmation, matching `truncate -y`. `--yes` skips. This is the only interactive prompt in the mutating verbs.

### I.6 REPL mirrors every command

The REPL uses the same `internal/ui/command` handlers. No parallel implementations.

### I.7 Tests

- `TestCLIAppendRoundTrip` — `mikyu --dir ./data append q "hello"` then `read`.
- `TestCLIDescribeJSONMatchesSDK` — CLI `--json` unmarshals into `mikyu.QueueDescription`; byte-equal to SDK output.
- `TestCLICreateWithAllParams` — every `LayerSpec` flag accepted; describe confirms each round-trips.
- `TestCLIUpdateImmutable` — `--segment-size` on `update` exits 2 with the field name in stderr.
- `TestCLIRetentionShrinkPrompts` — decreasing `retention_age` requires confirmation; `--yes` skips.
- `TestREPLMatchesCLIVerbs`.
- `TestCLIEnqueueAllParams` — every `LayerCall` flag on `enqueue`.
- `TestCLINackAllParams` — every `LayerCall` flag on `nack`.
- `TestNoLegacyVerbs` — reflection over the CLI command table: none of `produce`, `publish`, `pop`, `stats`.

### I.8 Gate

Full tree green. `mikyu --help`, `mikyu create --help`, `mikyu enqueue --help` list the expected flags. The sweep test from F.6 is green.

**Suggested commit:** `cli/repl: --dir mode, table-driven flags, verb reconciliation, --json`

---

## Phase J — Gates

**Goal:** every invariant the reconcile plan claims is enforced by CI. The parity gate is layer-aware. Naming, sentinels, dual-tags, comment style, and the arch-lint rules are all testable.

**Files (new):** `internal/spec/parity_test.go`, `internal/spec/table_test.go` (extended), `mikyu/surface_test.go`, `internal/audit/comment_test.go`, `.github/workflows/ci.yml`, `.go-arch-lint.yml`, `scripts/benchcheck.sh`.

### J.1 Parity gate — layer-aware

For every `Param p` in `spec.Table`, check the surfaces its layer can reach:

```
LayerSpec:
  SDK:       QueueOption with name p.OptionName exists
  CLI:       flag p.FlagName on `create`
             and on `update` iff p.Mutability == MutMutable
  env:       p.EnvName consulted by config.applyEnvOverrides
  yaml:      p.Path appears in the mikyu.yaml defaults schema
  queues:    p.Path appears in the queues.json schema
  wire:      p.Path appears in CreateQueue/UpdateQueue JSON schema

LayerCall:
  SDK:       EnqueueOption or NackOption with name p.OptionName exists
  CLI:       flag p.FlagName on `enqueue` or `nack`
  env/yaml/queues: (none — per-call values are arguments)

LayerSession:
  SDK:       Option with name p.OptionName exists
  CLI:       flag p.FlagName on `mikyu daemon` and on `--dir` mode
  env:       p.EnvName consulted
  yaml:      p.Path appears under `server:` in mikyu.yaml
  queues/wire: (none — session values never reach either)
```

Uses `reflect` over the exported API plus parsed ASTs of the two schemas. Fails with a table of missing surfaces per parameter.

### J.2 End-to-end parity

For each `LayerSpec` parameter, set a sentinel value through each of: embedded SDK, remote SDK via in-process server, CLI `create` on `--dir`, and `mikyu.yaml` defaults. Assert the four `DescribeQueue` outputs are byte-identical, and survive a `Close`+reopen.

### J.3 Naming law

Reflection over the public API:

- CLI command names contain none of: `produce`, `publish`, `pop`, `stats`, `fetchoffset`.
- Public method names contain none of: `Produce`, `Publish`, `Pop`, `Stats`, `TruncateQueue`, `FetchOffset`, `OriginalOffset`, `DequeueNow`.
- Public option constructors are exactly `spec.Param.OptionName`.

### J.4 Sentinel uniqueness

Walk every `Err*` re-exported by `mikyu`. Assert no two are `errors.Is`-equal and that each is a distinct `errors.New`.

### J.5 No `*ID` siblings; no `Close` on drivers

Reflection over `ClientTaskQueue` and `ClientLogQueue`:

- For each of `Ack`, `Nack`, `Extend`, `AckDeadLetter`, no sibling ending in `ID`.
- For both interfaces, no `Close` method.

### J.6 Dual-tag identity

Reflection over every `spec.Spec` field: `json` and `yaml` tag present, identical key text, identical `omitempty`/`omitzero` presence.

### J.7 Comment-style gate

A test using `go/parser` that walks every non-test `.go` file under `internal/**` and `cmd/**`. For every exported symbol (`ast.FuncDecl`, `ast.TypeSpec`, `ast.ValueSpec` with an exported name), the doc comment must be:

- either absent, or
- at most one sentence, on one line, immediately above the symbol.

Package clauses: the doc comment is one line.

The `mikyu` package is exempt; it is covered by the J.3 naming law and the H.5 godoc surface.

```go
func TestInternalCommentStyle(t *testing.T)
```

### J.8 Determinism

`go test -race -count=3 ./internal/queue/... ./internal/node/...` in CI.

### J.9 Benchmarks

Re-run `go test -bench=. -benchmem -tags bench ./...`. Record into `bench/coverage-after.txt`. Flag regressions >5% on `ns/op`. Flag any increase on `B/op` or `allocs/op` in `BenchmarkTaskQueueEnqueue`, `BenchmarkTaskQueueDequeueAck`, `BenchmarkWALFlushAllocations`, `BenchmarkEncodeRecord`.

### J.10 CI file

```yaml
- name: build
  run: go build ./...
- name: vet
  run: go vet ./...
- name: gofmt
  run: test -z "$(gofmt -l .)"
- name: go fix (dry run)
  run: |
    go fix -diff ./... > /tmp/fix.diff
    test ! -s /tmp/fix.diff || (cat /tmp/fix.diff; exit 1)
- name: architecture lint
  run: go-arch-lint check
- name: test (race)
  run: go test -race ./...
- name: test (determinism, repeated)
  run: go test -race -count=3 ./internal/queue/... ./internal/node/...
- name: parity gate
  run: go test -run 'TestParity|TestNamingLaw|TestSentinelUniqueness|TestNoIDSiblings|TestNoCloseOnDrivers|TestSpecTagsIdentical|TestNoLegacyNames|TestNoLegacyVerbs|TestInternalCommentStyle' ./...
- name: benchmarks (regression gate)
  run: |
    go test -bench=. -benchmem -tags bench ./... > /tmp/bench.txt
    ./scripts/benchcheck.sh bench/baseline.txt /tmp/bench.txt
```

### J.11 `ARCHITECTURE.md` — written last

Only now, with the code frozen, do you write `ARCHITECTURE.md`. It documents the layout that actually exists, not the layout the earlier plans imagined. Minimum content:

- One-page tree of `internal/` with the one-sentence role of each package.
- The `go-arch-lint` rules as they currently stand in `.go-arch-lint.yml`, quoted.
- A sentence on why `spec` is a bottom-layer package and why `engine`, `queue/*`, and `transport/*` do not import it.
- A sentence on why `protocol` is a pure DTO layer and does not import `spec`, `engine`, or `queue/*`.
- A sentence on why `node` does not import `protocol`, `server`, or `client`.
- A sentence on why `ui` imports only `client`, `command`, `daemon`, and `spec`, and why `ui/command` is the sole exception that also imports `daemon`.
- The comment-style rule from F.2.
- The pre-1.0 wire stability policy (already in `internal/transport/protocol/doc.go`, restated here for a reader who has not opened the code).

`ARCHITECTURE.md` at this point is descriptive, not prescriptive. It has nothing to enforce, because Phase J's tests already enforce everything it names.

### J.12 Gate

Every gate above is green in a clean clone. `bench/coverage-after.txt` updated. `ARCHITECTURE.md` committed. Working tree clean.

**Suggested commit:** `tests: layer-aware parity gate, naming law, sentinel uniqueness, dual-tag identity, comment style, CI wiring; docs: architecture`

---

## Layering notes to carry forward

After Phase J, `ls internal/` reads as a layer diagram:

```
spec/               schema, params table, codecs, validation
engine/             durable log, WAL, segments, offsets
enginetest/         in-memory Storage (test-only, engine-only import)
queue/
  logqueue/         log driver
  taskqueue/        task driver
  group/            consumer group offsets
  retention/        truncation arbitration
transport/
  protocol/         wire DTOs, opcodes, frames, errors
  ipc/              unix socket / named pipe
node/               local registry, pool, driver cache, retention wiring
client/             embedded and remote adapters over node / protocol
server/             per-connection dispatch over node / protocol
config/             one loader for mikyu.yaml, spec-typed
daemon/             process entry: flags, TLS, listen, serve, shutdown
ui/
  cli/              argv → client method
  repl/             stdin → client method
  command/          shared verb bodies (imports daemon only for shutdown)
```

Each arrow points one way. No cycles. `spec` and `engine` are the two bottoms. `transport` imports nothing internal. `client` and `server` never import each other. `daemon` is the only package importing both. `ui` imports `client`, `command`, `daemon`, `spec` — nothing lower.

## Comment-style rule (summary)

- `internal/**` and `cmd/**`: every package clause, type, function, method, and exported variable carries **at most one line** of comment, immediately above the symbol. Comments earn their place by naming an invariant the signature cannot express: locking discipline, ownership of a slice, ordering guarantees, which context cancels what. If the signature says it, the comment does not.
- `mikyu/**`: every exported symbol carries full godoc. This is the only package an end user reads. Package doc explains what Mikyu is and how to get started; each type and function documents its contract, concurrency, and error shape.
- Tests carry no package doc. Test functions carry at most one line.

## Definition of done (supersedes `reconcile.md` §10)

- [ ] `go build ./... ; go vet ./... ; go test -race ./...` green.
- [ ] `go-arch-lint check` green with `spec` as a bottom-layer component.
- [ ] Phase J parity, naming, sentinel, sibling, dual-tag, and comment-style tests present and green.
- [ ] `TestNoLegacyNames` and `TestNoLegacyVerbs` green.
- [ ] `mikyu --dir ./data create jobs --retention-age 72h --fsync=false` then `mikyu --dir ./data describe jobs --json` round-trips; restart `--dir`; describe identical.
- [ ] Remote `Enqueue` with no opts respects queue `default_visibility`.
- [ ] `update --segment-size` fails with `ErrImmutableField` naming the field.
- [ ] Corrupt `queues.json` refuses startup with the path and “invalid queue registry”.
- [ ] Two `mikyu.Open(dir)` with identical defaults share the node; with different defaults the second returns `ErrSessionDefaultsMismatch`.
- [ ] Stale `Task` on `Ack` returns `ErrTaskStale` over the wire; `TaskID` skips strict check.
- [ ] `mikyu info --json | ConvertFrom-Json` works; SDK `Info()` returns the same struct.
- [ ] No `*ID` sibling methods on `ClientTaskQueue`; no `Close()` on `ClientLogQueue` or `ClientTaskQueue`.
- [ ] No file outside the four-file table from `reconcile.md` §1.2 is created in a data dir.
- [ ] Every exported symbol under `internal/**` and `cmd/**` has at most one line of comment, or none.
- [ ] Every exported symbol in `mikyu` has descriptive godoc.
- [ ] Benchmarks within 5% of baseline; deltas recorded in `bench/`.
- [ ] `ARCHITECTURE.md` written after all code work; committed in Phase J.
- [ ] Owner has committed Phases A–J as separate commits; working tree clean.

## Suggested commit sequence

```
Phase A: spec: typed session/queue spec, params table, JSON+yaml codecs, strict validation
Phase B: engine: per-queue config injection; rename segment_size/segment_age; live ApplyConfig
Phase C: node: queues.json registry, directory lock, process pool — fixes per-queue config lost across restarts
Phase D: transport: define the wire protocol (nibble opcodes, specVersion on admin frames, TaskRef mode byte)
Phase E: retention: age/bytes policy floors, capped by driver safe floors
Phase F: reconcile: formalize spec layer, single-source spec version, process-pool default mismatch, live task-queue defaults
Phase G: taskqueue: TaskRef interface, EnqueueOption/NackOption, per-queue defaults, TryDequeue
Phase H: client: UpdateQueue/DescribeQueue/Info; spec-based options; config re-root onto spec.Spec
Phase I: cli/repl: --dir mode, table-driven flags, verb reconciliation, --json
Phase J: tests: layer-aware parity gate, naming law, sentinel uniqueness, dual-tag identity, comment style, CI wiring; docs: architecture
```