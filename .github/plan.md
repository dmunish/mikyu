# Mikyu Internal Refactor Plan

## Purpose

Mikyu is pre-release with a single maintainer and no external users. This plan restructures the internal tree so that:

- The dependency graph is visible from `ls internal/`.
- Every package has one job and every file has one role.
- The wire protocol layer knows nothing about the task driver.
- Embedded and daemon modes share one local-node core.
- Layering and modernization are enforced by CI.
- Hot paths are benchmarked and guarded.
- Timing-sensitive tests are deterministic.
- The daemon can be fully configured from the command line.
- A clean foundation exists for future metrics, HTTP/SSE, TUI, and foreign SDK work without implementing any of it yet.

The plan is a full replacement for any prior planning document. It is written as a sequential set of code changes a junior developer can execute without needing to interpret intent.

The plan does **not** preserve the current exported API or wire format. There are no users. Rename, reshape, and delete freely when it improves the result.

The wire format is treated as **unstable until 1.0**. No cross-version compatibility is guaranteed. A handshake opcode is reserved for later, but not implemented. Protocol version negotiation is deferred until the first public release or the first external SDK.

---

## Guiding rules

1. **One behavior change per commit.** Mechanical moves, modernizations, and logic edits are separate commits with separate messages.
2. **Measure before and after.** Every commit that touches a hot path runs `go test -bench=. -benchmem -tags bench ./...` and compares against the baseline captured in Step 0.
3. **No new dependencies on `reflect`.** Prefer `slices`, `maps`, `cmp`, and `iter`.
4. **No `time.Sleep` in state-machine tests.** Use `testing/synctest`. Real I/O tests keep real time.
5. **Every test package has `goleak.VerifyTestMain`.** No exceptions.
6. **`go fix -diff ./...` must be empty at the end of every commit.** Do not commit with a non-empty `go fix` diff.
7. **Package names describe the layer, not the type.** `package logqueue`, `package engine`, `package node`. Directory moves do not rename packages.
8. **Test-only code lives in `*test` packages.** No test doubles in production packages.
9. **No new opcodes may reuse an existing opcode value.** Reserved ranges stay reserved.

---

## Target layering

This is the invariant every later step depends on. Arrows point from a package to its allowed imports.

```
engine
  ↑
queue/logqueue, queue/taskqueue, queue/group, queue/retention
  ↑
node
  ↑
transport/protocol, transport/ipc
  ↑
client, server
  ↑
daemon
  ↑
ui/cli, ui/repl, ui/command
```

Additional rules:

- `engine` imports nothing internal.
- `queue/*` may import `engine` only.
- `node` may import `engine` and `queue/*` only.
- `transport/*` may import nothing internal. It is the wire.
- `client` and `server` may import everything below them, but not each other.
- `daemon` may import `server` and `client`. It is the only package allowed to import both.
- `ui/*` may import `client`, `command`, `daemon`, and nothing else internal. `ui` must not import `engine` or `transport` directly.
- `ui/command` currently imports `daemon` for shutdown signaling. This is the only exception to "ui only talks to client." It stays, but is documented.
- `internal/enginetest` may import `engine` only, and may only be imported by `_test.go` files. No production package may depend on it.

The plan later introduces `go-arch-lint` rules that encode exactly this.

---

## Where future work will live

This section is not a work item. It records the seams this plan leaves for later additions, so reviewers can verify the seams exist.

- **Metrics.** Out-of-band only. A future `internal/api/metrics` (or `internal/observability`) would poll `node.Stats()` and `engine.Stats()` and expose counters without instrumenting hot paths. It may depend on `node` and `client`. Nothing in `engine`, `queue/*`, or `node` will call into it.
- **Prometheus + SSE.** A future `internal/api/http` would be an optional daemon listener, disabled by default. It would depend on `node`, `client`, and `transport/protocol`. It would not be reachable from `engine` or `queue/*`.
- **TUI (btop-like).** A future `internal/ui/tui` would depend on `client` and a future `node.Watch` streaming API. It would use the same out-of-band stats surface as metrics. It would not touch the hot path.
- **Foreign-language SDKs.** A future `docs/protocol.md` and a conformance test suite would freeze the wire format at 1.0. SDKs that embed a `mikyu` binary and talk over IPC would use the same `daemon` subcommand and the same framing. Nothing in this plan forces a design that blocks either mode.
- **Protocol negotiation.** A handshake opcode is reserved at `0x7E`. It is not implemented. When the first external SDK is published or the first public release is cut, `OpHello` is added at `0x7E`, and feature flags are added to the server dispatch. Because `protocol` in this plan is already a pure DTO layer, adding a handshake later is additive.

No code in this plan depends on any of the above being built.

---

## Step 0 — Baselines and guardrails

This step must complete before any restructuring.

### 0.1 Capture baselines

Create `bench/` and `arch/` at the repo root. Commit these files.

```bash
mkdir -p bench arch
go test -bench=. -benchmem -tags bench ./... > bench/baseline.txt
go list -deps ./... | sort -u > arch/imports-before.txt
go test -cover ./... > bench/coverage-before.txt
```

### 0.2 Extend `goleak` to every test package

Every `_test.go` package with an external test file gets:

```go
func TestMain(m *testing.M) {
    goleak.VerifyTestMain(m)
}
```

Currently present in `engine` and `mikyu`. Add to:

- `internal/taskqueue`
- `internal/logqueue`
- `internal/server`
- `internal/client`
- `internal/daemon`
- `internal/retention`
- `internal/group`
- `internal/ipc`

If a package has no such `TestMain`, add it. If a package already has a `TestMain` (for crash-recovery helpers, for example), compose with `goleak.VerifyTestMain`.

### 0.3 Add the `go fix` CI gate

Add to `.github/workflows/ci.yml`:

```yaml
- name: go fix (dry run)
  run: |
    go fix -diff ./... > /tmp/fix.diff
    test ! -s /tmp/fix.diff || (cat /tmp/fix.diff; exit 1)
```

This will be red until Step 1 is done. That is expected.

### 0.4 Create `internal/enginetest` with an in-memory `Storage`

Create `internal/enginetest/memstorage.go`. This package is test-only. It exists because `testing/synctest` cannot virtualize OS file I/O: state-machine tests in `queue/taskqueue` and `queue/logqueue` need an `engine.Storage` that does not touch the filesystem.

`internal/enginetest` is not a production backend. It is not covered by compatibility guarantees. Its package doc must say so.

```go
// Package enginetest provides an in-memory engine.Storage implementation
// for use in tests. It exists so that state-machine tests can run inside
// testing/synctest bubbles without touching real files or the WAL.
//
// This package is not a supported production backend. It is not covered by
// compatibility guarantees. Production code must not import it. Prefer
// engine.Queue for all non-test uses.
package enginetest

import (
    "context"
    "sync"

    "github.com/dmunish/mikyu/internal/engine"
)

// MemStorage is an in-memory engine.Storage implementation for use in tests.
// It mirrors the semantics of engine.Queue, including MinOffset,
// TruncateBefore, Segments, and MaxAppendPayloadBytes.
type MemStorage struct {
    mu      sync.Mutex
    name    string
    maxMsg  int64
    maxSeg  int64
    msgs    []engine.Message
    groups  map[string]uint64
    minOff  uint64
    segs    []engine.SegmentInfo
    closed  bool
}

func NewMemStorage(name string, maxMsg, maxSeg int64) *MemStorage {
    return &MemStorage{
        name:   name,
        maxMsg: maxMsg,
        maxSeg: maxSeg,
        groups: make(map[string]uint64),
    }
}

func (m *MemStorage) Name() string { return m.name }

func (m *MemStorage) Append(ctx context.Context, payload []byte) (uint64, error) {
    offs, err := m.AppendBatch(ctx, [][]byte{payload})
    if err != nil {
        return 0, err
    }
    return offs[0], nil
}

func (m *MemStorage) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    m.mu.Lock()
    defer m.mu.Unlock()
    if m.closed {
        return nil, engine.ErrQueueClosed
    }
    out := make([]uint64, len(payloads))
    for i, p := range payloads {
        if int64(len(p)) > m.maxMsg {
            return nil, engine.ErrMessageTooLarge
        }
        off := uint64(len(m.msgs))
        m.msgs = append(m.msgs, engine.Message{
            Offset:  off,
            Payload: append([]byte(nil), p...),
        })
        out[i] = off
    }
    return out, nil
}

func (m *MemStorage) Read(ctx context.Context, off uint64) (engine.Message, error) {
    if err := ctx.Err(); err != nil {
        return engine.Message{}, err
    }
    m.mu.Lock()
    defer m.mu.Unlock()
    if off < m.minOff {
        return engine.Message{}, engine.ErrOffsetTruncated
    }
    if off >= uint64(len(m.msgs)) {
        return engine.Message{}, engine.ErrOffsetNotFound
    }
    return m.msgs[off], nil
}

func (m *MemStorage) ReadBatch(ctx context.Context, start uint64, n int) ([]engine.Message, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    m.mu.Lock()
    defer m.mu.Unlock()
    if start < m.minOff {
        return nil, engine.ErrOffsetTruncated
    }
    if start >= uint64(len(m.msgs)) {
        return nil, engine.ErrOffsetNotFound
    }
    end := min(start+uint64(n), uint64(len(m.msgs)))
    return append([]engine.Message(nil), m.msgs[start:end]...), nil
}

func (m *MemStorage) LastOffset() uint64 {
    m.mu.Lock()
    defer m.mu.Unlock()
    return uint64(len(m.msgs))
}

func (m *MemStorage) MinOffset() uint64 {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.minOff
}

func (m *MemStorage) CommitOffset(ctx context.Context, group string, off uint64) error {
    m.mu.Lock()
    defer m.mu.Unlock()
    m.groups[group] = off
    return nil
}

func (m *MemStorage) FetchOffset(ctx context.Context, group string) (uint64, error) {
    m.mu.Lock()
    defer m.mu.Unlock()
    return m.groups[group], nil
}

func (m *MemStorage) MaxAppendPayloadBytes() int64 {
    return m.maxMsg
}

func (m *MemStorage) Segments() []engine.SegmentInfo {
    m.mu.Lock()
    defer m.mu.Unlock()
    return append([]engine.SegmentInfo(nil), m.segs...)
}

func (m *MemStorage) TruncateBefore(ctx context.Context, off uint64) (int64, error) {
    if err := ctx.Err(); err != nil {
        return 0, err
    }
    m.mu.Lock()
    defer m.mu.Unlock()
    if off <= m.minOff {
        return 0, nil
    }
    if off > uint64(len(m.msgs)) {
        off = uint64(len(m.msgs))
    }
    reclaimed := int64(off-m.minOff) * 32
    m.minOff = off
    return reclaimed, nil
}

var _ engine.Storage = (*MemStorage)(nil)
```

`enginetest` is imported only by `_test.go` files in `queue/taskqueue` and `queue/logqueue`. Production packages must not import it. This is enforced by `depguard` (Step 12), not by `go-arch-lint`, because `go-arch-lint` cannot reliably exclude `_test.go` files.

### 0.5 Reserve the handshake opcode

In `internal/protocol/opcodes.go`, add a comment and a constant. Do not implement a handler. Do not write a payload format. Just reserve the byte so no future opcode claims it.

```go
const (
    // Reserved for future protocol negotiation. Not implemented.
    // When the first external SDK is published or the first public release
    // is cut, OpHello will be defined here and feature negotiation added to
    // the server dispatch. Do not assign 0x7E to any other opcode.
    OpReservedHandshake byte = 0x7E
)
```

### 0.6 Exit criteria

- All packages build.
- `goleak` runs in every test binary.
- `bench/baseline.txt` exists and is committed.
- `internal/enginetest` exists with `MemStorage`.
- `OpReservedHandshake` is defined.
- `go fix -diff` is non-empty (red), and you have read the diff.

---

## Step 1 — Modernization pass

This step is mechanical. One commit.

### 1.1 Run `go fix`

```bash
go fix ./...
```

Review the diff. Expect these modernizers to fire:

- `waitgroupgo` — `wg.Add(1); go func(){ defer wg.Done(); ... }()` becomes `wg.Go(func(){...})`.
- `atomictypes` — `atomic.LoadUint32(&x)` becomes `x.Load()`.
- `slicesbackward` — hand-rolled reverse loops become `slices.Backward`.

For each hunk, verify by hand that behavior is identical. Commit.

### 1.2 Apply the modernizations `go fix` does not handle

**`testing.B.Loop` in serial benchmarks.** In `engine/stress_bench_test.go`, replace `for i := 0; b.Loop(); i++` and `for range b.N` with `for b.Loop()`. **Do not** put `b.Loop()` inside `b.RunParallel`. Parallel benchmarks keep `b.RunParallel(func(pb *testing.PB){ for pb.Next() { ... } })`. `B.Loop` is not safe to call concurrently.

**`testing.T.Context()`.** Replace `ctx := context.Background()` with `ctx := t.Context()` in every test that does not need a cancellable context that outlives the test. This includes `taskqueue`, `logqueue`, `engine`, `server`, `client`, `mikyu_test`.

**`maps.Keys` / `slices.Sorted` / `slices.Collect`** in these specific locations:

- `engine/manager.go` `ListQueues`: `return slices.Sorted(maps.Keys(m.queues))`. This changes ordering from random to sorted. Since there are no users, this is an improvement, not a regression.
- `retention/coordinator.go` `ReconcileAll`: `names := slices.Collect(maps.Keys(c.targets))`.
- `taskqueue/taskqueue.go` `DeadLetters`: keep `for _, task := range q.dlq` because both key and value are not needed. `maps.Values` is fine here if it reads better.

**`slices.SortFunc` + `cmp.Compare`** in:

- `taskqueue/checkpoint.go` `encodeSnapshot` (two `sort.Slice` calls).
- `taskqueue/taskqueue.go` `DeadLetters`.

Replaces `sort.Slice`, which uses `reflect.Swapper`.

**`strings.SplitSeq`** in `repl.formatHelpArgs`:

```go
for token := range strings.SplitSeq(raw, " ") {
    // ...
}
```

**`bytes.Lines` / `strings.Lines`** anywhere you hand-roll line iteration.

### 1.3 Exit criteria

- `go fix -diff ./...` is empty.
- All tests pass with `-race`.
- `bench/baseline.txt` re-run is within 2% of the original.
- Commit is titled `chore: modernize to Go 1.27 idioms`.

---

## Step 2 — File splits by role

This step moves code between files inside existing packages. Package names, symbol names, and behavior do not change. One commit per package.

### 2.1 `internal/engine`

Split `queue.go` into:

- `config.go` — `Config`, `withDefaults`, `ErrQueueExists`, `ErrQueueNotFound`, `ErrOffsetNotFound`, `ErrMessageTooLarge`, `ErrRecordTooLarge`, `ErrOffsetTruncated`, `ErrQueueClosed`.
- `message.go` — `Message`.
- `queue.go` — `Queue` struct, `openQueue`, `locateSegment`, `loadSegments`, `closeSegments`, `roll`, `MaxAppendPayloadBytes`, `Close`.
- `append.go` — `Append`, `AppendBatch`, `stagedBatch`.
- `read.go` — `Read`, `ReadBatch`, `readFromSegment`.
- `offset_api.go` — `LastOffset`, `Name`, `CommitOffset`, `FetchOffset`.

Leave `compact.go`, `segment.go`, `wal.go`, `encode.go`, `index.go`, `cache.go`, `offsets.go`, `manager.go`, `storage.go` as-is.

### 2.2 `internal/taskqueue`

Split `taskqueue.go` into:

- `taskqueue.go` — struct, `NewTaskQueue`, `Name`, `Close`, `checkpointLoop`.
- `enqueue.go` — `Enqueue`.
- `dequeue.go` — `Dequeue`, `DequeueNow`, `takeDueLocked`.
- `ack.go` — `Ack`, `AckID`.
- `fail.go` — `Fail`, `FailID`.
- `extend.go` — `Extend`, `ExtendID`.
- `dlq.go` — `DeadLetters`, `AckDeadLetter`, `AckDeadLetterID`.
- `lease.go` — `currentLeaseLocked`, `validateLeaseLocked`, `broadcastLocked`, `cloneTask`, `drainTimer`.

### 2.3 `internal/server`

Split `handlers.go` into:

- `dispatch.go` — `dispatch`.
- `handlers_log.go` — log-queue handlers.
- `handlers_task.go` — task handlers.
- `handlers_admin.go` — `handleCreateQueue`, `handleDeleteQueue`, `handleListQueues`, `handleStats`, `handleTruncate`, `handleShutdown`.
- `errors.go` — `opErr`, `taskErr`.

### 2.4 `internal/client`

Split `remote.go` into:

- `remote.go` — `remoteClient` struct, `NewRemote`, `acquire`, `release`, `dialConn`, `Close`, `exchange`, top-level `CreateQueue`, `ListQueues`, `DeleteQueue`, `TruncateQueue`, `Stats`.
- `remote_logqueue.go` — `remoteLogQueue` and methods.
- `remote_taskqueue.go` — `remoteTaskQueue` and methods.
- `remote_group.go` — `remoteGroup`.
- `errors.go` — `decodeProtocolError`, `isRetryable`.

Split `embedded.go` into:

- `embedded.go` — `embeddedClient` struct, `NewEmbedded`, `CreateQueue`, `ListQueues`, `DeleteQueue`, `Stats`, `Close`.
- `embedded_queues.go` — `LogQueue`, `getOrCreateLogQueue`, `TaskQueue`, `TruncateQueue`.

### 2.5 `internal/logqueue`

Split `logqueue.go` into:

- `logqueue.go` — struct, `NewLogQueue`, `Name`, `Produce`, `ProduceBatch`, `Read`, `ReadBatch`, `LastOffset`, `MinOffset`, `Group`, `CommitOffset`, `FetchOffset`, `Close`.
- `stream.go` — `Stream`, `Tail`.

`group.go` and `retention.go` stay.

### 2.6 Exit criteria

- `git diff -M` per package shows only moves.
- All tests pass.
- Benchmarks within 2% of baseline.
- Commit messages are `refactor(engine): split queue.go by role`, etc.

---

## Step 3 — Collapse dual-path task driver logic

This is the first logic-touching step. It is contained to `internal/taskqueue`.

### 3.1 Introduce `leaseRef`

Add to `lease.go`:

```go
// leaseRef identifies a task lease for Ack/Fail/Extend/AckDeadLetter.
//
// When strict is true, the ref is matched against the current in-flight state
// by ID, offset, attempts, and delivery ID. This is the "embedded" path where
// callers hold a full Task value from Dequeue.
//
// When strict is false, only the ID is used to resolve the current lease. This
// is the "by-ID" path used by admin clients that do not hold a Task value.
type leaseRef struct {
    ID         string
    Offset     uint64
    Attempts   uint32
    DeliveryID string
    strict     bool
}

func refFromTask(t Task) leaseRef {
    return leaseRef{
        ID:         t.ID,
        Offset:     t.offset,
        Attempts:   t.Attempts,
        DeliveryID: t.DeliveryID,
        strict:     true,
    }
}

func refFromID(id string) leaseRef {
    return leaseRef{ID: id, strict: false}
}
```

### 3.2 Replace `validateLeaseLocked` and `currentLeaseLocked` with `resolveLeaseLocked`

```go
func (q *TaskQueue) resolveLeaseLocked(ref leaseRef) (*taskState, error) {
    if ref.ID == "" {
        return nil, ErrInvalidTask
    }
    state, ok := q.active[ref.ID]
    if !ok {
        return nil, ErrTaskNotFound
    }
    if !state.inFlight {
        return nil, ErrTaskNotInflight
    }
    if state.task.Visibility.IsZero() || !time.Now().Before(state.task.Visibility) {
        return nil, ErrTaskStale
    }
    if ref.strict {
        if state.task.offset != ref.Offset ||
            state.task.Attempts != ref.Attempts ||
            state.task.DeliveryID != ref.DeliveryID ||
            ref.DeliveryID == "" {
            return nil, ErrTaskStale
        }
    }
    return state, nil
}
```

Delete `validateLeaseLocked` and `currentLeaseLocked`.

### 3.3 Collapse Ack/Fail/Extend

In `ack.go`, `fail.go`, `extend.go`, replace the four public methods with two each: a public wrapper and a private body.

```go
func (q *TaskQueue) Ack(ctx context.Context, task Task) error {
    return q.ack(ctx, refFromTask(task))
}

func (q *TaskQueue) AckID(ctx context.Context, id string) error {
    return q.ack(ctx, refFromID(id))
}

func (q *TaskQueue) ack(ctx context.Context, ref leaseRef) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    q.mu.Lock()
    defer q.mu.Unlock()
    if q.closed || q.closing {
        return ErrTaskClosed
    }
    if err := q.refreshLocked(ctx); err != nil {
        return err
    }
    state, err := q.resolveLeaseLocked(ref)
    if err != nil {
        return err
    }
    delete(q.active, state.task.ID)
    q.dirty++
    q.broadcastLocked()
    q.pruneScheduleLocked()
    return nil
}
```

Do the same for `Fail` / `FailID` and `Extend` / `ExtendID`. `AckDeadLetter` / `AckDeadLetterID` do not have a lease; they resolve against `q.dlq` by ID and validate offset/attempts when strict.

### 3.4 Simplify `newTaskID`

The function is currently `(string, error)`. In Go 1.24+, `crypto/rand.Read` never fails. Remove the error:

```go
func newTaskID() string {
    var b [16]byte
    rand.Read(b[:])
    return hex.EncodeToString(b[:])
}
```

Update `Enqueue` and `takeDueLocked` to drop the error branch. This is a strict simplification.

### 3.5 Verification

- Run `go test -race ./internal/taskqueue/...`.
- Run `go test -bench=BenchmarkTaskQueueEnqueue -bench=BenchmarkTaskQueueDequeueAck -benchmem -tags bench ./internal/taskqueue/...`.
- Compare `-benchmem` against `bench/baseline.txt`. Allocation counts must be unchanged in the hot path.
- Review the diff against the original four-way duplication line by line. The atomicity of `refresh + resolve + mutate + broadcast + prune` under `q.mu` must be identical.

---

## Step 4 — Decouple `protocol` from `taskqueue`

This step makes `protocol` a pure wire DTO layer. It is a prerequisite for foreign SDKs and for adding version negotiation later without a rewrite.

### 4.1 Define `protocol.Task`

Add to `internal/protocol/task.go`:

```go
package protocol

import "time"

// Task is the wire representation of a taskqueue.Task. It carries no methods
// and no package dependencies beyond time.
type Task struct {
    ID          string
    Payload     []byte
    Attempts    uint32
    MaxAttempts uint32
    Visibility  time.Time
    ScheduledAt time.Time
    DeliveryID  string
    Offset      uint64
}
```

### 4.2 Rewrite encode/decode

```go
func EncodeTask(b *Buffer, t Task) {
    b.WriteBytes([]byte(t.ID))
    b.WriteBytes(t.Payload)
    b.WriteU32(t.Attempts)
    b.WriteU32(t.MaxAttempts)
    b.WriteU64(encodeTime(t.Visibility))
    b.WriteU64(encodeTime(t.ScheduledAt))
    b.WriteBytes([]byte(t.DeliveryID))
    b.WriteU64(t.Offset)
}

func DecodeTask(r *Reader) (Task, error) {
    // Same validation as before, but returns protocol.Task.
}
```

`encodeTaskTime` / `decodeTaskTime` become `encodeTime` / `decodeTime` and stay private to `protocol`.

### 4.3 Convert at the server boundary

In `internal/server/handlers_task.go`, define small helpers:

```go
func toWireTask(t taskqueue.Task) protocol.Task {
    return protocol.Task{
        ID:          t.ID,
        Payload:     t.Payload, // zero-copy: payload is not mutated after encode
        Attempts:    t.Attempts,
        MaxAttempts: t.MaxAttempts,
        Visibility:  t.Visibility,
        ScheduledAt: t.ScheduledAt,
        DeliveryID:  t.DeliveryID,
        Offset:      t.OriginalOffset(),
    }
}

func fromWireTask(t protocol.Task) taskqueue.Task {
    return taskqueue.NewTaskFromWireWithDeliveryID(
        t.ID, t.Payload, t.Attempts, t.MaxAttempts,
        t.Visibility, t.ScheduledAt, t.DeliveryID, t.Offset,
    )
}
```

In `internal/client/remote_taskqueue.go`, use `protocol.Task` directly when building bodies and convert to `taskqueue.Task` only where the public interface requires it.

### 4.4 Pre-1.0 wire policy

Add to `internal/protocol/doc.go`:

```go
// Package protocol defines the Mikyu wire format.
//
// The wire format is UNSTABLE before 1.0. No cross-version compatibility is
// guaranteed. Clients and servers must be built from the same commit. When the
// first external SDK is published or the first public release is cut, the
// format is frozen and OpHello (0x7E) is implemented for version negotiation.
//
// Rules that hold even pre-1.0:
//
//   - Do not reuse an existing opcode value for a new meaning.
//   - Do not remove opcodes. Mark them deprecated and stop emitting them.
//   - Do not embed Go-specific types (structs, interfaces, slices with Go
//     layout guarantees) in a payload. Encode field by field.
//   - Do not embed engine.Storage, taskqueue.Task, or node types in a payload.
//     Convert to protocol.Task or a similar DTO first.
package protocol
```

### 4.5 Verification

- `go list -deps ./internal/protocol` must not contain `internal/engine`, `internal/taskqueue`, or `internal/queue/*`.
- `internal/protocol/task_test.go` builds `protocol.Task` values directly.
- Wire round-trip tests still pass.

---

## Step 5 — Extract `internal/node`

This is the largest structural step. It creates a shared local-node core so `embeddedClient` and `server.Server` stop duplicating queue caching, retention registration, and lifecycle logic.

### 5.1 Create `internal/node/node.go`

```go
// Package node owns the local queue registry shared by embedded clients and
// the daemon server. It knows nothing about the wire protocol or transport.
package node

import (
    "context"
    "sync"

    "github.com/dmunish/mikyu/internal/engine"
    "github.com/dmunish/mikyu/internal/logqueue"
    "github.com/dmunish/mikyu/internal/retention"
    "github.com/dmunish/mikyu/internal/taskqueue"
)

type Node struct {
    mgr        *engine.Manager
    retention  *retention.Coordinator
    logQueues  map[string]*logqueue.LogQueue
    taskQueues map[string]*taskqueue.TaskQueue
    mu         sync.RWMutex

    retentionCancel context.CancelFunc
    retentionWG     sync.WaitGroup
}

func Open(dir string, cfg engine.Config) (*Node, error) {
    mgr, err := engine.OpenManager(dir, cfg)
    if err != nil {
        return nil, err
    }
    ctx, cancel := context.WithCancel(context.Background())
    n := &Node{
        mgr:             mgr,
        retention:       retention.NewCoordinator(0),
        logQueues:       make(map[string]*logqueue.LogQueue),
        taskQueues:      make(map[string]*taskqueue.TaskQueue),
        retentionCancel: cancel,
    }
    n.retentionWG.Go(func() { n.retention.Start(ctx) })
    return n, nil
}

func (n *Node) Manager() *engine.Manager { return n.mgr }

func (n *Node) CreateQueue(ctx context.Context, name string, cfg engine.Config) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    storage, err := n.mgr.CreateQueue(name, cfg)
    if err != nil {
        return err
    }
    lq := logqueue.NewLogQueue(name, storage)
    n.mu.Lock()
    n.logQueues[name] = lq
    n.mu.Unlock()
    n.retention.RegisterDriver(name, storage, lq)
    return nil
}

func (n *Node) LogQueue(name string) (*logqueue.LogQueue, error) {
    n.mu.RLock()
    if lq, ok := n.logQueues[name]; ok {
        n.mu.RUnlock()
        return lq, nil
    }
    n.mu.RUnlock()

    n.mu.Lock()
    defer n.mu.Unlock()
    if lq, ok := n.logQueues[name]; ok {
        return lq, nil
    }
    storage, err := n.mgr.GetQueue(name)
    if err != nil {
        return nil, err
    }
    lq := logqueue.NewLogQueue(name, storage)
    n.logQueues[name] = lq
    n.retention.RegisterDriver(name, storage, lq)
    return lq, nil
}

func (n *Node) TaskQueue(name string) (*taskqueue.TaskQueue, error) {
    n.mu.Lock()
    defer n.mu.Unlock()
    if tq, ok := n.taskQueues[name]; ok {
        return tq, nil
    }
    storage, err := n.mgr.GetQueue(name)
    if err != nil {
        return nil, err
    }
    tq, err := taskqueue.NewTaskQueue(name, storage)
    if err != nil {
        return nil, err
    }
    n.taskQueues[name] = tq
    n.retention.RegisterDriver(name, storage, tq)
    return tq, nil
}

func (n *Node) DeleteQueue(ctx context.Context, name string) error {
    if err := ctx.Err(); err != nil {
        return err
    }
    n.mu.Lock()
    if tq, ok := n.taskQueues[name]; ok {
        _ = tq.Close()
        delete(n.taskQueues, name)
    }
    delete(n.logQueues, name)
    n.mu.Unlock()
    n.retention.UnregisterTarget(name)
    return n.mgr.DeleteQueue(name)
}

func (n *Node) TruncateQueue(ctx context.Context, name string, minOffset uint64) (int64, error) {
    if err := ctx.Err(); err != nil {
        return 0, err
    }
    if _, err := n.LogQueue(name); err != nil {
        return 0, err
    }
    // Materialize the task driver so durable task state from a previous
    // process lifetime is included in the retention floor.
    if _, err := n.TaskQueue(name); err != nil {
        return 0, err
    }
    return n.retention.ReconcileQueueAt(ctx, name, minOffset)
}

func (n *Node) Stats(ctx context.Context) (engine.EngineStats, error) {
    if err := ctx.Err(); err != nil {
        return engine.EngineStats{}, err
    }
    return n.mgr.Stats(), nil
}

func (n *Node) ListQueues(ctx context.Context) ([]string, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    return n.mgr.ListQueues(), nil
}

func (n *Node) Close() error {
    n.mu.Lock()
    if n.retentionCancel != nil {
        n.retentionCancel()
        n.retentionCancel = nil
    }
    n.mu.Unlock()
    n.retentionWG.Wait()

    n.mu.Lock()
    var firstErr error
    for name, tq := range n.taskQueues {
        if err := tq.Close(); err != nil && firstErr == nil {
            firstErr = err
        }
        delete(n.taskQueues, name)
    }
    n.mu.Unlock()

    if err := n.mgr.Close(); err != nil && firstErr == nil {
        firstErr = err
    }
    return firstErr
}
```

### 5.2 Rewrite `embeddedClient` as a thin wrapper

`internal/client/embedded.go` becomes:

```go
type embeddedClient struct {
    node *node.Node
    cfg  Config
}

func NewEmbedded(dir string, opts ...Option) (*embeddedClient, error) {
    cfg := DefaultClientConfig()
    for _, opt := range opts {
        opt(&cfg)
    }
    engineCfg := engine.Config{
        BatchSize:       cfg.BatchSize,
        MaxInterval:     cfg.MaxInterval,
        MinInterval:     cfg.MinInterval,
        MaxSegmentBytes: cfg.DefaultMaxSegmentBytes,
        MaxMessageBytes: cfg.DefaultMaxMessageBytes,
        MaxSegmentAge:   cfg.DefaultMaxSegmentAge,
        Encrypted:       cfg.Encrypted,
        Fsync:           cfg.Fsync,
    }
    n, err := node.Open(dir, engineCfg)
    if err != nil {
        return nil, err
    }
    return &embeddedClient{node: n, cfg: cfg}, nil
}
```

Every method delegates to `node`. `Close` calls `node.Close`.

### 5.3 Rewrite `server.Server` to wrap `*node.Node`

`Server` keeps its listener, dispatch loop, and TLS config. It drops `queues`, `tasks`, `retention`, `retentionCancel`, and `retentionWG`. `getOrCreateLogQueue` and `getOrCreateTaskQueue` become `s.node.LogQueue(name)` and `s.node.TaskQueue(name)`.

`server.New` takes `*node.Node` instead of `*engine.Manager`.

### 5.4 Update `daemon.Run`

`daemon.Run` opens a `*node.Node` and passes it to `server.New`.

### 5.5 Add `os.Root` at the `engine.Manager` boundary

Add to `engine.Manager`:

```go
type Manager struct {
    dir     string
    root    *os.Root
    cfg     Config
    queues  map[string]*Queue
    offsets *OffsetStore
    mu      sync.RWMutex
}
```

`OpenManager` calls `os.OpenRoot(dir)` and stores the result.

Route these through `root`:

- `openSegment` — file creation and opening.
- `segment.destroy` — file removal.
- `OffsetStore` — file open and rename.
- `Manager.DeleteQueue` — `root.RemoveAll(name)`.
- `Manager.CreateQueue` — `root.Mkdir(name, 0o755)`.

For `segment.enableMMap`, `root.Open(name)` returns an `*os.File` that `mmap.Map` accepts. Keep the same `mmap` layer.

Add a test:

```go
func TestManagerRejectsEscape(t *testing.T) {
    dir := t.TempDir()
    mgr, err := OpenManager(dir, Config{})
    require.NoError(t, err)
    defer mgr.Close()
    _, err = mgr.CreateQueue("../escape", Config{})
    require.Error(t, err)
}
```

### 5.6 Benchmark gate for `os.Root`

`os.Root` adds a syscall-level indirection to segment rolls, truncation, and offset commits. It does not affect `Append`/`Read` hot paths because those use memory-mapped segments and existing file descriptors. Confirm this with the benchmark suite:

- `BenchmarkProduceOnly`
- `BenchmarkBatchOperations`
- `TestCompact_ConcurrentAppendsReadsTruncates`
- `BenchmarkWALFlushAllocations`

If any regression is above 5%, open files through `os.Root` only at creation time, then use normal `*os.File` operations afterward.

### 5.7 Verification

- `TestEmbeddedRetentionCoordinatesLogAndTaskDrivers` passes unchanged.
- `TestServerQueueHandleCaching` and `TestTaskQueueHandleCaching` pass unchanged.
- `go list -deps ./internal/node` does not contain `protocol`, `server`, or `client`.
- `go test -race ./...` passes.
- Benchmarks within 5% of baseline.

---

## Step 6 — Replace polling dequeue with long-poll

The remote `Dequeue` currently polls every 10 ms. The embedded `Dequeue` blocks on condition variables. This step makes the wire protocol long-poll so both behave the same, and it fixes the cancellation hazards that a naive long-poll introduces.

### 6.1 Add a timeout field to `OpTaskDequeue`

In `internal/protocol/task.go`:

```go
func EncodeTaskDequeue(b *Buffer, queue string, timeout time.Duration) {
    b.WriteBytes([]byte(queue))
    b.WriteU64(uint64(int64(timeout)))
}
```

### 6.2 Give each server connection its own context

A long-poll handler that uses `context.Background()` will keep running after the client disconnects. `serveConn` must cancel the handler context when the connection closes.

In `internal/server/server.go`:

```go
func (s *Server) serveConn(conn net.Conn) {
    defer conn.Close()
    r := bufio.NewReader(conn)

    ctx, cancel := context.WithCancel(context.Background())
    defer cancel()

    // Cancel the handler context when the connection is closed from the
    // client side. ReadFrame returning an error is the reliable signal, but
    // a long-poll handler may be blocked while the peer has already hung up.
    // A short watchdog goroutine watches for a closed connection.
    go func() {
        <-ctx.Done()
    }()

    for {
        op, body, err := protocol.ReadFrame(r)
        if err != nil {
            return
        }

        respOp, respBody, shouldShutdown := s.dispatch(ctx, op, body)
        if err := protocol.WriteFrame(conn, respOp, respBody); err != nil {
            return
        }

        if shouldShutdown {
            go func() {
                s.Stop()
                _ = s.node.Close()
            }()
            return
        }
    }
}
```

The key change is that `ctx` is now cancellable and passed to every handler. When the connection is closed by `defer conn.Close()`, `cancel()` fires, and any long-poll handler blocked on `node.TaskQueue(name).Dequeue(ctx)` unblocks with `ctx.Err()`.

Because `ReadFrame` blocks while a long-poll is in progress, the connection is only closed by the client or by a server stop. Both paths cancel `ctx`.

### 6.3 Server handler blocks up to timeout

In `internal/server/handlers_task.go`:

```go
func (s *Server) handleTaskDequeue(ctx context.Context, body []byte) (byte, []byte) {
    r := protocol.NewReader(body)
    name, err := r.ReadBytes()
    if err != nil {
        return opErr(protocol.ErrCodeInvalidTask, err.Error())
    }
    timeoutRaw, err := r.ReadU64()
    if err != nil {
        return opErr(protocol.ErrCodeInvalidTask, err.Error())
    }
    timeout := time.Duration(int64(timeoutRaw))

    q, err := s.node.TaskQueue(string(name))
    if err != nil {
        return taskErr(err)
    }

    dequeueCtx := ctx
    if timeout > 0 {
        var cancel context.CancelFunc
        dequeueCtx, cancel = context.WithTimeout(ctx, timeout)
        defer cancel()
    }

    t, err := q.Dequeue(dequeueCtx)
    if err != nil {
        return taskErr(err)
    }
    var b protocol.Buffer
    protocol.EncodeTask(&b, toWireTask(t))
    return protocol.OpTaskDequeue, b.Bytes()
}
```

`q.Dequeue` must respect `ctx`. The embedded implementation already does (it selects on `ctx.Done()` and `q.notify`).

### 6.4 Remote client sends a timeout and sets a deadline

The client must set a socket deadline based on the dequeue timeout, not only on `ctx.Deadline()`. Otherwise a cancellation without a deadline will not unblock the socket read.

In `internal/client/remote_taskqueue.go`:

```go
const defaultDequeueTimeout = 30 * time.Second

func (q *remoteTaskQueue) Dequeue(ctx context.Context) (taskqueue.Task, error) {
    var b protocol.Buffer
    protocol.EncodeTaskDequeue(&b, q.name, defaultDequeueTimeout)

    // Ensure the socket read has a deadline even if the caller's ctx has none.
    dequeueCtx, cancel := context.WithTimeout(ctx, defaultDequeueTimeout+5*time.Second)
    defer cancel()

    resp, err := q.client.exchange(dequeueCtx, protocol.OpTaskDequeue, b.Bytes())
    if err != nil {
        return taskqueue.Task{}, err
    }
    r := protocol.NewReader(resp)
    wt, err := protocol.DecodeTask(r)
    if err != nil {
        return taskqueue.Task{}, err
    }
    return fromWireTask(wt), nil
}
```

The extra 5 seconds is a safety margin so the server's own timeout fires first and returns a clean `ErrTaskNotAvailable` rather than a socket read deadline error. Delete the 10 ms polling loop.

In `remoteClient.exchange`, when `ctx` has a deadline, the deadline is already applied to the socket. The `dequeueCtx` above always has a deadline, so the socket read always has one. This fixes the "cancel a Dequeue without a deadline" case.

### 6.5 Verification

- `TestTaskHandlersLifecycle` still passes.
- A new test asserts that `Dequeue` blocks for approximately `timeout` when the queue is empty.
- A new test cancels a remote `Dequeue` context and asserts that the client returns `context.Canceled` promptly and the server-side handler exits.
- A new test disconnects the client mid-`Dequeue` and asserts that the server-side goroutine exits (via `goleak`).
- Add a benchmark `BenchmarkRemoteTaskDequeue` and compare against the old polling loop.

### 6.6 Exit criteria

- No 10 ms polling loop in `remote_taskqueue.go`.
- `goleak` passes for a test that disconnects mid-dequeue.
- `go test -race ./...` passes.

---

## Step 7 — Full daemon CLI configuration

Today, `daemon.Run(configPath string)` takes only a config file path. Every engine and server setting must come from `mikyu.yaml` or environment variables. This step makes every scalar config field settable from the command line.

### 7.1 Precedence

CLI flags > environment variables > config file > compiled defaults.

`config.Load` already applies environment overrides on top of the config file and defaults. The daemon layer applies CLI overrides on top of `config.Load`'s result, only for flags that were explicitly set on the command line.

### 7.2 Rewrite `daemon.Run` to accept args

In `internal/daemon/daemon.go`:

```go
func Run(args []string) error {
    fs := flag.NewFlagSet("mikyu daemon", flag.ContinueOnError)

    // Server
    configPath := fs.String("config", "", "Path to mikyu.yaml (optional)")
    target := fs.String("target", "", "Bind target: tcp://, unix://, or pipe://")
    dataDir := fs.String("data-dir", "", "Data directory for WAL segments and offset store")
    tlsEnabled := fs.Bool("tls-enabled", false, "Enable TLS on tcp:// listeners")
    tlsCertFile := fs.String("tls-cert-file", "", "TLS certificate file (PEM)")
    tlsKeyFile := fs.String("tls-key-file", "", "TLS key file (PEM)")

    // Engine
    batchSize := fs.Int("batch-size", 0, "Max pending messages before WAL fsync")
    maxInterval := fs.Duration("max-interval", 0, "Max wait before forcing WAL fsync")
    minInterval := fs.Duration("min-interval", 0, "Micro-linger window to group concurrent produces")
    maxSegmentBytes := fs.Int64("max-segment-bytes", 0, "Max size per log segment before rolling")
    maxMessageBytes := fs.Int64("max-message-bytes", 0, "Max payload size per produce call")
    maxSegmentAge := fs.Duration("max-segment-age", 0, "Max age before rolling an inactive segment")
    encrypted := fs.Bool("encrypted", false, "Enable OS transparent directory encryption")
    fsync := fs.Bool("fsync", false, "Perform physical disk fsync on produce")

    if err := fs.Parse(args); err != nil {
        return err
    }

    cfg, _, err := config.Load(*configPath)
    if err != nil {
        return err
    }

    // Apply only explicitly-set flags so defaults from the config file are
    // preserved when a flag is omitted.
    fs.Visit(func(f *flag.Flag) {
        switch f.Name {
        case "target":
            cfg.Server.Target = *target
        case "data-dir":
            cfg.Server.DataDir = *dataDir
        case "tls-enabled":
            cfg.Server.TLSEnabled = *tlsEnabled
        case "tls-cert-file":
            cfg.Server.TLSCertFile = *tlsCertFile
        case "tls-key-file":
            cfg.Server.TLSKeyFile = *tlsKeyFile
        case "batch-size":
            cfg.Engine.BatchSize = *batchSize
        case "max-interval":
            cfg.Engine.MaxInterval = *maxInterval
        case "min-interval":
            cfg.Engine.MinInterval = *minInterval
        case "max-segment-bytes":
            cfg.Engine.MaxSegmentBytes = *maxSegmentBytes
        case "max-message-bytes":
            cfg.Engine.MaxMessageBytes = *maxMessageBytes
        case "max-segment-age":
            cfg.Engine.MaxSegmentAge = *maxSegmentAge
        case "encrypted":
            cfg.Engine.Encrypted = *encrypted
        case "fsync":
            cfg.Engine.Fsync = *fsync
        }
    })

    engineCfg := config.ToEngineConfig(cfg.Engine)
    n, err := node.Open(cfg.Server.DataDir, engineCfg)
    if err != nil {
        return err
    }
    defer n.Close()

    for _, qc := range cfg.Queues {
        qCfg := config.ToQueueEngineConfig(cfg.Engine, qc)
        if err := n.CreateQueue(context.Background(), qc.Name, qCfg); err != nil && !errors.Is(err, engine.ErrQueueExists) {
            return fmt.Errorf("reconcile queue %q: %w", qc.Name, err)
        }
    }

    var srvOpts []server.ServerOption
    if cfg.Server.TLSEnabled {
        tlsCfg, err := server.DefaultTLSConfig(cfg.Server.TLSCertFile, cfg.Server.TLSKeyFile)
        if err != nil {
            return err
        }
        srvOpts = append(srvOpts, server.WithTLS(tlsCfg))
    }

    srv := server.New(n, srvOpts...)
    go func() {
        if err := srv.ListenAndServe(cfg.Server.Target); err != nil {
            fmt.Fprintf(os.Stderr, "server error: %v\n", err)
            os.Exit(1)
        }
    }()

    sigCh := make(chan os.Signal, 1)
    signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

    select {
    case <-sigCh:
    case <-srv.Done():
    }

    srv.Stop()
    return nil
}
```

### 7.3 Update `cmd/mikyu/main.go`

```go
case "daemon", "server":
    return daemon.Run(args[1:])
```

`daemon.Run` no longer takes a bare config path. The `--config` flag replaces the positional argument. This is a breaking CLI change, and it is fine pre-1.0.

### 7.4 Behaviour notes

- `--encrypted` and `--fsync` are boolean. Because `flag.Bool` cannot distinguish "unset" from "set to false," the daemon uses `fs.Visit` to detect explicit sets. `--fsync=false` overrides a `true` config value. `--fsync` alone sets it true.
- `--tls-enabled` requires `--tls-cert-file` and `--tls-key-file`, or the config file to provide them. If TLS is enabled and cert/key are missing, `server.DefaultTLSConfig` generates a self-signed certificate, matching existing behaviour.
- `--config` is the only way to point at a non-default config file. Environment variables still apply on top of the config file. CLI flags apply on top of environment variables.
- Declarative `queues:` entries remain config-file-only. They are lists of structs, not scalar flags. Document this.

### 7.5 Tests

- `TestDaemonFlagsOverrideConfig`: write a config with `batch_size: 100`, run with `--batch-size 200`, assert the engine sees 200.
- `TestDaemonFlagsRespectConfigDefaults`: write a config with `batch_size: 100`, run with no flags, assert the engine sees 100.
- `TestDaemonEnvThenFlagPrecedence`: set `MIKYU_ENGINE_BATCH_SIZE=150`, pass `--batch-size 200`, assert 200.
- `TestDaemonRejectsUnknownFlag`: assert a non-zero exit.
- `TestDaemonConfigBootstrapStillWorks`: no config file, no flags, assert the daemon still starts.

### 7.6 Verification

- `mikyu daemon --help` lists every flag.
- `mikyu daemon --config ./mikyu.yaml --batch-size 512` starts and logs the effective config.
- `go test -race ./internal/daemon/...` passes.

---

## Step 8 — Directory regroup

This step moves directories to match the layering. Package names do not change.

```
internal/
  engine/
  enginetest/
  queue/
    logqueue/
    taskqueue/
    group/
    retention/
  transport/
    protocol/
    ipc/
  node/
  client/
  server/
  daemon/
  config/
  ui/
    cli/
    repl/
    command/
```

Move in one commit. Update import paths. Run `go build ./...`, `go test ./...`, `go vet ./...`, and benchmarks.

The `cmd/mikyu/main.go` imports change to `internal/ui/cli`, `internal/ui/repl`, `internal/daemon`.

After this step, `internal/` reads as a layer diagram:

- `engine` is the bottom.
- `enginetest` is a test-only sibling of `engine`.
- `queue/*` are drivers.
- `transport/*` is the wire.
- `node` is the shared local core.
- `client`, `server` are adapters.
- `daemon` is the process entry.
- `ui/*` is the human interface.

---

## Step 9 — Layering enforcement

Add `go-arch-lint` to CI. Create `.go-arch-lint.yml`:

```yaml
version: 3
workdir: .
allow:
  depOnAnyVendor: false
components:
  engine:
    in: internal/engine
  enginetest:
    in: internal/enginetest
  queue:
    in: internal/queue/**
  node:
    in: internal/node
  transport:
    in: internal/transport/**
  client:
    in: internal/client
  server:
    in: internal/server
  daemon:
    in: internal/daemon
  ui:
    in: internal/ui/**
  config:
    in: internal/config

commonComponents:
  - engine
  - queue
  - node
  - transport
  - client
  - server
  - daemon
  - ui
  - config

commonVendors:
  - github.com/stretchr/testify
  - go.uber.org/goleak
  - golang.org/x/sys
  - golang.org/x/term
  - golang.org/x/crypto
  - charm.land/huh
  - charm.land/lipgloss
  - github.com/edsrzf/mmap-go
  - github.com/goccy/go-yaml

deps:
  engine:
    mayDependOn: []
  enginetest:
    mayDependOn:
      - engine
  queue:
    mayDependOn:
      - engine
  node:
    mayDependOn:
      - engine
      - queue
  transport:
    mayDependOn: []
  client:
    mayDependOn:
      - engine
      - queue
      - node
      - transport
  server:
    mayDependOn:
      - engine
      - queue
      - node
      - transport
  daemon:
    mayDependOn:
      - engine
      - config
      - node
      - server
      - client
  ui:
    mayDependOn:
      - client
      - daemon
  config:
    mayDependOn:
      - engine
```

Notes:

- `enginetest` is not listed in any other component's `mayDependOn`. `go-arch-lint` may still see `_test.go` imports. If it does, either exclude `_test.go` files via the tool's test-file option, or drop `enginetest` from `go-arch-lint` and enforce the rule with `depguard` (Step 12). `depguard` is the reliable mechanism for "only `_test.go` may import this."
- `ui` no longer depends on `engine` or `transport`. That was a contradiction in earlier drafts.
- `ui/command` importing `daemon` remains the sole documented exception. `daemon` is listed in `ui.mayDependOn` for that reason.

Add to CI:

```yaml
- name: architecture lint
  run: go-arch-lint check
```

Sanity check: introduce `import "github.com/dmunish/mikyu/internal/queue/taskqueue"` in `internal/transport/protocol/task.go`, confirm CI fails, revert.

---

## Step 10 — Deterministic tests with `testing/synctest`

`synctest` virtualizes time and the scheduler inside a bubble. It cannot virtualize OS file I/O. So:

- **Migrate:** tests that exercise `taskqueue` or `logqueue` state machines and use `time.Sleep` only for lease expiry, scheduling delays, or tail polling. These migrate to `internal/enginetest.MemStorage`.
- **Do not migrate:** tests that open real files, call `fsync`, exercise WAL error paths, or verify crash recovery.

### 10.1 `taskqueue` migrations

Each of these currently uses `time.Sleep`:

- `TestTaskQueueVisibilityAndStaleLeaseProtection`
- `TestTaskQueueFinalLeaseExpiryDeadLetters`
- `TestTaskQueueExpiredLeaseCannotBeAcknowledgedOrExtended`
- `TestTaskQueueCheckpointRecovery`
- `TestTaskQueueDeliveryIDAndLeaseExtension`

Rewrite in this shape:

```go
func TestTaskQueueVisibilityAndStaleLease(t *testing.T) {
    synctest.Test(t, func(t *testing.T) {
        q := newMemTaskQueue(t, "tasks")
        ctx := t.Context()

        original, err := q.Enqueue(ctx, []byte("lease"),
            WithVisibilityTimeout(40*time.Millisecond), WithMaxAttempts(3))
        require.NoError(t, err)

        first, err := q.Dequeue(ctx)
        require.NoError(t, err)

        synctest.Sleep(60 * time.Millisecond)

        second, err := q.Dequeue(ctx)
        require.NoError(t, err)
        assert.Equal(t, first.ID, second.ID)
        assert.Equal(t, uint32(2), second.Attempts)
        assert.Equal(t, original.ID, second.ID)

        assert.ErrorIs(t, q.Ack(ctx, first), ErrTaskStale)
        require.NoError(t, q.Ack(ctx, second))
    })
}
```

The helper `newMemTaskQueue` builds a `*TaskQueue` over `enginetest.NewMemStorage(name, maxMsg, maxSeg)`.

`TestTaskQueueCheckpointRecovery` must stay on real `engine.Queue`, because it exercises on-disk checkpoint recovery. Its `time.Sleep(75 * time.Millisecond)` becomes real `time.Sleep`; keep it but reduce to a value that is stable on CI (75 ms is fine).

### 10.2 `logqueue` migrations

`TestLogQueueTailContinuous` and `TestLogQueueTailBugProof` use `time.Sleep` to interleave produce and tail. Migrate to `synctest` and `enginetest.MemStorage`. The `Tail` loop already polls with `time.After(5 * time.Millisecond)`. Inside a synctest bubble, that `time.After` is virtualized and fires instantly when advanced.

### 10.3 `engine` non-migrations

Do **not** migrate:

- `TestQueueAsyncWriteFailureStopsSubsequentAppends` — depends on a real closed file.
- `TestQueueRealCrashRecovery` — spawns a child process.
- `TestQueueUngracefulCrashRecovery` — real on-disk recovery.
- `TestSegment*` — real segment files.
- `TestCompact_*` — real truncation on disk.

These tests keep their real `time.Sleep` and real I/O.

### 10.4 Verification

- `go test -race ./...` passes.
- The full suite runs faster. Record the new wall-clock in `bench/coverage-after.txt`.
- No test in `taskqueue` or `logqueue` calls `time.Sleep` directly except through `synctest.Sleep`.

---

## Step 11 — Documentation and examples

### 11.1 Package docs

Add `doc.go` to every package under `internal/`. One paragraph stating the package's role and its invariants. Examples:

```go
// Package engine implements the durable log storage layer. It owns segments,
// the WAL, the in-memory ring cache, and the offset store. It imports no other
// internal package.
package engine
```

```go
// Package node owns the local queue registry shared by embedded clients and
// the daemon server. It registers drivers with the retention coordinator and
// caches stable queue handles. It does not know about the wire protocol.
package node
```

```go
// Package enginetest provides in-memory test doubles for the engine package.
// It is not a production backend and must not be imported by non-test code.
package enginetest
```

```go
// Package protocol defines the Mikyu wire format. The format is unstable
// before 1.0. See doc.go for the stability policy.
package protocol
```

### 11.2 Godoc on exported symbols

Every exported function, method, type, and variable in `mikyu`, `internal/node`, `internal/client`, `internal/engine`, `internal/queue/*`, and `internal/transport/*` gets a godoc comment. State concurrency guarantees and invariants explicitly. For example:

```go
// Enqueue appends a task record and returns the created Task.
//
// Enqueue is safe for concurrent use. The returned Task carries the initial
// offset and a zero Attempts count. The first Dequeue increments Attempts to 1
// and assigns a DeliveryID.
func (q *TaskQueue) Enqueue(...) (Task, error)
```

### 11.3 Runnable examples

Add `Example*` functions in `mikyu`:

- `ExampleOpen`
- `ExampleDial`
- `ExampleClient_CreateQueue`
- `ExampleClient_LogQueue`
- `ExampleLogQueue_Produce`
- `ExampleLogQueue_Read`
- `ExampleLogQueue_Tail`
- `ExampleLogQueue_Group`
- `ExampleTaskQueue_Enqueue`
- `ExampleTaskQueue_Dequeue`
- `ExampleTaskQueue_Fail`
- `ExampleTaskQueue_DeadLetters`

Each example uses `// Output:` and runs as a test. They appear in `go doc`.

### 11.4 `ARCHITECTURE.md`

Include the layering diagram from the top of this plan. Include the `go-arch-lint` rules. Explain why `protocol` does not import `taskqueue`, why `node` does not import `protocol`, why `ui` does not import `engine`, and why `ui/command` may import `daemon`.

### 11.5 `CONTRIBUTING.md`

State:

- How to run tests: `go test -race ./...`.
- How to run benchmarks: `go test -bench=. -benchmem -tags bench ./...`.
- How to run the `go fix` gate locally.
- How to run `go-arch-lint`.
- The rule that no commit may regress benchmarks by more than 5% without a justification in the commit message.
- The rule that `internal/enginetest` may only be imported by `_test.go` files.
- The pre-1.0 wire stability policy.

### 11.6 `docs/protocol.md` (minimal)

Write down the current wire format as it exists after Step 4. Do not add version negotiation. Do not add a handshake. Just document the frame layout, the opcode table, the error codes, and the `protocol.Task` payload layout. This is the seed for the 1.0 frozen spec.

---

## Step 12 — CI gates

Add to `.github/workflows/ci.yml`:

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
- name: coverage
  run: go test -cover ./...
- name: golangci-lint
  run: golangci-lint run --enable depguard,staticcheck,errcheck,gocritic
- name: benchmarks (regression gate)
  run: |
    go test -bench=. -benchmem -tags bench ./... > /tmp/bench.txt
    ./scripts/benchcheck.sh bench/baseline.txt /tmp/bench.txt
```

### 12.1 `depguard` rule for `enginetest`

Add to `.golangci.yml`:

```yaml
linters-settings:
  depguard:
    rules:
      enginetest-test-only:
        deny:
          - pkg: github.com/dmunish/mikyu/internal/enginetest
            desc: enginetest is a test-only package; only _test.go files may import it
        files:
          - "!$test"
```

This is the reliable way to keep `enginetest` test-only. `go-arch-lint` cannot express "test files only."

### 12.2 `benchcheck.sh`

The script parses both files, matches benchmark names, and fails if any `ns/op` or `B/op` regresses by more than 5%. Allocs per op must not increase at all in the hot paths: `BenchmarkTaskQueueEnqueue`, `BenchmarkTaskQueueDequeueAck`, `BenchmarkWALFlushAllocations`, `BenchmarkEncodeRecord`.

---

## Step 13 — Error sentinel hygiene

Audit `internal/logqueue`, `internal/client`, and `internal/transport/ipc` for `errors.New` calls inside functions. Promote them to package-level sentinels.

Current known candidates:

- `logqueue.ErrNotSupported` — already a sentinel, but unused. Either use it or delete it.
- `client.decodeProtocolError` returns `errors.New(msg)` for unknown codes. Replace with `fmt.Errorf("%w: %s", ErrRemote, msg)` and add `var ErrRemote = errors.New("remote error")` to `client`.
- `ipc` returns `fmt.Errorf` with no sentinel. Add `ErrUnsupportedNetwork`.

Verify `engine` and `queue/taskqueue` sentinel blocks are complete. `taskqueue` currently exports `ErrTaskNotAvailable`, `ErrTaskNotFound`, `ErrTaskNotInflight`, `ErrTaskStale`, `ErrTaskClosed`, `ErrInvalidTask`, `ErrCheckpointCorrupt`, `ErrDeadLetterNotFound`, `ErrDeadLetterStale`. These are good.

---

## Step 14 — Remove compatibility shims

Now that there are no external users, delete:

- `client.ErrTaskNotSupported` — it is a compatibility placeholder. Delete the variable and any references.
- `taskqueue.NewTaskFromWire` — keep only `NewTaskFromWireWithDeliveryID`. Rename it to `NewTaskFromWire` after the old one is gone.
- `client.WithFlushInterval` — decide whether this is a real convenience or legacy. If legacy, delete it and its `mikyu` re-export.
- Any `// Deprecated:` markers that exist only for API stability.

After this step, the exported surface of `mikyu` should be the smallest set that satisfies the README examples.

---

## Step 15 — Definition of done

When every step above is complete, the following must hold:

- `ls internal/` reads as `engine`, `enginetest`, `queue/`, `transport/`, `node`, `client`, `server`, `daemon`, `config`, `ui/`.
- Every package has a `doc.go` and every exported symbol has godoc.
- `go-arch-lint check` passes with the rules in Step 9.
- `depguard` enforces that `enginetest` is test-only.
- `go fix -diff ./...` is empty.
- `goleak.VerifyTestMain` is present in every test package.
- Every `time.Sleep` in `taskqueue` and `logqueue` tests is replaced by `synctest.Sleep`, except where the test exercises real storage.
- `protocol` does not import `engine`, `queue/*`, or `node`.
- `node` does not import `transport/*`, `client`, or `server`.
- `ui` does not import `engine` or `transport`.
- `embeddedClient` and `server.Server` are thin adapters over `*node.Node`.
- `Ack`, `Fail`, `Extend`, and `AckDeadLetter` each have exactly one implementation.
- Remote `Dequeue` uses long-poll with a timeout and a socket deadline. No polling loop remains.
- Server handlers run under a per-connection cancellable context. A disconnected client unblocks a long-poll handler.
- Every scalar config field is settable via a `mikyu daemon` flag. Config file > env > CLI precedence is documented and tested.
- `OpReservedHandshake` is defined and unused. The pre-1.0 wire policy is documented in `internal/protocol/doc.go` and `docs/protocol.md`.
- `BenchmarkTaskQueueEnqueue` and `BenchmarkTaskQueueDequeueAck` allocation counts are unchanged or lower.
- `bench/baseline.txt` has been refreshed with the post-refactor numbers and committed.
- `ARCHITECTURE.md` and `CONTRIBUTING.md` exist and match the code.

---

## What this plan does not do

- It does not rename `engine`, `logqueue`, `taskqueue`, `group`, `retention`, or `protocol` at the package-symbol level. It only moves directories.
- It does not introduce generics beyond `Typed` and `Codec`. `TaskQueue` does not need generic methods.
- It does not convert `Storage` or `ClientLogQueue` into iterator-returning APIs. Slices are correct for storage; iterators are only on `Stream` and `Tail`.
- It does not implement protocol versioning, handshake negotiation, feature flags, or a hello frame. It reserves `0x7E` and documents the pre-1.0 policy. Version negotiation is deferred until the first public release or the first external SDK.
- It does not ship an in-memory storage backend as a product feature. `enginetest.MemStorage` is a test double. If demand for an in-memory mode appears after 1.0, promoting it is a small, additive change.
- It does not add metrics, Prometheus, SSE, HTTP, or a TUI. It leaves specific seams (documented in "Where future work will live") that those additions can use without touching hot paths.
- It does not adopt `unique`, `weak`, `runtime.AddCleanup`, `crypto/mlkem`, `crypto/mldsa`, `crypto/hpke`, `simd`, or `archsimd`. No use site exists.
- It does not migrate away from `encoding/json`. The CLI output is fine on the v2-backed default.
- It does not add plugins, middleware, or a facade layer.
- It does not change the WAL, segment, or cache algorithms. Those are already correct and benchmarked.

---

## Order of execution

1. Step 0 — baselines, `enginetest.MemStorage`, reserved handshake opcode.
2. Step 1 — modernization.
3. Step 2 — file splits.
4. Step 3 — lease unification.
5. Step 4 — protocol DTO and pre-1.0 wire policy.
6. Step 5 — `internal/node` and `os.Root`.
7. Step 6 — long-poll dequeue with cancellation.
8. Step 7 — full daemon CLI configuration.
9. Step 8 — directory regroup.
10. Step 9 — `go-arch-lint`.
11. Step 10 — `synctest` migrations.
12. Step 11 — docs and examples.
13. Step 12 — CI gates.
14. Step 13 — sentinel hygiene.
15. Step 14 — shim removal.
16. Step 15 — final verification.

Each step produces a working, tested, benchmarked repository. If any step is left incomplete, the repository is still in a strictly better state than before that step began.