# Phased Refactor Plan

## North Star

By the end, the repo should look like this from thirty thousand feet:

- `internal/` has an obvious visual hierarchy that mirrors the dependency graph.
- Every package has one job, and every file within a package has one role.
- The public `mikyu` package is unchanged and stays small.
- The wire protocol has no knowledge of the task driver.
- Embedded and daemon modes share a single local-node core.
- The layering is enforced by CI, not by convention.
- Performance is guarded by benchmarks that fail the build on regression.

And it should read like Kubernetes or etcd in structure, but with the kind of architectural clarity those repos often sacrifice to history.

---

## Sequencing Rationale

The plan is ordered so that:

1. Each phase is independently valuable — you can stop at any phase and have a strictly better repo.
2. Zero-behavior-change work comes before logic-touching work.
3. The layout is settled *before* it is enforced.
4. Hot paths (Enqueue, Dequeue, Ack, append, read) are touched as late as possible, with benchmarks as the gate.

Nothing in Phases 1–6 changes the exported `mikyu` API or the wire protocol. If any phase does, that is a bug in the phase.

---

## Phase 0 — Baselines and guardrails (1–2 days)

**Goal:** make every later phase measurable and reversible.

- Capture `go test ./...` coverage per package. Note any package below 70%.
- Run `go test -bench=. -benchmem` under `-tags bench` and commit the output to `bench/baseline.txt`. Treat it as the reference.
- Freeze `mikyu`'s exported surface: `go doc github.com/dmunish/mikyu > api/contract.txt` and commit it. Add a CI step that diffs against it.
- Record current import graph: `go list -deps ./... | sort -u > arch/imports.txt` (or use `go mod graph`).
- Add `goleak.VerifyTestMain` to any remaining test packages that don't have it. You already do in `engine` and `mikyu`; extend to `taskqueue`, `server`, `client`.

**Exit criteria:** you can prove, per package, that a refactor did not change coverage, benchmarks, or API.

---

## Phase 1 — Mechanical file splits (2–3 days)

**Goal:** decompose grab-bag files into role files. No logic change.

Split these files along role lines, keeping package and symbol names identical:

- `internal/engine/queue.go` →
  `config.go`, `errors.go`, `queue.go` (struct + constructor + lifecycle), `append.go`, `read.go`, `segment_management.go` (roll, loadSegments).
- `internal/server/handlers.go` →
  `dispatch.go`, `log_handlers.go`, `task_handlers.go`, `admin_handlers.go`, `errors.go` (taskErr, opErr).
- `internal/taskqueue/taskqueue.go` →
  `taskqueue.go` (struct, constructor, Close), `enqueue.go`, `dequeue.go`, `ack.go`, `fail.go`, `extend.go`, `dlq.go`, `registry.go` (validateLeaseLocked, currentLeaseLocked, takeDueLocked).
- `internal/client/remote.go` →
  `remote.go` (client), `remote_logqueue.go`, `remote_taskqueue.go`, `remote_group.go`, `errors.go` (decodeProtocolError, isRetryable).
- `internal/client/embedded.go` →
  `embedded.go`, `embedded_queues.go` (getOrCreateLogQueue, TaskQueue).

**Verification:**
- `git diff -M` should show only moves and package-line additions.
- All tests, all benchmarks, all API snapshots unchanged.
- `gofmt -l ./internal` clean.

**Risk:** near zero. This is the warmup.

---

## Phase 2 — Collapse dual-path duplication (3–5 days)

**Goal:** one internal implementation per semantic operation, regardless of how the caller identifies the lease.

In `taskqueue`, introduce a lease reference:

```go
type leaseRef struct {
    ID         string
    Offset     uint64
    Attempts   uint32
    DeliveryID string
    strict     bool
}
```

Then unify:

- `Ack` / `AckID` → `ackLocked(ctx, ref)`
- `Fail` / `FailID` → `failLocked(ctx, ref, opts)`
- `Extend` / `ExtendID` → `extendLocked(ctx, ref, timeout)`
- `AckDeadLetter` / `AckDeadLetterID` → `ackDeadLetterLocked(ctx, ref)`

`validateLeaseLocked` and `currentLeaseLocked` become a single `resolveLeaseLocked(ctx, ref)` that either strict-matches or resolves the current lease by ID.

Mirror this in:
- `server/handlers.go`: one task handler that decodes an optional lease body.
- `client/remote.go`: one remote method that builds the correct opcode from the ref shape.

**Verification:**
- Every existing test in `taskqueue`, `server`, `client` passes.
- Benchmarks `BenchmarkTaskQueueDequeueAck` and `BenchmarkTaskQueueEnqueue` within 2% of baseline.
- No new allocations in the hot path — check with `-benchmem`.

**Risk:** moderate. The atomicity of refresh + validate + mutate + broadcast + prune must be preserved exactly. Review the diff line-by-line against the original.

---

## Phase 3 — Decouple `protocol` from `taskqueue` (1–2 days)

**Goal:** the wire layer knows nothing about the driver layer.

- Introduce `protocol.Task` as a plain DTO (id, payload, attempts, maxAttempts, visibility, scheduledAt, deliveryID, offset).
- Move `EncodeTask` / `DecodeTask` to operate on `protocol.Task`.
- Convert at the server boundary: `protocol.Task` ⇄ `taskqueue.Task`.
- `protocol/task.go` no longer imports `taskqueue`.

**Verification:**
- `go list -deps ./internal/protocol` contains no `internal/queue/*` or `internal/engine`.
- Wire round-trip tests unchanged.

**Risk:** low. This is a type-boundary change, not a semantic one.

---

## Phase 4 — Shared local node registry (4–6 days)

**Goal:** embed and daemon share one core; only the transport differs.

Extract `internal/node` (or `internal/registry`):

```go
type Node struct {
    mgr       *engine.Manager
    retention *retention.Coordinator
    logQueues map[string]*logqueue.LogQueue
    taskQueues map[string]*taskqueue.TaskQueue
    mu        sync.RWMutex
}

func (n *Node) LogQueue(name string) (*logqueue.LogQueue, error)
func (n *Node) TaskQueue(name string) (*taskqueue.TaskQueue, error)
func (n *Node) DeleteQueue(ctx, name) error
func (n *Node) TruncateQueue(ctx, name, minOffset) (int64, error)
func (n *Node) Close() error
```

Then:
- `embeddedClient` becomes a thin wrapper around `*node.Node`.
- `server.Server` becomes a thin wrapper around `*node.Node` plus a listener and protocol dispatch.
- `retention` registration happens inside `Node`, once.

**Verification:**
- `TestEmbeddedRetentionCoordinatesLogAndTaskDrivers` passes unchanged.
- `TestServerQueueHandleCaching` and `TestTaskQueueHandleCaching` pass unchanged.
- The same test suite now drives both modes through `Node`.

**Risk:** moderate-to-high. Watch for import cycles — `Node` must sit *below* `server` and *above* `engine` in the layer ordering. It cannot know about `protocol`.

---

## Phase 5 — Directory regroup (1–2 days)

**Goal:** the layout shows the layering.

Move to:

```
internal/
├── engine/
├── queue/
│   ├── logqueue/
│   ├── taskqueue/
│   ├── group/
│   └── retention/
├── transport/
│   ├── protocol/
│   └── ipc/
├── node/
├── client/
├── server/
├── daemon/
├── config/
└── ui/
    ├── cli/
    ├── repl/
    └── command/
```

Keep leaf package names identical (`package logqueue`, not `package log`). Directory churn only; no symbol churn.

**Verification:**
- `go build ./...`, `go test ./...`, `go vet ./...` all clean.
- `mikyu`'s public import path unchanged.
- Public API snapshot unchanged.

**Risk:** low but tedious. Do it in one commit; a partial move is worse than no move.

---

## Phase 6 — Layering enforcement (1 day)

**Goal:** the layering stops being a suggestion.

Add `go-arch-lint` (or `depguard` via `golangci-lint`) with rules that mirror Phase 5:

- `internal/engine/**` may not import any other `internal/**`.
- `internal/queue/**` may import `internal/engine` only.
- `internal/transport/**` may import nothing above `internal/engine`.
- `internal/node/**` may import `engine`, `queue/**`, `retention`.
- `internal/client/**`, `internal/server/**`, `internal/daemon/**` may import everything below them, not each other.
- `internal/ui/**` may import `internal/client`, `internal/command`, and nothing else internal.

CI fails on violation.

**Verification:** introduce a deliberate violation, confirm CI fails, revert.

**Risk:** none.

---

## Phase 7 — Dual-mode symmetry completion (3–5 days)

**Goal:** embedded and remote feel like the same product.

- Add a long-poll `OpTaskDequeue` variant (with timeout) so `remoteTaskQueue.Dequeue` stops polling every 10ms.
- Make `LogQueue(name)` and `TaskQueue(name)` uniform: both should return a stable handle and validate existence eagerly (or lazily, but consistently).
- Align the public surface of `LogQueue` and `TaskQueue` — both as interfaces, both with documented differences where the domain genuinely differs.
- Ensure `remoteLogQueue.Group` and the embedded equivalent behave identically under commit failure.

**Verification:**
- Run the existing embedded test suite against a remote client in-process. They should both pass.
- Add a CI job that spins up a real daemon and runs a subset of `mikyu` tests through `Dial`.

**Risk:** moderate. This is the only phase that may add public API surface. Additions only, never removals.

---

## Phase 8 — Masterclass polish (ongoing)

**Goal:** the repo reads like a reference implementation.

- `doc.go` in every package: one paragraph stating role and invariants.
- Godoc on every exported symbol, including invariants and concurrency guarantees.
- Runnable `Example*` tests in `mikyu` for every public flow.
- `ARCHITECTURE.md` describing the layer model, with the diagram from Phase 5.
- `CONTRIBUTING.md` stating the layering rules and the benchmark gate.
- CI gates:
  - arch lint
  - API contract diff
  - benchmark regression >5%
  - coverage floor per package
  - `goleak` on all test binaries
- Replace any remaining `errors.New` in the hot path with `errors.Is`-compatible sentinels.

---

## What this plan deliberately does not do

- It does not rename `engine`, `logqueue`, `taskqueue`, or any exported symbol.
- It does not introduce generics beyond what already exists (`Typed`, `Codec`).
- It does not split `engine` into multiple packages. `engine` is coherent; keep it whole.
- It does not add a plugin system, a middleware chain, or a metrics façade. Those are features, not architecture.
- It does not chase etcd's or Prometheus's layout wholesale. They have layers of history you don't.

---

## Definition of done

- `ls internal/` communicates the layer model without a README.
- Every file has a single role you can name in one sentence.
- `taskqueue` has exactly one implementation of Ack, Fail, Extend, AckDeadLetter.
- `protocol` does not import any driver.
- `embeddedClient` and `server.Server` share one local-node core.
- The public API diff against `api/contract.txt` in Phase 0 is empty except for Phase 7 additions.
- Benchmarks are within 5% of baseline.
- CI fails on any of the above being violated.