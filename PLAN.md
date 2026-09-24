# Metrics v2: implementation plan

A model observability surface for Mikyu. One `Snapshot` type flows from the collector through every surface: embedded SDK, remote SDK, CLI, REPL, Prometheus, and HTTP JSON.
---

## 0. Ground rules

1. **`Snapshot` is the single wire and SDK type.** It is built by the collector, marshaled by the server over `OpMetrics`, returned directly by embedded clients, and rendered by CLI/REPL/Prometheus. No parallel representations.
2. **Pull for gauges, push for counters.** Anything instantaneous (depth, in-flight ops, WAL fill, cache bytes, config dials) is read at snapshot time. Anything monotonic (counts, latency observations) is one atomic add on the hot path.
3. **Reuse the runtime.** `runtime/metrics` gives GC pauses, scheduler latency, goroutine states, mutex wait, memory classes — all free. Read them at tick time; never instrument them yourself.
4. **Never `Lock` to observe a hot lock.** Use `TryLock` then time only the contended path (see §6). Never instrument `engine.Queue.mu` or `taskqueue.queue.mu` — they're too hot and the Go runtime's global mutex counter already covers them.
5. **Every new hot-path atomic must be benchmarked.** The `Benchmark*` files already exist (`internal/engine/stress_bench_test.go`, `internal/metrics/bench_test.go`); extend them, don't create new ones.

---

## 1. Target `Snapshot` shape

All types live in `internal/metrics/snapshot.go`. Every field is JSON-tagged.

```go
type Snapshot struct {
    Disabled  bool          `json:"disabled,omitempty"`
    Timestamp time.Time     `json:"timestamp"`
    Build     BuildView     `json:"build"`
    Runtime   RuntimeView   `json:"runtime"`
    Process   ProcessView   `json:"process"`
    GC        GCView        `json:"gc"`
    Scheduler SchedulerView `json:"scheduler"`
    Locks     LocksView     `json:"locks"`
    Session   spec.Spec     `json:"session,omitzero"`
    Storage   StorageView   `json:"storage"`
    Queues    []QueueView   `json:"queues"`
    Totals    TotalsView    `json:"totals"`
}

type BuildView struct {
    SpecVersion uint16 `json:"spec_version"`
    GoVersion   string `json:"go_version"`
    OS, Arch    string `json:"os,arch"`
}

type RuntimeView struct {
    Goroutines     int             `json:"goroutines"`
    GOMAXPROCS     int             `json:"gomaxprocs"`
    GoroutineStates GoroutineStates `json:"goroutine_states"`
    Heap           HeapView        `json:"heap"`
    Stack          MemoryClassView `json:"stack"`
    Metadata       MemoryClassView `json:"metadata"`
    UptimeSec      float64         `json:"uptime_seconds"`
}

type GoroutineStates struct {
    Running  int64 `json:"running"`
    Runnable int64 `json:"runnable"`
    Waiting  int64 `json:"waiting"`
}

type HeapView struct {
    Alloc      int64 `json:"alloc"`
    Sys        int64 `json:"sys"`
    Inuse      int64 `json:"inuse"`
    Idle       int64 `json:"idle"`
    Released   int64 `json:"released"`
    Objects    int64 `json:"objects"`
    TotalAlloc int64 `json:"total_alloc"`
    TotalFree  int64 `json:"total_free"`
}

type MemoryClassView struct {
    Inuse int64 `json:"inuse"`
    Sys   int64 `json:"sys"`
}

type ProcessView struct {
    CPUPercent     float64 `json:"cpu_percent"`
    CPUSecondsUser float64 `json:"cpu_seconds_user"`
    CPUSecondsSys  float64 `json:"cpu_seconds_sys"`
    RSSBytes       int64   `json:"rss_bytes"`
    VSZBytes       int64   `json:"vsz_bytes"`
    Threads        int64   `json:"threads"`
    OpenFDs        int64   `json:"open_fds"`
}

type GCView struct {
    Cycles        int64     `json:"cycles"`
    PauseTotalMs  float64   `json:"pause_total_ms"`
    NextGCBytes   int64     `json:"next_gc_bytes"`
    MemLimitBytes int64     `json:"mem_limit_bytes"`
    Pause         PauseView `json:"pause"`
}

type PauseView struct {
    P50Ms float64 `json:"p50_ms"`
    P90Ms float64 `json:"p90_ms"`
    P99Ms float64 `json:"p99_ms"`
    MaxMs float64 `json:"max_ms"`
}

type SchedulerView struct {
    Latency LatencyView `json:"latency"`
}

type LatencyView struct {
    P50 float64 `json:"p50"` // seconds
    P90 float64 `json:"p90"`
    P99 float64 `json:"p99"`
}

type LocksView struct {
    GlobalWaitSec float64    `json:"global_wait_seconds"`
    Observers     []LockView `json:"observers,omitempty"`
}

type LockView struct {
    Name        string  `json:"name"`
    Waits       int64   `json:"waits"`
    TotalWaitMs float64 `json:"total_wait_ms"`
    MaxWaitMs   float64 `json:"max_wait_ms"`
    P50WaitMs   float64 `json:"p50_wait_ms"`
    P99WaitMs   float64 `json:"p99_wait_ms"`
    CurrentHeld int64   `json:"current_held"`
}

type StorageView struct {
    DataDir          string `json:"data_dir,omitempty"`
    Queues           int    `json:"queues"`
    Segments         int64  `json:"segments"`
    SizeBytes        int64  `json:"size_bytes"`
    CacheBytes       int64  `json:"cache_bytes"`
    CacheLimit       int64  `json:"cache_limit"`
    CacheHits        int64  `json:"cache_hits"`
    CacheMisses      int64  `json:"cache_misses"`
    CommittedBytes   int64  `json:"committed_bytes"`
    FlushedBytes     int64  `json:"flushed_bytes"`
    PendingBytes     int64  `json:"pending_bytes"`
    OffsetStoreBytes int64  `json:"offset_store_bytes"`
}

type QueueView struct {
    Name       string                  `json:"name"`
    Kind       string                  `json:"kind"`
    Spec       spec.Spec               `json:"spec,omitzero"`
    Derived    spec.Spec               `json:"derived,omitzero"`
    Effective  spec.Spec               `json:"effective,omitzero"`
    Counters   map[string]int64        `json:"counters,omitempty"`
    Gauges     map[string]int64        `json:"gauges,omitempty"`
    Rates      Rates                   `json:"rates,omitzero"`
    Histograms map[string]HistogramView `json:"histograms,omitempty"`

    Producers    ProducerView     `json:"producers"`
    Consumers    ConsumerView     `json:"consumers"`
    Backpressure BackpressureView `json:"backpressure"`
    Tasks        TaskView         `json:"tasks,omitzero"`
    Groups       []GroupView      `json:"groups,omitempty"`
}

type HistogramView struct {
    Bounds []float64 `json:"bounds"`
    Counts []uint64  `json:"counts"`
    Sum    float64   `json:"sum"`
    Total  uint64    `json:"total"`
    P50    float64   `json:"p50"`
    P90    float64   `json:"p90"`
    P95    float64   `json:"p95"`
    P99    float64   `json:"p99"`
    P999   float64   `json:"p999"`
    Max    float64   `json:"max"`
}

type ProducerView struct {
    InFlight     int64 `json:"in_flight"`
    BlockedOnWAL int64 `json:"blocked_on_wal"`
    BlockEvents  int64 `json:"block_events"`
}

type ConsumerView struct {
    InFlight          int64 `json:"in_flight"`
    ParkedFetch       int64 `json:"parked_fetch"`
    ParkedDequeue     int64 `json:"parked_dequeue"`
    FetchParkEvents   int64 `json:"fetch_park_events"`
    DequeueParkEvents int64 `json:"dequeue_park_events"`
}

type BackpressureView struct {
    WALChannelFill   float64 `json:"wal_channel_fill"`
    WALByteFill      float64 `json:"wal_byte_fill"`
    QueueFullEvents  int64   `json:"queue_full_events"`
    ProducerBlocks   int64   `json:"producer_blocks"`
    ConsumerParks    int64   `json:"consumer_parks"`
    RetryBudgetDrops int64   `json:"retry_budget_drops"`
}

type TaskView struct {
    Active         int64 `json:"active"`
    InFlight       int64 `json:"in_flight"`
    Scheduled      int64 `json:"scheduled"`
    DeadLetters    int64 `json:"dead_letters"`
    Uncheckpointed int64 `json:"uncheckpointed"`
    ScheduleHeap   int64 `json:"schedule_heap"`
    CheckpointLSN  int64 `json:"checkpoint_lsn"`
    Dirty          int64 `json:"dirty"`
    CheckpointLag  int64 `json:"checkpoint_lag"`
}
```

`Snapshot.Queue(name)`, `Snapshot.IterQueues()`, `GroupView`, `Rates`, `TotalsView` remain as they are.

---

## 2. Percentile math

Add to `internal/metrics/instruments.go` (method on `HistogramSnapshot`).

```go
// Percentile returns the approximate p-th quantile by linear interpolation
// inside the bucket that contains the target rank (Prometheus histogram_quantile
// semantics). Accuracy is bounded by the bucket width.
func (h HistogramSnapshot) Percentile(p float64) float64
func (h HistogramSnapshot) Max() float64
```

### Tests (`instruments_test.go`)

- `TestHistogramPercentileEmpty` — zero total returns zero, no NaN, no panic.
- `TestHistogramPercentileSingleValue` — one observation at 1.5 with bounds `{1,2,3}` returns 1.5 exactly.
- `TestHistogramPercentileAllInFirstBucket` — all observations under the first bound interpolate within `[0, bound]`.
- `TestHistogramPercentileAllInLastBucket` — no overflow panic; returns a value `<= bounds[len-1]`.
- `TestHistogramPercentileAtBoundary` — an observation exactly at a bound lands in the correct bucket (matches `slices.BinarySearch` semantics).
- `TestHistogramPercentileMonotonic` — for a fixed histogram, `p50 <= p90 <= p99 <= p999 <= max`, evaluated with floating-point tolerance.
- `TestHistogramPercentileKnownDistribution` — 100 observations uniformly in `[0,100]` with bounds every 10; assert p50 within 5 of 50, p99 within 5 of 99.
- `TestHistogramMaxFromCounts` — Max returns the highest bound with a non-zero count, zero if empty.

---

## 3. Pull/push cost model (the contract)

| Signal | Model | Steady-state cost per op |
|---|---|---|
| `appends`, `reads`, `enqueues`, `dequeues` | push atomic add | ~5 ns |
| `cache_hits`, `cache_misses` | push atomic add | ~5 ns |
| `in_flight` (all ops) | push atomic inc/dec | ~5 ns |
| `blocked_on_wal`, `parked_fetch`, `parked_dequeue` | push inc/dec **only when blocking** | 0 ns |
| `block_events`, `fetch_park_events`, `dequeue_park_events` | push inc **only when blocking** | 0 ns |
| Latency histograms | push bucket add + sum CAS | ~10 ns |
| WAL byte fill, async channel fill | pull from `len()`/fields | 0 ns |
| Queue depth, cache bytes, task maps | pull | 0 ns |
| `Spec`, `Derived`, `Effective`, `Session` | pull | 0 ns |
| All Go runtime stats | pull from `runtime/metrics` | 0 ns |
| Lock observers | `TryLock` fast path; time only contended | ~3 ns |

Disabled path: every instrument method is nil-safe (`if c == nil { return }`), so an uninstrumented queue pays ~1 ns. Prove it with `BenchmarkQueueAppendWithoutMetrics` (existing) and a new `BenchmarkQueueAppendMetricsDisabled` that calls the same code path with a nil scope.

---

## 4. Phase-by-phase implementation

Each phase is independently shippable and has its own test file. **Do not skip phases.**

### Phase 1 — Snapshot shape, no new instrumentation

Change `internal/metrics/snapshot.go` to the new struct. Leave every existing field's value as-is; new fields stay zero. Bump `spec.CurrentSpecVersion`.

Files:
- `internal/metrics/snapshot.go` — add types above; leave `Snapshot.Queue`/`IterQueues` alone.
- `internal/metrics/collector.go` — no logic change; the new struct fields marshal as zero.
- `internal/spec/codec.go` — bump `CurrentSpecVersion`.

Tests:
- `TestSnapshotJSONRoundTrip` — build a fully populated snapshot, marshal, unmarshal, `require.Equal`.
- `TestSnapshotOmitemptyRespected` — empty optional maps/structs are absent from JSON.
- `TestSnapshotQueueLookup` — `Queue("missing")` returns `(QueueView{}, false)`; `Queue("present")` returns the exact view.
- Golden file `internal/metrics/testdata/snapshot.json` regenerated via `-update`.

Risk check: run `mikyu metrics --json` against an embedded client and eyeball the shape.

### Phase 2 — Runtime source via `runtime/metrics`

Rewrite `internal/node/runtime_source.go` to read from `runtime/metrics` instead of `runtime.ReadMemStats`. Precompute the sample name list once at construction, reuse a `[]runtime/metrics.Sample` slice across ticks, and skip metrics the running Go build does not expose. OS-specific process readings live in build-tagged files, exactly like `lock_unix.go` / `lock_windows.go` and `sync_darwin.go` / `sync_windows.go` already do elsewhere in the tree.

#### Files

- `internal/node/runtime_source.go` (no build tag) — defines `RuntimeSource`, `newRuntimeSource`, `Collect`, `cpuPercent`, and the portable interface the tagged files satisfy.
- `internal/node/runtime_source_linux.go` (`//go:build linux`) — `/proc/self/stat` (VSZ), `/proc/self/status` (`Threads:`), `/proc/self/fd` (open FDs).
- `internal/node/runtime_source_darwin.go` (`//go:build darwin`) — `sysctl(KERN_PROC)` and `proc_pidinfo(PROC_PIDTASKALLINFO)` for VSZ, threads, FDs.
- `internal/node/runtime_source_bsd.go` (`//go:build freebsd || netbsd || openbsd`) — same via `sysctl`; return zeros for FDs (not worth the syscall on those platforms).
- `internal/node/runtime_source_unix.go` (`//go:build unix`) — `syscall.Getrusage` for RSS, user seconds, sys seconds. Shared by Linux, Darwin, and the BSDs; the VSZ/threads/FDs readers come from the per-OS files above.
- `internal/node/runtime_source_windows.go` (`//go:build windows`) — `GetProcessMemoryInfo` (RSS, PagefileUsage → VSZ), `GetProcessTimes` (user/sys seconds), `GetProcessHandleCount` (FDs), and a `Toolhelp32Snapshot` thread count.
- `internal/node/runtime_source_other.go` (`//go:build !linux && !darwin && !freebsd && !netbsd && !openbsd && !windows`) — every reader returns 0. Keeps the build green on js/wasm, plan9, and future ports.
- `internal/node/runtime_source_test.go` — portable tests (see below).
- `internal/node/runtime_source_unix_test.go` (`//go:build unix`) — asserts the POSIX readers return non-negative values.
- `internal/node/runtime_source_windows_test.go` (`//go:build windows`) — asserts the Windows readers return non-negative values.

Every symbol declared by the portable file (`readProcessRSS`, `readProcessVSZ`, `readProcessThreads`, `readProcessFDs`, `readProcessUserSeconds`, `readProcessSysSeconds`) has exactly one definition per GOOS/GOARCH combination. A missing symbol on a platform fails the build, which is the point.

#### Portable `RuntimeSource`

```go
// runtimeMetricNames is the set of runtime/metrics samples the collector reads.
// Names not present in the running build are filtered out at construction, so
// a build on a platform that lacks, say, /cpu/classes/* still starts cleanly.
var runtimeMetricNames = []string{
    "/sched/goroutines:goroutines",
    "/sched/gomaxprocs:threads",
    "/sched/latencies:seconds",
    "/gc/cycles/total:gc-cycles",
    "/gc/pauses:seconds",
    "/gc/heap/goal:bytes",
    "/gc/gomemlimit:bytes",
    "/gc/heap/allocs:bytes",
    "/gc/heap/frees:bytes",
    "/memory/classes/total:bytes",
    "/memory/classes/heap/objects:bytes",
    "/memory/classes/heap/unused:bytes",
    "/memory/classes/heap/released:bytes",
    "/memory/classes/heap/free:bytes",
    "/memory/classes/stack:bytes",
    "/memory/classes/metadata/mcache/inuse:bytes",
    "/memory/classes/metadata/mspan/inuse:bytes",
    "/sync/mutex/wait/total:seconds",
    "/cpu/classes/user/total:cpu-seconds",
    "/cpu/classes/gc/total:cpu-seconds",
}

func newRuntimeSource() *RuntimeSource {
    have := map[string]bool{}
    for _, d := range runtime_metrics.All() {
        have[d.Name] = true
    }
    names := make([]string, 0, len(runtimeMetricNames))
    for _, n := range runtimeMetricNames {
        if have[n] {
            names = append(names, n)
        }
    }
    return &RuntimeSource{
        start:   time.Now(),
        samples: make([]runtime_metrics.Sample, len(names)),
    }
}
```

`Collect` iterates the samples slice, calls `runtime_metrics.Read(s.samples)`, and switches on `s.Name` to populate `dst.Runtime`, `dst.GC`, and `dst.Scheduler`. `s.Value.Kind()` may be `KindBad`; check it and skip rather than call `Float64()` / `Uint64()` on a bad value.

Histogram-valued metrics (`/gc/pauses:seconds`, `/sched/latencies:seconds`) decode to `*runtime_metrics.Float64Histogram`. Copy their `Buckets` and `Counts` into a `metrics.HistogramSnapshot` and let Phase 1's `Percentile` method fill the quantile fields. Do **not** replace the runtime's bucket layout — its bounds are already tuned for the underlying distributions.

`GOMAXPROCS` is read from `runtime.GOMAXPROCS(0)` at each tick, not cached. Since Go 1.25 the runtime re-evaluates it when a cgroup CPU limit changes, and a cached value would drift.

The existing `cpuPercent` helper moves here unchanged: it takes cumulative process CPU nanoseconds, converts to a percentage against wall-clock delta, and reports zero on the first sample.

#### Tests (`internal/node/runtime_source_test.go`)

- `TestRuntimeSourceShape` — tick once, assert every `RuntimeView`, `GCView`, `SchedulerView`, and `ProcessView` field is populated and non-negative.
- `TestRuntimeSourceUptimeGrows` — tick, sleep, tick; second `UptimeSec > first`.
- `TestRuntimeSourceGCPercentiles` — call `runtime.GC()` ten times, tick; assert `GC.Pause.Total > 0`, `P50 <= P90 <= P99 <= Max`, and every value is finite.
- `TestRuntimeSourceSchedulerLatency` — run a busy loop for 50ms, tick, assert `Scheduler.Latency.P99 > 0`.
- `TestRuntimeSourceGoroutineStates` — spawn ten goroutines parked on a channel, `synctest.Wait()`, tick; assert `GoroutineStates.Waiting >= 10`. Release the channel in a defer so `goleak` stays quiet.
- `TestRuntimeSourceMemoryClassesSum` — sum of `Heap.Alloc + Heap.Idle + Heap.Released + Stack.Inuse + Metadata.Inuse` is within 10% of `Heap.Sys` on the same tick. Loose enough to tolerate GC races, tight enough to catch a misread.
- `TestRuntimeSourceNoAllocationSteadyState` — using `testing.AllocsPerRun(1000, tick)`, assert fewer than 5 allocations per tick after the first. The sample slice must be reused; a per-tick `make` will fail this immediately.
- `TestRuntimeSourceSkipsUnavailableMetrics` — run `newRuntimeSource()` on every CI OS; assert no panic, and that `Collect` succeeds even if some metrics are missing.
- `TestRuntimeSourceFirstTickWithoutGC` — construct a source, tick before any `runtime.GC()` has run; assert no panic and `GC.Pause.Total == 0`.
- `TestRuntimeSourceBucketCountsMatchBounds` — for both runtime histograms, `len(Counts) == len(Bounds)+1`; `Total` equals the sum of `Counts`.
- `TestRuntimeSourceConcurrentSnapshot` — eight goroutines call `Collect` on one source while another goroutine mutates runtime state (goroutine spawn/exit); run under `-race`.


Both are tagged so they compile only on their GOOS. Cross-compiled binaries cannot run, so these tests are executed by the CI matrix on their native OS.

#### Edge cases

- A runtime histogram with zero observations: `Percentile` returns 0, `Max` returns 0. Covered by `TestRuntimeSourceFirstTickWithoutGC`.
- A `Sample.Value.Kind() == KindBad`: skipped silently; no `Float64()` panic. Add an explicit unit test that fakes a sample slice with one bad entry and asserts the field stays zero.
- A platform where `/proc/self/status` is unreadable (chroot, hardened container): `readProcessThreads` returns 0 and logs once. Do not fail the tick.
- A Windows process whose handle count query fails under low privilege: same — return 0.

#### Acceptance criteria

- `go test -race ./internal/node/...` passes on all three native platforms.
- `go vet ./...` passes with `stdversion`, `hostport`, and `waitgroup` enabled (they are the defaults since Go 1.25 — no flags needed).
- `TestRuntimeSourceNoAllocationSteadyState` reports fewer than 5 allocs per tick.

### Phase 3 — Engine counters and WAL block events

Add instruments to `internal/engine/metrics.go`:

```go
type queueMetrics struct {
    // existing...
    cacheHits    *metrics.Counter
    cacheMisses  *metrics.Counter
    inFlightApp  *metrics.Gauge
    inFlightRead *metrics.Gauge
}
```

Wire them:
- `internal/engine/read.go` — bump `cacheHits`/`cacheMisses` in both `Read` and `ReadBatch`, once per offset.
- `internal/engine/append.go` — inc/dec `inFlightApp` around the whole `AppendBatch` body (defer).
- `internal/engine/read.go` — inc/dec `inFlightRead` around `ReadBatch`.

WAL (`internal/engine/wal.go`): add `blockEvents`, `blockedProducers`, and a `pendingBytes`/`publishedBytes` accessor. Change `pushAsync` to a two-step select (see §5 below for exact shape).

Add a pull method:

```go
// StorageSnapshot returns a fixed-shape view of the queue's on-disk state.
// Caller may not hold q.mu.
func (q *Queue) StorageSnapshot() metrics.StorageQueueView
```

Emits from `Manager.Collect` in `internal/engine/metrics.go`.

Tests:
- `TestCacheHitMissCounters` — append 10, read 10 twice. Assert `hits == 10`, `misses == 10`.
- `TestInFlightAppends` — launch 8 goroutines blocked on a WAL write (use a slow `writerHook`), tick, assert `Producers.InFlight == 8`.
- `TestWALBlockEvents` — fill `asyncChan` to capacity, try one more push with a cancelable ctx, assert `blockEvents == 1` while blocked.
- `TestWALBlockedGaugeDecreases` — after cancel, `blockedProducers` returns to 0.
- `TestWALFillRatioBounded` — `WALChannelFill` and `WALByteFill` are always in `[0, 1]`.
- `TestStorageSnapshotShape` — after mixed traffic, every `StorageView` field is populated; `CommittedBytes >= FlushedBytes >= PendingBytes == 0` at rest.
- Benchmark: extend `BenchmarkQueueAppendWithMetrics` and `BenchmarkQueueAppendWithoutMetrics`; assert the difference is < 30 ns/op on the same machine. If it isn't, a hot atomic was added in the wrong place.

Edge cases:
- `AppendBatch` with a mix of successes and errors — in-flight gauge still returns to zero.
- `ReadBatch` of a fully compacted range — hits/misses still correct.
- WAL blocked with `ctx` already canceled — must not increment `blockEvents` (the fast path fails immediately).

### Phase 4 — Task queue view

Add to `internal/queue/taskqueue/metrics.go`:

```go
type taskMetrics struct {
    // existing...
    inFlightEnq *metrics.Gauge
    parkedDeq   *metrics.Gauge
    dequeueParkEvents *metrics.Counter
    queueFullEvents   *metrics.Counter
    retryBudgetDrops  *metrics.Counter
}
```

Wire:
- `enqueue.go` — inc/dec `inFlightEnq` around the whole method; `queueFullEvents.Inc()` when `ErrQueueFull` is about to return.
- `dequeue.go` — two-step select in `Dequeue` (see §5); inc `dequeueParkEvents` only when the second select is entered. `parkedDeq` inc/dec around the second select.
- `nack.go` — `retryBudgetDrops.Inc()` at the point a task moves to the DLQ.

Add a pull method:

```go
func (q *queue) TaskSnapshot() metrics.TaskView
```

Called from `Manager.Collect`; walks `active`, `schedule`, `dlq` under `q.mu.Lock` (or `RLock` if you split the lock — don't, for now).

Tests (`taskqueue_test.go`):
- `TestTaskSnapshotAtRest` — after N enqueues and zero dequeues, `Active == N`, `InFlight == 0`, `Scheduled == N`.
- `TestTaskSnapshotInFlight` — after K dequeues of M leases, `InFlight == K`.
- `TestTaskSnapshotDLQ` — after dead-lettering, `DeadLetters` counts match `DeadLetters()`.
- `TestTaskSnapshotCheckpointLag` — force a `checkpointLocked`, assert `CheckpointLag == 0`; enqueue more, assert `CheckpointLag > 0`.
- `TestDequeueParkEvents` — start a Dequeue with a 500ms budget on an empty queue, assert `dequeueParkEvents` goes from 0 to 1.
- `TestQueueFullEvent` — with `maxInFlight=1`, second Enqueue bumps `queueFullEvents` to 1.
- `TestRetryBudgetDrop` — with `maxAttempts=1`, one Nack bumps `retryBudgetDrops` to 1.
- Race test: `TestTaskSnapshotConcurrentWithDequeue` — 32 goroutines dequeueing while a goroutine snapshots 100 times; `go test -race` must be clean.

Edge cases:
- Snapshot called on a closed queue returns zero-value view, no panic. Cover with `TestTaskSnapshotClosed`.
- Snapshot while `refreshLocked` is mid-scan (simulate by holding `mu` from a test goroutine) — must wait, not deadlock.

### Phase 5 — Lock observers

New file `internal/metrics/lock.go`:

```go
type LockObserver struct {
    name  string
    mu    sync.Mutex
    waits, totalNs, maxNs, held atomic.Int64
    hist  *Histogram
}

func NewLockObserver(name string, bounds []float64) *LockObserver
func (l *LockObserver) Lock()
func (l *LockObserver) Unlock()
func (l *LockObserver) Snapshot() LockView
```

`Lock()` uses `TryLock` first; only on contention does it time. **Never wraps `engine.Queue.mu` or `taskqueue.queue.mu`.**

Register observers in a process-wide slice in `internal/metrics` and expose `RegisterLockObserver` / `SnapshotLockObservers`. Apply to:
- `internal/node/node.go` — the `Node.mu`.
- `internal/manager/manager.go` — the `Manager.mu`.
- `internal/engine/offsets.go` — the `OffsetStore.mu`.
- `internal/queue/retention/coordinator.go` — the `Coordinator.mu`.

Tests (`internal/metrics/lock_test.go`):
- `TestLockObserverUncontended` — 10k locks from one goroutine, `Waits == 0`, `TotalWaitMs < 1`.
- `TestLockObserverContended` — two goroutines fight for 1000 lock cycles; `Waits > 0`, `P99WaitMs > 0`, `MaxWaitMs >= P99WaitMs`.
- `TestLockObserverHeld` — lock, snapshot from another goroutine, `CurrentHeld == 1`; unlock, snapshot, `CurrentHeld == 0`.
- `TestLockObserverConcurrentSnapshot` — 8 goroutines lock/unlock while 1 goroutine snapshots 500 times; no race, no panic.
- `TestLockObserverMaxMonotonic` — `MaxWaitMs` never decreases.
- `TestLockObserverNilSafe` — a nil `*LockObserver` does not panic (defensive, since packages may register nothing).

### Phase 6 — Rendering package

New package `internal/ui/render`:

```go
func Metrics(w io.Writer, snap mikyu.Metrics, opts Options) error
func MetricsJSON(w io.Writer, snap mikyu.Metrics) error

type Options struct {
    Color    bool
    Width    int
    Queue    string // optional filter
    Verbose  bool   // include per-observer lock rows
}
```

CLI handler `handleMetrics` and REPL handler `handleMetrics` both call `render.Metrics`. The `--json` path calls `render.MetricsJSON`.

Layout target (see §8 of the original design for the exact text). Ship the same output from both surfaces; the only difference is the prompt line the REPL adds after it.

Tests (`internal/ui/render/metrics_test.go`):
- `TestMetricsEmptySnapshot` — a snapshot with no queues and zero values renders without panicking, no `NaN%`, no `-0s`.
- `TestMetricsDisabled` — `Disabled: true` renders a single line, no per-queue section.
- `TestMetricsQueueFilter` — `Options{Queue: "events"}` shows only `events`.
- `TestMetricsQueueFilterMissing` — filter for an absent queue prints "no queue named X" and exits 0.
- `TestMetricsColorOffNoEscape` — with `Color: false`, output contains no `\x1b[`.
- `TestMetricsWidthNarrow` — `Width: 40` produces output that wraps without truncating numbers.
- `TestMetricsDeterministic` — same snapshot, two calls, byte-identical output.
- Golden files: `testdata/metrics_full.txt`, `metrics_disabled.txt`, `metrics_filtered.txt`, regenerated with `-update`.
- `TestMetricsJSONMatchesWire` — `MetricsJSON(w, snap)` unmarshals back to `snap` exactly.

### Phase 7 — Prometheus encoder

Extend `internal/metrics/prometheus.go`. Add mappings from §9 of the original design. Keep the `mikyu_` prefix, `_total` suffix for counters, `_seconds` for time histograms.

Tests (`prometheus_test.go`):
- Extend `goldenSnapshot()` in the test to populate every new field.
- `TestPrometheusPercentileQuantiles` — histograms emit `{quantile="0.5"}`, `{quantile="0.9"}`, `{quantile="0.99"}`, `{quantile="+Inf"}` with monotone values.
- `TestPrometheusInfoDials` — `mikyu_queue_info{queue,kind,durability,...} 1` appears exactly once per queue, with stable label ordering (sort keys, use `strings.Builder`).
- `TestPrometheusNoNaNOrInf` — walk output, fail on literal `NaN`, `+Inf`, `-Inf` in value position (not label position).
- `TestPrometheusCounterSuffixInvariant` — every line whose name does not end in `_total`, `_seconds`, `_bytes`, `_ratio`, or `_count` and whose `# TYPE` is counter must end in `_total`.
- Golden file regenerated via `-update`.
- `TestPrometheusNilMapsSkipped` — a queue with nil histograms/counters emits only its gauge lines, no empty label sets.

---

## 5. The two-step select pattern (needed in three places)

Do **not** rewrite the existing single-select; the current code is correct, just does not count. Insert the fast-path check first:

```go
// Before:
select {
case ch <- x:
case <-ctx.Done():
case <-shutdown:
}

// After:
select {
case ch <- x:
    return nil
default:
}
c.blockEvents.Inc()
c.blocked.Add(1)
defer c.blocked.Add(-1)
select {
case ch <- x:
    return nil
case <-ctx.Done():
    return ctx.Err()
case <-shutdown:
    return ErrQueueClosed
}
```

This is faster (no runtime park setup on the fast path) *and* gives the counter for free. Apply in:
- `internal/engine/wal.go` — `pushAsync`.
- `internal/engine/fetch.go` — the `select { ctx / notify / timer }` on parked fetch.
- `internal/queue/taskqueue/dequeue.go` — the `select { ctx / notify }` and `{ ctx / notify / timer }` on parked dequeue.

### Tests for the pattern (each location)

- Fast path: value goes through, no event counter increment.
- Slow path: channel full / no data, event counter increments exactly once.
- Cancel while blocked: event counter incremented, gauge returns to zero, correct error returned.
- Shutdown while blocked: same, correct error returned.
- Race: `go test -race ./internal/engine/... ./internal/queue/taskqueue/...` clean.

---

## 6. Cross-cutting tests every phase must add

These run on CI and catch problems that unit tests miss.

- `TestParityEmbeddedRemote` — in `internal/client/parity_test.go`, already exists. Extend `runOpScript` so it also reads `Metrics` and asserts `jsonKeys` match between embedded and remote. Both must include the same top-level fields and the same per-queue fields. Machine-local values (`Timestamp`, `RSSBytes`, `CPUPercent`, `UptimeSec`, `Locks`) are zeroed before comparison.
- `TestGoleak` — every package already has `goleak.VerifyTestMain`. Confirm new goroutines (WAL flusher, checkpoint loop, lock observer) drain on `Close`. If you add a goroutine, add a test that closes the owner and asserts goleak is quiet.
- `TestSnapshotJSONStable` — marshal a fixed snapshot 100 times, assert byte-identical output. Catches map-ordering non-determinism (Go maps randomize iteration order; use `slices.Sorted(maps.Keys(...))` where you walk them).
- `TestSnapshotMarshalsUnderLoad` — 32 goroutines append/read while one goroutine calls `Snapshot` 1000 times; `-race` clean, no panics, no `-Inf`/`NaN` in JSON.
- `TestMetricsEndpointUnderLoad` — start the daemon, hammer it with 8 clients, scrape `/api/metrics` 200 times concurrently; every response is valid JSON, no 5xx.
- `TestSSEBackpressure` — a stalled SSE subscriber does not block the ticker (already exists as `TestMetricsHTTPSSESlowClientDoesNotBlock`; keep it).
- `TestCollectorTickDeterministicUnderSynctest` — using `testing/synctest`, advance virtual time by 10 ticks, assert exactly 10 snapshots in history.
- `TestCollectorSubscribeDropUnderLoad` — 100 subscribers, one is never read; after 50 ticks, it has buffered at most 1 snapshot and no other subscriber was delayed.
- `TestMetricsDisabledZeroCost` — start an embedded client with `WithMetricsEnabled(false)`; `Metrics(ctx)` returns `Snapshot{Disabled: true}`; the collector's `Subscribe` returns a closed channel immediately.

---

## 7. Benchmark gates (must pass before merge)

Add to `internal/metrics/bench_test.go` and `internal/engine/stress_bench_test.go`:

- `BenchmarkAppendWithMetrics` vs `BenchmarkAppendWithoutMetrics` — delta < 30 ns/op.
- `BenchmarkDequeueWithMetrics` vs `BenchmarkDequeueWithoutMetrics` — delta < 50 ns/op (taskqueue also has a checkpoint loop).
- `BenchmarkCollectorSnapshot` — allocate < 1 MiB per snapshot (existing benchmark; set a threshold).
- `BenchmarkRuntimeSource` — `AllocsPerRun < 5`.
- `BenchmarkLockObserverUncontended` — < 5 ns/op.
- `BenchmarkLockObserverContended` — < 200 ns/op.

Commit these as failing tests if the numbers regress. The Go test harness can compare against a threshold via `testing.B.ReportMetric` and a `-benchtime=1s` gate in CI; if you'd rather not wire that now, at minimum keep them in the file so a reviewer can run them manually.

---

## 8. Rendering target (put this in the CLI/REPL golden test)

```
$ mikyu metrics
mikyu (spec v5, go1.24, linux/amd64)     uptime 1h23m45s
process    cpu 12.4%  rss 128MiB  threads 12  fds 48
gc         cycles 42  pause p50 0.3ms p90 0.9ms p99 2.1ms
heap       64MiB / 128MiB (50%)  stack 512KiB  metadata 1.1MiB
scheduler  latency p50 12µs  p90 48µs  p99 220µs
locks      global wait 0.42s
storage    dir /var/lib/mikyu   queues 4  segments 42  size 1.2GiB
           cache 32MiB / 32MiB   hits 1.2M   misses 2.3K (0.19%)
           wal committed 12.3MiB  flushed 12.2MiB  pending 64KiB

events  (log)
  messages 1.2M   segments 10   size 200MiB   offsets 1000000..1200000
  rates    append 1.2K/s   read 800/s   in 12MiB/s   out 8MiB/s
  latency  append p50 120µs p90 340µs p99 1.2ms    read p50 80µs p90 200µs p99 600µs
  producers  in-flight 8   blocked-on-wal 0   block-events 0
  consumers  in-flight 4   parked-fetch 2   fetch-parks 42
  backpressure  wal-fill 0.3%  wal-bytes 0.06%  queue-full 0
  groups   [analytics lag 200] [alerts lag 50]
  dials    durability=sync  max-segment=64MiB  max-payload=1MiB
           max-segment-age=1h  retention-age=24h  max-cache=32MiB

jobs  (task)
  active 42   in-flight 8   scheduled 10   dead 2
  rates    enq 500/s   deq 480/s   ack 470/s   nack 10/s
  latency  enqueue p50 200µs p90 800µs p99 3.1ms    dequeue-wait p50 5ms p90 220ms p99 3.4s
  producers  in-flight 12   blocked 0
  consumers  in-flight 8   parked-dequeue 2   dequeue-parks 480
  backpressure  queue-full 0   retry-budget-drops 2
  dials    lease=30s  max-attempts=5  max-in-flight=100000  max-dlq=100000
```

Rules the renderer must obey:
- Never print `NaN`, `+Inf`, `-Inf`, or `-0s`.
- Zero values collapse to a single word (`0`, `off`, `none`) rather than the raw zero struct.
- Every number is human-scaled (`1.2K`, `64MiB`, `1.2ms`).
- `Color: false` output has zero ANSI bytes.
- Column widths adapt to the terminal; a narrow terminal wraps a section rather than truncating a number.

---

## 9. Order of work for a junior dev

1. Read `internal/metrics/snapshot.go`, `collector.go`, `instruments.go`, `registry.go` end-to-end. Run `go test ./internal/metrics/...`.
2. Land Phase 1 (shape only, no new signals). Get `TestSnapshotJSONRoundTrip` and the JSON golden passing.
3. Land Phase 2 (runtime source). The only package you touch is `internal/node`.
4. Land Phase 3 (engine counters + WAL). Add one instrument at a time, one test at a time.
5. Land Phase 4 (task queue).
6. Land Phase 5 (lock observers). Register them one at a time; each new observer is a one-line change.
7. Land Phase 6 (renderer). Write the golden output file once; every subsequent test compares against it.
8. Land Phase 7 (Prometheus). Regenerate the golden file last.

At the end of every phase: `go test -race ./...` must be clean, `go test ./internal/audit/...` must be clean (comment-style rules are enforced by CI), and `goleak` must be quiet in every package you touched.

---

## 10. What to check before calling it done

- `mikyu metrics` in CLI and REPL produce byte-identical output for the same snapshot.
- `mikyu metrics --json` round-trips through `internal/metrics.Snapshot` with `require.Equal`.
- `mikyu.Open(dir).Metrics(ctx)` and `mikyu.Dial(target).Metrics(ctx)` return the same JSON keys, same queue set, same kinds.
- `GET /api/metrics` returns the same bytes as `mikyu metrics --json`.
- `GET /metrics` emits every new series with no `NaN` and monotone quantiles.
- `go test -race ./...` passes on Linux, macOS, and Windows.
- `BenchmarkAppendWithMetrics - BenchmarkAppendWithoutMetrics < 30 ns/op`.
- A GC cycle during a snapshot does not corrupt any value (run `TestSnapshotMarshalsUnderLoad` with `GOGC=10`).

That's the model implementation.