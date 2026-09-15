# Phased implementation plan

Assumptions before we start:

- Target toolchain is Go 1.27 (all modern features in the attached doc are available).
- Every phase is one PR, self-contained, and green on `go test ./...`.
- Phases land in order. Later phases depend on earlier ones.
- The overall rule from the design discussion: **`metrics.Snapshot` is the state; everything else is a view.** Nothing outside `internal/metrics` invents its own stats type.

A short list of the modern-Go features we'll lean on, and where:

| Feature | Since | Where we use it |
|---|---|---|
| `iter.Seq` / `Seq2` | 1.23 | Walking sources, instruments, and per-queue views |
| `slices.Collect`, `maps.Keys`, `slices.Sorted` | 1.23 | Deterministic snapshot ordering |
| Generic type aliases | 1.24 | `mikyu.Metrics = metrics.Snapshot` |
| `testing.B.Loop` | 1.24 | Collector and instrument benchmarks |
| `T.Context` | 1.24 | All new tests |
| `testing/synctest` | 1.25 (GA) | Collector tick, ring buffer, rate math, SSE drain |
| `atomic.Int64` / `atomic.Uint64` / `atomic.Pointer[T]` | 1.19+ | Instruments, snapshot pointer swap |
| `sync.OnceValue` | 1.21 | Lazily-built static labels |
| `new(expr)` | 1.26 | Snapshot construction (optional fields) |
| `encoding/json/v2` (transparently via 1.27) | 1.27 | Snapshot wire/HTTP encoding, no code change required |

We will not use: `weak`, `unique`, `runtime.AddCleanup`, `runtime/secret`, `simd`, generic methods. They solve problems we don't have (or would obscure the code).

---

## Phase 1 — `internal/metrics` core package

**Goal.** A standalone, stdlib-only package that owns instruments, snapshot shape, ring buffer, collector, and the Prometheus encoder. Nothing else depends on this yet.

**New files** under `internal/metrics/`:

```
doc.go            package clause (one line, per audit rules)
registry.go       Registry, Scope
instruments.go    Counter, Gauge, Histogram
snapshot.go       Snapshot, QueueView, GroupView, Rate
history.go        RingBuffer[T]
source.go         Source interface
collector.go      Collector
prometheus.go     Encode
```

**Steps.**

1. `instruments.go`. Wrap `atomic.Int64` / `atomic.Uint64`. Fixed buckets for histograms. Signatures:

    ```go
    type Counter struct{ v atomic.Int64 }
    func (c *Counter) Add(n int64)
    func (c *Counter) Inc()
    func (c *Counter) Load() int64

    type Gauge struct{ v atomic.Int64 }
    func (g *Gauge) Set(n int64)
    func (g *Gauge) Add(n int64)
    func (g *Gauge) Load() int64

    // Histogram buckets are set once at construction; Observe is atomic.
    type Histogram struct {
        bounds []float64           // upper bounds, monotonically increasing
        counts []atomic.Uint64     // one per bucket, plus one overflow bucket
        sum    atomic.Uint64       // sum of observations (nanoseconds for durations)
        total  atomic.Uint64
    }
    func (h *Histogram) Observe(v float64)
    func (h *Histogram) Snapshot() HistogramSnapshot
    ```

    `Histogram` must be safe for concurrent `Observe` and `Snapshot`. Bucket selection uses `slices.BinarySearch`.

2. `snapshot.go`. Define the immutable view type. This is the only thing that leaves the package.

    ```go
    type Snapshot struct {
        Disabled  bool          `json:"disabled,omitempty"`
        Timestamp time.Time     `json:"timestamp"`
        Runtime   RuntimeView   `json:"runtime"`
        Process   ProcessView   `json:"process"`
        Storage   StorageView   `json:"storage"`
        Queues    []QueueView   `json:"queues"`
        Totals    TotalsView    `json:"totals"`
    }

    type QueueView struct {
        Name      string
        Kind      string          // "log" | "task" | "shared"
        Spec      spec.Spec       // stored spec, JSON/YAML tagged
        Effective spec.Spec       // resolved
        Counters  map[string]int64
        Gauges    map[string]int64
        Histograms map[string]HistogramSnapshot
        Groups    []GroupView
        Rates     Rates           // derived from ring buffer by the collector
    }

    type Rates struct {
        AppendsPerSec      float64
        ReadsPerSec        float64
        EnqueuesPerSec     float64
        DequeuesPerSec     float64
        BytesInPerSec      float64
        BytesOutPerSec     float64
    }
    ```

    Use `maps.Keys` + `slices.Sorted` everywhere ordering matters. All fields are value types (no pointers) so a `Snapshot` can be passed by value safely.

    Add an iterator for consumers who want per-queue views lazily:

    ```go
    func (s Snapshot) IterQueues() iter.Seq[QueueView]
    func (s Snapshot) Queue(name string) (QueueView, bool)
    ```

3. `history.go`. Generic ring buffer.

    ```go
    type RingBuffer[T any] struct { /* mu, buf, head, size */ }
    func NewRingBuffer[T any](capacity int) *RingBuffer[T]
    func (r *RingBuffer[T]) Push(v T)
    func (r *RingBuffer[T]) Snapshot() []T       // oldest → newest copy
    func (r *RingBuffer[T]) Latest() (T, bool)
    func (r *RingBuffer[T]) Len() int
    ```

4. `registry.go`. `Registry` holds instruments keyed by `(scope, name)`. `Scope` is the per-queue / per-server attach point.

    ```go
    type Registry struct { /* mu, scopes map[string]*Scope, cfg Config */ }
    func NewRegistry(cfg Config) *Registry
    func (r *Registry) Scope(name string) *Scope     // create-on-first-use
    func (r *Registry) Scopes() iter.Seq[*Scope]

    type Scope struct { /* name, ref to parent, instruments */ }
    func (s *Scope) Counter(name string) *Counter
    func (s *Scope) Gauge(name string) *Gauge
    func (s *Scope) Histogram(name string, bounds []float64) *Histogram
    func (s *Scope) Name() string
    ```

    `Scope` must be safe on `nil` so hot paths can accept `nil *Scope` when metrics are disabled.

    ```go
    func (s *Scope) Counter(name string) *Counter {
        if s == nil { return noopCounter }
        ...
    }
    ```

    Use a package-level `noopCounter` / `noopGauge` / `noopHistogram`.

5. `source.go`.

    ```go
    type Source interface {
        Collect(ctx context.Context, dst *Snapshot)
    }

    type SourceFunc func(context.Context, *Snapshot)
    func (f SourceFunc) Collect(ctx context.Context, dst *Snapshot) { f(ctx, dst) }
    ```

6. `collector.go`.

    ```go
    type Config struct {
        Interval   time.Duration
        History    int
        Histograms bool
    }

    type Collector struct { /* ... */ }
    func New(reg *Registry, cfg Config, sources ...Source) *Collector
    func (c *Collector) Start(ctx context.Context)          // launches ticker goroutine
    func (c *Collector) Stop()                              // idempotent; blocks until goroutine exits
    func (c *Collector) Snapshot(ctx context.Context) (Snapshot, error)
    func (c *Collector) History() []Snapshot
    func (c *Collector) Subscribe() (<-chan Snapshot, func())
    ```

    Implementation contract:

    - `Start` is idempotent. Ticker uses `time.NewTicker(cfg.Interval)`.
    - Each tick: build a fresh `Snapshot`, run each `Source.Collect`, read each scope's instruments into `QueueView`s, compute rates from the previous snapshot's counters, append to ring buffer, fan out to subscribers.
    - Subscriber fan-out uses non-blocking sends. Slow subscribers drop frames; log once via `slog`.
    - `Stop` cancels the context and waits for the ticker goroutine with a `sync.WaitGroup`.

7. `prometheus.go`. Text-format 0.0.4 encoder.

    ```go
    func Encode(w io.Writer, s Snapshot, opts EncodeOptions) error

    type EncodeOptions struct {
        PerQueue   bool     // emit per-queue series
        TopNQueues int      // if PerQueue, cap at N by size
        Histograms bool
    }
    ```

    Names: `mikyu_queue_messages`, `mikyu_queue_bytes`, `mikyu_append_total`, `mikyu_append_duration_seconds_bucket`, etc. Counters always end in `_total`.

8. `doc.go`.

    ```go
    // Package metrics is the single source of truth for live Mikyu telemetry.
    package metrics
    ```

**Tests** (`internal/metrics/*_test.go`):

- `TestCounterConcurrentInc` — 100 goroutines × 1000 increments = 100 000, `-race`.
- `TestHistogramBuckets` — observe a known distribution, check bucket counts and sum.
- `TestRingBufferWrap` — push `capacity + 5`, verify oldest 5 gone and order preserved.
- `TestCollectorTicksAtInterval` — using `synctest.Test`. Assert 3–4 snapshots after 3.5× interval.
- `TestCollectorSubscribeDropOnSlow` — subscribe, never read, push many ticks; tick loop never blocks.
- `TestCollectorStopIdempotent` — call `Stop` twice.
- `TestScopeNilSafe` — `var s *Scope; s.Counter("x").Inc()` does not panic.
- `TestSnapshotIterQueues` — build a snapshot with 3 queues; `IterQueues` yields in name order.
- `TestPrometheusEncodeShape` — golden-file comparison for a hand-built snapshot.
- `BenchmarkCounterInc`, `BenchmarkHistogramObserve`, `BenchmarkCollectorSnapshot` — all with `b.Loop()`.

**Exit criteria.** `go test -race ./internal/metrics/...` green. Zero imports from the rest of the module.

---

## Phase 2 — `spec.Table` metrics parameters

**Goal.** The metrics system is configured through the same single table that configures everything else.

**Files touched.**

- `internal/spec/table.go` — add `sessionParam` entries and setters/getters.
- `internal/spec/spec.go` — extend `spec.Session` with the new fields.
- `internal/spec/validate.go` — validate `metrics_level`-style enums if introduced.
- `internal/spec/spec_test.go` — extend `TestTableMatchesSessionFields` (it already enforces every `spec.Session` field has a param).
- `internal/spec/env.go` — no change; `ApplyEnvSession` iterates the table.
- `internal/config/config.go` — no change; the loader uses the same table.

**New session params.**

| Key | Kind | Env | Flag | Default |
|---|---|---|---|---|
| `metrics_enabled` | `KindBool` | `MIKYU_METRICS_ENABLED` | `--metrics-enabled` | `true` |
| `metrics_interval` | `KindDuration` | `MIKYU_METRICS_INTERVAL` | `--metrics-interval` | `1s` |
| `metrics_history` | `KindInt` | `MIKYU_METRICS_HISTORY` | `--metrics-history` | `300` |
| `metrics_listen` | `KindString` | `MIKYU_METRICS_LISTEN` | `--metrics-listen` | `""` |
| `metrics_per_queue_prometheus` | `KindBool` | `MIKYU_METRICS_PER_QUEUE_PROMETHEUS` | `--metrics-per-queue-prometheus` | `false` |
| `metrics_histograms` | `KindBool` | `MIKYU_METRICS_HISTOGRAMS` | `--metrics-histograms` | `true` |

Also add `Spec.Session` fields with yaml tags matching the key text.

**Steps.**

1. Add the six `sessionParam` rows to `buildTable()`.
2. Add the six `sessionSetters` entries.
3. Extend `spec.Session`:

    ```go
    type Session struct {
        Target        string   `yaml:"target"`
        DataDir       string   `yaml:"data_dir"`
        TLSEnabled    bool     `yaml:"tls_enabled"`
        TLSCertFile   string   `yaml:"tls_cert_file"`
        TLSKeyFile    string   `yaml:"tls_key_file"`
        MaxConns      int      `yaml:"max_conns"`

        MetricsEnabled            bool          `yaml:"metrics_enabled"`
        MetricsInterval           Duration      `yaml:"metrics_interval"`
        MetricsHistory            int           `yaml:"metrics_history"`
        MetricsListen             string        `yaml:"metrics_listen"`
        MetricsPerQueuePrometheus bool          `yaml:"metrics_per_queue_prometheus"`
        MetricsHistograms         bool          `yaml:"metrics_histograms"`
    }
    ```

4. Extend `BuiltinSession()` to set the defaults above.
5. Validation in `validate.go`: `metrics_interval` must be positive when enabled; `metrics_history` must be `>= 1`; `metrics_listen` if non-empty must parse as `host:port`.

**Tests.**

- `TestTableMatchesSessionFields` — no change, it will fail until the new fields and params match.
- `TestSessionSettersLandOnTheirOwnField` — no change, will iterate the new fields automatically.
- `TestMetricsSessionDefaults` — assert `spec.BuiltinSession()` produces the table above.
- `TestMetricsEnvOverride` in `config_test.go` — round-trip each new env var.
- `TestMetricsFlagOverride` in `daemon_config_test.go` — round-trip each new flag.

**Exit criteria.** `go test ./internal/spec/... ./internal/config/... ./internal/daemon/...` green. Every new param reachable via env, flag, and YAML.

---

## Phase 3 — node ownership of the collector + runtime source

**Goal.** `node.Node` owns exactly one collector. It is created on `openNode` and cancelled on `closeInternal`. A runtime source populates Go runtime and process metrics. Nothing else in the tree consumes it yet.

**New files.**

- `internal/node/metrics.go` — node-side wiring.
- `internal/node/runtime_source.go` — `RuntimeSource`.
- `internal/node/runtime_source_test.go`.

**Files touched.**

- `internal/node/node.go` — add collector field, start/stop.
- `internal/node/node_test.go` — add collector lifecycle tests.
- `internal/daemon/daemon.go` — pass metrics config through to `node.Acquire` (only needs to plumb `spec.Session` metrics fields into the session defaults).
- `internal/client/options.go` — extend `SessionConfig` to carry metrics config (needed by embedded and remote clients later).

**Steps.**

1. Add to `SessionConfig`:

    ```go
    MetricsEnabled            bool
    MetricsInterval           time.Duration
    MetricsHistory            int
    MetricsHistograms         bool
    MetricsListen             string
    MetricsPerQueuePrometheus bool
    ```

    Defaults from `DefaultSessionConfig`.

2. `RuntimeSource`. Zero configuration. Fills `Snapshot.Runtime` and `Snapshot.Process`.

    ```go
    type RuntimeSource struct {
        start time.Time
    }

    func (s *RuntimeSource) Collect(ctx context.Context, dst *metrics.Snapshot) {
        var m runtime.MemStats
        runtime.ReadMemStats(&m)
        dst.Runtime.Goroutines = runtime.NumGoroutine()
        dst.Runtime.HeapAlloc  = int64(m.HeapAlloc)
        dst.Runtime.HeapInuse  = int64(m.HeapInuse)
        dst.Runtime.GCCount    = int64(m.NumGC)
        dst.Runtime.UptimeSec  = time.Since(s.start).Seconds()
        dst.Process.CPUPercent = readProcessCPU()
        dst.Process.RSSBytes   = readProcessRSS()
    }
    ```

    `readProcessCPU` and `readProcessRSS` are platform-specific (`runtime_source_unix.go`, `runtime_source_windows.go`). Use `gopsutil`? No — stdlib only. Read `/proc/self/stat` and `/proc/self/status` on Linux, `GetProcessMemoryInfo` on Windows, `getrusage` on darwin.

3. `Node` gains:

    ```go
    metrics   *metrics.Collector
    ```

    `openNode`:

    ```go
    reg := metrics.NewRegistry(metrics.Config{Histograms: defaults.MetricsHistograms})
    collector := metrics.New(reg, metrics.Config{
        Interval:   defaults.MetricsInterval,
        History:    defaults.MetricsHistory,
        Histograms: defaults.MetricsHistograms,
    }, &RuntimeSource{start: time.Now()})
    if defaults.MetricsEnabled {
        collector.Start(ctx)
    }
    n.metrics = collector
    ```

    `closeInternal`: `n.metrics.Stop()` before releasing the lock, then `mgr.Close()`. Order matters — sources may walk the manager, so the collector must be stopped first.

4. Expose:

    ```go
    func (n *Node) Metrics(ctx context.Context) (metrics.Snapshot, error)
    func (n *Node) Collector() *metrics.Collector
    ```

    `Metrics` calls `n.metrics.Snapshot(ctx)`.

5. Do **not** delete `node.Info` yet. It stays until Phase 5 so the intermediate state remains green.

**Tests.**

- `TestNodeCollectorLifecycle` — open node, wait a tick, call `Metrics`, assert `Runtime.Goroutines > 0`, close node, assert no leak (`goleak` is already wired).
- `TestNodeCollectorDisabled` — `MetricsEnabled: false` → `Snapshot.Disabled` is `true`, and `Start` is not called (verify `subscribe` returns a closed channel after one push).
- `TestRuntimeSourceShape` — collect once, assert non-zero uptime, non-zero heap.
- `TestRuntimeSourcePlatform` — `runtime_source_unix_test.go` asserts `readProcessCPU` returns `>= 0` (best-effort; skip if the platform can't be read).

**Exit criteria.** `go test -race ./internal/node/...` green. `Info()` still works.

---

## Phase 4 — instrumentation and the engine source

**Goal.** Every hot path updates atomics. Pull-based sources fill derived gauges. `engine.EngineStats` and its type are deleted.

**Files touched.**

- `internal/engine/metrics.go` — **new**: `Manager.Collect(ctx, *Snapshot)`.
- `internal/engine/queue.go`, `internal/engine/manager.go` — accept a `*metrics.Scope`, delete `EngineStats`.
- `internal/engine/append.go`, `read.go`, `fetch.go`, `wal.go` — instrument.
- `internal/queue/logqueue/logqueue.go` — accept a scope, instrument.
- `internal/queue/taskqueue/taskqueue.go` — accept a scope, instrument.
- `internal/queue/taskqueue/{enqueue,dequeue,ack,nack,extend,dlq}.go` — instrument.
- `internal/server/server.go` — accept a scope, instrument connection and op counters.
- `internal/node/node.go` — pass scopes when constructing queues and the manager; register `engine.Manager` as a source.

**Steps.**

1. Add `*metrics.Scope` parameters to constructors:

    ```go
    // engine
    func OpenManager(dir string, cfg Config, perQueue map[string]Config, scope *metrics.Scope) (*Manager, error)
    func openQueue(parentDir, name string, cfg Config, store *OffsetStore, scope *metrics.Scope) (*Queue, error)

    // logqueue
    func NewLogQueue(name string, storage engine.Storage, scope *metrics.Scope) *LogQueue

    // taskqueue
    func NewTaskQueue(name string, storage engine.Storage, defaults Defaults, scope *metrics.Scope) (*TaskQueue, error)

    // server
    func New(n *node.Node, opts ...ServerOption) *Server  // scope obtained from n
    ```

    `nil` scopes must be safe (see Phase 1). This is the key to incremental rollout — a test can pass `nil` while a phase is in progress.

2. Hot-path instrumentation, minimal set for this phase:

    | Location | Instrument |
    |---|---|
    | `Queue.Append`/`AppendBatch` | `appends` counter, `bytes_in` counter, `append_duration` histogram |
    | `Queue.Read`/`ReadBatch`/`Fetch` | `reads` counter, `bytes_out` counter, `read_duration` histogram |
    | `wal.flush` | `wal_flush_duration` histogram, `wal_flushes` counter, `wal_pending` gauge |
    | `Queue.roll` | `segment_rolls` counter |
    | `LogQueue.CommitOffset` | `group_commits` counter |
    | `TaskQueue.Enqueue` | `enqueues` counter, `enqueue_duration` histogram |
    | `TaskQueue.takeDueLocked` | `dequeues` counter, `dequeue_wait` histogram |
    | `TaskQueue.Ack`/`Nack`/`Extend` | counters |
    | `TaskQueue.AckDeadLetter` | `dlq_acks` counter |
    | `Server.dispatch` | `op_total{op=...}` counter, `op_duration{op=...}` histogram |
    | `Server.serveConn` | `connections` gauge |

    Histograms for `op_duration` should be created per opcode at server construction, so `OpLookup` is not needed at tick time. Pre-create the `*Histogram` in a small map.

3. `engine.Manager.Collect`. Walk every queue, fill gauges that require real work:

    ```go
    func (m *Manager) Collect(ctx context.Context, dst *metrics.Snapshot) {
        m.mu.RLock()
        defer m.mu.RUnlock()
        totalBytes := int64(0)
        for _, q := range m.queues {
            q.mu.RLock()
            qv := metrics.QueueView{
                Name: q.name,
                Kind: "shared",
                Gauges: map[string]int64{
                    "messages":      int64(q.publishedOffset),
                    "segments":      int64(len(q.segments)),
                    "last_offset":   int64(q.nextOffset),
                    "min_offset":    int64(q.minOffset),
                    "size_bytes":    queueOnDiskSize(q),
                },
            }
            q.mu.RUnlock()
            dst.Queues = append(dst.Queues, qv)
            totalBytes += qv.Gauges["size_bytes"]
        }
        dst.Totals.Queues = len(m.queues)
        dst.Totals.Bytes = totalBytes
        dst.Totals.Messages = ...
    }
    ```

    Delete `EngineStats` and its `EngineStats()` method. Delete `QueueStats` from `engine`. Any leftover uses in `node` get replaced in step 4.

4. `node.go`. Update `openNode` to build a `metrics.Registry` and pass scopes down. Register `mgr` as a source:

    ```go
    collector := metrics.New(reg, metrics.Config{...},
        &RuntimeSource{start: time.Now()},
        mgr, // *engine.Manager implements metrics.Source
    )
    ```

    `n.LogQueue(name)` and `n.TaskQueue(name)` create scopes:

    ```go
    lq := logqueue.NewLogQueue(name, storage, n.metrics.Registry().Scope(name))
    ```

5. Add `TotalsView` aggregation into the collector's tick pipeline. `Collector` should ask its `Registry` for every scope and fold counters into `QueueView`s. **Scopes declare their counter names** via a small registration at construction:

    ```go
    // engine.NewQueue
    scope.DeclareCounters("appends", "reads", "bytes_in", "bytes_out")
    scope.DeclareHistograms("append_duration", appendBounds)
    ```

    Or simpler: the collector reads them eagerly by name, and the set of names is a package constant. I prefer explicit declaration — it keeps the collector dumb.

    Add to `Scope`:

    ```go
    func (s *Scope) DeclareCounter(names ...string)
    func (s *Scope) DeclareGauge(names ...string)
    func (s *Scope) DeclareHistogram(name string, bounds []float64)
    ```

    Declarations are idempotent. The collector's tick walks declared names, not a global registry.

**Tests.**

- `TestEngineAppendCounters` — append 100, assert `Counter("appends").Load() == 100`.
- `TestEngineReadHistogram` — read 100, assert histogram total is 100.
- `TestWALFlushHistogram` — append in buffered mode with `BatchSize: 1`, assert `wal_flushes > 0`.
- `TestTaskQueueCounters` — enqueue/dequeue/ack/nack/extend round trip, check each counter.
- `TestServerOpCounters` — start a server, issue 5 `OpInfo` and 5 `OpListQueues`, check per-op counters.
- `TestManagerCollectShape` — create 2 queues, call `Manager.Collect` into a fresh snapshot, assert 2 `QueueView`s with non-zero gauges.
- `TestScopeDeclarationIdempotent` — declare the same counter twice, verify no panic and one registration.
- `TestEngineStatsDeleted` — compile-time assertion: grep the tree for `EngineStats`. This becomes an `audit` test in Phase 8.
- Benchmarks: `BenchmarkQueueAppendWithMetrics` vs `WithoutMetrics` (nil scope) to prove near-zero overhead.

**Exit criteria.** `go test -race ./...` green. `grep -r EngineStats` returns nothing.

---

## Phase 5 — client surface swap: `Metrics` replaces `Info`

**Goal.** Delete the parallel stats path. `Client.Metrics` is the one method. `OpMetrics` replaces `OpInfo`.

**Files touched.**

- `internal/transport/protocol/opcodes.go` — replace `OpInfo = 0x07` with `OpMetrics = 0x07` (reuse the byte, since the table is frozen and `Info` was never shipped). Add a comment: "0x07 formerly reserved; now carries Metrics".
- `internal/transport/protocol/wire_test.go` — rename `info` cases to `metrics`.
- `internal/server/handlers_admin.go` — `handleInfo` → `handleMetrics`; response is a u16 version + bytes of JSON `metrics.Snapshot`.
- `internal/server/dispatch.go` — rename dispatch case.
- `internal/server/handlers_test.go` — update.
- `internal/client/remote.go` — replace `Info` method with `Metrics`.
- `internal/client/embedded.go` — replace `Info` with `Metrics`.
- `internal/client/types.go` — delete `Info` and `QueueStats` types.
- `internal/client/errors.go` — no change; the error block already covers spec-too-new.
- `internal/client/remote_logqueue.go` — `MinOffset` currently uses `q.client.Info`. Change to `q.client.Metrics` and read `Queues[name].Gauges["min_offset"]`. If the queue isn't present, return `engine.ErrQueueNotFound`.
- `mikyu/mikyu.go` — delete `Info` type alias, replace interface method, add `type Metrics = metrics.Snapshot`, extend the re-exports. Also update `Err` block if any `Info`-related errors existed (none do).
- `mikyu/options.go` — no change.
- `mikyu/surface_test.go` — remove `Info` from the sentinel test if it was there; remove `Info` from method-name checks.
- `mikyu/mikyu_test.go` — replace `client.Info` calls with `client.Metrics`.

**Steps.**

1. Add the snapshot wire envelope. On the response side:

    ```go
    // server/handlers_admin.go
    func (s *Server) handleMetrics(ctx context.Context) (byte, []byte) {
        snap, err := s.node.Metrics(ctx)
        if err != nil {
            return taskErr(err)
        }
        data, err := json.Marshal(snap)
        if err != nil {
            return opErr(protocol.ErrCodeUnknown, err.Error())
        }
        var buf protocol.Buffer
        buf.WriteU16(spec.CurrentSpecVersion)
        buf.WriteBytes(data)
        return protocol.OpMetrics, buf.Bytes()
    }
    ```

    Use the same `u16 specVersion + bytes(payload)` shape as `OpDescribeQueue`. No new framing.

2. Client side:

    ```go
    func (c *remoteClient) Metrics(ctx context.Context) (metrics.Snapshot, error) {
        resp, err := c.exchange(ctx, protocol.OpMetrics, nil)
        if err != nil { return metrics.Snapshot{}, err }
        r := protocol.NewReader(resp)
        version, err := r.ReadU16()
        if err != nil { return metrics.Snapshot{}, fmt.Errorf("%w: %v", ErrProtocol, err) }
        if err := spec.CheckSpecVersion(version); err != nil { return metrics.Snapshot{}, err }
        data, err := r.ReadBytes()
        if err != nil { return metrics.Snapshot{}, fmt.Errorf("%w: %v", ErrProtocol, err) }
        if len(data) > spec.MaxSpecPayload {
            return metrics.Snapshot{}, fmt.Errorf("%w: metrics payload too large", ErrProtocol)
        }
        var snap metrics.Snapshot
        if err := json.Unmarshal(data, &snap); err != nil {
            return metrics.Snapshot{}, fmt.Errorf("%w: %v", ErrProtocol, err)
        }
        return snap, nil
    }
    ```

3. Embedded client:

    ```go
    func (c *embeddedClient) Metrics(ctx context.Context) (metrics.Snapshot, error) {
        return c.node.Metrics(ctx)
    }
    ```

4. Facade:

    ```go
    type Metrics = metrics.Snapshot

    type Client interface {
        io.Closer
        CreateQueue(...) error
        UpdateQueue(...) error
        DescribeQueue(...) (QueueDescription, error)
        DeleteQueue(...) error
        ListQueues(...) ([]string, error)
        Truncate(...) (int64, error)
        Metrics(ctx context.Context) (Metrics, error)   // replaces Info
        LogQueue(name string) (LogQueue, error)
        TaskQueue(name string) (TaskQueue, error)
    }
    ```

    `QueueDescription` keeps its `Spec`/`Effective`/`Defaults` fields; its `QueueStats` field is **deleted**. `describe` no longer carries stats.

5. Delete everything tied to `Info`:
   - `node.Info`, `node.QueueStats`, `node.Info()`.
   - `client.Info` type, `embeddedClient.Info`, `remoteClient.Info`.
   - `mikyu.Info`, the interface method.

6. Update `mikyu/surface_test.go`:
   - Remove `Info` from `TestNamingLaw`'s banned-parts list? No — `Info` should be **added** to banned-parts. The naming law now says: no `Info`, no `Stats`.
   - `TestSentinelUniqueness` unaffected.

**Tests.**

- `TestRemoteMetricsRoundTrip` — create queue, append, call `Metrics`, assert non-zero counts, correct queue view.
- `TestEmbeddedMetricsMatchesRemote` — same op against embedded and remote, compare `Snapshot` shape (timestamps differ; strip them first).
- `TestRemoteMinOffsetUsesMetrics` — no `Info` call anywhere; `LogQueue("q").MinOffset(ctx)` still works.
- `TestDescribeNoStats` — `describe` output JSON no longer contains a `stats` key.
- `TestSurfaceNoInfo` in `surface_test.go` — the interface must not have an `Info` method.

**Exit criteria.** `go test -race ./...` green. `grep -rn "\.Info(" internal/ mikyu/` returns nothing outside of comments. `OpInfo` fully removed from `protocol`.

---

## Phase 6 — CLI and REPL swap

**Goal.** `info` verb becomes `metrics`. `describe` becomes spec-only. Add a `metrics` verb that renders the snapshot as text, and `--json` for machine output.

**Files touched.**

- `internal/ui/command/verbs.go` — replace the `info` verb with `metrics`.
- `internal/ui/command/command.go` — replace `Info(...)` helper with `Metrics(...)`.
- `internal/ui/cli/cli.go` — replace `handleInfo` with `handleMetrics`; add `--json` and `--watch` flags.
- `internal/ui/cli/task.go` — no change.
- `internal/ui/cli/cli_test.go` — replace info test with metrics test.
- `internal/ui/repl/repl.go` — replace `handleInfo` with `handleMetrics`; add `metrics` to `replHandlers`.
- `internal/ui/repl/repl_test.go` — update `TestREPLMatchesCLIVerbs`.
- `internal/ui/repl/style.go` — add styles for metrics output (bars, colors).
- `internal/ui/repl/task.go` — no change.
- `mikyu/surface_test.go` — no change; still requires the CLI verbs to match the table.

**Steps.**

1. Replace the `info` verb:

    ```go
    {Name: "metrics", Args: "[queue] [--json] [--watch]", Summary: "Show live queue and server metrics", Kind: KindAdmin},
    ```

2. `command.Metrics`:

    ```go
    func Metrics(ctx context.Context, client mikyu.Client) (mikyu.Metrics, error) {
        return client.Metrics(ctx)
    }
    ```

3. `cli.handleMetrics`:

    - No `--queue` filter in v1: the CLI renders the whole snapshot, with a `--queue NAME` filter for convenience.

    ```go
    func (in *invocation) handleMetrics(args []string) error {
        var asJSON, watch bool
        var queue string
        fs := newFlagSet("metrics")
        fs.BoolVar(&asJSON, "json", false, "emit the snapshot as JSON")
        fs.BoolVar(&watch, "watch", false, "stream snapshots until interrupted")
        fs.StringVar(&queue, "queue", "", "restrict to one queue")
        rest, err := command.ParseAnywhere(fs, args)
        if err != nil { return in.app.usage("metrics: %v", err) }
        if len(rest) > 0 { return in.app.usage("usage: mikyu metrics [--json] [--watch] [--queue name]") }

        if watch {
            // Same subscription path the daemon HTTP SSE uses.
            return in.streamMetrics(queue, asJSON)
        }
        snap, err := in.client.Metrics(in.ctx)
        if err != nil { return err }
        return in.renderMetrics(snap, queue, asJSON)
    }
    ```

4. `renderMetrics` — text renderer walks `Snapshot` and prints human-readable output. Format is not contractual; `--json` is.

5. `--watch` — remote clients poll `Metrics()` every second (or the collector's interval when known). Local clients subscribe to `node.Collector().Subscribe()`. Same renderer.

6. REPL `handleMetrics` mirrors the CLI, plus a persistent `metrics watch` mode that reads until Ctrl-C.

7. Delete `handleInfo` and `command.Info`.

8. `printVerbUsage` — add a `metrics` case that lists the flags.

**Tests.**

- `TestCLIMetricsJSON` — create a queue, append, call `metrics --json`, decode into `mikyu.Metrics`, assert queue present and counter > 0.
- `TestCLIMetricsQueueFilter` — `metrics --queue q` returns only `q`.
- `TestCLIMetricsWatch` — `metrics --watch` with a 200 ms test context; assert at least 2 snapshots and a clean exit on ctx cancel.
- `TestCLINoInfoVerb` — `mikyu info` returns usage error.
- `TestREPLMetricsVerb` — same, in REPL dispatch.
- `TestCLIDescribeNoStats` — `describe --json` output has no `stats` key.
- `TestCLIHelpListsMetrics` — `mikyu --help` lists `metrics` and not `info`.

**Exit criteria.** `go test -race ./internal/ui/...` green. `grep -rn "\"info\"" internal/ui/` returns nothing.

---

## Phase 7 — Daemon HTTP surface

**Goal.** Daemon optionally serves `/metrics` (Prometheus), `/api/metrics` (JSON), `/api/history?limit=N`, and `/api/events` (SSE). Off by default; enabled via `--metrics-listen`.

**New files.**

- `internal/daemon/metrics_http.go`.
- `internal/daemon/metrics_http_test.go`.

**Files touched.**

- `internal/daemon/daemon.go` — start the HTTP listener when `cfg.Server.MetricsListen != ""`.
- `internal/daemon/daemon_config_test.go` — extend with metrics-listen coverage.

**Steps.**

1. `metrics_http.go`:

    ```go
    type metricsServer struct {
        node  *node.Node
        srv   *http.Server
        ln    net.Listener
    }

    func startMetricsHTTP(n *node.Node, addr string) (*metricsServer, error) {
        mux := http.NewServeMux()
        ms := &metricsServer{node: n}
        mux.HandleFunc("GET /metrics", ms.handlePrometheus)
        mux.HandleFunc("GET /api/metrics", ms.handleJSON)
        mux.HandleFunc("GET /api/history", ms.handleHistory)
        mux.HandleFunc("GET /api/events", ms.handleSSE)
        ln, err := net.Listen("tcp", addr)
        ...
        go ms.srv.Serve(ln)
        return ms, nil
    }

    func (ms *metricsServer) Stop(ctx context.Context) error {
        return ms.srv.Shutdown(ctx)
    }
    ```

    Use Go 1.22+ `ServeMux` method patterns (`GET /path`) — the codebase already uses them implicitly via `net/http`.

2. `/api/events` (SSE):

    ```go
    func (ms *metricsServer) handleSSE(w http.ResponseWriter, r *http.Request) {
        w.Header().Set("Content-Type", "text/event-stream")
        w.Header().Set("Cache-Control", "no-cache")
        w.Header().Set("Connection", "keep-alive")
        flusher, _ := w.(http.Flusher)

        ch, cancel := ms.node.Collector().Subscribe()
        defer cancel()

        for {
            select {
            case <-r.Context().Done():
                return
            case snap, ok := <-ch:
                if !ok { return }
                data, err := json.Marshal(snap)
                if err != nil { return }
                fmt.Fprintf(w, "event: snapshot\ndata: %s\n\n", data)
                if flusher != nil { flusher.Flush() }
            }
        }
    }
    ```

    Note the `Subscribe` contract from Phase 1: **slow subscribers drop frames**, the SSE handler must never block the collector. If `ch` is full, the collector drops; the SSE handler sees the next available snapshot.

3. `/metrics` (Prometheus):

    ```go
    func (ms *metricsServer) handlePrometheus(w http.ResponseWriter, r *http.Request) {
        snap, err := ms.node.Metrics(r.Context())
        if err != nil { http.Error(w, err.Error(), 500); return }
        w.Header().Set("Content-Type", "text/plain; version=0.0.4; charset=utf-8")
        opts := metrics.EncodeOptions{
            PerQueue:   ms.node.MetricsPerQueuePrometheus(),
            TopNQueues: 50,
            Histograms: ms.node.MetricsHistograms(),
        }
        if err := metrics.Encode(w, snap, opts); err != nil {
            // Header is already written; just log.
            slog.Error("prometheus encode", "err", err)
        }
    }
    ```

4. Daemon `Run`: after `server.New`, before `ListenAndServe`:

    ```go
    var httpSrv *metricsServer
    if cfg.Server.MetricsListen != "" {
        s, err := startMetricsHTTP(n, cfg.Server.MetricsListen)
        if err != nil { return fmt.Errorf("metrics http: %w", err) }
        httpSrv = s
    }
    defer func() {
        if httpSrv != nil {
            ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
            defer cancel()
            _ = httpSrv.Stop(ctx)
        }
    }()
    ```

    Ordering: shut down HTTP **after** the wire server stops, so `Client.Metrics` calls that race with daemon shutdown still work.

5. Security default: `metrics_listen` must bind to `127.0.0.1` unless the user explicitly writes `0.0.0.0` or a specific interface. This is a **documentation** concern, not enforced — but log a warning at startup if the listener is not loopback.

**Tests.**

- `TestMetricsHTTPPrometheus` — spin up a daemon, hit `/metrics`, assert content-type and at least one `mikyu_` metric line.
- `TestMetricsHTTPJSON` — hit `/api/metrics`, decode into `metrics.Snapshot`, assert shape.
- `TestMetricsHTTPHistory` — wait a few ticks, hit `/api/history?limit=3`, assert 3 or fewer snapshots.
- `TestMetricsHTTPSSE` — connect, read one event, cancel; assert event arrives within one tick.
- `TestMetricsHTTPSSESlowClientDoesNotBlock` — connect, never read, wait 3× interval, hit `/api/metrics` — must still respond within 100 ms.
- `TestMetricsHTTPDisabledByDefault` — no `metrics_listen` → no HTTP listener started; port 9090 not bound.
- `TestMetricsHTTPUseOfSynctest` — for `/api/events` timing, use `synctest.Test`.

**Exit criteria.** `go test -race ./internal/daemon/...` green. Manual smoke: `mikyu daemon --metrics-listen 127.0.0.1:9090` then `curl localhost:9090/metrics`.

---

## Phase 8 — Audit, arch-lint, docs

**Goal.** Lock in the removal of the legacy surfaces and add `metrics` to the dependency graph.

**Files touched.**

- `internal/audit/legacy_test.go` — extend `bannedIdentifiers`.
- `internal/audit/comment_test.go` — no change (already covers every package clause and export).
- `.go-arch-lint.yml` — add `metrics` as a bottom, extend every list.
- `internal/audit/doc.go` — no change.
- `README.md` / `ARCHITECTURE.md` — new section on metrics.

**Steps.**

1. Extend `bannedIdentifiers` in `legacy_test.go`:

    ```go
    var bannedIdentifiers = []string{
        // ... existing ...
        "Info", "Stats", "EngineStats", "QueueStats",
        "InfoResult", "MetricsResult", "OpInfo",
    }
    ```

    Watch out: `Info` will false-positive on legitimate uses of `os.FileInfo` or method names on third-party types. So add a match refinement: only flag `Info` when it's a top-level `Ident` whose parent is a `SelectorExpr` with `X` being `mikyu`/`client`/`node` — or simpler: continue matching identifiers exactly, but review the resulting grep hits. In practice, `os.FileInfo` is `FileInfo`, not `Info`. An identifier `Info` on its own is a bare type name; `os.Stat(...).Info` doesn't exist. Run it once, verify.

    If false positives arise, restrict to identifiers in files under our module and exclude `_test.go` (already excluded).

2. `.go-arch-lint.yml`:

    ```yaml
    components:
      # ... existing ...
      metrics: { in: internal/metrics }

    deps:
      metrics:
        anyVendorDeps: true

      engine:
        mayDependOn: [metrics]
      logqueue:
        mayDependOn: [engine, group, metrics]
      taskqueue:
        mayDependOn: [engine, metrics]
      retention:
        mayDependOn: [engine, metrics]
      node:
        mayDependOn: [spec, engine, logqueue, taskqueue, retention, metrics]
      server:
        mayDependOn: [spec, engine, node, logqueue, taskqueue, ipc, protocol, metrics]
      client:
        mayDependOn: [spec, engine, node, group, logqueue, taskqueue, ipc, protocol, metrics]
      daemon:
        mayDependOn: [spec, config, engine, node, server, protocol, metrics]
      mikyu:
        mayDependOn: [spec, client, engine, node, logqueue, taskqueue, metrics]
      command:
        mayDependOn: [mikyu, client, daemon, spec, metrics]
      cli:
        mayDependOn: [mikyu, client, daemon, command, spec, metrics]
      repl:
        mayDependOn: [mikyu, client, daemon, command, spec, metrics]
    ```

    Run `go-arch-lint check` and confirm green.

3. `ARCHITECTURE.md` — new section:

    > **Metrics.** `internal/metrics` is the new bottom layer (no internal deps). `node.Node` owns one `metrics.Collector` per data directory. Hot paths update atomic instruments via `*metrics.Scope`; the collector runs a ticker, runs pull-based `Source` implementations, and appends immutable `metrics.Snapshot` values to a ring buffer. `Client.Metrics` reads the current snapshot; the daemon HTTP surface encodes the same snapshot for Prometheus and SSE. The TUI, when it lands, is another snapshot consumer — either in-process via `Collector.Subscribe()` or remote via polling `Client.Metrics`.

4. `README.md` — user-facing metrics section: how to enable, what endpoints exist, how to scrape.

5. Update `mikyu/surface_test.go`:
   - `TestNamingLaw` — add `"Info"`, `"Stats"` to `bannedParts`.
   - `TestNoIDSiblings` — no change.
   - Add `TestMetricsIsOnlyStatsSurface` — reflection over `mikyu.Client` asserts exactly one method with "Metric" in the name and zero with "Info" or "Stats".

6. Update `mikyu/internal/spec/parity_test.go` if any new session param is missing from `assertEnvSession` / `assertYAMLSession` coverage. The existing loop over `spec.Params(spec.LayerSession)` should pick them up automatically; assert the daemon flags also carry them via `daemonFlagNames`.

**Tests.**

- `TestNoLegacyNames` — will fail on the first run if any `Info`/`Stats` reference survives. Fix them.
- `TestCommentStyle` — no change; new files must comply with single-line docs on exports.
- `go-arch-lint check` — CI gate.

**Exit criteria.** `go test -race ./...` and `go-arch-lint check` green. `grep -rn "Info\b" internal/ mikyu/ cmd/ --include=*.go` returns only `os.FileInfo`, `runtime.MemStats`-adjacent types (allowed), and no Mikyu-owned `Info`.

---

## Cross-phase conventions

1. **Nil scope safety.** Every instrument accessor is safe on `nil`. This lets phases 3–4 land incrementally with a `nil` scope in tests.
2. **Every new test uses `T.Context()`**, not `context.Background()`.
3. **Every new benchmark uses `b.Loop()`**, never `for i := 0; i < b.N; i++`.
4. **Every new package compiles with `-race`**; the existing `goleak` tests already assert no leaks per package.
5. **`encoding/json/v2` is used implicitly** — Go 1.27 backs `encoding/json` with v2 by default. New code uses `encoding/json`, not the v2 import, so future v1/v2 divergence is a toolchain decision, not a code one.
6. **Snapshot encoding is on the wire** (over `OpMetrics`) and over HTTP. Both use the same `json.Marshal` on the same type. No bespoke encoders.
7. **No new opcodes beyond `OpMetrics`.** The TUI, when it lands, polls this. If sub-second remote TUI refresh is ever needed, add `OpMetricsStream` — but only after a real need is demonstrated.
8. **Prometheus is daemon-only.** Embedded clients expose metrics through the SDK and (optionally) through a subscription channel; they do not open HTTP listeners.

## Estimated PR sizing

| Phase | LOC delta (approx.) | Risk |
|---|---|---|
| 1 | +900 / -0 | Medium — new package, but self-contained |
| 2 | +120 / -10 | Low |
| 3 | +180 / -0 | Low |
| 4 | +400 / -250 | High — touches every hot path; needs benchmarks |
| 5 | +150 / -300 | Medium — wire and SDK break, all callers must move |
| 6 | +250 / -150 | Low |
| 7 | +300 / -20 | Medium — new HTTP surface, first SSE |
| 8 | +60 / -10 | Low |

Net: the codebase gets **smaller** on public surface (Info, QueueStats, EngineStats, EngineStats() all gone) and only adds one new internal package. That is the point.