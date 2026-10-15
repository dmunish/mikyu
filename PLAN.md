# Migration Plan: Replace Hand-Rolled Prometheus with `prometheus/client_golang`

This plan migrates `internal/metrics` from custom instruments and a hand-written Prometheus encoder to the official `prometheus/client_golang` library. Each step is gated by `go test ./...`, `go vet ./...`, and `go fix ./...`. Commit after each gated step when the tree is clean.

The plan assumes Go 1.24+ (the repo uses `testing/synctest`, `iter.Seq2`, and `weak` patterns that require it). If the module targets an older Go, pin `client_golang` to v1.16.0 (the last release supporting Go < 1.22) and adjust the bucket helpers accordingly.

---

## Step 0 — Preparation and dependency audit

**Goal:** Establish a clean baseline and add the dependency without breaking the build.

### 0.1 Record the baseline

Run the full suite and capture the current state:

```bash
go test ./... 2>&1 | tee /tmp/before-tests.txt
go vet ./... 2>&1 | tee /tmp/before-vet.txt
go fix -diff ./... 2>&1 | tee /tmp/before-fix.txt
```

Confirm no pre-existing failures. If there are, fix them or note them as out-of-scope before continuing.

### 0.2 Add the dependency

```bash
go get github.com/prometheus/client_golang@latest
go get github.com/prometheus/client_golang/prometheus/collectors@latest
go get github.com/prometheus/client_golang/prometheus/promhttp@latest
go get github.com/prometheus/client_golang/prometheus/testutil@latest
go mod tidy
```

### 0.3 Identify all call sites

Find every reference to the custom instruments and encoder:

```bash
grep -rn "metrics\.Counter\|metrics\.Gauge\|metrics\.Histogram\|metrics\.Registry\|metrics\.Scope\|metrics\.Encode\|metrics\.NewHistogram\|metrics\.NewRegistry" --include='*.go' | grep -v _test.go
```

Expected packages touched:
- `internal/engine` (metrics.go, queue.go, wal.go, manager.go, offsets.go, cache.go, compaction.go)
- `internal/queue/taskqueue` (metrics.go, taskqueue.go)
- `internal/queue/logqueue` (logqueue.go)
- `internal/queue/retention` (coordinator.go)
- `internal/node` (metrics.go, node.go, runtime_source.go, runtime_source_*.go, catalog.go)
- `internal/server` (metrics.go, server.go)
- `internal/daemon` (metrics_http.go, daemon.go)
- `internal/client` (embedded.go, remote.go, types.go)
- `internal/metrics` itself
- `internal/bench` (observe.go, bench.go)

This is a large surface. Do not attempt to migrate everything in one commit. The steps below stage the migration so the tree is always green.

### 0.4 Gate 0

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[chore] go.mod : add prometheus/client_golang dependency`

---

## Step 1 — Replace custom instruments with Prometheus types

**Goal:** Delete `internal/metrics/instruments.go`'s custom `Counter`, `Gauge`, and `Histogram` types. Replace every usage with `prometheus.Counter`, `prometheus.Gauge`, and `prometheus.Histogram` (or their `*Vec` variants).

### 1.1 Understand the mapping

| Custom type | Prometheus replacement | Notes |
|---|---|---|
| `metrics.Counter` | `prometheus.Counter` | `Inc()` and `Add(n)` are identical. `Load()` becomes `testutil.ToFloat64(c)`. |
| `metrics.Gauge` | `prometheus.Gauge` | `Set(n)` and `Add(n)` are identical. `Load()` becomes `testutil.ToFloat64(g)`. |
| `metrics.Histogram` | `prometheus.Histogram` | `Observe(v)` is identical. The custom `Percentile(p)` and `Max()` have no direct equivalent — see 1.3. |
| `metrics.HistogramSnapshot` | `prometheus.Metric` / `dto.Histogram` | Only used internally by the custom encoder. Deleted with the encoder. |
| `metrics.RingBuffer` | Keep as-is | Generic, unrelated to Prometheus. |

### 1.2 Replace the instruments

Open `internal/metrics/instruments.go`. Delete `Counter`, `Gauge`, `Histogram`, `HistogramSnapshot`, and `NewHistogram`. Keep `RingBuffer` and its methods.

In every consumer, replace the type. For example, in `internal/engine/metrics.go`:

```go
// Before
type queueMetrics struct {
    appends     *metrics.Counter
    reads       *metrics.Counter
    appendDur   *metrics.Histogram
}

func newQueueMetrics(scope *metrics.Scope) *queueMetrics {
    return &queueMetrics{
        appends:   scope.Counter("appends"),
        reads:     scope.Counter("reads"),
        appendDur: scope.Histogram("append_duration", ioDurationBounds),
    }
}
```

After (using a custom registry, see Step 2):

```go
type queueMetrics struct {
    appends   prometheus.Counter
    reads     prometheus.Counter
    appendDur prometheus.Histogram
}

func newQueueMetrics(reg prometheus.Registerer, queue string) *queueMetrics {
    labels := prometheus.Labels{"queue": queue}
    appends := prometheus.NewCounter(prometheus.CounterOpts{
        Name:        "mikyu_queue_appends_total",
        Help:        "Log appends.",
        ConstLabels: labels,
    })
    reads := prometheus.NewCounter(prometheus.CounterOpts{
        Name:        "mikyu_queue_reads_total",
        Help:        "Log reads.",
        ConstLabels: labels,
    })
    appendDur := prometheus.NewHistogram(prometheus.HistogramOpts{
        Name:        "mikyu_queue_append_duration_seconds",
        Help:        "Append latency in seconds.",
        ConstLabels: labels,
        Buckets:     ioDurationBuckets,
    })
    reg.MustRegister(appends, reads, appendDur)
    return &queueMetrics{
        appends:   appends,
        reads:     reads,
        appendDur: appendDur,
    }
}
```

Do this for every instrument in `internal/engine`, `internal/queue/taskqueue`, `internal/queue/logqueue`, `internal/queue/retention`, and `internal/server`.

### 1.3 Handle the custom histogram percentile logic

The custom `Histogram.Percentile(p)` and `Histogram.Max()` are used by `internal/metrics/prometheus.go` and by `internal/metrics/collector.go` to populate `Snapshot`. These methods interpolate within buckets and are **not** part of the Prometheus client API.

**Decision:** Remove client-side percentile computation entirely. Prometheus computes percentiles from histogram buckets at query time via `histogram_quantile()`. The `Snapshot` struct should carry the raw bucket counts, not pre-computed percentiles.

In `internal/metrics/snapshot.go`, change `HistogramView` from:

```go
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
```

to:

```go
type HistogramView struct {
    Bounds []float64 `json:"bounds"`
    Counts []uint64  `json:"counts"`
    Sum    float64   `json:"sum"`
    Total  uint64    `json:"total"`
}
```

Then update every consumer of `P50`, `P90`, etc.:
- `internal/ui/metricsview/queuecard.go` — `latencyLine` reads `pair.view.P50`, `.P90`, `.P99`, `.Max`. Either compute percentiles from `Bounds` and `Counts` in the view layer, or remove the latency line and rely on the Prometheus endpoint for latency queries. Given this is a terminal UI, keep the computation but move it to `internal/ui/metricsview` as a helper:

```go
// internal/ui/metricsview/histogram.go
func percentile(h metrics.HistogramView, p float64) float64 {
    if h.Total == 0 || len(h.Counts) == 0 {
        return 0
    }
    rank := max(uint64(p*float64(h.Total)), 1)
    var seen uint64
    for i, n := range h.Counts {
        seen += n
        if seen >= rank {
            lo := 0.0
            if i > 0 {
                lo = h.Bounds[i-1]
            }
            hi := lo
            if i < len(h.Bounds) {
                hi = h.Bounds[i]
            }
            if n == 0 || hi <= lo {
                return hi
            }
            frac := float64(rank-(seen-n)) / float64(n)
            return lo + frac*(hi-lo)
        }
    }
    return h.Bounds[len(h.Bounds)-1]
}
```

- `internal/bench/result/result.go` — `HistogramView` has `P50`, `P90`, etc. The bench package computes these itself in `internal/bench/measure.go` via `Histogram.Percentile`. Keep that computation there — it is bench-specific and does not come from the metrics registry. Do not touch it in this step.

### 1.4 Replace `scope.read()` and `HistogramSnapshot`

The `Scope.read()` method returns `map[string]HistogramSnapshot`. Delete it. The collector will read from Prometheus instruments directly (Step 3).

### 1.5 Gate 1

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[feat] internal/metrics : replace custom counter/gauge/histogram with prometheus/client_golang types`

---

## Step 2 — Replace the custom registry and scopes

**Goal:** Delete `internal/metrics/registry.go`'s `Registry` and `Scope`. Replace with `prometheus.Registry` and `prometheus.CounterVec`/`GaugeVec`/`HistogramVec` labeled by queue name (or scope name).

### 2.1 Understand the mapping

The custom `Registry` and `Scope` model is:

- One `Registry` per node.
- One `Scope` per queue (named by queue name).
- Instruments created lazily on first `Counter("appends")` call.

The Prometheus equivalent is:

- One `prometheus.Registry` per node.
- One `*Vec` per instrument family, labeled by `"queue"` (and `"scope"` for internal scopes like `"server"`).
- Instruments created eagerly at construction time, or lazily via `GetMetricWithLabelValues`.

**Decision:** Use `prometheus.NewCounterVec`, `NewGaugeVec`, and `NewHistogramVec` with a `"queue"` label. Create them once per node and store them in a struct. This avoids the lazy-creation complexity and gives Prometheus a stable set of descriptors at registration time.

### 2.2 Build a node-level metrics struct

Create `internal/metrics/node_metrics.go`:

```go
package metrics

import (
    "github.com/prometheus/client_golang/prometheus"
)

type NodeMetrics struct {
    reg *prometheus.Registry

    Appends        *prometheus.CounterVec
    Reads          *prometheus.CounterVec
    BytesIn        *prometheus.CounterVec
    BytesOut       *prometheus.CounterVec
    SegmentRolls   *prometheus.CounterVec
    GroupCommits   *prometheus.CounterVec
    CacheHits      *prometheus.CounterVec
    CacheMisses    *prometheus.CounterVec
    FetchParkEvents *prometheus.CounterVec

    AppendDuration *prometheus.HistogramVec
    ReadDuration   *prometheus.HistogramVec
    WALFlushDuration *prometheus.HistogramVec

    KeyIndexSize    *prometheus.GaugeVec
    InFlightApp     *prometheus.GaugeVec
    InFlightRead    *prometheus.GaugeVec
    ParkedFetch     *prometheus.GaugeVec
    WALPending      *prometheus.GaugeVec
    WALBlockedProducers *prometheus.GaugeVec
}

func NewNodeMetrics() *NodeMetrics {
    reg := prometheus.NewRegistry()
    m := &NodeMetrics{reg: reg}

    m.Appends = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "mikyu_queue_appends_total",
        Help: "Log appends.",
    }, []string{"queue"})
    m.Reads = prometheus.NewCounterVec(prometheus.CounterOpts{
        Name: "mikyu_queue_reads_total",
        Help: "Log reads.",
    }, []string{"queue"})
    // ... repeat for every counter, gauge, and histogram.

    reg.MustRegister(
        m.Appends, m.Reads, m.BytesIn, m.BytesOut,
        m.SegmentRolls, m.GroupCommits, m.CacheHits, m.CacheMisses,
        m.FetchParkEvents,
        m.AppendDuration, m.ReadDuration, m.WALFlushDuration,
        m.KeyIndexSize, m.InFlightApp, m.InFlightRead, m.ParkedFetch,
        m.WALPending, m.WALBlockedProducers,
    )
    return m
}

func (m *NodeMetrics) Registry() *prometheus.Registry { return m.reg }
```

### 2.3 Register runtime and process collectors

Add the standard Go and process collectors. Use the `collectors` subpackage, not the deprecated `prometheus.NewGoCollector` / `prometheus.NewProcessCollector`:

```go
import "github.com/prometheus/client_golang/prometheus/collectors"

func (m *NodeMetrics) RegisterRuntimeCollectors() {
    m.reg.MustRegister(
        collectors.NewGoCollector(),
        collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
        collectors.NewBuildInfoCollector(),
    )
}
```

The `sync.Once` guard from the reference commit is a good pattern; use it to make repeated calls safe:

```go
var runtimeOnce sync.Once

func (m *NodeMetrics) RegisterRuntimeCollectors() {
    runtimeOnce.Do(func() {
        m.reg.MustRegister(
            collectors.NewGoCollector(),
            collectors.NewProcessCollector(collectors.ProcessCollectorOpts{}),
            collectors.NewBuildInfoCollector(),
        )
    })
}
```

### 2.4 Wire `NodeMetrics` into `node.Node`

In `internal/node/node.go`, replace `metrics *metrics.Collector` with `nodeMetrics *metrics.NodeMetrics`. Create it in `openNode`:

```go
n := &Node{
    // ...
    nodeMetrics: metrics.NewNodeMetrics(),
}
n.nodeMetrics.RegisterRuntimeCollectors()
```

The old `newRegistry(mc MetricsConfig)` in `internal/node/metrics.go` is deleted.

### 2.5 Pass `*NodeMetrics` to consumers

Change every constructor that took `...*metrics.Scope` to take `*metrics.NodeMetrics` instead. For example:

```go
// Before
func New(name string, storage engine.Storage, defaults Defaults, archiveLog ArchiveLogFor, scopes ...*metrics.Scope) (Queue, error)

// After
func New(name string, storage engine.Storage, defaults Defaults, archiveLog ArchiveLogFor, nm *metrics.NodeMetrics) (Queue, error)
```

Inside `New`, replace `scope.Counter("enqueues")` with `nm.Enqueues.WithLabelValues(name)`.

Do this for:
- `internal/engine/manager.go` — `OpenManager` and `Create`
- `internal/engine/queue.go` — `openQueue`
- `internal/engine/wal.go` — `newWAL`
- `internal/queue/taskqueue/taskqueue.go` — `New`
- `internal/queue/logqueue/logqueue.go` — `New`
- `internal/queue/retention/coordinator.go` — `NewCoordinator`
- `internal/server/server.go` — `New`

### 2.6 Gate 2

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[feat] internal/metrics, internal/node : replace custom registry/scopes with prometheus vectors`

---

## Step 3 — Delete the hand-rolled Prometheus encoder

**Goal:** Remove `internal/metrics/prometheus.go` entirely. Replace its single caller (`internal/daemon/metrics_http.go`) with `promhttp.HandlerFor`.

### 3.1 Delete the encoder

```bash
rm internal/metrics/prometheus.go
```

This removes `Encode`, `EncodeOptions`, the `encoder` struct, all `counterNames`/`gaugeNames`/`histogramNames` maps, `infoLabels`, and the per-queue series emission.

### 3.2 Update the daemon metrics server

In `internal/daemon/metrics_http.go`, replace the custom `handlePrometheus`:

```go
// Before
func (ms *metricsServer) handlePrometheus(w http.ResponseWriter, r *http.Request) {
    snap, err := ms.node.Metrics(r.Context())
    if err != nil {
        http.Error(w, err.Error(), http.StatusInternalServerError)
        return
    }
    w.Header().Set("Content-Type", prometheusContentType)
    if err := metrics.Encode(w, snap, metrics.EncodeOptions{...}); err != nil {
        log.Printf("mikyu daemon: prometheus encode: %v", err)
    }
}
```

After:

```go
import (
    "github.com/prometheus/client_golang/prometheus/promhttp"
)

func (ms *metricsServer) handlePrometheus(w http.ResponseWriter, r *http.Request) {
    promhttp.HandlerFor(ms.registry, promhttp.HandlerOpts{
        ErrorHandling: promhttp.ContinueOnError,
        ErrorLog:      slog.NewLogLogger(ms.logger.Handler(), slog.LevelWarn),
    }).ServeHTTP(w, r)
}
```

`promhttp.HandlerFor` takes a `prometheus.Gatherer`. Pass `ms.node.MetricsRegistry()` (a new method that returns `n.nodeMetrics.Registry()`).

Update `startMetricsHTTP` to accept the registry:

```go
func startMetricsHTTP(n *node.Node, perQueue, histograms bool, addr string) (*metricsServer, error) {
    ln, err := net.Listen("tcp", addr)
    if err != nil {
        return nil, err
    }
    warnIfNotLoopback(ln.Addr())

    ms := &metricsServer{
        node: n,
        registry: n.MetricsRegistry(),
        logger: slog.Default(),
    }
    mux := http.NewServeMux()
    mux.Handle("GET /metrics", promhttp.HandlerFor(ms.registry, promhttp.HandlerOpts{
        ErrorHandling: promhttp.ContinueOnError,
    }))
    mux.HandleFunc("GET /api/metrics", ms.handleJSON)
    mux.HandleFunc("GET /api/history", ms.handleHistory)
    mux.HandleFunc("GET /api/events", ms.handleSSE)
    ms.srv = &http.Server{Handler: mux, ReadHeaderTimeout: 10 * time.Second}
    go ms.srv.Serve(ln)
    return ms, nil
}
```

The `perQueue` and `histograms` flags become no-ops for the Prometheus endpoint. Per-queue series are now always present (labeled by `queue`). Histograms are always emitted as Prometheus histograms; the `histograms` toggle only affects the JSON `Snapshot` endpoint if you choose to keep it.

**Failure mode:** `promhttp.HandlerFor` with `ContinueOnError` logs collection errors but still serves a 200 with whatever metrics succeeded. This preserves the old behavior where a single collector failure did not blank the endpoint. Do not use `HTTPErrorOnError` — it would break scrapes on a transient instrument error.

### 3.3 Delete the custom content type constant

`prometheusContentType` is defined in `internal/daemon/metrics_http.go`. Delete it. `promhttp` sets the correct content type automatically.

### 3.4 Gate 3

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[feat] internal/metrics, internal/daemon : replace hand-rolled prometheus encoder with promhttp handler`

---

## Step 4 — Rewire `Snapshot` collection

**Goal:** The JSON `/api/metrics`, `/api/history`, and `/api/events` endpoints still need a `Snapshot`. Populate it from Prometheus instruments instead of from the deleted `Scope.read()`.

### 4.1 Replace `Collector.mergeScopes`

In `internal/metrics/collector.go`, `mergeScopes` iterated over `Scope` instances and called `scope.read()`. Delete that method. Replace it with a `PrometheusGatherer` source that gathers from the registry and maps `dto.MetricFamily` into `QueueView`:

```go
type prometheusSource struct {
    reg *prometheus.Registry
}

func (s prometheusSource) Collect(ctx context.Context, dst *Snapshot) {
    families, err := s.reg.Gather()
    if err != nil {
        return
    }
    index := make(map[string]*QueueView, len(dst.Queues))
    for i := range dst.Queues {
        index[dst.Queues[i].Name] = &dst.Queues[i]
    }
    for _, mf := range families {
        for _, m := range mf.GetMetric() {
            queue := labelValue(m, "queue")
            if queue == "" {
                continue
            }
            qv, ok := index[queue]
            if !ok {
                dst.Queues = append(dst.Queues, QueueView{Name: queue, Kind: "internal"})
                qv = &dst.Queues[len(dst.Queues)-1]
                index[queue] = qv
            }
            applyMetric(qv, mf.GetName(), m)
        }
    }
}
```

`applyMetric` switches on the metric family name and writes into `Counters`, `Gauges`, `Histograms`, `Producers`, `Consumers`, `Backpressure`, or `Tasks`. This is mechanical but tedious. Keep it in a single file, `internal/metrics/prometheus_source.go`, so the mapping is auditable.

### 4.2 Task gauge routing

The old `mergeScopes` routed `active`, `in_flight`, `scheduled`, `dead_letters`, etc. into `QueueView.Tasks` instead of `QueueView.Gauges`. Preserve that behavior. In `applyMetric`, when the metric name is `mikyu_queue_tasks_active`, write to `qv.Tasks.Active`, not `qv.Gauges["tasks_active"]`.

### 4.3 Gate 4

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[feat] internal/metrics : populate snapshot from prometheus gatherer instead of custom scope reads`

---

## Step 5 — Rewrite tests

**Goal:** Replace tests that asserted on custom instruments with `prometheus/testutil` helpers.

### 5.1 Delete obsolete tests

Delete:
- `internal/metrics/instruments_test.go` — tests custom `Counter`, `Gauge`, `Histogram`.
- `internal/metrics/prometheus_test.go` — tests the hand-rolled encoder.
- `internal/metrics/registry_test.go` — tests custom `Registry`/`Scope`.
- `internal/metrics/collector_test.go` — `mergeScopes` and `computeRates` tests that depend on `Scope`.
- `internal/metrics/lock_test.go` — tests `LockObserver`, which uses the custom `Histogram`. Rewrite in 5.2.

Keep:
- `internal/metrics/history_test.go` — `RingBuffer` is unchanged.
- `internal/metrics/state_test.go` — `Health`, `QueueHealth`, `NodeHealth` are pure functions over `Snapshot`.
- `internal/metrics/snapshot_test.go` — `Snapshot` JSON shape. Update the golden after Step 1.3 removes percentile fields.

### 5.2 Rewrite lock observer tests

`LockObserver` and `RWLockObserver` currently embed a custom `*Histogram`. Replace with `prometheus.Histogram`:

```go
type LockObserver struct {
    name    string
    mu      sync.Mutex
    waits   prometheus.Counter
    maxWait prometheus.Gauge
    hist    prometheus.Histogram
}
```

The `LockViews()` method assembles a `LockView` by reading `testutil.ToFloat64` for the counter/gauge and `testutil.CollectAndCount` or a gathered histogram for the percentiles. Tests use `testutil.CollectAndCompare`:

```go
func TestLockObserverContended(t *testing.T) {
    l := NewLockObserver("test.mu", nil)
    // ... contend ...
    if err := testutil.CollectAndCompare(l.waits,
        strings.NewReader(`
            # HELP mikyu_lock_waits_total Contended lock acquisitions.
            # TYPE mikyu_lock_waits_total counter
            mikyu_lock_waits_total{lock="test.mu"} 4
        `), "mikyu_lock_waits_total"); err != nil {
        t.Fatal(err)
    }
}
```

### 5.3 Rewrite engine, taskqueue, and server metrics tests

Tests like `TestEngineAppendCounters` currently do:

```go
scope := reg.Scope("q")
assert.EqualValues(t, 100, scope.Counter("appends").Load())
```

Replace with:

```go
nm := metrics.NewNodeMetrics()
q := openQueue(t, "q", nm)
// ... append 100 ...
got := testutil.ToFloat64(nm.Appends.WithLabelValues("q"))
assert.EqualValues(t, 100, got)
```

Do this in:
- `internal/engine/metrics_test.go`
- `internal/queue/taskqueue/metrics_test.go`
- `internal/server/metrics_test.go`
- `internal/node/metrics.go` tests

### 5.4 Add linting tests

Use `testutil.CollectAndLint` to enforce naming conventions:

```go
func TestNodeMetricsLint(t *testing.T) {
    nm := NewNodeMetrics()
    // Touch every vec so a series exists for each.
    nm.Appends.WithLabelValues("q").Inc()
    nm.Reads.WithLabelValues("q").Inc()
    // ... etc ...
    if err := testutil.CollectAndLint(nm.reg); err != nil {
        t.Fatal(err)
    }
}
```

This catches counters missing `_total`, histograms missing `_seconds`, and inconsistent help text.

### 5.5 Update golden files

Run with `-update` for the affected packages:

```bash
go test ./internal/metrics -run TestSnapshotJSONGolden -update
go test ./internal/ui/metricsview -run TestDetailGolden -update
go test ./internal/ui/metricsview -run TestSummaryGolden -update
go test ./internal/ui/command -run TestGolden -update
go test ./internal/daemon -run TestMetricsHTTP -update
```

Review every golden diff carefully. The `Snapshot` loses `p50`, `p90`, `p95`, `p99`, `p999`, `max` from `HistogramView`. The `metricsview` golden files lose the `latency` line unless you re-add percentile computation in the view layer (see 1.3). Decide explicitly: either keep the latency line by computing percentiles in `metricsview`, or drop it and rely on `/metrics` for latency queries.

### 5.6 Gate 5

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[test] internal/metrics, internal/engine, internal/queue, internal/server : migrate tests to prometheus/testutil`

---

## Step 6 — Handle label cardinality and naming

**Goal:** Ensure the new Prometheus metrics follow Prometheus naming conventions and do not risk cardinality explosions.

### 6.1 Apply `_total` and `_seconds` suffixes

`client_golang` enforces `_total` on counters and `_seconds` on histograms at registration time. Run `testutil.CollectAndLint` (added in 5.4) and fix every violation. Common fixes:

- `mikyu_queue_appends` → `mikyu_queue_appends_total`
- `mikyu_queue_append_duration` → `mikyu_queue_append_duration_seconds`
- `mikyu_queue_wal_flushes` → `mikyu_queue_wal_flushes_total`

### 6.2 Bound label cardinality

The only label is `"queue"`, whose value is the queue name. Queue names are user-controlled but are typically a bounded set per node. If a deployment creates unbounded queues dynamically, add a cardinality cap. `client_golang` supports `VariableLabels` constraints via `prometheus.WrapRegistererWith` and `prometheus.Labels` validation, but the simplest defense is a check in the queue-creation path:

```go
const maxQueues = 1000

func (n *Node) CreateLogQueue(ctx context.Context, name string, s spec.Spec) (logqueue.Log, error) {
    n.mu.Lock()
    if len(n.logCatalog) >= maxQueues {
        n.mu.Unlock()
        return nil, fmt.Errorf("%w: queue limit %d reached", ErrInvalidConfig, maxQueues)
    }
    n.mu.Unlock()
    // ...
}
```

If you prefer a library-level cap, use `prometheus.NewCounterVec` with a `prometheus.Labels` map and validate the label value before calling `WithLabelValues`. Do not use the `VariableLabels` constraints feature unless you pin `client_golang` to a version that supports it and you have a fallback for rejected labels.

### 6.3 Native histograms (optional, recommended)

Prometheus 3.x supports native histograms. Enable them on the latency histograms by setting `NativeHistogramBucketFactor`:

```go
appendDur := prometheus.NewHistogram(prometheus.HistogramOpts{
    Name:    "mikyu_queue_append_duration_seconds",
    Help:    "Append latency in seconds.",
    ConstLabels: labels,
    Buckets: ioDurationBuckets,
    NativeHistogramBucketFactor:     1.1,
    NativeHistogramMaxBucketNumber:  100,
    NativeHistogramMinResetDuration: time.Hour,
})
```

This emits both classic buckets and a native histogram. Prometheus scrapes whichever it is configured to ingest. The classic buckets remain for backward compatibility.

### 6.4 Gate 6

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[fix] internal/metrics : enforce prometheus naming conventions and bound label cardinality`

---

## Step 7 — Remove dead code and update documentation

**Goal:** Delete everything that existed only to support the custom encoder.

### 7.1 Delete dead files

- `internal/metrics/prometheus.go` (already deleted in Step 3)
- `internal/metrics/registry.go` (deleted in Step 2)
- `internal/metrics/instruments.go` — keep only `RingBuffer`. Move it to `internal/metrics/history.go` and delete the file.
- Any `testdata/snapshot.prom` in `internal/metrics/testdata/`

### 7.2 Update `internal/metrics/doc.go`

Replace the package doc with one line:

```go
// Package metrics exposes Mikyu's Prometheus instrumentation and snapshot model.
package metrics
```

### 7.3 Update `.go-arch-lint.yml`

The architecture lint rule for `metrics` currently allows `mayDependOn: [spec]`. Prometheus types are vendor dependencies, so `anyVendorDeps: true` already covers them. No change needed.

### 7.4 Update README

The README's "Rich metrics available across HTTP, SDK and CLI" line still holds. The `/metrics` endpoint now serves the standard Prometheus format via `promhttp`. Update the daemon section if it documents the old endpoint behavior.

### 7.5 Gate 7

```bash
go test ./...
go vet ./...
go fix ./...
```

Commit: `[chore] internal/metrics : remove dead code from the pre-prometheus instrumentation layer`

---

## Step 8 — Final verification

**Goal:** Prove the migration is complete and no behavior regressed.

### 8.1 Full suite with race detector

```bash
go test -race ./...
```

### 8.2 Full suite with `goleak`

Every package with a `goleak_test.go` must still pass. The Prometheus registry does not spawn goroutines, so no new leaks should appear. If a leak appears, check whether `promhttp.HandlerFor`'s internal instrumentation (enabled by `HandlerOpts.Registry`) starts a goroutine. It does not.

### 8.3 `go vet` and `go fix`

```bash
go vet ./...
go fix ./...
```

`go fix` should report no changes. If it proposes changes, apply them.

### 8.4 Verify the `/metrics` endpoint manually

Start the daemon:

```bash
go run ./cmd/mikyu daemon --target tcp://127.0.0.1:7420 --data-dir /tmp/mikyu-metrics-test --metrics-listen 127.0.0.1:9090
```

Scrape:

```bash
curl -s http://127.0.0.1:9090/metrics | head -50
```

Confirm:
- Content-Type is `text/plain; version=0.0.4; charset=utf-8`.
- `go_goroutines`, `process_cpu_seconds_total`, and `mikyu_build_info` are present.
- Queue-specific series carry a `queue="..."` label.
- Histogram families have `_bucket`, `_sum`, and `_count`.

### 8.5 Verify the JSON snapshot still works

```bash
curl -s http://127.0.0.1:9090/api/metrics | jq .
```

Confirm the `Snapshot` shape matches the updated `HistogramView` (no percentile fields).

### 8.6 Verify the CLI metrics view

```bash
go run ./cmd/mikyu --dir /tmp/mikyu-metrics-test metrics --detail
```

Confirm the text view renders. If you kept the latency line, confirm it shows percentiles computed in `metricsview`.

### 8.7 Commit

```bash
git add -A
git commit -m "[chore] all : finalize prometheus/client_golang migration"
```

---

## Failure modes and how to handle them

| Failure | Cause | Fix |
|---|---|---|
| `panic: duplicate metrics collector registration attempted` | Two `prometheus.NewCounterVec` with the same name registered on the same registry, or `promauto` registering to the default registry twice. | Use `prometheus.NewRegistry()` (never the default) and construct each `*Vec` exactly once in `NewNodeMetrics`. If a vec must be recreated, use `reg.Unregister` first or `reg.Register` with `AlreadyRegisteredError` handling. |
| `panic: duplicate metrics collector registration attempted` on daemon restart in tests | `node.Acquire` reuses a pooled node; the second `NewNodeMetrics` registers on a fresh registry, which is fine. The panic occurs if you register runtime collectors with `MustRegister` more than once on the same registry. | Guard with `sync.Once` (see 2.3). |
| `testutil.CollectAndCompare` fails with extra metrics | The pedantic registry includes runtime collectors. | Pass only the metric names you care about as the variadic `metricNames` argument. `CollectAndCompare(c, want, "mikyu_queue_appends_total")` ignores everything else. |
| Histogram percentiles disappeared from the CLI | `HistogramView` no longer carries `P50`/`P90`/`P99`. | Either compute them in `metricsview` (see 1.3) or drop the latency line and document that latency is queried from `/metrics`. |
| `promhttp.HandlerFor` returns 500 on scrape | `ErrorHandling` defaults to `HTTPErrorOnError`. | Set `ErrorHandling: promhttp.ContinueOnError`. |
| Prometheus reports `metric name is invalid` | Counter without `_total`, histogram without `_seconds`, or a name with uppercase/dashes. | Run `testutil.CollectAndLint` (5.4) and fix every violation. |
| Label cardinality explodes | Unbounded queue names. | Add a queue-count cap (6.2). |
| `go fix` proposes changes after migration | Stale code in deleted files still referenced elsewhere. | Run `go build ./...` first to find dangling references, fix them, then re-run `go fix`. |
| `client_golang` requires Go 1.25+ and the module targets older | Version skew. | Pin to `github.com/prometheus/client_golang@v1.16.0` for Go 1.19–1.21, or `v1.17.x` for Go 1.22. |

---

## Summary of commits

| Step | Commit message |
|---|---|
| 0 | `[chore] go.mod : add prometheus/client_golang dependency` |
| 1 | `[feat] internal/metrics : replace custom counter/gauge/histogram with prometheus/client_golang types` |
| 2 | `[feat] internal/metrics, internal/node : replace custom registry/scopes with prometheus vectors` |
| 3 | `[feat] internal/metrics, internal/daemon : replace hand-rolled prometheus encoder with promhttp handler` |
| 4 | `[feat] internal/metrics : populate snapshot from prometheus gatherer instead of custom scope reads` |
| 5 | `[test] internal/metrics, internal/engine, internal/queue, internal/server : migrate tests to prometheus/testutil` |
| 6 | `[fix] internal/metrics : enforce prometheus naming conventions and bound label cardinality` |
| 7 | `[chore] internal/metrics : remove dead code from the pre-prometheus instrumentation layer` |
| 8 | `[chore] all : finalize prometheus/client_golang migration` |

Each commit is independently green. A junior engineer can follow the steps linearly, run the gates, and stop at any step with a working tree.