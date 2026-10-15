# Migration Plan: Replace the Hand-Rolled Prometheus Encoder with `prometheus/client_golang`

This plan migrates the Prometheus surface of `internal/metrics` from the hand-written text encoder to the official `prometheus/client_golang` library. Each step is gated by `go test ./...`, `go vet ./...`, and `go fix` on the modules touched. Commit after each gated step when the tree is clean.

## Why the original plan was revised

The original plan (replacing custom instruments with `prometheus.Counter`/`Gauge`/`Histogram` and rebuilding `Snapshot` from `reg.Gather()`) does not survive contact with the code:

1. **The encoder is a pure projection of `Snapshot`.** `metrics.Encode` reads only the `Snapshot` struct — never the instruments. The instruments and `Registry`/`Scope` are the *recording layer* behind the single observation surface, which the CLI, the SDK's `SubscribeSnapshots`, the bench observer, the health functions in `state.go`, and the JSON API all consume. Replacing the instruments inverts the dataflow: `Snapshot` would have to be rebuilt from `dto` every tick, re-implementing the task-gauge routing, group rows, rates, and totals in a second, lossy mapping.
2. **The inversion drops series the standard collectors cannot supply.** `mikyu_runtime_*`, `mikyu_process_*`, `mikyu_gc_pause_seconds` quantiles, `mikyu_scheduler_latency_seconds`, `mikyu_lock_*`, `mikyu_storage_*`, and `mikyu_totals_*` are produced by `Source` implementations that write `Snapshot` directly (e.g. `node/runtime_source.go` reads `runtime/metrics`). `collectors.NewGoCollector` / `NewProcessCollector` do not carry goroutine states, scheduler latency, GC pause quantiles, lock contention, or storage aggregates. The original plan never says where these families would come from after `Scope.read()` dies.
3. **`HistogramSnapshot` is not encoder-internal.** `node/runtime_source.go` uses it to hold GC pause and scheduler-latency samples. The original plan deletes it in Step 1; the build breaks.
4. **The original plan cannot keep the tree green between steps.** Deleting `Scope.read()` in Step 1 leaves the collector with no data until Step 4's gather source exists, violating every gate.
5. **The custom instruments are load-bearing.** They are nil-safe (`Scope(nil)` hands back no-op instruments), lock-free, and allocation-free on the hot append/read paths. `prometheus.Counter` and friends are none of those things, and the WAL/task hot paths would pay for it.

**Corrected strategy:** keep the recording layer (instruments, `Registry`/`Scope`, `Snapshot`, collector, sources) untouched, and replace only the wire format. A `prometheus.Collector` adapter renders one `Snapshot` per scrape; `promhttp` owns the exposition format, content negotiation, escaping, and headers. All existing series names, labels, help text, and options (`PerQueue`, `TopNQueues`, `Histograms`) are preserved so dashboards keep working.

---

## Step 0 — Preparation and dependency audit (done)

- Baseline green: `go build ./...`, `go vet ./...`, `go test ./...` all pass.
- Added `github.com/prometheus/client_golang v1.25.0` (Go 1.27.2 module). The require is `// indirect` until Step 1 imports it; `go mod tidy` therefore stays a no-op until then.

Commit: `[chore] go.mod : add prometheus/client_golang dependency`

---

## Step 1 — Adapt `Snapshot` to `prometheus.Collector`

**Goal:** add an adapter that renders a `Snapshot` as Prometheus metrics, proven equivalent to the hand-rolled encoder before anything changes for users.

### 1.1 The adapter

New file `internal/metrics/exposition.go`:

```go
type CollectOptions struct {
    PerQueue   bool
    TopNQueues int
    Histograms bool
}

type SnapshotCollector struct{ /* source func + options */ }

func NewSnapshotCollector(src func() (Snapshot, error), opts CollectOptions) *SnapshotCollector
func (c *SnapshotCollector) Describe(chan<- *prometheus.Desc) {} // unchecked: families derive from the snapshot
func (c *SnapshotCollector) Collect(ch chan<- prometheus.Metric)
```

- The collector emits `dto.Metric` values through a small `prometheus.Metric` wrapper, because three families cannot be expressed with the `MustNewConst*` helpers:
  - quantile-only summaries (GC pause, scheduler latency) have no `_count`/`_sum`,
  - per-queue quantile summaries carry a `quantile="+Inf"` sample for `Max`, which `MustNewConstSummary` rejects (it validates quantiles to `[0,1]`).
- Sample values are coerced to `0` when NaN or infinite, exactly like the old `formatFloat`.
- Label pairs are sorted by name (the registry's consistency check depends on it).
- Histograms are emitted as cumulative `dto.Bucket`s; exposition's `le="+Inf"` bucket comes from `SampleCount`.
- The adapter reuses the encoder's name/help tables (`gaugeNames`, `counterNames`, `histogramNames`, `rateHelp`, …) and helpers (`selectQueues`, `queueViewFields`, `infoLabels`), so the two paths cannot drift. `infoLabels` is refactored into `infoPairs` (`[][2]string`) with a thin string-rendering wrapper for the encoder.

### 1.2 Prove parity before switching

`internal/metrics/exposition_test.go`:

- `TestExpositionParityWithEncoder` — render the same `Snapshot` through `Encode` and through the adapter (fresh `prometheus.Registry` + `Gather` + `expfmt.MetricFamilyToText`), parse both texts with `expfmt.TextParser`, sort label pairs, and require `reflect.DeepEqual` of the family maps. Family *ordering* is free (both are maps); within-family sample ordering must match, which both sides inherit from `selectQueues`.
- Port every behavioral test (`TestPrometheus*`) as `TestExposition*`, adapting label-order assertions to the sorted order (`group=` before `queue=`, dial labels alphabetical). The coverage audit (`TestPrometheusCoversSnapshot`) is ported as `TestExpositionCoversSnapshot`, reusing the existing `coveredFields`/`skipFields`/`walkLeaves` helpers until they move in Step 2.
- `TestExpositionLint` runs `promlint.New(families).Lint()` over a fully populated scrape and records the baseline. Naming violations that would break the frozen surface are handled in Step 3, not here.

### 1.3 Gate 1

```powershell
go test ./...
go vet ./...
go fix ./internal/metrics
```

Commit: `[feat] internal/metrics : adapt snapshot to prometheus.Collector for client_golang exposition`

---

## Step 2 — Serve `/metrics` through `promhttp`; delete the encoder

**Goal:** the daemon's Prometheus endpoint becomes `promhttp.HandlerFor`; the hand-rolled encoder is deleted.

### 2.1 Daemon wiring

`internal/daemon/metrics_http.go`:

- Build one `prometheus.Registry` at startup holding `metrics.NewSnapshotCollector(...)` bound to the node's snapshot source (the node's `Metrics(ctx)`; the request context is not available inside `Collect`, so the source closes over the collector and uses `context.Background()` — a snapshot build is a few microseconds of atomic reads).
- `GET /metrics` → `promhttp.HandlerFor(reg, promhttp.HandlerOpts{ErrorHandling: promhttp.ContinueOnError})`. `ContinueOnError`, never `HTTPErrorOnError`: a transient instrument error must not blank a scrape.
- `promhttp` sets `Content-Type: text/plain; version=0.0.4; charset=utf-8` — identical to the deleted `prometheusContentType` constant, which is removed.
- `perQueue` and `histograms` still flow into `CollectOptions`. `TopNQueues` stays 50.

### 2.2 Delete the encoder

- Delete `Encode`, `EncodeOptions`, the `encoder` struct, and the section functions from `internal/metrics/prometheus.go`; move the surviving name/help tables and helpers into `exposition.go` and delete the file.
- Move `coveredFields`, `skipFields`, `walkLeaves`, `goldenSnapshot`, and the `-update` flag into `exposition_test.go`; delete `prometheus_test.go`.
- Regenerate `internal/metrics/testdata/snapshot.prom` from the adapter (`-update`) and review the diff: family order becomes alphabetical and label pairs sort alphabetically; sample values and families are unchanged.

### 2.3 Gate 2

```powershell
go test ./...
go vet ./...
go fix ./internal/metrics ./internal/daemon
```

Commit: `[feat] internal/metrics, internal/daemon : serve /metrics through promhttp and delete the hand-rolled encoder`

---

## Step 3 — Naming conventions and cardinality

**Goal:** the exported surface passes `promlint` without breaking the frozen series names.

**Outcome:** every published name is already convention-clean — counters end in `_total`, histograms and summaries in `_seconds`, rates use the documented `_per_second` form, and every help text ends with a period. The full-surface lint (`TestExpositionLintFullSurface`) populates every name-table key, the unknown-key fallbacks, typed views, groups, rates, and all global views, and passes with zero problems. One promlint rule is worth recording: metric names must not embed type words (`gauge`, `counter`, `sum`), so future instrument keys must avoid them; the unknown-key fallback (`sanitize`) passes names through verbatim.

- `TestExpositionLintFullSurface` — lints every family the adapter can emit, not just the golden snapshot's.
- `TestExpositionLabelCardinalityIsBounded` — asserts every series label is in a fixed allowlist; the only per-sample variable dimensions are `queue`, `group`, and `lock`, so cardinality is bounded by the node's queues.
- `TestExpositionTopNCap` — pins the daemon's `TopNQueues = 50` behavior.

### Gate 3

```powershell
go test ./...
go vet ./...
go fix ./internal/metrics
```

Commit: `[test] internal/metrics : enforce prometheus naming conventions and bounded label cardinality`

---

## Step 4 — Dead code, documentation, final verification

- Delete anything that existed only for the encoder (`testdata` leftovers, unused helpers).
- Update `internal/metrics/doc.go` and the README's metrics section: `/metrics` is served by `promhttp` over a `Snapshot`-backed collector.
- Full verification:

```powershell
go test -race ./...
go vet ./...
go fix ./internal/metrics ./internal/daemon
go run ./cmd/mikyu daemon --target tcp://127.0.0.1:7420 --data-dir <tmp> --metrics-listen 127.0.0.1:9090
# scrape /metrics: content type, mikyu_* families, queue labels, histogram bucket/sum/count
# scrape /api/metrics: JSON snapshot unchanged
```

Commit: `[chore] all : finalize prometheus/client_golang migration`

---

## Failure modes

| Failure | Cause | Fix |
|---|---|---|
| `duplicate metrics collector registration attempted` | registering the adapter twice or reusing a registry | one `prometheus.NewRegistry()` per node, one adapter on it |
| Gather reports unsorted label pairs | emitting `dto.LabelPair`s out of name order | `labelPairs` sorts before emission |
| Gather reports duplicate series | two queue views with the same name, or a family emitted per queue twice | emit one sample per (family, queue[, group]) tuple; queue views are name-keyed upstream |
| `MustNewConstSummary` panic | not applicable — the adapter writes `dto` directly and never uses the const helpers for quantile families |
| `promhttp` returns 200 with an empty body | snapshot source errored and the collector stayed silent | source errors are logged by the daemon; with a background context the source effectively cannot fail |
| Sample values print as `6.7108864e+07` | both `formatFloat` and `expfmt` use `%g` — this is the pre-existing rendering, not a regression | none; parity test pins it |

## Summary of commits

| Step | Commit message |
|---|---|
| 0 | `[chore] go.mod : add prometheus/client_golang dependency` |
| 1 | `[feat] internal/metrics : adapt snapshot to prometheus.Collector for client_golang exposition` |
| 2 | `[feat] internal/metrics, internal/daemon : serve /metrics through promhttp and delete the hand-rolled encoder` |
| 3 | `[fix] internal/metrics : enforce prometheus naming conventions on the snapshot exposition` |
| 4 | `[chore] all : finalize prometheus/client_golang migration` |

Each commit is independently green; the tree can be abandoned at any step with a working Prometheus surface.
