# mikyu bench

This plan adds a benchmark tool to mikyu. The tool ships with the binary. It mimics the vocabulary of OMB / nats-bench / iggy-bench and produces honest, complete, self-describing result artifacts.

**A finished run looks like this:**

```
$ mikyu bench produce --dir /tmp/mikyu --duration 5s --workers 8

bench      produce · embedded
spec       durability=sync  max_segment=64.0 MiB
workload   log  1 queue  1024 B  8 workers  batch=1
warmup     ......
measure    ########  5.0s  1.2M msgs  1.2 GiB  240K msg/s  245 MiB/s
drain      ......
latency    p50 42µs  p90 180µs  p99 1.2ms  max 8.1ms
written    ./bench/bench-1727619830123456789
```

---

## 1. Vocabulary and invariants

Learn these words before you write any code.

- **Run.** One execution of the bench. Owns a mode, an access, a workload, and a result directory. Never touches another run's data.
- **Mode.** What the run does: `produce`, `consume`, `mix`, `recovery`.
- **Access.** How the client talks: `embedded`, `daemon`, `remote`.
- **Phase.** A named stretch inside a run: `populate`, `warmup`, `measure`, `drain`, `abort`. Every mode runs a fixed sequence.
- **Envelope.** The result artifact: `result.json` (metadata + final metrics) and `series.csv` (per-second observations). Streamed to disk, never held in memory.
- **Observation families.** Three, never mixed: client-side (latency histograms), system-side (queue depth, WAL fill), host-side (RSS, CPU, FDs, threads).

### Non-negotiables

1. **Two tables, one config surface each.** `spec.Table` (exists) drives queue config. `bench.Table` (new) drives workload config. Never invent a third.
2. **Kind is structural.** `log` vs `task` is decided by the queue's parent directory. `bench` declares it via `--kind`; never infer it from a name.
3. **Streaming envelope.** `result.json` is written at start with `complete:false`, atomically rewritten at end with `complete:true`. `series.csv` opens at start and appends per tick, unbuffered per row. A `SIGKILL` mid-run loses nothing already written.
4. **CO correction on by default.** Open-loop runs record both corrected and uncorrected histograms. Closed-loop runs record one.
5. **Verification off by default.** `--verify` turns it on. Envelope labels `verified: true|false`.
6. **`--persist` governs *system state*** (queues, daemons), never output. Output always survives.
7. **`.bench-runs` is the ownership record.** Reconciled on next acquire, mirroring `node.Acquire`'s lock pattern.
8. **Errors are stable sentinels.** Every bench error lives in `internal/bench/errors.go` and has a wire code in `errs.Table`'s admin block. Never return a bare `errors.New` from bench code.
9. **Layer purity.** `bench` imports only `spec`, `metrics`, `mikyu`, `daemon`, `human`. `.go-arch-lint.yml` fails the build otherwise.
10. **No goroutine leaks.** Every goroutine has a documented stop condition. `goleak.VerifyTestMain` guards both bench packages.

### Layering (added edges only)

```
bench       →  spec, metrics, mikyu, daemon, human, bench/result
benchview   →  bench/result, spec, style, human
command     →  ..., bench, benchview
cli, repl   →  ..., bench, benchview
```

Add the `bench` and `benchview` components to `.go-arch-lint.yml` and extend `command`, `cli`, `repl` to depend on them.

---

## 2. File layout (final state)

```
internal/bench/
  bench.go            Run entry point; phase orchestration
  options.go          Options struct; flag/file/env resolution
  table.go            Param rows + Workload value + Validate
  access.go           Access enum; client opener (embedded/daemon/remote)
  lifecycle.go        .bench-runs, stale reconciliation, queue naming
  phases.go           Phase enum + phase sequences
  measure.go          Histogram, Measure (CO correction), tail samples
  observe.go          Per-second sampler (three families)
  host.go             Host stats dispatcher (platform-agnostic)
  host_linux.go       /proc readers
  host_darwin.go      getrusage + /dev/fd readers
  host_windows.go     GetProcessMemoryInfo etc.
  verify.go           PRNG payload generation + inline verify
  envelope.go         Result/Series writers (streaming)
  errors.go           Sentinels + VerifyError
  mode_produce.go     Produce mode
  mode_consume.go     Consume mode
  mode_mix.go         Mix mode
  mode_recovery.go    Recovery mode
  proc_posix.go       spawnDetached, kill (Unix)
  proc_windows.go     spawnDetached, kill (Windows)

internal/bench/result/
  result.go           Result, PhaseView, EnvironmentView, MetricsView
  series.go           SeriesRow, column schema
  write.go            WriteResult (atomic), OpenSeries (streaming)
  read.go             ReadResult, ReadSeries
  version.go          CurrentVersion = 1

internal/ui/benchview/
  summary.go          Renders a Result
  show.go             Load + summary
  compare.go          Multi-run diff renderer
  plot.go             series.csv → terminal chart

internal/ui/command/
  bench.go            RunBench + flag set from bench.Table
  verb.go             (modified) Verb.Subs + recursive lookup

internal/ui/cli/bench.go     CLI handlers
internal/ui/repl/bench.go    REPL handlers
```

---

## 3. Phase 1 — Foundations

**Goal.** All shared types, tables, sentinels, and small extensions exist and are tested. No bench behavior yet.

### 3.1 `internal/bench/result/` (new package)

This package owns the on-disk contract. Nothing else writes `.json` or `.csv`. Its version is independent of `spec.CurrentSpecVersion`.

**`version.go`**

```go
package result

// CurrentVersion is the envelope schema version. Bumped on any change
// to the Result or SeriesRow shape. Independent of spec.CurrentSpecVersion.
const CurrentVersion = 1
```

**`result.go`**

```go
type Result struct {
    Version      int              `json:"version"`
    RunID        string           `json:"run_id"`
    Mode         string           `json:"mode"`
    Access       string           `json:"access"`
    AccessTarget string           `json:"access_target,omitempty"`
    StartedAt    time.Time        `json:"started_at"`
    EndedAt      time.Time        `json:"ended_at,omitzero"`
    Complete     bool             `json:"complete"`
    Aborted      string           `json:"aborted,omitempty"`
    Spec         spec.Spec        `json:"spec"`
    Workload     WorkloadView     `json:"workload"`
    Environment  EnvironmentView  `json:"environment"`
    Phases       []PhaseView      `json:"phases"`
    Metrics      *MetricsView     `json:"metrics,omitempty"`
    Notes        []string         `json:"notes,omitempty"`
}

type WorkloadView struct {
    Kind         string      `json:"kind"`
    QueueCount   int         `json:"queues"`
    MsgSize      int         `json:"msg_size"`
    Bytes        int64       `json:"bytes,omitempty"`
    Duration     string      `json:"duration,omitempty"`
    Rate         uint64      `json:"rate,omitempty"`
    RateSchedule []RatePoint `json:"rate_schedule,omitempty"`
    Workers      int         `json:"workers"`
    Batch        int         `json:"batch"`
    Keys         int         `json:"keys,omitempty"`
    Preload      int64       `json:"preload,omitempty"`
    Warmup       string      `json:"warmup"`
    AckRate      float64     `json:"ack_rate,omitempty"`
    CrashAfter   int64       `json:"crash_after_bytes,omitempty"`
}

type RatePoint struct {
    At   string `json:"at"`   // duration string
    Rate uint64 `json:"rate"`
}

type EnvironmentView struct {
    Host            string `json:"host"`
    OS              string `json:"os"`
    Arch            string `json:"arch"`
    Kernel          string `json:"kernel,omitempty"`
    Filesystem      string `json:"filesystem,omitempty"`
    GoVersion       string `json:"go_version"`
    SpecVersion     uint16 `json:"spec_version"`
    BenchVersion    int    `json:"bench_version"`
    GitSHA          string `json:"git_sha,omitempty"`
    ClientCPU       int    `json:"client_cpu"`
    DaemonCPU       int    `json:"daemon_cpu,omitempty"`
    CacheState      string `json:"cache_state"`   // "cold"|"warm"|"unknown"
    ColdVerified    bool   `json:"cold_verified,omitempty"`
    TimerOverheadNS int64  `json:"timer_overhead_ns,omitempty"`
}

type PhaseView struct {
    Name     string `json:"name"`   // "populate"|"warmup"|"measure"|"drain"|"abort"
    Duration string `json:"duration"`
    Bytes    int64  `json:"bytes"`
}

type MetricsView struct {
    ThroughputMsgs     float64           `json:"throughput_msgs_per_sec"`
    ThroughputBytes    float64           `json:"throughput_bytes_per_sec"`
    LatencyCorrected   HistogramView     `json:"latency_corrected"`
    LatencyUncorrected HistogramView     `json:"latency_uncorrected"`
    TailSamples        []int64           `json:"tail_samples_ns,omitempty"`
    Verified           bool              `json:"verified"`
    BoundFired         string            `json:"bound_fired"`   // "bytes"|"duration"
    Messages           int64             `json:"messages"`
    Recovery           *RecoveryMetrics  `json:"recovery,omitempty"` // Phase 3
}

// RecoveryMetrics is added in Phase 3; its presence bumps CurrentVersion to 2.
type RecoveryMetrics struct {
    TimeToOpenNS         int64 `json:"time_to_open_ns"`
    TimeToFirstReadNS    int64 `json:"time_to_first_read_ns"`
    TimeToFullRecoveryNS int64 `json:"time_to_full_recovery_ns"`
}

type HistogramView struct {
    P50  int64 `json:"p50_ns"`
    P90  int64 `json:"p90_ns"`
    P95  int64 `json:"p95_ns"`
    P99  int64 `json:"p99_ns"`
    P999 int64 `json:"p999_ns"`
    Max  int64 `json:"max_ns"`
}
```

**`series.go`**

```go
// SeriesRow is one per-second observation. Column names are stable;
// the CSV writer emits them in Schema order with a header row.
type SeriesRow struct {
    T      float64 `json:"t"`     // seconds since run start
    Phase  string  `json:"phase"`
    Client ClientCols
    System SystemCols
    Host   HostCols
}

type ClientCols struct {
    MsgsInFlight int64   `json:"msgs_in_flight"`
    Throughput   float64 `json:"throughput_msgs_per_sec"`
    P50NS        int64   `json:"p50_ns"`
    P90NS        int64   `json:"p90_ns"`
    P99NS        int64   `json:"p99_ns"`
}

type SystemCols struct {
    QueueDepth     int64   `json:"queue_depth,omitempty"`
    Segments       int64   `json:"segments,omitempty"`
    SizeBytes      int64   `json:"size_bytes,omitempty"`
    CacheHits      int64   `json:"cache_hits,omitempty"`
    CacheMisses    int64   `json:"cache_misses,omitempty"`
    CacheBytes     int64   `json:"cache_bytes,omitempty"`
    WALChannelFill float64 `json:"wal_channel_fill,omitempty"`
    WALByteFill    float64 `json:"wal_byte_fill,omitempty"`
}

type HostCols struct {
    RSSBytes   int64   `json:"rss_bytes,omitempty"`
    CPUPercent float64 `json:"cpu_percent,omitempty"`
    OpenFDs    int64   `json:"open_fds,omitempty"`
    Threads    int64   `json:"threads,omitempty"`
}
```

**`write.go`**

```go
// WriteResult writes the Result atomically: to <dir>/.result.json.tmp then
// renames over <dir>/result.json, fsyncing the file and its parent dir.
func WriteResult(dir string, r Result) error

// OpenSeries opens <dir>/series.csv for streaming. Each Append issues a
// single unbuffered write() call. Caller must Close.
func OpenSeries(dir string) (*SeriesWriter, error)

type SeriesWriter struct { /* ... */ }

func (w *SeriesWriter) Append(r SeriesRow) error
func (w *SeriesWriter) Close() error
```

**`read.go`**

```go
// ReadResult reads and validates <dir>/result.json. Returns
// ErrEnvelopeVersion if Version != CurrentVersion.
func ReadResult(dir string) (Result, error)

func ReadSeries(dir string) ([]SeriesRow, error)
func IsRun(dir string) bool

var (
    ErrNotARun         = errors.New("not a bench run")
    ErrEnvelopeVersion = errors.New("unsupported envelope version")
)
```

**Design notes.**

- `WriteResult` uses `os.Root`-style confinement (`os.OpenRoot(dir)`) and `root.Rename` so a hostile `dir` cannot escape its parent. This is a public API; treat the input as untrusted.
- `OpenSeries` writes the header row immediately. `Append` uses a single `os.File.Write` per row. This is why "SIGKILL loses nothing" is true — there is no buffering to lose.
- `ReadResult` rejects `Version != CurrentVersion` with `ErrEnvelopeVersion`. `IsRun` checks for both files' existence.

**Tests (`result_test.go`).**

- `TestResultGolden` — a fixed `Result` serializes byte-for-byte to a checked-in JSON file. Use `encoding/json` (Go 1.27 map keys are stable).
- `TestResultRoundTrip` — `WriteResult` → `ReadResult` preserves all fields.
- `TestVersionCheck` — an envelope with `version: 999` returns `ErrEnvelopeVersion`.
- `TestSeriesStreaming` — write 3 rows via `Append`, read back via `ReadSeries`, compare.
- `TestIsRun` — empty dir, dir with only `result.json`, dir with both.

### 3.2 `internal/bench/table.go`

This file is the sole source of workload config. It mirrors `spec.Table`'s contract: every row has a `Doc`, an `EnvName`, a `FlagName`, and an `Apply`.

```go
package bench

// Param is one row in the workload configuration table. Same shape as
// spec.Param but Mode-scoped and applying to *Workload.
type Param struct {
    Key      string
    Kind     spec.Kind
    Modes    []Mode        // nil = all modes
    EnvName  string        // "MIKYU_BENCH_" + UPPER(Key)
    FlagName string        // Key with _→-
    Doc      string
    Default  string        // for help only
    Apply    func(*Workload, spec.Value)
}

// Table is the canonical workload parameter list. Tests require every
// row to carry a non-empty Doc (mirrors spec.Table's contract).
var Table = []Param{ /* 15 rows, see below */ }

// Workload is the resolved, validated workload configuration.
type Workload struct {
    Kind            string
    Queues          int
    MsgSize         int
    Bytes           int64
    Duration        time.Duration
    Rate            uint64
    RateSchedule    []RatePoint
    Workers         int
    Batch           int
    Keys            int
    Preload         int64
    Warmup          time.Duration
    AckRate         float64
    CrashAfterBytes int64
    Seed            uint32
}

type RatePoint struct {
    At   time.Duration
    Rate uint64
}

// Validate checks the Workload in isolation. Cross-field rules from
// options.go are validated separately.
func (w Workload) Validate() error

// View returns the wire view for the envelope.
func (w Workload) View() result.WorkloadView
```

**Table rows (15, in this order):**

| Key | Kind | Modes | Default | Doc |
|---|---|---|---|---|
| `kind` | String (allowed: log, task) | all | `log` | "Queue kind: log or task" |
| `queues` | Int | all | `1` | "Number of queues to spread load across" |
| `msg_size` | Bytes | all | `256` | "Payload size in bytes" |
| `bytes` | Bytes | all | — | "Total bytes to write before stopping" |
| `duration` | Duration | all | — | "Total measure-phase duration" |
| `rate` | Uint32 | produce, mix | `0` | "Target messages/s; 0 = unthrottled" |
| `rate_schedule` | String | produce, mix | — | "Rate schedule: '0:0,30s:100k,60s:10k'" |
| `workers` | Int | all | `4` | "Concurrent client workers" |
| `batch` | Int | all | `1` | "Messages per API call" |
| `keys` | Int | produce, mix | `0` | "Distinct key cardinality; 0 = unkeyed" |
| `preload` | Bytes | consume | `0` | "Bytes written before measurement (populate phase)" |
| `warmup` | Duration | all | `5s` | "Warmup duration excluded from measurement" |
| `ack_rate` | Float | consume, mix | `0.9` | "Fraction of task dequeues that Ack (rest Nack)" |
| `crash_after_bytes` | Bytes | recovery | — | "Bytes written before simulated crash" |
| `seed` | Uint32 | all | `0` | "PRNG seed for payload generation and ack decisions" |

`Validate` enforces:

- `msg_size ≥ 1`, `queues ≥ 1`, `workers ≥ 1`, `batch ≥ 1`, `warmup ≥ 0`
- exactly one of `bytes` / `duration` set (except `recovery`, which requires neither)
- `rate > 0` XOR `rate_schedule` non-empty (both allowed for produce/mix; both unset = unthrottled)
- `rate_schedule` timestamps strictly increasing
- `ack_rate ∈ [0, 1]`
- `keys == 0 || kind == "log"` (task queues cannot be keyed for compaction)

**Tests (`table_test.go`).**

- `TestEveryParamHasDoc` — every row's `Doc` non-empty.
- `TestParamNames` — `EnvName == "MIKYU_BENCH_"+strings.ToUpper(Key)`, `FlagName` derived correctly.
- `TestTableCoversWorkload` — every `Workload` field is reachable from exactly one row's `Apply`.
- `TestValidate` — table-driven, one case per rule violation.

### 3.3 `internal/bench/errors.go` (new) and `errs` update

```go
var (
    ErrAborted         = errors.New("bench run aborted")
    ErrVerify          = errors.New("payload verification failed")
    ErrCorruptResult   = errors.New("bench result is unreadable")
    ErrCompareMismatch = errors.New("bench runs cannot be compared")
    ErrNotARun         = result.ErrNotARun          // re-exported for callers
    ErrEnvelopeVersion = result.ErrEnvelopeVersion  // re-exported
    ErrUsage           = errors.New("bench usage error")
)

// VerifyError carries offset and reason so errors.Is(err, ErrVerify) works.
type VerifyError struct {
    Offset uint64
    Reason string
}

func (e *VerifyError) Error() string {
    return fmt.Sprintf("verify at offset %d: %s", e.Offset, e.Reason)
}
func (e *VerifyError) Unwrap() error { return ErrVerify }
```

Add to `errs.Table`:

```go
{bench.ErrAborted,         0x50},
{bench.ErrVerify,          0x51},
{bench.ErrCorruptResult,   0x52},
{bench.ErrCompareMismatch, 0x53},
```

The existing taxonomy test (`TestErrorTaxonomyTotal`) walks every declared sentinel. It will fail until these four are mapped — that is the point.

### 3.4 `cmd/mikyu/main.go` — exit code 3 for verify failures

```go
func exitCode(err error) int {
    if err == nil {
        return 0
    }
    var ve *bench.VerifyError
    if errors.As(err, &ve) {
        return 3
    }
    if errors.Is(err, daemon.ErrUsage) {
        return 2
    }
    return 1
}
```

Add a `main_test.go` case that a wrapped `*bench.VerifyError` yields 3.

### 3.5 `internal/daemon/spawn.go` (new file in existing package)

```go
// Process is a handle to a spawned daemon subprocess.
type Process struct {
    PID    int
    Target string // resolved target, e.g. "tcp://127.0.0.1:54321"
    cmd    *exec.Cmd
}

// Spawn launches a detached daemon subprocess and returns immediately;
// it does not wait for the daemon to become ready.
func Spawn(opts SpawnOptions) (*Process, error)

// Kill sends SIGKILL (or TerminateProcess on Windows) and reaps.
func (p *Process) Kill() error

// Wait blocks until the process exits.
func (p *Process) Wait() error

// Alive reports whether the process is still running.
func (p *Process) Alive() bool
```

`SpawnOptions` gains a `Target` field that accepts `""`. When empty, `Spawn` picks a free loopback port, passes it as `--target`, and returns the resolved target via `Process.Target`.

`EnsureDaemonRunning` is refactored to call `Spawn` and then poll `IsDaemonAlive`.

**Tests (`spawn_test.go`).** Spawn a real daemon on an ephemeral port. Check `Alive()`. Kill it. Verify exit. Use `t.Cleanup` so a failed test never strands a daemon.

### 3.6 `.go-arch-lint.yml` and audit whitelist

Add the `bench` and `benchview` components. Extend `command`, `cli`, `repl` to include them. In `internal/audit/legacy_test.go`, extend `isSurfaceWhitelisted` to allow `produce` under `internal/bench/` (it is a mode name, not a banned verb) and `"produce"` string literals in `table.go`.

**Acceptance for Phase 1.**

- `go test ./internal/bench/result/... ./internal/bench/... ./internal/errs/... ./internal/daemon/... ./cmd/mikyu/...` passes.
- `go-arch-lint check` passes.
- `internal/audit` tests pass.
- No bench logic yet.

---

## 4. Phase 2 — Runner core (embedded, all modes except recovery)

**Goal.** A complete bench run against an embedded client, producing a valid envelope. `produce`, `consume`, `mix`. All three observation families.

### 4.1 `internal/bench/options.go`

```go
type Mode string
const (
    ModeProduce  Mode = "produce"
    ModeConsume  Mode = "consume"
    ModeMix      Mode = "mix"
    ModeRecovery Mode = "recovery"
)

type Access string
const (
    AccessEmbedded Access = "embedded"
    AccessDaemon   Access = "daemon"
    AccessRemote   Access = "remote"
)

// Options is the fully resolved configuration for one run.
type Options struct {
    Mode           Mode
    Access         Access
    Target         string   // remote target; empty for embedded/daemon
    Dir            string   // data dir; required for embedded and daemon
    Out            string   // output dir; default "./bench"
    Persist        bool
    NoCleanupStale bool
    Quiet          bool
    Verify         bool
    CPU            int      // GOMAXPROCS override; 0 = leave alone
    Metrics        []string // Phase 4
    Workload       Workload
    Spec           spec.Spec
    Progress       func(ProgressEvent)
    Stderr         io.Writer
    // DisableObservation is test-only; it hides the observer goroutine
    // so the self-perturbation test can compare.
    DisableObservation bool
}

// ProgressKind classifies a progress event.
type ProgressKind int
const (
    ProgressStart ProgressKind = iota
    ProgressTick
    ProgressEnd
)

// ProgressEvent is emitted by the runner as phases progress.
type ProgressEvent struct {
    Kind    ProgressKind
    Phase   Phase
    Elapsed time.Duration
    Bytes   int64
    Total   int64  // 0 if unknown
}

// Resolve builds Options from flags, file, and env, in that precedence order.
func Resolve(flags map[string]string, file string, env []string) (Options, error)
```

`Resolve` uses `bench.Table` for workload flags and `spec.Table` for queue-config flags. A scenario file has two YAML blocks:

```yaml
spec:
  storage:
    durability: async
workload:
  kind: log
  msg_size: 1024
  duration: 5s
  workers: 8
```

`spec.DecodeSpecYAML` handles `spec:`. A new `decodeWorkloadYAML` handles `workload:`.

**Tests.**

- `TestFlagRoundTrip` — every row parses to `Workload`, prints back identical.
- `TestPrecedence` — flag > file > env > default.
- `TestScenarioFile` — resolves to merged `spec.Spec`.
- `TestMalformedYAML` — returns `ErrUsage`.

### 4.2 `internal/bench/phases.go`

```go
type Phase string
const (
    PhasePopulate Phase = "populate"
    PhaseWarmup   Phase = "warmup"
    PhaseMeasure  Phase = "measure"
    PhaseDrain    Phase = "drain"
    PhaseAbort    Phase = "abort"
)

// phaseSequence returns the phases a mode runs, in order.
func phaseSequence(m Mode, preload int64) []Phase

// boundary tracks a phase's start time and byte count.
type boundary struct {
    phase Phase
    start time.Time
    bytes int64
}
```

Rules:

- `produce`, `mix`: `[warmup, measure, drain]`.
- `consume`: `[populate?, warmup, measure, drain]` — `populate` only when `Preload > 0`. If `Preload == 0`, emit one line to `Stderr` noting no pre-population.
- `recovery`: `[populate, abort]` (Phase 3).
- On `ctx` cancellation or a `VerifyError`, append an `abort` row to the series before returning.

### 4.3 `internal/bench/access.go`

```go
// openClient opens a client per Access. Returns the client, a cleanup
// func, and the run's AccessTarget for the envelope.
func openClient(ctx context.Context, opts Options) (mikyu.Client, func() error, string, error)
```

- `AccessEmbedded`: `mikyu.Open(opts.Dir)`.
- `AccessDaemon`: Phase 3. For now, `errors.New("not implemented")`.
- `AccessRemote`: Phase 3. Same.

### 4.4 `internal/bench/measure.go`

Two public types: `Histogram` and `Measure`.

```go
// Histogram is a log-linear histogram. Bounds 1ns to 1h, 3 significant
// figures. Zero value is ready to use. Safe for concurrent use.
type Histogram struct { /* ... */ }

func (h *Histogram) Record(v time.Duration)
func (h *Histogram) Merge(other *Histogram)
func (h *Histogram) Percentile(p float64) time.Duration
func (h *Histogram) Max() time.Duration
func (h *Histogram) Len() int64

// Observation is the token returned by Start.
type Observation struct {
    scheduled time.Time // when the send was scheduled
    started   time.Time // when we actually called Start
}

// Measure accumulates client-side latency for one run. Safe for concurrent use.
type Measure struct { /* ... */ }

func NewMeasure() *Measure

// Start records the intended send time and returns a token for Finish that
// carries scheduled + actual times; closed-loop callers use Record instead.
func (m *Measure) Start(scheduled time.Time) Observation

// Finish records one open-loop observation: corrected gets (now - obs.scheduled),
// uncorrected gets (now - obs.started).
func (m *Measure) Finish(obs Observation)

// Record is the closed-loop path: latency is measured directly and
// lands only in the uncorrected histogram.
func (m *Measure) Record(latency time.Duration)

// Snapshot returns corrected + uncorrected histograms and the tail
// samples. It does not reset; callers may call it repeatedly.
func (m *Measure) Snapshot() result.MetricsView

// TimerOverhead returns the package-init measured time.Now() cost.
func TimerOverhead() time.Duration
```

**Design amendment.** The original plan had `Start` return `time.Time` and `Finish` take it. That is ambiguous: it cannot populate the uncorrected histogram for open-loop runs. This plan uses an `Observation` token. If you disagree, amend the plan — do not improvise.

**Histogram layout.** 3-significant-figure log-linear buckets over `[1ns, 1h]`. Precompute the bucket array once at package init; share it read-only across all histograms. Per-histogram state is a `[]atomic.Uint64` of counts. Merge sums the counts.

**Tail samples.** Keep the top 0.1% by latency, capped at 4096 entries, in a min-heap. `Snapshot` copies them out as nanoseconds into `MetricsView.TailSamples`.

**Timer overhead.** At package init, benchmark `time.Now()` 1000 times and store the average. Report it in the envelope so results can be corrected for it.

**Tests.**

- `TestHistogramAccuracy` — 1M known-value samples; p50/p99 within 1%.
- `TestCOCorrection` — simulate a slow consumer; corrected p99 > uncorrected.
- `TestConcurrentRecord` — 100 goroutines, no race, snapshot validates.
- `TestTimerOverhead` — non-zero and < 1µs.

### 4.5 `internal/bench/verify.go`

```go
// payloadFor fills dst in place with the deterministic payload for offset:
// first 8 bytes are a fnv64 of the offset, the rest pseudo-random from seed.
func payloadFor(seed uint32, offset uint64, size int, dst []byte)

// verifyPayload returns nil if payload matches what payloadFor would
// produce; otherwise returns a *VerifyError.
func verifyPayload(seed uint32, offset uint64, payload []byte) error
```

Use `math/rand/v2` seeded from `(seed, offset)`.