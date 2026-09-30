# mikyu bench — Implementation Plan

This plan adds a benchmark tool to mikyu. The tool ships with the binary, mimics the vocabulary of OMB / nats-bench / iggy-bench, and produces honest, complete, self-describing result artifacts.

**A finished run looks like this:**

```
$ mikyu bench produce --dir /tmp/mikyu --duration 5s --workers 8

[measure] ========== 100%  5.0s  1.2M msgs  1.2 GiB  240K msg/s  245 MiB/s

bench-1727619830123456789  produce  embedded
────────────────────────────────────────────────────────────
spec       log · sync · 64MiB segment · 32MiB cache
workload   256 B × 1 GiB · 8 workers · batch=1
────────────────────────────────────────────────────────────
throughput 240K msg/s · 245 MiB/s
latency    p50 42µs · p90 180µs · p99 1.2ms · max 8.1ms
verified   no
bound      duration
cache      warm
────────────────────────────────────────────────────────────
written    ./bench/bench-1727619830123456789
```

---

## 1. Vocabulary and invariants

Learn these words before writing any code.

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
8. **Errors are stable sentinels.** Every bench error lives in `internal/bench/errors.go`. Bench errors are client-side, never cross the wire, and therefore carry no wire code. Never return a bare `errors.New` from bench code.
9. **Layer purity.** `bench` imports only `spec`, `metrics`, `mikyu`, `daemon`, `human`, `result`. `.go-arch-lint.yml` fails the build otherwise.
10. **No goroutine leaks.** Every goroutine has a documented stop condition. `goleak.VerifyTestMain` guards both bench packages.
11. **One comment line per symbol.** `TestInternalCommentStyle` enforces ≤1 line on every exported symbol and package doc under `internal/`. The root `mikyu` package is exempt; `internal/bench` is not.

### Layering (added edges only)

```
bench       →  spec, metrics, mikyu, daemon, human, result
benchview   →  result, spec, style, human
command     →  ..., bench, benchview
cli, repl   →  ..., bench, benchview
cmd         →  ..., bench
```

Add `bench`, `benchview`, and `result` components to `.go-arch-lint.yml`; extend `command`, `cli`, `repl` to depend on `bench` and `benchview`; extend `cmd` to depend on `bench` so the binary's `exitCode` can recognize `*bench.VerifyError`.

### Read these existing files before starting

- `internal/spec/table.go` — the `Param` shape `bench.Param` mirrors.
- `internal/spec/codec.go` — `Duration` encoding, `CheckSpecVersion`.
- `internal/node/pool.go` — the `.bench-runs` reconciliation pattern.
- `internal/engine/sync_dir.go` — `SyncDirHook` and the four durability sites.
- `internal/audit/legacy_test.go` — the banned-identifier list.
- `cmd/mikyu/main.go` — the existing 0/1/2 exit-code contract.

---

## 2. File layout (final state)

```
internal/bench/
  bench.go            Run entry point; phase orchestration
  options.go          Options struct
  table.go            Param rows + Workload value + Validate + View
  scenario.go         YAML scenario loader (mode/spec/workload blocks)
  access.go           Access enum; client opener (embedded/daemon/remote)
  lifecycle.go        .bench-runs, stale reconciliation, queue naming
  phases.go           Phase enum + phase sequences + boundary tracking
  measure.go          Histogram, Measure (CO correction), tail samples
  observe.go          Per-second sampler (client+system+host)
  host.go             Host stats dispatcher (platform-agnostic)
  host_linux.go       /proc readers
  host_darwin.go      getrusage + /dev/fd readers
  host_windows.go     GetProcessMemoryInfo etc.
  host_other.go       Zeros for unsupported platforms
  verify.go           PRNG payload generation + inline verify
  envelope.go         Envelope type; wraps result writers
  errors.go           Sentinels + VerifyError
  mode_produce.go     Produce mode
  mode_consume.go     Consume mode
  mode_mix.go         Mix mode
  mode_recovery.go    Recovery mode
  proc_posix.go       spawnDetached, kill (Unix)
  proc_windows.go     spawnDetached, kill (Windows)

internal/bench/result/
  result.go           Result, PhaseView, EnvironmentView, MetricsView
  series.go           SeriesRow, ClientCols, SystemCols, HostCols
  write.go            WriteResult (atomic), OpenSeries (streaming)
  read.go             ReadResult, ReadSeries, IsRun
  version.go          CurrentVersion = 1

internal/ui/benchview/
  summary.go          Renders a Result summary
  show.go             Load + summary
  compare.go          Multi-run diff renderer
  plot.go             series.csv → terminal chart

internal/ui/command/
  bench.go            BenchFlagSet, RunBench, ResultPresenter
  verb.go             Verb.Subs + recursive lookup

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
    Version      int             `json:"version"`
    RunID        string          `json:"run_id"`
    Mode         string          `json:"mode"`
    Access       string          `json:"access"`
    AccessTarget string          `json:"access_target,omitempty"`
    StartedAt    time.Time       `json:"started_at"`
    EndedAt      time.Time       `json:"ended_at,omitzero"`
    Complete     bool            `json:"complete"`
    Aborted      string          `json:"aborted,omitempty"`
    Spec         spec.Spec       `json:"spec"`
    Workload     WorkloadView    `json:"workload"`
    Environment  EnvironmentView `json:"environment"`
    Phases       []PhaseView     `json:"phases"`
    Metrics      *MetricsView    `json:"metrics,omitempty"`
    Notes        []string        `json:"notes,omitempty"`
}

type WorkloadView struct {
    Kind         string      `json:"kind"`         // "log" | "task"
    Queues       int         `json:"queues"`
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
    Seed         uint32      `json:"seed"`
}

type RatePoint struct {
    At   string `json:"at"`   // duration string, e.g. "30s"
    Rate uint64 `json:"rate"` // messages/s
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
    CacheState      string `json:"cache_state"` // "cold"|"warm"|"unknown"
    ColdVerified    bool   `json:"cold_verified,omitempty"`
    TimerOverheadNS int64  `json:"timer_overhead_ns,omitempty"`
}

type PhaseView struct {
    Name     string `json:"name"` // "populate"|"warmup"|"measure"|"drain"|"abort"
    Duration string `json:"duration"`
    Bytes    int64  `json:"bytes"`
}

type MetricsView struct {
    ThroughputMsgs     float64          `json:"throughput_msgs_per_sec"`
    ThroughputBytes    float64          `json:"throughput_bytes_per_sec"`
    LatencyCorrected   HistogramView    `json:"latency_corrected"`
    LatencyUncorrected HistogramView    `json:"latency_uncorrected"`
    TailSamplesNS      []int64          `json:"tail_samples_ns,omitempty"`
    Verified           bool             `json:"verified"`
    BoundFired         string           `json:"bound_fired"` // "bytes"|"duration"|"crash"
    Messages           int64            `json:"messages"`
    Recovery           *RecoveryMetrics `json:"recovery,omitempty"`
}

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
    T      float64 `json:"t"` // seconds since run start
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
// Creates dir if missing. Uses os.OpenRoot(dir) so a hostile dir cannot escape.
func WriteResult(dir string, r Result) error

// OpenSeries opens <dir>/series.csv for streaming. Writes the header row
// immediately. Each Append issues a single unbuffered write() call.
// Caller must Close.
func OpenSeries(dir string) (*SeriesWriter, error)

type SeriesWriter struct { /* os.File + schema */ }

func (w *SeriesWriter) Append(r SeriesRow) error
func (w *SeriesWriter) Close() error
```

**`read.go`**

```go
// ReadResult reads and validates <dir>/result.json. Returns
// ErrEnvelopeVersion if Version != CurrentVersion.
func ReadResult(dir string) (Result, error)

// ReadSeries reads all rows from <dir>/series.csv.
func ReadSeries(dir string) ([]SeriesRow, error)

// IsRun reports whether dir contains a bench run (both files present).
func IsRun(dir string) bool

var (
    ErrNotARun         = errors.New("not a bench run")
    ErrEnvelopeVersion = errors.New("unsupported envelope version")
)
```

**Design notes.**

- `WriteResult` uses `os.OpenRoot(dir)` and `root.Rename` so a hostile `dir` cannot escape its parent.
- `OpenSeries` writes the header row immediately. `Append` uses a single `os.File.Write` per row. This is why "SIGKILL loses nothing" is true — there is no buffering to lose.
- `ReadResult` rejects `Version != CurrentVersion` with `ErrEnvelopeVersion`. `IsRun` checks for both files' existence.
- Series CSV column order: `t,phase,client_msgs_in_flight,client_throughput_msgs_per_sec,client_p50_ns,client_p90_ns,client_p99_ns,system_queue_depth,system_segments,system_size_bytes,system_cache_hits,system_cache_misses,system_cache_bytes,system_wal_channel_fill,system_wal_byte_fill,host_rss_bytes,host_cpu_percent,host_open_fds,host_threads`. Header is a single row; empty values are `0`.

**Tests (`result_test.go`).**

- `TestResultGolden` — a fixed `Result` serializes byte-for-byte to a checked-in JSON file.
- `TestResultRoundTrip` — `WriteResult` → `ReadResult` preserves all fields.
- `TestVersionCheck` — an envelope with `version: 999` returns `ErrEnvelopeVersion`.
- `TestSeriesStreaming` — write 3 rows via `Append`, read back via `ReadSeries`, compare.
- `TestIsRun` — empty dir, dir with only `result.json`, dir with both.

### 3.2 `internal/bench/table.go`

Sole source of workload config. Mirrors `spec.Table`'s contract: every row has a `Doc`, an `EnvName`, a `FlagName`, and an `Apply`.

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

**Table rows (15, in this exact order):**

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
- exactly one of `bytes` / `duration` set, except `recovery` (which uses neither and requires `crash_after_bytes`)
- not both `rate > 0` and `rate_schedule` non-empty (either alone is fine; both unset = unthrottled)
- `rate_schedule` timestamps strictly increasing, first must be at `0s`
- `ack_rate ∈ [0, 1]`
- `keys == 0 || kind == "log"` (task queues cannot be keyed for compaction)

**`rate_schedule` parsing.** The CLI accepts `"0:0,30s:100k,60s:10k"` (duration:rate pairs, suffix `k` = 1000, `M` = 1,000,000). The YAML form is a list of single-entry maps:

```yaml
rate_schedule:
  - 0s: 0
  - 30s: 100000
  - 60s: 10000
```

Both forms parse to `[]RatePoint`. A `rate_schedule` parser lives in `scenario.go`.

**Tests (`table_test.go`).**

- `TestEveryParamHasDoc` — every row's `Doc` non-empty.
- `TestParamNames` — `EnvName == "MIKYU_BENCH_"+strings.ToUpper(Key)`, `FlagName` derived correctly.
- `TestTableCoversWorkload` — every field of `Workload` is reachable from exactly one row's `Apply`.
- `TestValidate` — table-driven, one case per rule violation.
- `TestRateScheduleParse` — string form and YAML form produce identical `[]RatePoint`.

### 3.3 `internal/bench/errors.go` (new)

```go
package bench

var (
    ErrAborted         = errors.New("bench run aborted")
    ErrVerify          = errors.New("payload verification failed")
    ErrCorruptResult   = errors.New("bench result is unreadable")
    ErrCompareMismatch = errors.New("bench runs cannot be compared")
    ErrUsage           = errors.New("bench usage error")
    ErrNotARun         = result.ErrNotARun
    ErrEnvelopeVersion = result.ErrEnvelopeVersion
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

Bench errors stay within `internal/bench`. They never cross the wire and never appear in `errs.Table`, which keeps `errs` free of any `internal/bench` import and preserves the acyclic layering of the binary. `TestErrorTaxonomyTotal` does not scan `internal/bench`, so no update to that test is required.

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
    if errors.Is(err, daemon.ErrUsage) || errors.Is(err, bench.ErrUsage) {
        return 2
    }
    return 1
}
```

Update `main_test.go` with a case that a wrapped `*bench.VerifyError` yields 3.

### 3.5 `internal/daemon/spawn.go` (new file in existing package)

```go
// Process is a handle to a spawned daemon subprocess.
type Process struct {
    PID    int
    Target string // resolved target, e.g. "tcp://127.0.0.1:54321"
    cmd    *exec.Cmd
}

// Spawn launches a detached daemon subprocess and returns immediately;
// it does not wait for the daemon to become ready. When opts.Target is
// empty, Spawn picks a free loopback port, passes it as --target, and
// returns the resolved target via Process.Target.
func Spawn(opts SpawnOptions) (*Process, error)

// Kill sends SIGKILL (or TerminateProcess on Windows) and reaps.
func (p *Process) Kill() error

// Wait blocks until the process exits.
func (p *Process) Wait() error

// Alive reports whether the process is still running.
func (p *Process) Alive() bool
```

`SpawnOptions` already carries a `Target` field; its semantics extend so that the empty string selects ephemeral loopback binding.

`EnsureDaemonRunning` is refactored to call `Spawn` and then poll `IsDaemonAlive`.

**Tests (`spawn_test.go`).** Spawn a real daemon on an ephemeral port. Check `Alive()`. Kill it. Verify exit. Use `t.Cleanup` so a failed test never strands a daemon. This requires a helper-process pattern: the test binary re-execs itself with `daemon` as the first arg when an env var is set, mirroring `internal/daemon/goleak_test.go`'s existing `daemonHelperEnv` pattern.

### 3.6 `.go-arch-lint.yml` and audit gates

Add:

```yaml
bench:
  mayDependOn: [spec, metrics, mikyu, daemon, human, result]
benchview:
  mayDependOn: [result, spec, style, human]
result:
  mayDependOn: [spec]
```

Extend `command`/`cli`/`repl` to include `bench`, `benchview`. Extend `cmd` to include `bench`.

`internal/audit/legacy_test.go` bans the identifier `Produce`. The bench's `ModeProduce` constant and the string literal `"produce"` are not identifier matches and do not trip the check, so no whitelist change is needed there. `TestNamingLaw` in `mikyu/surface_test.go` scans verb names for banned substrings such as `"produce"`; extend that scan to recurse into `Verb.Subs` and to recognize the `produce` subcommand under `bench` as the accepted spelling.

**Acceptance for Phase 1.**

- `go test ./internal/bench/result/... ./internal/bench/... ./internal/errs/... ./internal/daemon/... ./cmd/mikyu/...` passes.
- `go-arch-lint check` passes.
- `internal/audit` tests pass.
- No bench logic yet.

---

## 4. Phase 2 — Runner core (embedded, produce/consume/mix)

**Goal.** A complete bench run against an embedded client, producing a valid envelope. All three modes. All three observation families.

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

// Options is the fully resolved configuration for one run. The command
// layer populates this from flags and a scenario file before calling Run.
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
    Cold           bool     // Phase 4; Phase 2 ignores
    CPU            int      // GOMAXPROCS override; 0 = leave alone
    Metrics        []string // Phase 4; Phase 2 ignores
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
```

The command layer's `BenchFlagSet` builds flags from `bench.Table` and `spec.Table`, then calls `bench.Run`. `Options` is populated directly by the flag parser; there is no separate `Resolve` function.

Scenario files: a `--file scenario.yaml` flag is not implemented in Phase 2. If the user wants file-driven config, they set env vars (`MIKYU_BENCH_*`) or pass flags. Scenario file support is Phase 6 (`ui/command/bench.go`).

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

// emitProgress sends a ProgressEvent to opts.Progress, or drops it if nil.
func emitProgress(opts Options, e ProgressEvent)
```

Rules:

- `produce`, `mix`: `[warmup, measure, drain]`.
- `consume`: `[populate?, warmup, measure, drain]` — `populate` only when `Preload > 0`. If `Preload == 0`, emit one line to `Stderr` noting no pre-population: `note: consuming from an empty queue; use --preload to measure against a backlog`.
- `recovery`: `[populate, abort]` (Phase 3).
- On `ctx` cancellation or a `VerifyError`, append an `abort` row to the series before returning.

### 4.3 `internal/bench/access.go`

```go
// accessHandle carries the client opened for a run and everything the
// runner needs to describe and close it.
type accessHandle struct {
    Client    mikyu.Client
    Cleanup   func() error
    Target    string // resolved target for the envelope
    DaemonPID int    // 0 when the run owns no daemon
}

// openClient opens a client per Access. Before opening an embedded or
// daemon client it kills any stale daemon PID recorded in the run
// directory's .bench-runs, so the lock is released before this run takes it.
// It never creates queues; the runner does that after lifecycle.acquire.
func openClient(ctx context.Context, opts Options) (*accessHandle, error)
```

- `AccessEmbedded`: `mikyu.Open(opts.Dir)`.
- `AccessDaemon`: Phase 3. For now, `errors.New("daemon access is implemented in Phase 3")`.
- `AccessRemote`: Phase 3. Same.

The runner creates the run's queues after `lifecycle.acquire` writes the ownership entry, so a crash between record-writing and queue-creation cannot leave untracked queues behind.

### 4.4 `internal/bench/measure.go`

Two public types: `Histogram` and `Measure`.

```go
// Histogram is a log-linear histogram. Bounds 1ns to 1h, ~10000 buckets
// (~0.3% resolution). Zero value is ready to use. Safe for concurrent use.
type Histogram struct { /* []atomic.Uint64 + bounds + sum + total */ }

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
// carries scheduled + actual times. Closed-loop callers use Record instead.
func (m *Measure) Start(scheduled time.Time) Observation

// Finish records one open-loop observation: corrected gets (now - obs.scheduled),
// uncorrected gets (now - obs.started).
func (m *Measure) Finish(obs Observation)

// Record is the closed-loop path: latency is measured directly and lands
// only in the uncorrected histogram.
func (m *Measure) Record(latency time.Duration)

// Snapshot returns corrected + uncorrected histograms and the tail
// samples. It does not reset; callers may call it repeatedly.
func (m *Measure) Snapshot() result.MetricsView

// TimerOverhead returns the package-init measured time.Now() cost.
func TimerOverhead() time.Duration
```

**Histogram bucketing.** At package init, precompute bounds:

```go
const histogramBuckets = 10000

var histogramBounds = func() []float64 {
    const minNs = 1.0
    const maxNs = float64(time.Hour) // 3.6e12 ns
    ratio := math.Pow(maxNs/minNs, 1.0/float64(histogramBuckets-1))
    b := make([]float64, histogramBuckets)
    v := minNs
    for i := range b {
        b[i] = v
        v *= ratio
    }
    return b
}()
```

Per-histogram state is `counts []atomic.Uint64` (length `histogramBuckets+1`, one extra for the overflow tail). `Record` uses `slices.BinarySearch(histogramBounds, float64(v.Nanoseconds()))`, adds to `counts[i]`, and atomically accumulates sum and total.

`Percentile(p)` walks the cumulative counts to find the bucket containing rank `p*total`, then linearly interpolates between `bounds[i-1]` and `bounds[i]`. This is approximate; the accuracy test requires ≤5% error at p50/p99 on a 1M-sample uniform-in-log distribution. If that fails, increase to 20000 buckets.

**Tail samples.** Keep the top 0.1% by latency, capped at 4096 entries, in a min-heap (use `container/heap`). `Snapshot` copies them out as nanoseconds into `MetricsView.TailSamplesNS`.

**Timer overhead.** At package init, benchmark `time.Now()` 1000 times and store the average in a package variable `timerOverheadNS`. `TimerOverhead()` returns it as a `time.Duration`.

**Tests.**

- `TestHistogramAccuracy` — 1M samples from a known distribution; p50/p99 within 5%.
- `TestCOCorrection` — simulate a slow consumer; corrected p99 > uncorrected.
- `TestConcurrentRecord` — 100 goroutines, no race, snapshot validates.
- `TestTimerOverhead` — non-zero and < 1µs.

### 4.5 `internal/bench/verify.go`

```go
// payloadFor fills dst in place with the deterministic payload for offset.
// First 8 bytes are a big-endian fnv64a of the offset; the rest is filled
// from a per-(seed,offset) PRNG stream.
func payloadFor(seed uint32, offset uint64, dst []byte)

// verifyPayload returns nil if payload matches what payloadFor would
// produce; otherwise returns a *VerifyError.
func verifyPayload(seed uint32, offset uint64, payload []byte) error
```

Implementation:

- `fnv64a(offset)` — standard FNV-1a over the 8-byte big-endian encoding of `offset`. Store in `dst[0:8]`.
- The remaining `len(dst)-8` bytes come from `rand.NewPCG(uint64(seed), offset)` seeded with `rand.NewPCG(uint64(seed), offset^0x9E3779B97F4A7C15)`, filled bytewise. Any deterministic choice works as long as it is documented; the tests pin the exact bytes.
- `verifyPayload` regenerates into a scratch buffer and byte-compares.

Use `math/rand/v2`. The exact seed-combination formula is contractual: write it once in the file's doc comment and never change it.

**Tests.**

- `TestPayloadDeterministic` — same seed+offset yields identical bytes.
- `TestVerifyCatchesCorruption` — a flipped byte fails.
- `TestVerifyOverhead` — 1M verifications of a 256-byte payload in < 100ms.

### 4.6 `internal/bench/host.go` and platform files

```go
// HostStats is a point-in-time host-level observation.
type HostStats struct {
    RSSBytes   int64
    VSZBytes   int64
    CPUPercent float64
    OpenFDs    int64
    Threads    int64
}

// readHostStats reads the current process's stats. Results are cached for
// up to one second between calls. A first call returns CPUPercent 0.
func readHostStats() HostStats
```

Platform files: `host_linux.go`, `host_darwin.go`, `host_windows.go`, `host_other.go`. Each implements a private `readPlatform() HostStats`. The dispatcher in `host.go` caches the result with a 1-second TTL guarded by a `sync.Mutex`.

- Linux: `/proc/self/status` (VmRSS, VmSize, Threads), `/proc/self/stat` (utime+stime), `/proc/self/fd` count.
- Darwin: `getrusage(RUSAGE_SELF).Maxrss` (bytes on Darwin), `/dev/fd` count.
- Windows: `GetProcessMemoryInfo`, `GetProcessTimes`, `GetProcessHandleCount`, toolhelp thread snapshot.
- Other: return zeros.

CPU percent derives from cumulative CPU seconds vs wall clock; the first sample establishes the baseline and returns 0. Exactly mirrors `internal/node/runtime_source.go`'s `cpuPercent`.

**Tests.** `TestReadHostStats` verifies non-zero RSS on Linux/Darwin and non-negative values elsewhere.

### 4.7 `internal/bench/lifecycle.go`

```go
// benchRuns is the JSON shape of <dir>/.bench-runs.
type benchRuns struct {
    Runs []runEntry `json:"runs"`
}
type runEntry struct {
    RunID     string     `json:"run_id"`
    StartedAt time.Time  `json:"started_at"`
    Queues    []queueRef `json:"queues"`
    DaemonPID int        `json:"daemon_pid,omitempty"`
}
type queueRef struct {
    Name string `json:"name"`
    Kind string `json:"kind"`
}

// acquire registers a new run in <dir>/.bench-runs after reconciling stale
// entries through the supplied client. It removes orphaned queues from
// previous runs and kills orphaned daemon PIDs, then appends the current
// run's entry. Returns a release func that removes the entry. Creates dir
// if missing.
func acquire(ctx context.Context, client mikyu.Client, dir, runID string, queues []queueRef, daemonPID int, cleanupStale bool, log func(string)) (release func() error, err error)

// reconcileStale removes orphaned queues and daemons recorded in the file
// using the supplied client. Called from acquire before adding the new entry.
func reconcileStale(ctx context.Context, client mikyu.Client, dir string, log func(string)) error

// killStaleDaemons reads <dir>/.bench-runs and sends SIGTERM to any daemon
// PID recorded there. Best-effort; a missing or unkillable PID is not fatal.
// Called by openClient before it takes the directory, so a previous daemon
// has released its lock before this run opens its own.
func killStaleDaemons(dir string, log func(string)) error

// generateRunID returns "bench-<unix-nanos>".
func generateRunID() string

// queueNames returns the names for a run: ["bench-<id>-0", ...].
func queueNames(runID string, count int) []string
```

**File format** is a plain JSON array of `runEntry`. `<dir>/.bench-runs` is written atomically via `os.OpenRoot(dir)` + temp + rename, mirroring `internal/node/registry.go`. A separate advisory lock file, `<dir>/.bench-runs.lock`, guards concurrent bench invocations; it is distinct from `mikyu.lock`, which is held by whatever node actually owns the directory.

**Reconciliation.** For each entry in `.bench-runs`:

- For each queue in `entry.Queues`: attempt `client.DeleteQueue(ctx, name)`. A `ErrQueueNotFound` is fine; log `cleaned: queue <name> (stale)` for successes and nothing for misses.
- If `entry.DaemonPID != 0`: on Unix, `os.FindProcess(pid)` then `process.Signal(syscall.Signal(0))` to test aliveness. If alive, best-effort SIGTERM; log if it does not die within 5s. On Windows, use `OpenProcess(PROCESS_TERMINATE)` and `TerminateProcess`.
- Remove the entry from `.bench-runs`.

`--no-cleanup-stale` skips reconciliation but still records the current run's entry.

**Tests.**

- `TestAcquireRelease` — file empty after a normal lifecycle.
- `TestStaleReconcile` — pre-seed a `.bench-runs` with a nonexistent queue, verify cleanup runs and entries are removed.
- `TestQueueNames` — `queueNames("bench-123", 3)` returns `["bench-123-0","bench-123-1","bench-123-2"]`.

### 4.8 `internal/bench/envelope.go`

```go
// envelope manages the streaming write of result.json + series.csv for
// one run. It is not safe for concurrent use; callers serialize.
type envelope struct {
    dir    string
    r      result.Result
    series *result.SeriesWriter
    notes  []string
}

func newEnvelope(dir string, r result.Result) (*envelope, error)

// AppendRow writes one series row. Called once per second.
func (e *envelope) AppendRow(row result.SeriesRow) error

// AddNote records a note to be included in the final result.json.
func (e *envelope) AddNote(note string)

// Finish atomically rewrites result.json with the final metrics and
// closes series.csv.
func (e *envelope) Finish(m *result.MetricsView, phases []result.PhaseView, aborted string) error
```

`newEnvelope` writes `result.json` with `Complete: false, Aborted: ""` and opens `series.csv`. `Finish` sets `Complete: true` (or leaves false when `aborted != ""`), fills `Metrics` and `Phases`, then calls `result.WriteResult` and `series.Close()`.

**Tests.** `TestEnvelopeStreaming` — spawn a subprocess that runs a produce loop, SIGKILL it mid-run, verify the partial file is valid and readable by `ReadResult`/`ReadSeries`.

### 4.9 `internal/bench/bench.go`

```go
// Run executes one bench run. Returns the final Result.
func Run(ctx context.Context, opts Options) (result.Result, error)
```

Flow:

1. If `opts.CPU > 0`, call `runtime.GOMAXPROCS(opts.CPU)`.
2. Validate `opts.Workload.Validate()`. Validate `opts.Spec` via `spec.ValidateCreate(spec.Resolve(opts.Spec))`.
3. Resolve access. Require `--dir` for embedded/daemon, `--target` for remote. Refuse `--dir` and `--target` together.
4. Determine `runID := generateRunID()`, `queueNames := queueNames(runID, opts.Workload.Queues)`.
5. `handle := openClient(ctx, opts)`. The client is opened before `.bench-runs` is touched because reconciliation needs to delete stale queues. Defer `handle.Cleanup`.
6. If `opts.Access != AccessRemote`, call `lifecycle.acquire(ctx, handle.Client, opts.Dir, runID, queueRefs, handle.DaemonPID, !opts.NoCleanupStale, log)`; defer release.
7. Create each queue via `handle.Client` with the workload's kind and `opts.Spec`. A create failure aborts before the envelope opens.
8. Build `result.Result{Version: result.CurrentVersion, RunID, Mode: string(opts.Mode), Access: string(opts.Access), AccessTarget: handle.Target, StartedAt: time.Now(), Spec: opts.Spec, Workload: opts.Workload.View(), Environment: buildEnvironment(opts)}`.
9. `newEnvelope(opts.Out, r)`. Defer `finishEnvelope` with any error captured in a named return.
10. If `!opts.DisableObservation`, start observer (Phase 2/4).
11. Start `Measure`. If `opts.Cold`, drop cache (Phase 4).
12. Dispatch to `runProduce`/`runConsume`/`runMix`/`runRecovery`. The mode fills the phases, produces the envelope rows, and returns.
13. After the mode returns, stop the observer, collect the final `Measure.Snapshot()`, collect phase views, and finalize the envelope.
14. If the mode returned a `*VerifyError`, propagate it.
15. Call cleanup (daemon kill, client close) — order: stop observer, stop measures, close client, then daemon.
16. Call lifecycle release.

The full `Run` is roughly 150 lines. It is the only function that sees the whole lifecycle.

### 4.10 `internal/bench/observe.go`

```go
// observer samples client + system + host once per second and appends to
// the envelope. One goroutine, one ticker, started by Run and stopped
// before envelope finalization.
type observer struct { /* ... */ }

func newObserver(client mikyu.Client, env *envelope, m *Measure, phase func() Phase, queues []string, stderr io.Writer) *observer
func (o *observer) Start(ctx context.Context)
func (o *observer) Stop()
```

Tick behavior:

1. Compute `t = time.Since(o.start).Seconds()`.
2. Read the client's last N seconds of samples from `Measure.Snapshot()` — but only the deltas. To do this cleanly, `Measure` exposes a `SnapshotSince(time.Time)` method that returns metrics for samples recorded since the timestamp. (Add this method; it is required by the observer.)
3. Compute throughput from the delta of `Messages`.
4. Read `Client.Metrics(ctx)`. Extract the run's queues and aggregate their system-side counters and gauges. The queue names are `queueNames` from `lifecycle`.
5. Call `readHostStats()`.
6. Build `result.SeriesRow`. `AppendRow`.

The observer's own `Metrics(ctx)` call latency is recorded in a small internal histogram; on Stop it is added to the envelope as a note. Skip the call entirely when `opts.DisableObservation` is set.

**Tests.** `TestObserverStop` — start an observer, verify the goroutine exits within 1 second of Stop. Guarded by `goleak`.

### 4.11 `internal/bench/mode_produce.go`

```go
func runProduce(ctx context.Context, r *runner) error
```

`runner` is an internal struct holding all resolved state: `opts Options`, `client mikyu.Client`, `env *envelope`, `measure *Measure`, `logQueues []mikyu.LogQueue`, `taskQueues []mikyu.TaskQueue`, `queueNames []string`, `phases []Phase`.

Produce loop:

- If `opts.Workload.Rate == 0` and `RateSchedule` empty: closed-loop. Each worker loops appending `batch` messages until `bytes` or `duration` fires.
- If `Rate > 0`: open loop. A scheduler goroutine dispatches offsets at the configured rate; workers pull from a channel.
- If `RateSchedule` set: open loop with a time-varying rate. The scheduler consults the schedule via linear interpolation.

Phase transitions are recorded by pushing `PhaseView` entries into `r.phases`. Bytes are counted cumulatively by the mode.

Warmup and drain:

- Before warmup: nothing (produce is the first phase).
- During warmup: append, but `Measure.Record` is suppressed. Count bytes.
- During measure: append, record.
- Drain: after `bytes` or `duration` fires, wait for in-flight appends to complete. Their latencies are recorded in the measure histogram.

Bound fires when:

- `bytes` mode: cumulative written bytes ≥ `opts.Workload.Bytes`.
- `duration` mode: `time.Since(measureStart) ≥ opts.Workload.Duration`.
- Both set: whichever fires first. Record in `MetricsView.BoundFired`.

For `--kind task` produce means `Enqueue`. `--kind log` produce means `Append`.

**Message size.** Each worker allocates one `[]byte` buffer of `MsgSize` at start and reuses it. Payloads are regenerated per offset via `payloadFor(seed, offset, buf)`.

**Keys.** When `Keys > 0`, each append uses `mikyu.WithRecordKey(fmt.Appendf(nil, "key-%d", offset % uint64(Keys)))` for log queues. Keyed task queues are not supported by the bench; `Workload.Validate` rejects them at config time.

### 4.12 `internal/bench/mode_consume.go`

```go
func runConsume(ctx context.Context, r *runner) error
```

- Populate phase (if `Preload > 0`): write `Preload` bytes with `payloadFor`, no measurement. Emits progress events with `Phase: populate`.
- Warmup: read, verify, advance offset, don't record.
- Measure: read, verify, record.
- Drain: read until `bytes` or `duration`.

Offset tracking is per-worker via `Read(offset)`. `ErrOffsetNotFound` after `LastOffset` is treated as end-of-stream for closed-loop; for `--duration` runs, the mode re-arms after a short park (`10ms`). Reads use `ReadBatch(batch)` for log queues and `Dequeue` + `Ack`/`Nack` for task queues.

For task queues, each successful dequeue is followed by Ack with probability `AckRate` (seeded via `--seed`); Nack otherwise.

### 4.13 `internal/bench/mode_mix.go`

```go
func runMix(ctx context.Context, r *runner) error
```

Two worker pools: producers append, consumers read+ack/nack. `--workers N` means N producers and N consumers (symmetric default). `--ack-rate` governs the task-queue ack fraction.

Composition is exactly what the two counts say it is. There is no ratio math, no auto-balancing. If the user wants 4 fast producers and 1 slow consumer, that's `--workers 4` on produce and a separate `--workers 1` run — or accept the symmetric default.

For `mix`, both producers and consumers run concurrently during the measure phase. The mode orchestrates them via `sync.WaitGroup`, terminates both on the phase boundary.

### 4.14 Test surface for Phase 2

- `TestProduceClosedLoop` — 1MB of messages, verifies count and envelope shape.
- `TestProduceOpenLoop` — `--rate 10000 --duration 1s`, verifies throughput within 10%.
- `TestConsumeWithPreload` — preload 1MB, consume, verify `Messages` matches.
- `TestMixSymmetric` — mix with 4 workers, verify both produce and consume counters incremented.
- `TestAbortOnCancel` — start produce, cancel context, verify `complete: false` and `aborted: "context canceled"`.
- `TestEnvelopeShape` — golden-file test against a checked-in `result.json` for a 1-second run at fixed seed.

---

## 5. Phase 3 — Recovery + daemon access

**Goal.** `recovery` mode and `--access daemon`.

### 5.1 `internal/bench/proc_posix.go` and `proc_windows.go`

```go
// spawnDetached re-executes the current binary with the given args,
// detached from the parent's process group.
func spawnDetached(args []string) (*os.Process, error)

// kill forcibly terminates the process.
func kill(p *os.Process) error
```

POSIX: `syscall.SysProcAttr{Setsid: true}` on spawn; `p.Signal(syscall.SIGKILL)` on kill.

Windows: `CreationFlags: 0x08000000` (CREATE_NO_WINDOW) on spawn; `p.Kill()` on kill.

### 5.2 `internal/bench/mode_recovery.go`

```go
func runRecovery(ctx context.Context, r *runner) error
```

Refuse if `r.opts.Workload.Queues > 1` with `errors.New("recovery mode requires --queues 1")` (return before opening any client).

Flow:

1. Populate: write `CrashAfterBytes` bytes as a child process. Re-exec the current binary with `bench produce --internal-child --dir <dir> --kind <kind> --msg-size <n> --bytes <n> --queue <name> --access embedded`. Wait for the child to exit.
2. The parent SIGKILLs a second child that is writing. Use `spawnDetached`; the child runs `bench produce --internal-child ... --duration 1h`. Wait 200ms for the child to start writing, then `kill`.
   - Alternatively: don't spawn; the parent opens the queue, writes `CrashAfterBytes` bytes, then closes the queue without a graceful `Close()` (calling `runtime.Goexit` or just letting the process exit). This is simpler and is the recommended approach.
3. Reopen the data directory via `mikyu.Open`.
4. Record:
   - `TimeToOpenNS`: wall clock around `mikyu.Open`.
   - `TimeToFirstReadNS`: from open-returned to first `Read` succeeding.
   - `TimeToFullRecoveryNS`: from open-returned to `LastOffset` being readable.
5. Set `MetricsView.Recovery` with the three durations.
6. Emit `PhaseView`s for `populate` and `abort`.

`--internal-child` is a hidden flag that runs produce silently and exits on completion. It is registered on the CLI flag set in Phase 6; in Phase 3 the same flag is registered on the bench command's flag set directly, alongside the top-level verbs, so recovery mode can re-exec itself without depending on the subcommand structure.

**Tests.**

- `TestRecoveryKill` — spawn recovery with `--crash-after-bytes 10MB`, verify the child dies and recovery completes.
- `TestRecoveryRefusesMultipleQueues` — error.
- `TestRecoveryTimesArePositive` — all three durations > 0.

### 5.3 `internal/bench/access.go` — daemon and remote

```go
case AccessDaemon:
    if opts.Dir == "" {
        return nil, errors.New("daemon access requires --dir")
    }
    proc, err := daemon.Spawn(daemon.SpawnOptions{
        DataDir:    opts.Dir,
        Target:     "", // ephemeral
        TLSEnabled: false,
        // TLS/auth support deferred; daemon access is local-only for v1.
    })
    if err != nil { return nil, err }
    target := proc.Target
    if !waitForDaemon(target, 5*time.Second) {
        _ = proc.Kill()
        return nil, errors.New("daemon did not accept connections within 5s")
    }
    client, err := mikyu.Dial(target)
    if err != nil {
        _ = proc.Kill()
        return nil, err
    }
    cleanup := func() error {
        _ = client.Close()
        if opts.Persist { return nil }
        return proc.Kill()
    }
    return &accessHandle{Client: client, Cleanup: cleanup, Target: target, DaemonPID: proc.PID}, nil

case AccessRemote:
    client, err := mikyu.Dial(opts.Target)
    if err != nil { return nil, err }
    return &accessHandle{Client: client, Cleanup: client.Close, Target: opts.Target}, nil
```

`waitForDaemon` polls `daemon.IsDaemonAlive(target)` every 50ms.

**Tests.**

- `TestDaemonAccess` — spawn a daemon via bench, run produce 1s, verify envelope records `access: daemon` and non-empty `access_target`.
- `TestDaemonPersistKeepsDaemon` — with `--persist`, daemon is still alive after bench returns. Cleanup manually.
- `TestRemoteAccessRequiresTarget` — error when `--access remote` with no `--target`.

---

## 6. Phase 4 — Observation enrichment + `--metric` list support

**Goal.** The observation series captures all three families fully. `--cold` cache handling.

### 6.1 Host stats caching

Already implemented in Phase 2's `host.go`. Phase 4 verifies it against real `--duration 10s` runs.

### 6.2 `--cold`

Add to `internal/bench/options.go` and implement `dropCache(dir string) (verified bool, err error)` in `host_linux.go` / `host_darwin.go` / `host_windows.go`:

- **Linux**: `posix_fadvise(POSIX_FADV_DONTNEED)` on every file under `dir` walked with `filepath.WalkDir`. Verified by reading a known-hot file and confirming a cache miss via `/proc/self/stat`'s `major_faults` delta. Return `verified: true` when the delta is positive.
- **Darwin**: `fcntl(F_NOCACHE)` is per-fd, not global; report `verified: false` and `cache_state: "unknown"`.
- **Windows**: no cheap global drop; `verified: false`, `cache_state: "unknown"`.

Set `EnvironmentView.CacheState` to `"warm"` (default), `"cold"` (verified), or `"unknown"` (best-effort failed).

### 6.3 `--metric` list

Add to `Options.Metrics []string`. Default `["throughput"]`. Parsed by the command layer via `fs.Func("metric", ...)` with comma-splitting.

Phase 4 does not implement rendering (that's Phase 5's `bench plot`); it only ensures the field is populated and passed through.

**Acceptance:** `--cold` run on Linux records `cache_state: "cold"`; `--metric p50,p99` populates `Options.Metrics`.

---

## 7. Phase 5 — benchview

**Goal.** Terminal rendering for summary, show, compare, plot.

### 7.1 `internal/ui/benchview/summary.go`

```go
// Summary renders a Result to w as a TitledBox-based summary.
func Summary(w io.Writer, r result.Result, theme style.Theme, fm human.Formatter) error
```

Renders:

```
bench-<id>  <mode>  <access>
────────────────────────────────────────────────────────────
spec       <kind> · <durability> · <max_segment> segment · <max_cache> cache
workload   <msg_size> B × <bytes> · <workers> workers · <duration>
────────────────────────────────────────────────────────────
throughput <msg/s> msg/s · <MiB/s> MiB/s
latency    p50 <p50> · p90 <p90> · p99 <p99> · max <max>
verified   yes|no
bound      bytes|duration|crash
cache      cold|warm|unknown
────────────────────────────────────────────────────────────
written    <dir>
```

If `!r.Complete`, prepend `⚠ incomplete — <reason>` before the config block. `Reason` is `r.Aborted` or `"the run was killed externally"` when `Aborted` is empty.

Uses `style.TitledBox` for the header, `style.KVSection` for the config/workload blocks, `style.StatBand` for throughput. No new primitives.

### 7.2 `show.go`

```go
func Show(w io.Writer, dir string, theme style.Theme, fm human.Formatter) error
```

Loads `result.json`, calls `Summary`. Refuses with `result.ErrNotARun` if the directory lacks the file.

### 7.3 `compare.go`

```go
// Compare renders N runs side by side, diffing config blocks.
func Compare(w io.Writer, dirs []string, theme style.Theme, fm human.Formatter) error
```

- Refuses if any two runs have different `Mode` values: returns `bench.ErrCompareMismatch`.
- Refuses if any run has `Version != result.CurrentVersion`.
- Renders config rows: rows that differ across columns marked `*`, identical rows muted.
- Renders metrics rows: throughput, p50, p90, p99, max.
- `notes` block: `* durability differs`, `<run-a>, <run-b> use embedded; <run-c> uses daemon`.

Cap: warn above 6 columns, don't refuse. Terminal width is the user's problem.

### 7.4 `plot.go`

```go
// Plot renders one terminal chart per requested metric.
func Plot(w io.Writer, dir string, metrics []string, theme style.Theme) error
```

Reads `series.csv`, renders ASCII charts (`=` for bars, spaces for empty). Supported metrics: `throughput`, `p50`, `p90`, `p99`, `queue_depth`, `size_bytes`, `rss_bytes`, `cpu_percent`. Unknown metric → error listing supported names.

Chart layout: 60 rows wide, one row per second, scaled to max value. Simple ASCII, no Unicode.

**Tests.**

- `TestSummaryGolden` — fixed `Result` renders to a checked-in expected string.
- `TestCompareRefusesMixedModes`.
- `TestPlotRefusesUnknownMetric`.
- `TestShowMissingFiles` — returns `ErrNotARun`.

---

## 8. Phase 6 — Subcommand structure + CLI/REPL

### 8.1 `internal/ui/command/verb.go` (modified)

Add to `Verb`:

```go
type Verb struct {
    // ... existing fields
    Subs []Verb  // nil = leaf verb
}
```

Extend `LookupVerb` to variadic path:

```go
func LookupVerb(path ...string) (Verb, bool)
```

`LookupVerb("bench")` returns the top-level. `LookupVerb("bench", "produce")` returns the sub. `VerbNames()` returns flat strings: `["bench", "bench produce", "bench consume", ...]`. `WriteIndex`/`WriteVerb` gain one level of recursion.

`HasVerb("bench", "produce")` returns true if both levels exist.

`cli.handlers` and `repl.replHandlers` key on the space-joined string. The CLI dispatcher in `internal/ui/cli/cli.go` and the REPL dispatcher in `internal/ui/repl/repl.go` descend through `Subs` when the first token names a verb with children, joining `rest[0]` and `rest[1]` into the handler key.

### 8.2 `internal/ui/command/bench.go`

```go
// BenchFlagSet returns a flag set built from bench.Table, spec.Table, and
// the extra presentation flags. Used by both CLI and REPL.
func BenchFlagSet(mode bench.Mode) *flag.FlagSet

// RunBench parses args, resolves Options, calls bench.Run with a progress
// callback that renders a progress bar to errw.
func RunBench(ctx context.Context, args []string, out, errw io.Writer, target string) error

// Present renders a completed Result to w.
func Present(w io.Writer, r result.Result, theme style.Theme, fm human.Formatter) error
```

`BenchFlagSet` registers:

- Every row from `bench.Table` as a `flag.Value`, applying to `*bench.Workload`.
- Every row from `spec.Table` (spec layer) as a `flag.Value`, applying to `*spec.Spec`.
- Extra flags: `--dir`, `--target`, `--access`, `--out`, `--persist`, `--cleanup-stale` (bool, default true), `--quiet`, `--verify`, `--cold`, `--cpu`, `--metric` (repeatable), `--internal-child` (hidden).
- Mode-specific rows are gated on the `Modes` field of `Param`.

Scenario file support: `--file <path>` loads a YAML doc with three top-level keys: `mode`, `spec`, `workload`. `spec` is decoded by `spec.DecodeSpecYAML`; `workload` is decoded by a `bench.DecodeWorkloadYAML` function in `scenario.go`. Flags override file. File overrides env.

### 8.3 `internal/ui/cli/bench.go` and `internal/ui/repl/bench.go`

CLI handlers keyed on `"bench produce"`, `"bench consume"`, `"bench mix"`, `"bench recovery"`, `"bench show"`, `"bench compare"`, `"bench plot"`. The dispatcher descends through `Subs` when the first token names a verb with children.

REPL specifics:

- Blocks the prompt during a run.
- `Ctrl+C` aborts the current run, not the REPL. `signal.NotifyContext` wraps the run's context.
- Maintains `lastBenchRun string` for `bench show` and `bench compare` defaults.

### 8.4 Verb table additions

```go
{
    Name:  "bench",
    Kind:  KindAdmin,
    Modes: ModeCLI | ModeREPL,
    Subs: []Verb{
        {Name: "produce", ...},
        {Name: "consume", ...},
        {Name: "mix", ...},
        {Name: "recovery", ...},
        {Name: "show", Args: "<run-dir>", ...},
        {Name: "compare", Args: "<dir>...", ...},
        {Name: "plot", Args: "<run-dir>", ...},
    },
}
```

Each sub has its own `Args`, `Summary`, `Long`, `Usage`, `Examples`, `ExtraFlags`. `bench` (the parent) has no args and renders its `Subs` list when called without a sub.

### 8.5 Progress bar

Uses `github.com/charmbracelet/bubbles/progress` with:

- `WithFillCharacter('=')`, `WithEmptyCharacter('-')`.
- One bar per phase, sequential (not shared).
- Written to stderr.
- Updated once per second.
- Suppressed when `errw` is not a TTY or `--quiet` is set.

Format:

```
[populate] ========== 100%  42.1s  10.0 GiB
[warmup]   ========== 100%   5.0s
[measure]  =====-----  50%  15.0s
```

### 8.6 Acceptance

- `mikyu bench produce --dir ./tmp --duration 5s` prints a progress bar, then a summary.
- `mikyu bench show ./bench/bench-1727619830` re-renders the summary.
- `mikyu bench compare a b` diffs two runs.
- `mikyu bench plot ./bench/run-a --metric p99` renders a chart.
- `mikyu bench` without a subcommand prints sub-help.
- REPL: `bench produce --dir ./tmp --duration 5s` blocks, Ctrl+C aborts, `bench show` works.
- All Phase 1–5 tests still pass.

---

## 9. Cross-cutting tests

1. `TestSelfPerturbation` — run the same workload twice, once with `Options.DisableObservation: true` and once without. Assert the throughput delta is within 2%.
2. `TestKilledRunEnvelope` — spawn a subprocess that runs a produce loop, SIGKILL it mid-run, verify the parent can read the partial envelope.
3. `TestNoLegacyNames` — passes with the audit scan.
4. `TestEnvelopeVersionGate` — a `result.json` with `version: 999` fails `bench show` with `ErrEnvelopeVersion`.
5. `TestGoLeak` — `goleak.VerifyTestMain` in `internal/bench/` and `internal/bench/result/`.
6. `TestBenchTableParity` — every `bench.Table` row is reachable from `BenchFlagSet` for every mode it applies to.
7. `TestBenchEnvVars` — every `bench.Table` row has an env override and is honored by `BenchFlagSet`.
8. `TestEveryBenchParamHasDoc` — mirrors `TestEverySpecParamHasDoc`.
9. `TestNamingLawRecurses` — the `TestNamingLaw` scan visits `Verb.Subs` and treats the `produce` sub under `bench` as the accepted spelling.

---

## 10. Order of implementation

Strictly sequential. Each phase's acceptance must pass before the next begins.

1. **Phase 1 — Foundations.** No bench behavior. Types, tables, sentinels, `daemon.Spawn`, exit code 3, arch-lint, audit gate updates.
2. **Phase 2 — Runner core.** Produce/consume/mix against embedded. Envelope streaming. Observation. All the measurements.
3. **Phase 3 — Recovery + daemon.** Crash simulation and remote access.
4. **Phase 4 — Observation enrichment.** `--cold` and `--metric` lists.
5. **Phase 5 — benchview.** Terminal rendering. Depends only on Phase 1's `result` package plus real envelopes from Phase 2 for golden tests.
6. **Phase 6 — CLI/REPL.** Subcommand structure and user surface.

Phases 1–3 are core. Phase 4 enriches. Phases 5–6 are the user surface. Nothing in a later phase changes the shape of an earlier phase's types — the envelope schema is stable from Phase 1.

**Review checklist before marking a phase complete:**

- No `errors.New` in bench code outside `errors.go` and `mode_recovery.go`'s refused-queries sentinel.
- No `time.Now()` outside `measure.go`, `observe.go`, `mode_*.go`, and `lifecycle.go`.
- Every field in `Workload` is set by exactly one `bench.Table` row.
- Every goroutine has a stop path documented in a `//` comment.
- `go test ./...` passes.
- `go-arch-lint check` passes.
- `internal/audit` tests pass.

---

## 11. Contract

- **Do not invent flags, modes, or envelope fields.** Every field, row, and mode in this plan is deliberate. If something is missing, amend the plan first.
- **Do not reorder phases.** Each phase's acceptance gates the next.
- **Idiomatic Go.** Small files, one concern each, explicit error returns, no `panic` outside `init`, no context-less I/O.
- **Tests are not optional.** Every file in Phases 1–5 has a matching `_test.go` file with the cases listed. `goleak` in the two bench packages.
- **Layer purity is a hard rule.** `.go-arch-lint.yml` will fail the build. If a new import is genuinely needed, amend the plan.
- **Comments follow `TestInternalCommentStyle`.** At most one line per doc comment on internal packages and exported symbols.
- **If a decision in this plan conflicts with your judgment, stop and ask.** The plan is meant to be followed exactly.
- **Reference packages for style.** `internal/engine/sync_linux.go` for platform files, `internal/node/registry.go` for atomic writes, `internal/metrics/collector.go` for a well-structured package with one goroutine, `internal/ui/metricsview/` for view rendering.