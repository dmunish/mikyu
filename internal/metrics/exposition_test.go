package metrics

import (
	"bytes"
	"flag"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
	"github.com/prometheus/common/expfmt"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/dmunish/mikyu/internal/spec"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files")

func goldenSnapshot() Snapshot {
	age := spec.Duration(24 * time.Hour)
	segment := int64(64 << 20)
	return Snapshot{
		Timestamp: time.Unix(0, 0).UTC(),
		Runtime: RuntimeView{
			Goroutines: 7,
			Heap:       HeapView{Alloc: 1024, Inuse: 2048},
			UptimeSec:  12.5,
		},
		GC:      GCView{Cycles: 3},
		Process: ProcessView{CPUPercent: 1.25, RSSBytes: 4096},
		Totals:  TotalsView{Queues: 1, Messages: 10, Bytes: 2048},
		Queues: []QueueView{{
			Name:     "events",
			Kind:     "log",
			Counters: map[string]int64{"appends": 10, "reads": 4, "queue_faults": 1},
			Gauges:   map[string]int64{"messages": 10, "size_bytes": 2048, "segments": 1},
			Histograms: map[string]HistogramView{
				"append_duration": viewOf(HistogramSnapshot{
					Bounds: []float64{0.001, 0.01}, Counts: []uint64{5, 3, 2}, Sum: 0.5, Total: 10,
				}),
			},
			Producers:    ProducerView{InFlight: 8, BlockedOnWAL: 2, BlockEvents: 3},
			Consumers:    ConsumerView{InFlight: 4, ParkedFetch: 2, FetchParkEvents: 42},
			Backpressure: BackpressureView{WALChannelFill: 0.003, WALByteFill: 0.0006, QueueFullEvents: 1},
			Effective: spec.Spec{
				Storage:   spec.StorageSpec{MaxSegment: &segment},
				Retention: spec.RetentionSpec{MaxAge: &age},
			},
		}, {
			Name:     "jobs",
			Kind:     "task",
			Counters: map[string]int64{"enqueues": 5},
			Gauges:   map[string]int64{"messages": 2, "size_bytes": 1024, "segments": 1},
			Tasks:    TaskView{Active: 42, InFlight: 8, Scheduled: 34, DeadLetters: 2, CheckpointLag: 3},
		}},
	}
}

// normalizeNewlines makes the golden comparison independent of checkout line endings.
func normalizeNewlines(s string) string { return strings.ReplaceAll(s, "\r\n", "\n") }

// TestExpositionGolden: the rendered surface matches the checked-in golden file.
func TestExpositionGolden(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true, Histograms: true})

	golden := filepath.Join("testdata", "snapshot.prom")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, []byte(out), 0o644))
		return
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	assert.Equal(t, normalizeNewlines(string(want)), normalizeNewlines(out))
}

// renderCollector scrapes a SnapshotCollector and renders it as exposition text.
func renderCollector(t *testing.T, snap Snapshot, opts CollectOptions) string {
	t.Helper()
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewSnapshotCollector(func() (Snapshot, error) { return snap, nil }, opts))
	families, err := reg.Gather()
	require.NoError(t, err)
	var buf bytes.Buffer
	for _, mf := range families {
		_, err := expfmt.MetricFamilyToText(&buf, mf)
		require.NoError(t, err)
	}
	return buf.String()
}

func TestExpositionGlobalOnlyByDefault(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{})
	assert.Contains(t, out, "mikyu_runtime_goroutines 7")
	assert.Contains(t, out, "mikyu_totals_queues 1")
	assert.NotContains(t, out, "mikyu_queue_messages")
	assert.NotContains(t, out, "mikyu_queue_appends_total")
}

func TestExpositionTopNQueues(t *testing.T) {
	snap := Snapshot{
		Queues: []QueueView{
			{Name: "small", Gauges: map[string]int64{"size_bytes": 1, "messages": 1}},
			{Name: "large", Gauges: map[string]int64{"size_bytes": 100, "messages": 5}},
			{Name: "medium", Gauges: map[string]int64{"size_bytes": 50, "messages": 3}},
		},
	}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true, TopNQueues: 2})
	assert.Contains(t, out, `mikyu_queue_messages{queue="large"}`)
	assert.Contains(t, out, `mikyu_queue_messages{queue="medium"}`)
	assert.NotContains(t, out, `queue="small"`)
}

func TestExpositionCounterSuffix(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{{Name: "q", Counters: map[string]int64{"appends": 1, "custom_ops": 2}}}}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true})
	assert.Contains(t, out, `mikyu_queue_appends_total{queue="q"} 1`)
	assert.Contains(t, out, `mikyu_queue_custom_ops_total{queue="q"} 2`)
}

// TestExpositionPercentileQuantiles: histograms expose quantile summaries.
func TestExpositionPercentileQuantiles(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues[0].Histograms["append_duration"] = HistogramView{
		Bounds: []float64{0.001, 0.01}, Counts: []uint64{5, 3, 2}, Sum: 0.5, Total: 10,
		P50: 0.0005, P90: 0.005, P99: 0.009, Max: 0.01,
	}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true, Histograms: true})

	assert.Contains(t, out, "# TYPE mikyu_queue_append_duration_quantiles_seconds summary")
	for _, label := range []string{`quantile="0.5"`, `quantile="0.9"`, `quantile="0.99"`, `quantile="+Inf"`} {
		assert.Contains(t, out, `mikyu_queue_append_duration_quantiles_seconds{queue="events",`+label+"}")
	}
	assert.Contains(t, out, `mikyu_queue_append_duration_quantiles_seconds_count{queue="events"} 10`)
	assert.Contains(t, out, `mikyu_queue_append_duration_quantiles_seconds_sum{queue="events"} 0.5`)
}

// TestExpositionQuantilesAreMonotone: computed quantiles never decrease as p rises.
func TestExpositionQuantilesAreMonotone(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true, Histograms: true})

	var prev float64
	var seen bool
	for line := range strings.SplitSeq(out, "\n") {
		if !strings.HasPrefix(line, "mikyu_queue_append_duration_quantiles_seconds{") {
			continue
		}
		fields := strings.Fields(line)
		value, err := strconv.ParseFloat(fields[len(fields)-1], 64)
		require.NoError(t, err)
		if seen {
			assert.GreaterOrEqual(t, value, prev, "quantiles must not decrease: %s", line)
		}
		prev, seen = value, true
	}
	assert.True(t, seen, "expected quantile samples")
}

// TestExpositionInfoDials: one info series per queue with sorted label keys.
func TestExpositionInfoDials(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true})

	assert.Equal(t, 1, strings.Count(out, "# TYPE mikyu_queue_info gauge"))
	assert.Equal(t, 2, strings.Count(out, "mikyu_queue_info{"))
	assert.Contains(t, out, `kind="log"`)
	assert.Contains(t, out, `durability="sync"`)
	assert.Contains(t, out, `max_segment="67108864"`)
	assert.Contains(t, out, `retention_age="24h0m0s"`)
	assert.Contains(t, out, `kind="task"`)

	start := strings.Index(out, "mikyu_queue_info{")
	open := strings.Index(out[start:], "{")
	series := out[start+open+1 : start+strings.Index(out[start:], "}")]
	var keys []string
	for pair := range strings.SplitSeq(series, ",") {
		keys = append(keys, strings.SplitN(pair, "=", 2)[0])
	}
	assert.IsIncreasing(t, keys, "label keys must be sorted")
}

// TestExpositionInfoSkipsInternalScopes: a non-queue scope is not a queue.
func TestExpositionInfoSkipsInternalScopes(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{
		{Name: "events", Kind: "log"},
		{Name: "server", Kind: internalKind, Gauges: map[string]int64{"connections": 3},
			Counters: map[string]int64{"connections_rejected": 1}},
	}}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true})

	assert.Equal(t, 1, strings.Count(out, "mikyu_queue_info{"))
	assert.Contains(t, out, `mikyu_queue_info{`)
	assert.NotContains(t, out, `kind="internal"`)
	assert.Contains(t, out, `mikyu_queue_connections{queue="server"} 3`)
	assert.Contains(t, out, `mikyu_queue_op_connections_rejected_total{queue="server"} 1`)
}

// TestExpositionNoNaNOrInf: no sample value is NaN or infinite.
func TestExpositionNoNaNOrInf(t *testing.T) {
	snap := goldenSnapshot()
	snap.Runtime.UptimeSec = math.NaN()
	snap.Process.CPUPercent = math.Inf(1)
	snap.Queues[0].Backpressure.WALChannelFill = math.Inf(-1)
	hist := snap.Queues[0].Histograms["append_duration"]
	hist.Sum = math.NaN()
	snap.Queues[0].Histograms["append_duration"] = hist

	out := renderCollector(t, snap, CollectOptions{PerQueue: true, Histograms: true})

	for line := range strings.SplitSeq(out, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.NotEmpty(t, fields)
		value := fields[len(fields)-1]
		assert.NotEqual(t, "NaN", value, "sample value is NaN: %s", line)
		assert.NotEqual(t, "+Inf", value, "sample value is +Inf: %s", line)
		assert.NotEqual(t, "-Inf", value, "sample value is -Inf: %s", line)
	}
	assert.Contains(t, out, "mikyu_runtime_uptime_seconds 0")
	assert.Contains(t, out, `mikyu_queue_backpressure_wal_channel_fill{queue="events"} 0`)
}

// TestExpositionCounterSuffixInvariant: every counter family ends in _total.
func TestExpositionCounterSuffixInvariant(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues = append(snap.Queues, QueueView{
		Name:     "custom",
		Counters: map[string]int64{"already_total": 1, "ops": 2, "queue_faults": 3},
	})
	out := renderCollector(t, snap, CollectOptions{PerQueue: true})

	var counters int
	for line := range strings.SplitSeq(out, "\n") {
		if !strings.HasPrefix(line, "# TYPE ") || !strings.HasSuffix(line, " counter") {
			continue
		}
		counters++
		assert.True(t, strings.HasSuffix(strings.Fields(line)[2], "_total"), "counter family must end in _total: %s", line)
	}
	assert.Positive(t, counters)
}

// TestExpositionFaultsSeries: a faulted queue reports faults; a clean queue does not.
func TestExpositionFaultsSeries(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true})

	assert.Contains(t, out, "# TYPE mikyu_queue_faults_total counter")
	assert.Contains(t, out, `mikyu_queue_faults_total{queue="events"} 1`)
	assert.NotContains(t, out, `mikyu_queue_faults_total{queue="jobs"}`)

	zero := Snapshot{Queues: []QueueView{{Name: "clean", Counters: map[string]int64{"queue_faults": 0}}}}
	assert.NotContains(t, renderCollector(t, zero, CollectOptions{PerQueue: true}), `mikyu_queue_faults_total{queue="clean"}`)
}

// TestExpositionNilMapsSkipped: a queue with nil maps emits only gauge lines.
func TestExpositionNilMapsSkipped(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{
		{Name: "bare"},
		{Name: "gauged", Gauges: map[string]int64{"messages": 1}},
	}}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true, Histograms: true})

	assert.Contains(t, out, `mikyu_queue_messages{queue="gauged"} 1`)
	assert.NotContains(t, out, `mikyu_queue_messages{queue="bare"}`)
	assert.NotContains(t, out, "_bucket")
	assert.NotContains(t, out, "mikyu_queue_appends_total")
	assert.NotContains(t, out, faultsMetric)
	assert.NotContains(t, out, "mikyu_queue_tasks_active")
	assert.NotContains(t, out, "mikyu_queue_producers_in_flight")
}

// TestExpositionTaskViewSeries: task occupancy reaches Prometheus as gauges.
func TestExpositionTaskViewSeries(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true})

	assert.Contains(t, out, `mikyu_queue_tasks_active{queue="jobs"} 42`)
	assert.Contains(t, out, `mikyu_queue_tasks_in_flight{queue="jobs"} 8`)
	assert.Contains(t, out, `mikyu_queue_tasks_dead_letters{queue="jobs"} 2`)
	assert.Contains(t, out, `mikyu_queue_tasks_checkpoint_lag{queue="jobs"} 3`)
	assert.NotContains(t, out, `mikyu_queue_tasks_active{queue="events"}`)
	assert.NotContains(t, out, `active=42`)
}

// TestExpositionCoversSnapshot: every Snapshot leaf is exported or explicitly skipped.
func TestExpositionCoversSnapshot(t *testing.T) {
	covered := make(map[string]bool, len(coveredFields))
	for _, path := range coveredFields {
		require.False(t, covered[path], "duplicate entry in coveredFields: %s", path)
		covered[path] = true
	}
	for path := range skipFields {
		assert.False(t, covered[path], "%s cannot be both covered and skipped", path)
	}

	walkLeaves(reflect.TypeFor[Snapshot](), "Snapshot", func(path string) {
		if covered[path] {
			return
		}
		for skip := range skipFields {
			if strings.HasPrefix(path, skip) {
				return
			}
		}
		t.Errorf("%s is neither covered nor skipped: add it to coveredFields or skipFields", path)
	})
	for path := range covered {
		assert.True(t, strings.HasPrefix(path, "Snapshot."), "covered path %s is not a Snapshot path", path)
	}
}

// TestExpositionBuildInfo: build identity is one labeled series.
func TestExpositionBuildInfo(t *testing.T) {
	snap := goldenSnapshot()
	snap.Build = BuildView{SpecVersion: 5, GoVersion: "go1.27", OS: "linux", Arch: "amd64"}
	out := renderCollector(t, snap, CollectOptions{})

	assert.Contains(t, out,
		`mikyu_build_info{arch="amd64",go_version="go1.27",os="linux",spec_version="5"} 1`)
}

// TestExpositionStorageAndProcess: storage aggregates and process detail are exported.
func TestExpositionStorageAndProcess(t *testing.T) {
	snap := goldenSnapshot()
	snap.Storage = StorageView{
		DataDir: `C:\data\mikyu`, Queues: 2, SizeBytes: 4096,
		CommittedBytes: 1000, FlushedBytes: 900, PendingBytes: 100, CacheHits: 30, CacheMisses: 2,
	}
	snap.Process = ProcessView{CPUSecondsUser: 1.5, CPUSecondsSys: 0.5, VSZBytes: 8192, Threads: 9, OpenFDs: 21}
	out := renderCollector(t, snap, CollectOptions{})

	assert.Contains(t, out, `data_dir="C:\\data\\mikyu"`)
	assert.Contains(t, out, "mikyu_storage_committed_bytes_total 1000")
	assert.Contains(t, out, "mikyu_storage_flushed_bytes_total 900")
	assert.Contains(t, out, "mikyu_storage_pending_bytes 100")
	assert.Contains(t, out, "mikyu_storage_cache_hits_total 30")
	assert.Contains(t, out, "mikyu_storage_cache_misses_total 2")
	assert.Contains(t, out, `mikyu_process_cpu_seconds_total{state="user"} 1.5`)
	assert.Contains(t, out, `mikyu_process_cpu_seconds_total{state="sys"} 0.5`)
	assert.Contains(t, out, "mikyu_process_vsz_bytes 8192")
	assert.Contains(t, out, "mikyu_process_threads 9")
	assert.Contains(t, out, "mikyu_process_open_fds 21")
}

// TestExpositionLockObservers: each observed lock becomes a labeled series.
func TestExpositionLockObservers(t *testing.T) {
	snap := goldenSnapshot()
	snap.Locks = LocksView{
		GlobalWaitSec: 0.25,
		Observers: []LockView{{
			Name: "node.mu", Waits: 4, TotalWaitMs: 2, MaxWaitMs: 1.5, P50WaitMs: 0.1, P99WaitMs: 1.2,
		}},
	}
	out := renderCollector(t, snap, CollectOptions{})

	assert.Contains(t, out, "mikyu_lock_global_wait_seconds_total 0.25")
	assert.Contains(t, out, `mikyu_lock_waits_total{lock="node.mu"} 4`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds_total{lock="node.mu"} 0.002`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds_max{lock="node.mu"} 0.0015`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds{lock="node.mu",quantile="0.5"} 0.0001`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds{lock="node.mu",quantile="0.99"} 0.0012`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds_count{lock="node.mu"} 4`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds_sum{lock="node.mu"} 0.002`)
}

// TestExpositionLockFamiliesOmittedWhenIdle: no observers, no lock families.
func TestExpositionLockFamiliesOmittedWhenIdle(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{})

	assert.Contains(t, out, "mikyu_lock_global_wait_seconds_total")
	assert.NotContains(t, out, "mikyu_lock_waits_total")
	assert.NotContains(t, out, "mikyu_lock_wait_seconds_max")
}

// TestExpositionGroupSeries: consumer groups carry queue and group labels.
func TestExpositionGroupSeries(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues[0].Groups = []GroupView{{Name: "analytics", Committed: 900, HighWater: 1000, Lag: 100}}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true})

	assert.Contains(t, out, `mikyu_queue_group_committed{group="analytics",queue="events"} 900`)
	assert.Contains(t, out, `mikyu_queue_group_high_water{group="analytics",queue="events"} 1000`)
	assert.Contains(t, out, `mikyu_queue_group_lag{group="analytics",queue="events"} 100`)
}

// TestExpositionGroupFamiliesOmittedWhenNoGroups: no groups, no group families.
func TestExpositionGroupFamiliesOmittedWhenNoGroups(t *testing.T) {
	assert.NotContains(t, renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true}), "mikyu_queue_group_lag")
}

// TestExpositionRates: the collector's derivatives are exported per queue.
func TestExpositionRates(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues[0].Rates = Rates{AppendsPerSec: 1200, ReadsPerSec: 800, BytesInPerSec: 4096}
	out := renderCollector(t, snap, CollectOptions{PerQueue: true})

	assert.Contains(t, out, "mikyu_queue_appends_per_second{queue=\"events\"} 1200")
	assert.Contains(t, out, "mikyu_queue_reads_per_second{queue=\"events\"} 800")
	assert.Contains(t, out, "mikyu_queue_bytes_in_per_second{queue=\"events\"} 4096")
}

// TestExpositionStartTimeOmittedWhenUptimeUnknown: no uptime, no bogus start time.
func TestExpositionStartTimeOmittedWhenUptimeUnknown(t *testing.T) {
	snap := goldenSnapshot()
	snap.Runtime.UptimeSec = 0
	assert.NotContains(t, renderCollector(t, snap, CollectOptions{}), "mikyu_process_start_time_seconds")
}

// TestExpositionWellFormed: every sample belongs to a declared family and uses a
// name that family's type permits. A quantile sample sharing a histogram family
// name, for instance, would fail here.
func TestExpositionWellFormed(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true, Histograms: true})

	types := map[string]string{}
	helps := map[string]bool{}
	for line := range strings.SplitSeq(out, "\n") {
		switch {
		case strings.HasPrefix(line, "# HELP "):
			helps[strings.Fields(line)[2]] = true
		case strings.HasPrefix(line, "# TYPE "):
			f := strings.Fields(line)
			_, dup := types[f[2]]
			require.False(t, dup, "duplicate TYPE for %s", f[2])
			types[f[2]] = f[3]
		}
	}

	allowed := map[string]string{}
	for family, typ := range types {
		allowed[family] = family
		if typ == "histogram" {
			allowed[family+"_bucket"] = family
			allowed[family+"_sum"] = family
			allowed[family+"_count"] = family
		}
		if typ == "summary" {
			allowed[family+"_sum"] = family
			allowed[family+"_count"] = family
		}
	}
	for family := range types {
		assert.True(t, helps[family], "family %s has no HELP line", family)
	}

	for line := range strings.SplitSeq(out, "\n") {
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		require.Len(t, fields, 2, "malformed sample line: %s", line)
		name := fields[0]
		if i := strings.IndexByte(name, '{'); i >= 0 {
			name = name[:i]
		}
		family, ok := allowed[name]
		require.True(t, ok, "sample %q has no declared family", line)
		if name != family {
			assert.Contains(t, []string{"histogram", "summary"}, types[family],
				"%s suffix is only valid on a histogram or summary", strings.TrimPrefix(name, family+"_"))
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, err, "unparseable value in: %s", line)
		assert.False(t, math.IsNaN(value) || math.IsInf(value, 0), "non-finite value in: %s", line)
	}
}

// TestExpositionSummaryCarriesOnlyQuantiles: a summary family has no bucket samples.
func TestExpositionSummaryCarriesOnlyQuantiles(t *testing.T) {
	out := renderCollector(t, goldenSnapshot(), CollectOptions{PerQueue: true, Histograms: true})

	for line := range strings.SplitSeq(out, "\n") {
		if !strings.HasPrefix(line, "mikyu_queue_append_duration_seconds{") {
			continue
		}
		assert.Contains(t, line, `quantile=`, "summary sample must be labeled with a quantile: %s", line)
	}
}

// TestExpositionNameTablesAreConsistent: every table entry yields a unique,
// correctly suffixed metric name. Two keys mapping to one name would silently
// merge two series, and a wrong suffix would break the _total counter rule.
func TestExpositionNameTablesAreConsistent(t *testing.T) {
	type table struct {
		name    string
		entries map[string]string
		typ     string
	}
	tables := []table{
		{"gauge", gaugeNames, "gauge"},
		{"counter", counterNames, "counter"},
		{"histogram", histogramNames, "histogram"},
	}
	seen := map[string]string{}
	for _, tbl := range tables {
		for key, name := range tbl.entries {
			require.True(t, strings.HasPrefix(name, "mikyu_"), "%s[%q] = %q lacks the mikyu_ prefix", tbl.name, key, name)
			if tbl.typ == "counter" {
				assert.True(t, strings.HasSuffix(name, "_total"), "counter %q must end in _total", name)
			}
			if tbl.typ == "histogram" {
				assert.True(t, strings.HasSuffix(name, "_seconds"), "histogram %q must end in _seconds", name)
			}
			if prior, dup := seen[name]; dup {
				t.Errorf("%q is produced by both %s and %s", name, prior, tbl.name)
			}
			seen[name] = tbl.name
		}
	}
	for key := range rateHelp {
		name := "mikyu_queue_" + key
		if prior, dup := seen[name]; dup {
			t.Errorf("%q is produced by both %s and rates", name, prior)
		}
		seen[name] = "rates"
	}
}

// TestExpositionServerOpHelpDerived: server op counters describe themselves.
func TestExpositionServerOpHelpDerived(t *testing.T) {
	assert.Equal(t, "Server append operations.", counterHelpFor("op_append"))
	assert.Equal(t, "Server create queue operations.", counterHelpFor("op_create_queue"))
	assert.Equal(t, "Queue counter.", counterHelpFor("not_an_op"))
	assert.Equal(t, "Log appends.", counterHelpFor("appends"))
}

// TestExpositionLint: the gathered surface passes promlint's naming and help checks.
func TestExpositionLint(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewSnapshotCollector(
		func() (Snapshot, error) { return goldenSnapshot(), nil },
		CollectOptions{PerQueue: true, Histograms: true},
	))
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	var texts []string
	for _, p := range problems {
		texts = append(texts, p.Metric+": "+p.Text)
	}
	assert.Empty(t, texts, "promlint problems:\n%s", strings.Join(texts, "\n"))
}

// lintHistogram is a representative shape reused by every histogram family.
var lintHistogram = HistogramView{
	Bounds: []float64{0.001, 0.01, 0.1}, Counts: []uint64{3, 2, 1, 1},
	Sum: 0.9, Total: 7,
	P50: 0.001, P90: 0.01, P99: 0.05, P999: 0.1, Max: 0.1,
}

// fullSurfaceSnapshot populates every family the exposition can emit — every
// name-table key, the unknown-key fallbacks, typed views, groups, rates, and
// every global view — so the lint pass sees every published name.
func fullSurfaceSnapshot() Snapshot {
	gauges := make(map[string]int64, len(gaugeNames)+1)
	for key := range gaugeNames {
		gauges[key] = 1
	}
	gauges["custom_ratio"] = 2

	counters := make(map[string]int64, len(counterNames)+2)
	for key := range counterNames {
		counters[key] = 1
	}
	counters["custom_ops"] = 2
	counters["already_total"] = 3

	histograms := make(map[string]HistogramView, len(histogramNames)+3)
	for key := range histogramNames {
		histograms[key] = lintHistogram
	}
	histograms["op_append_duration"] = lintHistogram
	histograms["op_0xff_duration"] = lintHistogram
	histograms["custom_latency"] = lintHistogram

	logQueue := QueueView{
		Name:     "events",
		Kind:     "log",
		Counters: counters,
		Gauges:   gauges,
		Histograms: map[string]HistogramView{
			"append_duration": lintHistogram,
		},
		Rates:        Rates{AppendsPerSec: 1, ReadsPerSec: 2, EnqueuesPerSec: 3, DequeuesPerSec: 4, BytesInPerSec: 5, BytesOutPerSec: 6},
		Producers:    ProducerView{InFlight: 1, BlockedOnWAL: 2, BlockEvents: 3},
		Consumers:    ConsumerView{InFlight: 4, ParkedFetch: 5, ParkedDequeue: 6, FetchParkEvents: 7, DequeueParkEvents: 8},
		Backpressure: BackpressureView{WALChannelFill: 0.1, WALByteFill: 0.2, QueueFullEvents: 3, ProducerBlocks: 4, ConsumerParks: 5, RetryBudgetDrops: 6},
		Groups:       []GroupView{{Name: "analytics", Committed: 1, HighWater: 2, Lag: 1}},
	}
	taskQueue := QueueView{
		Name:     "jobs",
		Kind:     "task",
		Counters: map[string]int64{"enqueues": 1},
		Gauges:   map[string]int64{"messages": 1},
		Tasks: TaskView{
			Active: 1, InFlight: 2, Scheduled: 3, DeadLetters: 4, Uncheckpointed: 5,
			ScheduleHeap: 6, CheckpointLSN: 7, Dirty: 8, CheckpointLag: 9,
		},
	}

	return Snapshot{
		Timestamp: time.Unix(0, 0).UTC(),
		Build:     BuildView{SpecVersion: 5, GoVersion: "go1.27", OS: "linux", Arch: "amd64"},
		Runtime: RuntimeView{
			Goroutines: 7, GOMAXPROCS: 4, UptimeSec: 12.5,
			GoroutineStates: GoroutineStates{Running: 1, Runnable: 2, Waiting: 4},
			Heap:            HeapView{Alloc: 1, Sys: 2, Inuse: 3, Idle: 4, Released: 5, Objects: 6, TotalAlloc: 7, TotalFree: 8},
			Stack:           MemoryClassView{Inuse: 1, Sys: 2},
			Metadata:        MemoryClassView{Inuse: 3, Sys: 4},
		},
		Process: ProcessView{CPUPercent: 1.25, CPUSecondsUser: 1.5, CPUSecondsSys: 0.5, RSSBytes: 4096, VSZBytes: 8192, Threads: 9, OpenFDs: 21},
		GC: GCView{
			Cycles: 3, PauseTotalMs: 4, NextGCBytes: 5, MemLimitBytes: 6,
			Pause: PauseView{P50Ms: 0.1, P90Ms: 0.2, P99Ms: 0.3, MaxMs: 0.4},
		},
		Scheduler: SchedulerView{Latency: LatencyView{P50: 0.1, P90: 0.2, P99: 0.3}},
		Locks: LocksView{
			GlobalWaitSec: 0.25,
			Observers: []LockView{{
				Name: "node.mu", Waits: 4, TotalWaitMs: 2, MaxWaitMs: 1.5, P50WaitMs: 0.1, P99WaitMs: 1.2,
			}},
		},
		Storage: StorageView{
			DataDir: "/data/mikyu", Queues: 2, Segments: 3, SizeBytes: 4096,
			CacheBytes: 1, CacheLimit: 2, CacheHits: 30, CacheMisses: 2,
			CommittedBytes: 1000, FlushedBytes: 900, PendingBytes: 100, OffsetStoreBytes: 7,
		},
		Totals: TotalsView{Queues: 2, Messages: 11, Bytes: 5120},
		Queues: []QueueView{logQueue, taskQueue},
	}
}

// TestExpositionLintFullSurface: every published family name passes promlint,
// including the unknown-key fallbacks the golden snapshot never exercises.
func TestExpositionLintFullSurface(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewSnapshotCollector(
		func() (Snapshot, error) { return fullSurfaceSnapshot(), nil },
		CollectOptions{PerQueue: true, Histograms: true},
	))
	problems, err := testutil.GatherAndLint(reg)
	require.NoError(t, err)
	var texts []string
	for _, p := range problems {
		texts = append(texts, p.Metric+": "+p.Text)
	}
	assert.Empty(t, texts, "promlint problems:\n%s", strings.Join(texts, "\n"))
}

// labelAllowlist is every label name the exposition may place on a series.
// The only per-sample variable dimensions are queue, group, and lock; the rest
// are fixed per series, so cardinality stays bounded by the node's queues.
var labelAllowlist = map[string]bool{
	// fixed identity and dial labels on mikyu_queue_info
	"arch": true, "compaction": true, "durability": true, "encryption": true,
	"go_version": true, "kind": true, "lease": true, "max_attempts": true,
	"max_cache": true, "max_dead_letters": true, "max_flush_bytes": true,
	"max_flush_wait": true, "max_in_flight": true, "max_payload": true,
	"max_retention_age": true, "max_retention_bytes": true, "max_segment": true,
	"max_segment_age": true, "os": true, "queue": true, "spec_version": true,
	// variable label dimensions
	"group": true, "lock": true, "state": true, "quantile": true, "le": true,
	// fixed single-value labels
	"data_dir": true,
}

// TestExpositionLabelCardinalityIsBounded: no series carries a label outside
// the allowlist, so a scrape cannot grow unbounded per queue.
func TestExpositionLabelCardinalityIsBounded(t *testing.T) {
	reg := prometheus.NewRegistry()
	reg.MustRegister(NewSnapshotCollector(
		func() (Snapshot, error) { return fullSurfaceSnapshot(), nil },
		CollectOptions{PerQueue: true, Histograms: true},
	))
	families, err := reg.Gather()
	require.NoError(t, err)

	for _, mf := range families {
		for _, m := range mf.GetMetric() {
			for _, lp := range m.GetLabel() {
				assert.True(t, labelAllowlist[lp.GetName()],
					"family %s carries unlisted label %q", mf.GetName(), lp.GetName())
			}
		}
	}
}

// TestExpositionTopNCap: the daemon's TopN bound keeps per-queue series at 50.
func TestExpositionTopNCap(t *testing.T) {
	queues := make([]QueueView, 60)
	for i := range queues {
		queues[i] = QueueView{
			Name:   fmt.Sprintf("queue-%02d", i),
			Gauges: map[string]int64{"messages": int64(i + 1)},
		}
	}
	out := renderCollector(t, Snapshot{Queues: queues}, CollectOptions{PerQueue: true, TopNQueues: 50})

	assert.Equal(t, 50, strings.Count(out, `mikyu_queue_messages{queue="queue-`))
}

// coveredFields lists every Snapshot leaf the exposition exports, as a series or a label.
var coveredFields = []string{
	"Snapshot.Build.SpecVersion", "Snapshot.Build.GoVersion", "Snapshot.Build.OS", "Snapshot.Build.Arch",
	"Snapshot.Runtime.Goroutines", "Snapshot.Runtime.GOMAXPROCS", "Snapshot.Runtime.UptimeSec",
	"Snapshot.Runtime.GoroutineStates.Running", "Snapshot.Runtime.GoroutineStates.Runnable", "Snapshot.Runtime.GoroutineStates.Waiting",
	"Snapshot.Runtime.Heap.Alloc", "Snapshot.Runtime.Heap.Sys", "Snapshot.Runtime.Heap.Inuse",
	"Snapshot.Runtime.Heap.Idle", "Snapshot.Runtime.Heap.Released", "Snapshot.Runtime.Heap.Objects",
	"Snapshot.Runtime.Heap.TotalAlloc", "Snapshot.Runtime.Heap.TotalFree",
	"Snapshot.Runtime.Stack.Inuse", "Snapshot.Runtime.Stack.Sys",
	"Snapshot.Runtime.Metadata.Inuse", "Snapshot.Runtime.Metadata.Sys",
	"Snapshot.Process.CPUPercent", "Snapshot.Process.CPUSecondsUser", "Snapshot.Process.CPUSecondsSys",
	"Snapshot.Process.RSSBytes", "Snapshot.Process.VSZBytes", "Snapshot.Process.Threads", "Snapshot.Process.OpenFDs",
	"Snapshot.GC.Cycles", "Snapshot.GC.PauseTotalMs", "Snapshot.GC.NextGCBytes", "Snapshot.GC.MemLimitBytes",
	"Snapshot.GC.Pause.P50Ms", "Snapshot.GC.Pause.P90Ms", "Snapshot.GC.Pause.P99Ms", "Snapshot.GC.Pause.MaxMs",
	"Snapshot.Scheduler.Latency.P50", "Snapshot.Scheduler.Latency.P90", "Snapshot.Scheduler.Latency.P99",
	"Snapshot.Locks.GlobalWaitSec",
	"Snapshot.Locks.Observers[].Name", "Snapshot.Locks.Observers[].Waits", "Snapshot.Locks.Observers[].TotalWaitMs",
	"Snapshot.Locks.Observers[].MaxWaitMs", "Snapshot.Locks.Observers[].P50WaitMs", "Snapshot.Locks.Observers[].P99WaitMs",
	"Snapshot.Storage.DataDir", "Snapshot.Storage.Queues", "Snapshot.Storage.Segments", "Snapshot.Storage.SizeBytes",
	"Snapshot.Storage.CacheBytes", "Snapshot.Storage.CacheLimit", "Snapshot.Storage.CacheHits", "Snapshot.Storage.CacheMisses",
	"Snapshot.Storage.CommittedBytes", "Snapshot.Storage.FlushedBytes", "Snapshot.Storage.PendingBytes",
	"Snapshot.Storage.OffsetStoreBytes",
	"Snapshot.Queues[].Name", "Snapshot.Queues[].Kind", "Snapshot.Queues[].Counters", "Snapshot.Queues[].Gauges",
	"Snapshot.Queues[].Histograms",
	"Snapshot.Queues[].Rates.AppendsPerSec", "Snapshot.Queues[].Rates.ReadsPerSec",
	"Snapshot.Queues[].Rates.EnqueuesPerSec", "Snapshot.Queues[].Rates.DequeuesPerSec",
	"Snapshot.Queues[].Rates.BytesInPerSec", "Snapshot.Queues[].Rates.BytesOutPerSec",
	"Snapshot.Queues[].Producers.InFlight", "Snapshot.Queues[].Producers.BlockedOnWAL", "Snapshot.Queues[].Producers.BlockEvents",
	"Snapshot.Queues[].Consumers.InFlight", "Snapshot.Queues[].Consumers.ParkedFetch", "Snapshot.Queues[].Consumers.ParkedDequeue",
	"Snapshot.Queues[].Consumers.FetchParkEvents", "Snapshot.Queues[].Consumers.DequeueParkEvents",
	"Snapshot.Queues[].Backpressure.WALChannelFill", "Snapshot.Queues[].Backpressure.WALByteFill",
	"Snapshot.Queues[].Backpressure.QueueFullEvents", "Snapshot.Queues[].Backpressure.ProducerBlocks",
	"Snapshot.Queues[].Backpressure.ConsumerParks", "Snapshot.Queues[].Backpressure.RetryBudgetDrops",
	"Snapshot.Queues[].Tasks.Active", "Snapshot.Queues[].Tasks.InFlight", "Snapshot.Queues[].Tasks.Scheduled",
	"Snapshot.Queues[].Tasks.DeadLetters", "Snapshot.Queues[].Tasks.Uncheckpointed", "Snapshot.Queues[].Tasks.ScheduleHeap",
	"Snapshot.Queues[].Tasks.CheckpointLSN", "Snapshot.Queues[].Tasks.Dirty", "Snapshot.Queues[].Tasks.CheckpointLag",
	"Snapshot.Queues[].Groups[].Name", "Snapshot.Queues[].Groups[].Committed",
	"Snapshot.Queues[].Groups[].HighWater", "Snapshot.Queues[].Groups[].Lag",
	"Snapshot.Totals.Queues", "Snapshot.Totals.Messages", "Snapshot.Totals.Bytes",
}

// skipFields lists Snapshot paths the exposition deliberately does not export, with a reason.
var skipFields = map[string]string{
	"Snapshot.Disabled":           "not a measurement; the exporter emits nothing useful when metrics are off",
	"Snapshot.Timestamp":          "Prometheus timestamps its own scrapes",
	"Snapshot.Session":            "static session config; the JSON endpoint's job",
	"Snapshot.Queues[].Spec":      "static config; collapsed into the mikyu_queue_info labels",
	"Snapshot.Queues[].Derived":   "static config; collapsed into the mikyu_queue_info labels",
	"Snapshot.Queues[].Effective": "static config; collapsed into the mikyu_queue_info labels",
}

// walkLeaves reports every leaf field path of t, using [] for slice elements.
func walkLeaves(t reflect.Type, path string, fn func(string)) {
	for f := range t.Fields() {
		p := path + "." + f.Name
		ft := f.Type
		for ft.Kind() == reflect.Pointer {
			ft = ft.Elem()
		}
		switch {
		case ft.Kind() == reflect.Struct && ft != reflect.TypeFor[time.Time]():
			walkLeaves(ft, p, fn)
		case (ft.Kind() == reflect.Slice || ft.Kind() == reflect.Array) &&
			ft.Elem().Kind() == reflect.Struct && ft.Elem() != reflect.TypeFor[time.Time]():
			walkLeaves(ft.Elem(), p+"[]", fn)
		default:
			fn(p)
		}
	}
}
