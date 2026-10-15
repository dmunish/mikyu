package metrics

import (
	"bytes"
	"flag"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
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

func TestPrometheusEncodeShape(t *testing.T) {
	var buf bytes.Buffer
	err := Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true, Histograms: true})
	require.NoError(t, err)

	golden := filepath.Join("testdata", "snapshot.prom")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, buf.Bytes(), 0o644))
		return
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	assert.Equal(t, normalizeNewlines(string(want)), normalizeNewlines(buf.String()))
}

// normalizeNewlines makes the golden comparison independent of checkout line endings.
func normalizeNewlines(s string) string { return strings.ReplaceAll(s, "\r\n", "\n") }

func TestPrometheusGlobalOnlyByDefault(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{}))
	out := buf.String()
	assert.Contains(t, out, "mikyu_runtime_goroutines 7")
	assert.Contains(t, out, "mikyu_totals_queues 1")
	assert.NotContains(t, out, "mikyu_queue_messages")
	assert.NotContains(t, out, "mikyu_queue_appends_total")
}

func TestPrometheusTopNQueues(t *testing.T) {
	snap := Snapshot{
		Queues: []QueueView{
			{Name: "small", Gauges: map[string]int64{"size_bytes": 1, "messages": 1}},
			{Name: "large", Gauges: map[string]int64{"size_bytes": 100, "messages": 5}},
			{Name: "medium", Gauges: map[string]int64{"size_bytes": 50, "messages": 3}},
		},
	}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true, TopNQueues: 2}))
	out := buf.String()
	assert.Contains(t, out, `mikyu_queue_messages{queue="large"}`)
	assert.Contains(t, out, `mikyu_queue_messages{queue="medium"}`)
	assert.NotContains(t, out, `queue="small"`)
}

func TestPrometheusCounterSuffix(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{{Name: "q", Counters: map[string]int64{"appends": 1, "custom_ops": 2}}}}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true}))
	out := buf.String()
	assert.Contains(t, out, `mikyu_queue_appends_total{queue="q"} 1`)
	assert.Contains(t, out, `mikyu_queue_custom_ops_total{queue="q"} 2`)
}

// TestPrometheusPercentileQuantiles: histograms expose quantile summaries.
func TestPrometheusPercentileQuantiles(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues[0].Histograms["append_duration"] = HistogramView{
		Bounds: []float64{0.001, 0.01}, Counts: []uint64{5, 3, 2}, Sum: 0.5, Total: 10,
		P50: 0.0005, P90: 0.005, P99: 0.009, Max: 0.01,
	}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true, Histograms: true}))
	out := buf.String()

	assert.Contains(t, out, "# TYPE mikyu_queue_append_duration_quantiles_seconds summary")
	for _, label := range []string{`quantile="0.5"`, `quantile="0.9"`, `quantile="0.99"`, `quantile="+Inf"`} {
		assert.Contains(t, out, `mikyu_queue_append_duration_quantiles_seconds{queue="events",`+label+"}")
	}
}

// TestPrometheusQuantilesAreMonotone: computed quantiles never decrease as p rises.
func TestPrometheusQuantilesAreMonotone(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true, Histograms: true}))

	var prev float64
	var seen bool
	for line := range strings.SplitSeq(buf.String(), "\n") {
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

// TestPrometheusInfoDials: one info series per queue with stable label ordering.
func TestPrometheusInfoDials(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Equal(t, 1, strings.Count(out, "# TYPE mikyu_queue_info gauge"))
	assert.Equal(t, 2, strings.Count(out, "mikyu_queue_info{"))
	assert.Contains(t, out, `mikyu_queue_info{queue="events",kind="log"`)
	assert.Contains(t, out, `durability="sync"`)
	assert.Contains(t, out, `max_segment="67108864"`)
	assert.Contains(t, out, `retention_age="24h0m0s"`)
	assert.Contains(t, out, `mikyu_queue_info{queue="jobs",kind="task"`)

	start := strings.Index(out, `mikyu_queue_info{queue="events"`)
	open := strings.Index(out[start:], "{")
	series := out[start+open+1 : start+strings.Index(out[start:], "}")]
	var keys []string
	for pair := range strings.SplitSeq(series, ",") {
		keys = append(keys, strings.SplitN(pair, "=", 2)[0])
	}
	assert.Equal(t, "queue", keys[0])
	assert.Equal(t, "kind", keys[1])
	assert.IsIncreasing(t, keys[2:], "dial labels must be emitted in sorted order")
}

// TestPrometheusInfoSkipsInternalScopes: a non-queue scope is not a queue.
func TestPrometheusInfoSkipsInternalScopes(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{
		{Name: "events", Kind: "log"},
		{Name: "server", Kind: internalKind, Gauges: map[string]int64{"connections": 3},
			Counters: map[string]int64{"connections_rejected": 1}},
	}}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Equal(t, 1, strings.Count(out, "mikyu_queue_info{"))
	assert.Contains(t, out, `mikyu_queue_info{queue="events"`)
	assert.NotContains(t, out, `queue="server",kind="internal"`)
	assert.Contains(t, out, `mikyu_queue_connections{queue="server"} 3`)
	assert.Contains(t, out, `mikyu_queue_op_connections_rejected_total{queue="server"} 1`)
}

// TestPrometheusNoNaNOrInf: no sample value is NaN or infinite.
func TestPrometheusNoNaNOrInf(t *testing.T) {
	snap := goldenSnapshot()
	snap.Runtime.UptimeSec = math.NaN()
	snap.Process.CPUPercent = math.Inf(1)
	snap.Queues[0].Backpressure.WALChannelFill = math.Inf(-1)
	hist := snap.Queues[0].Histograms["append_duration"]
	hist.Sum = math.NaN()
	snap.Queues[0].Histograms["append_duration"] = hist

	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true, Histograms: true}))

	for line := range strings.SplitSeq(buf.String(), "\n") {
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
	assert.Contains(t, buf.String(), "mikyu_runtime_uptime_seconds 0")
	assert.Contains(t, buf.String(), `mikyu_queue_backpressure_wal_channel_fill{queue="events"} 0`)
}

// TestPrometheusCounterSuffixInvariant: every counter family ends in _total.
func TestPrometheusCounterSuffixInvariant(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues = append(snap.Queues, QueueView{
		Name:     "custom",
		Counters: map[string]int64{"already_total": 1, "ops": 2, "queue_faults": 3},
	})
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true}))

	var counters int
	for line := range strings.SplitSeq(buf.String(), "\n") {
		if !strings.HasPrefix(line, "# TYPE ") || !strings.HasSuffix(line, " counter") {
			continue
		}
		counters++
		assert.True(t, strings.HasSuffix(strings.Fields(line)[2], "_total"), "counter family must end in _total: %s", line)
	}
	assert.Positive(t, counters)
}

// TestPrometheusFaultsSeries: a faulted queue reports faults; a clean queue does not.
func TestPrometheusFaultsSeries(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Contains(t, out, "# TYPE mikyu_queue_faults_total counter")
	assert.Contains(t, out, `mikyu_queue_faults_total{queue="events"} 1`)
	assert.NotContains(t, out, `mikyu_queue_faults_total{queue="jobs"}`)

	zero := Snapshot{Queues: []QueueView{{Name: "clean", Counters: map[string]int64{"queue_faults": 0}}}}
	buf.Reset()
	require.NoError(t, Encode(&buf, zero, EncodeOptions{PerQueue: true}))
	assert.NotContains(t, buf.String(), `mikyu_queue_faults_total{queue="clean"}`)
}

// TestPrometheusNilMapsSkipped: a queue with nil maps emits only gauge lines.
func TestPrometheusNilMapsSkipped(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{
		{Name: "bare"},
		{Name: "gauged", Gauges: map[string]int64{"messages": 1}},
	}}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true, Histograms: true}))
	out := buf.String()

	assert.Contains(t, out, `mikyu_queue_messages{queue="gauged"} 1`)
	assert.NotContains(t, out, `mikyu_queue_messages{queue="bare"}`)
	assert.NotContains(t, out, "_bucket")
	assert.NotContains(t, out, "mikyu_queue_appends_total")
	assert.NotContains(t, out, faultsMetric)
	assert.NotContains(t, out, "mikyu_queue_tasks_active")
	assert.NotContains(t, out, "mikyu_queue_producers_in_flight")
}

// TestPrometheusTaskViewSeries: task occupancy reaches Prometheus as gauges.
func TestPrometheusTaskViewSeries(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Contains(t, out, `mikyu_queue_tasks_active{queue="jobs"} 42`)
	assert.Contains(t, out, `mikyu_queue_tasks_in_flight{queue="jobs"} 8`)
	assert.Contains(t, out, `mikyu_queue_tasks_dead_letters{queue="jobs"} 2`)
	assert.Contains(t, out, `mikyu_queue_tasks_checkpoint_lag{queue="jobs"} 3`)
	assert.NotContains(t, out, `mikyu_queue_tasks_active{queue="events"}`)
	assert.NotContains(t, out, `active=42`)
}

// coveredFields lists every Snapshot leaf the encoder exports, as a series or a label.
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

// skipFields lists Snapshot paths the encoder deliberately does not export, with a reason.
var skipFields = map[string]string{
	"Snapshot.Disabled":           "not a measurement; the exporter emits nothing useful when metrics are off",
	"Snapshot.Timestamp":          "Prometheus timestamps its own scrapes",
	"Snapshot.Session":            "static session config; the JSON endpoint's job",
	"Snapshot.Queues[].Spec":      "static config; collapsed into the mikyu_queue_info labels",
	"Snapshot.Queues[].Derived":   "static config; collapsed into the mikyu_queue_info labels",
	"Snapshot.Queues[].Effective": "static config; collapsed into the mikyu_queue_info labels",
}

// TestPrometheusCoversSnapshot: every Snapshot leaf is exported or explicitly skipped.
func TestPrometheusCoversSnapshot(t *testing.T) {
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

// walkLeaves reports every leaf field path of t, using [] for slice elements.
func walkLeaves(t reflect.Type, path string, fn func(string)) {
	for f := range t.Fields() {
		f := f
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

// TestPrometheusBuildInfo: build identity is one labeled series.
func TestPrometheusBuildInfo(t *testing.T) {
	snap := goldenSnapshot()
	snap.Build = BuildView{SpecVersion: 5, GoVersion: "go1.27", OS: "linux", Arch: "amd64"}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{}))

	assert.Contains(t, buf.String(),
		`mikyu_build_info{spec_version="5",go_version="go1.27",os="linux",arch="amd64"} 1`)
}

// TestPrometheusStorageAndProcess: storage aggregates and process detail are exported.
func TestPrometheusStorageAndProcess(t *testing.T) {
	snap := goldenSnapshot()
	snap.Storage = StorageView{
		DataDir: `C:\data\mikyu`, Queues: 2, SizeBytes: 4096,
		CommittedBytes: 1000, FlushedBytes: 900, PendingBytes: 100, CacheHits: 30, CacheMisses: 2,
	}
	snap.Process = ProcessView{CPUSecondsUser: 1.5, CPUSecondsSys: 0.5, VSZBytes: 8192, Threads: 9, OpenFDs: 21}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{}))
	out := buf.String()

	assert.Contains(t, out, `mikyu_storage_info{data_dir="C:\\data\\mikyu"} 1`)
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

// TestPrometheusLockObservers: each observed lock becomes a labeled series.
func TestPrometheusLockObservers(t *testing.T) {
	snap := goldenSnapshot()
	snap.Locks = LocksView{
		GlobalWaitSec: 0.25,
		Observers: []LockView{{
			Name: "node.mu", Waits: 4, TotalWaitMs: 2, MaxWaitMs: 1.5, P50WaitMs: 0.1, P99WaitMs: 1.2,
		}},
	}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{}))
	out := buf.String()

	assert.Contains(t, out, "mikyu_lock_global_wait_seconds_total 0.25")
	assert.Contains(t, out, `mikyu_lock_waits_total{lock="node.mu"} 4`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds_total{lock="node.mu"} 0.002`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds_max{lock="node.mu"} 0.0015`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds{lock="node.mu",quantile="0.5"} 0.0001`)
	assert.Contains(t, out, `mikyu_lock_wait_seconds{lock="node.mu",quantile="0.99"} 0.0012`)
}

// TestPrometheusLockFamiliesOmittedWhenIdle: no observers, no lock families.
func TestPrometheusLockFamiliesOmittedWhenIdle(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{}))
	out := buf.String()

	assert.Contains(t, out, "mikyu_lock_global_wait_seconds_total")
	assert.NotContains(t, out, "mikyu_lock_waits_total")
	assert.NotContains(t, out, "mikyu_lock_wait_seconds_max")
}

// TestPrometheusGroupSeries: consumer groups carry queue and group labels.
func TestPrometheusGroupSeries(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues[0].Groups = []GroupView{{Name: "analytics", Committed: 900, HighWater: 1000, Lag: 100}}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Contains(t, out, `mikyu_queue_group_committed{queue="events",group="analytics"} 900`)
	assert.Contains(t, out, `mikyu_queue_group_high_water{queue="events",group="analytics"} 1000`)
	assert.Contains(t, out, `mikyu_queue_group_lag{queue="events",group="analytics"} 100`)
}

// TestPrometheusGroupFamiliesOmittedWhenNoGroups: no groups, no group families.
func TestPrometheusGroupFamiliesOmittedWhenNoGroups(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true}))
	assert.NotContains(t, buf.String(), "mikyu_queue_group_lag")
}

// TestPrometheusRates: the collector's derivatives are exported per queue.
func TestPrometheusRates(t *testing.T) {
	snap := goldenSnapshot()
	snap.Queues[0].Rates = Rates{AppendsPerSec: 1200, ReadsPerSec: 800, BytesInPerSec: 4096}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Contains(t, out, "mikyu_queue_appends_per_second{queue=\"events\"} 1200")
	assert.Contains(t, out, "mikyu_queue_reads_per_second{queue=\"events\"} 800")
	assert.Contains(t, out, "mikyu_queue_bytes_in_per_second{queue=\"events\"} 4096")
}

// TestPrometheusStartTimeOmittedWhenUptimeUnknown: no uptime, no bogus start time.
func TestPrometheusStartTimeOmittedWhenUptimeUnknown(t *testing.T) {
	snap := goldenSnapshot()
	snap.Runtime.UptimeSec = 0
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{}))
	assert.NotContains(t, buf.String(), "mikyu_process_start_time_seconds")
}

// TestPrometheusExpositionIsWellFormed: every sample belongs to a declared family
// and uses a name that family's type permits. A quantile sample sharing a
// histogram family name, for instance, would fail here.
func TestPrometheusExpositionIsWellFormed(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true, Histograms: true}))

	types := map[string]string{}
	helps := map[string]bool{}
	for line := range strings.SplitSeq(buf.String(), "\n") {
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
	}
	for family := range types {
		assert.True(t, helps[family], "family %s has no HELP line", family)
	}

	for line := range strings.SplitSeq(buf.String(), "\n") {
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
			assert.Equal(t, "histogram", types[family], "%s suffix is only valid on a histogram", name)
		}
		value, err := strconv.ParseFloat(fields[1], 64)
		require.NoError(t, err, "unparseable value in: %s", line)
		assert.False(t, math.IsNaN(value) || math.IsInf(value, 0), "non-finite value in: %s", line)
	}
}

// TestPrometheusSummaryCarriesOnlyQuantiles: a summary family has no bucket samples.
func TestPrometheusSummaryCarriesOnlyQuantiles(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true, Histograms: true}))

	for line := range strings.SplitSeq(buf.String(), "\n") {
		if !strings.HasPrefix(line, "mikyu_queue_append_duration_seconds{") {
			continue
		}
		assert.Contains(t, line, `quantile=`, "summary sample must be labeled with a quantile: %s", line)
	}
}

// TestPrometheusNameTablesAreConsistent: every table entry yields a unique,
// correctly suffixed metric name. Two keys mapping to one name would silently
// merge two series, and a wrong suffix would break the _total counter rule.
func TestPrometheusNameTablesAreConsistent(t *testing.T) {
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

// TestPrometheusServerOpHelpDerived: server op counters describe themselves.
func TestPrometheusServerOpHelpDerived(t *testing.T) {
	assert.Equal(t, "Server append operations.", counterHelpFor("op_append"))
	assert.Equal(t, "Server create queue operations.", counterHelpFor("op_create_queue"))
	assert.Equal(t, "Queue counter.", counterHelpFor("not_an_op"))
	assert.Equal(t, "Log appends.", counterHelpFor("appends"))
}
