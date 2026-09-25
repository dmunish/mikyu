package metrics

import (
	"bytes"
	"flag"
	"math"
	"os"
	"path/filepath"
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
	assert.Contains(t, out, "mikyu_goroutines 7")
	assert.Contains(t, out, "mikyu_queues 1")
	assert.NotContains(t, out, "mikyu_queue_messages")
	assert.NotContains(t, out, "mikyu_append_total")
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
	assert.Contains(t, out, "mikyu_append_total{queue=\"q\"} 1")
	assert.Contains(t, out, "mikyu_custom_ops_total{queue=\"q\"} 2")
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

	assert.Contains(t, out, "# TYPE mikyu_append_duration_quantiles_seconds summary")
	for _, label := range []string{`quantile="0.5"`, `quantile="0.9"`, `quantile="0.99"`, `quantile="+Inf"`} {
		assert.Contains(t, out, `mikyu_append_duration_quantiles_seconds{queue="events",`+label+"}")
	}
}

// TestPrometheusQuantilesAreMonotone: computed quantiles never decrease as p rises.
func TestPrometheusQuantilesAreMonotone(t *testing.T) {
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, goldenSnapshot(), EncodeOptions{PerQueue: true, Histograms: true}))

	var prev float64
	var seen bool
	for _, line := range strings.Split(buf.String(), "\n") {
		if !strings.HasPrefix(line, "mikyu_append_duration_quantiles_seconds{") {
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
	for _, pair := range strings.Split(series, ",") {
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
		{Name: "server", Kind: internalKind, Counters: map[string]int64{"connections": 3}},
	}}
	var buf bytes.Buffer
	require.NoError(t, Encode(&buf, snap, EncodeOptions{PerQueue: true}))
	out := buf.String()

	assert.Equal(t, 1, strings.Count(out, "mikyu_queue_info{"))
	assert.Contains(t, out, `mikyu_queue_info{queue="events"`)
	assert.NotContains(t, out, `queue="server",kind="internal"`)
	assert.Contains(t, out, `mikyu_connection_total{queue="server"} 3`)
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

	for _, line := range strings.Split(buf.String(), "\n") {
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
	assert.Contains(t, buf.String(), "mikyu_uptime_seconds 0")
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
	for _, line := range strings.Split(buf.String(), "\n") {
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
	assert.NotContains(t, out, "mikyu_append_total")
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
