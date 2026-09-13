package metrics

import (
	"bytes"
	"flag"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

var updateGolden = flag.Bool("update", false, "rewrite golden files")

func goldenSnapshot() Snapshot {
	return Snapshot{
		Timestamp: time.Unix(0, 0).UTC(),
		Runtime: RuntimeView{
			Goroutines: 7,
			HeapAlloc:  1024,
			HeapInuse:  2048,
			GCCount:    3,
			UptimeSec:  12.5,
		},
		Process: ProcessView{CPUPercent: 1.25, RSSBytes: 4096},
		Totals:  TotalsView{Queues: 1, Messages: 10, Bytes: 2048},
		Queues: []QueueView{{
			Name:     "events",
			Kind:     "shared",
			Counters: map[string]int64{"appends": 10, "reads": 4},
			Gauges:   map[string]int64{"messages": 10, "size_bytes": 2048, "segments": 1},
			Histograms: map[string]HistogramSnapshot{
				"append_duration": {Bounds: []float64{0.001, 0.01}, Counts: []uint64{5, 3, 2}, Sum: 0.5, Total: 10},
			},
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
	assert.Equal(t, string(want), buf.String())
}

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
