package metrics

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// fullSnapshot builds a snapshot with every view populated for golden and
// round-trip tests.
func fullSnapshot() Snapshot {
	builtin := spec.Builtin()
	return Snapshot{
		Timestamp: time.Unix(1_700_000_000, 0).UTC(),
		Build:     BuildView{SpecVersion: 5, GoVersion: "go1.27", OS: "linux", Arch: "amd64"},
		Runtime: RuntimeView{
			Goroutines:      21,
			GOMAXPROCS:      8,
			GoroutineStates: GoroutineStates{Running: 2, Runnable: 3, Waiting: 16},
			Heap: HeapView{
				Alloc:      64 << 20,
				Sys:        128 << 20,
				Inuse:      70 << 20,
				Idle:       58 << 20,
				Released:   16 << 20,
				Objects:    123456,
				TotalAlloc: 1 << 32,
				TotalFree:  512 << 20,
			},
			Stack:     MemoryClassView{Inuse: 512 << 10, Sys: 1 << 20},
			Metadata:  MemoryClassView{Inuse: 1<<20 + 128<<10, Sys: 2 << 20},
			UptimeSec: 5025.5,
		},
		Process: ProcessView{
			CPUPercent:     12.4,
			CPUSecondsUser: 120.5,
			CPUSecondsSys:  30.25,
			RSSBytes:       128 << 20,
			VSZBytes:       2 << 30,
			Threads:        12,
			OpenFDs:        48,
		},
		GC: GCView{
			Cycles:        42,
			PauseTotalMs:  1234.5,
			NextGCBytes:   128 << 20,
			MemLimitBytes: 512 << 20,
			Pause:         PauseView{P50Ms: 0.3, P90Ms: 0.9, P99Ms: 2.1, MaxMs: 4.2},
		},
		Scheduler: SchedulerView{Latency: LatencyView{P50: 12e-6, P90: 48e-6, P99: 220e-6}},
		Locks: LocksView{
			GlobalWaitSec: 0.42,
			Observers: []LockView{{
				Name:        "node.mu",
				Waits:       17,
				TotalWaitMs: 3.5,
				MaxWaitMs:   1.2,
				P50WaitMs:   0.1,
				P99WaitMs:   1.1,
			}},
		},
		Session: builtin,
		Storage: StorageView{
			DataDir:          "/var/lib/mikyu",
			Queues:           1,
			Segments:         42,
			SizeBytes:        1 << 30,
			CacheBytes:       32 << 20,
			CacheLimit:       32 << 20,
			CacheHits:        1_200_000,
			CacheMisses:      2300,
			CommittedBytes:   12_300_000,
			FlushedBytes:     12_200_000,
			PendingBytes:     64 << 10,
			OffsetStoreBytes: 4096,
		},
		Queues: []QueueView{{
			Name:      "events",
			Kind:      "log",
			Spec:      builtin,
			Derived:   builtin,
			Effective: builtin,
			Counters:  map[string]int64{"appends": 1_200_000, "reads": 800_000},
			Gauges:    map[string]int64{"messages": 1_200_000, "segments": 10, "size_bytes": 200 << 20},
			Rates:     Rates{AppendsPerSec: 1200, ReadsPerSec: 800, BytesInPerSec: 12 << 20, BytesOutPerSec: 8 << 20},
			Histograms: map[string]HistogramView{
				"append_duration": {
					Bounds: []float64{1e-4, 1e-3, 1e-2},
					Counts: []uint64{10, 20, 30, 40},
					Sum:    1.5,
					Total:  100,
					P50:    5e-4,
					P90:    5e-3,
					P95:    7e-3,
					P99:    9e-3,
					P999:   9.9e-3,
					Max:    1e-2,
				},
			},
			Producers:    ProducerView{InFlight: 8, BlockedOnWAL: 0, BlockEvents: 3},
			Consumers:    ConsumerView{InFlight: 4, ParkedFetch: 2, FetchParkEvents: 42},
			Backpressure: BackpressureView{WALChannelFill: 0.003, WALByteFill: 0.0006, ProducerBlocks: 5, ConsumerParks: 42},
			Groups: []GroupView{
				{Name: "analytics", Committed: 999_800, HighWater: 1_000_000, Lag: 200},
				{Name: "alerts", Committed: 999_950, HighWater: 1_000_000, Lag: 50},
			},
		}},
		Totals: TotalsView{Queues: 1, Messages: 1_200_000, Bytes: 200 << 20},
	}
}

func TestSnapshotJSONRoundTrip(t *testing.T) {
	snap := fullSnapshot()
	data, err := json.Marshal(snap)
	require.NoError(t, err)

	var got Snapshot
	require.NoError(t, json.Unmarshal(data, &got))
	require.Equal(t, snap, got)
}

func TestSnapshotOmitemptyRespected(t *testing.T) {
	snap := Snapshot{Timestamp: time.Unix(0, 0).UTC()}
	data, err := json.Marshal(snap)
	require.NoError(t, err)
	text := string(data)

	for _, absent := range []string{`"disabled"`, `"data_dir"`, `"session"`, `"counters"`, `"gauges"`, `"histograms"`, `"observers"`, `"groups"`} {
		assert.NotContains(t, text, absent)
	}
	assert.Contains(t, text, `"timestamp"`)
	assert.Contains(t, text, `"queues":null`)
}

func TestSnapshotQueueLookup(t *testing.T) {
	snap := Snapshot{Queues: []QueueView{{Name: "charlie"}, {Name: "alpha"}, {Name: "bravo"}}}
	var names []string
	for q := range snap.IterQueues() {
		names = append(names, q.Name)
	}
	assert.Equal(t, []string{"alpha", "bravo", "charlie"}, names)

	q, ok := snap.Queue("bravo")
	require.True(t, ok)
	assert.Equal(t, QueueView{Name: "bravo"}, q)

	_, ok = snap.Queue("missing")
	assert.False(t, ok)
}

func TestSnapshotJSONGolden(t *testing.T) {
	data, err := json.MarshalIndent(fullSnapshot(), "", "  ")
	require.NoError(t, err)
	data = append(data, '\n')

	golden := filepath.Join("testdata", "snapshot.json")
	if *updateGolden {
		require.NoError(t, os.MkdirAll("testdata", 0o755))
		require.NoError(t, os.WriteFile(golden, data, 0o644))
		return
	}
	want, err := os.ReadFile(golden)
	require.NoError(t, err)
	assert.Equal(t, normalizeNewlines(string(want)), normalizeNewlines(string(data)))
}

func TestSnapshotJSONStable(t *testing.T) {
	snap := fullSnapshot()
	first, err := json.Marshal(snap)
	require.NoError(t, err)
	for range 100 {
		next, err := json.Marshal(snap)
		require.NoError(t, err)
		require.Equal(t, string(first), string(next))
	}
}
