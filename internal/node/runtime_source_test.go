package node

import (
	"math"
	"runtime"
	runtime_metrics "runtime/metrics"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMemLimitBytesNormalizesUnset: the runtime's MaxInt64 sentinel becomes the 0-means-unlimited contract.
func TestMemLimitBytesNormalizesUnset(t *testing.T) {
	assert.EqualValues(t, 0, memLimitBytes(math.MaxInt64), "unset reads as unlimited")
	assert.EqualValues(t, 512<<20, memLimitBytes(512<<20), "a set limit passes through")
	assert.EqualValues(t, 0, memLimitBytes(0))
}

// TestRuntimeSourceMemLimitNeverSentinels: a real tick never publishes the MaxInt64 sentinel.
func TestRuntimeSourceMemLimitNeverSentinels(t *testing.T) {
	src := newRuntimeSource()
	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)
	assert.NotEqual(t, int64(math.MaxInt64), snap.GC.MemLimitBytes,
		"an unset GOMEMLIMIT must not surface as a petabyte-scale limit")
	assert.GreaterOrEqual(t, snap.GC.MemLimitBytes, int64(0))
}

func TestRuntimeSourceShape(t *testing.T) {
	src := newRuntimeSource()
	time.Sleep(time.Millisecond)

	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)

	require.NotEmpty(t, snap.Build.GoVersion)
	require.NotEmpty(t, snap.Build.OS)
	require.NotEmpty(t, snap.Build.Arch)
	require.Positive(t, snap.Build.SpecVersion)
	require.Positive(t, snap.Runtime.Goroutines)
	require.Positive(t, snap.Runtime.GOMAXPROCS)
	require.Positive(t, snap.Runtime.UptimeSec)
	require.GreaterOrEqual(t, snap.Runtime.GoroutineStates.Running, int64(0))
	require.GreaterOrEqual(t, snap.Runtime.GoroutineStates.Runnable, int64(0))
	require.GreaterOrEqual(t, snap.Runtime.GoroutineStates.Waiting, int64(0))
	for name, v := range map[string]int64{
		"heap.alloc":      snap.Runtime.Heap.Alloc,
		"heap.sys":        snap.Runtime.Heap.Sys,
		"heap.inuse":      snap.Runtime.Heap.Inuse,
		"heap.idle":       snap.Runtime.Heap.Idle,
		"heap.released":   snap.Runtime.Heap.Released,
		"heap.objects":    snap.Runtime.Heap.Objects,
		"heap.totalalloc": snap.Runtime.Heap.TotalAlloc,
		"heap.totalfree":  snap.Runtime.Heap.TotalFree,
		"stack.inuse":     snap.Runtime.Stack.Inuse,
		"stack.sys":       snap.Runtime.Stack.Sys,
		"metadata.inuse":  snap.Runtime.Metadata.Inuse,
		"metadata.sys":    snap.Runtime.Metadata.Sys,
	} {
		assert.GreaterOrEqual(t, v, int64(0), name)
	}
	require.Positive(t, snap.Process.RSSBytes)
	require.GreaterOrEqual(t, snap.Process.VSZBytes, int64(0))
	require.Positive(t, snap.Process.Threads)
	require.GreaterOrEqual(t, snap.Process.OpenFDs, int64(0))
	require.GreaterOrEqual(t, snap.GC.Cycles, int64(0))
	require.GreaterOrEqual(t, snap.GC.PauseTotalMs, 0.0)
	for name, v := range map[string]float64{
		"pause.p50":        snap.GC.Pause.P50Ms,
		"pause.p90":        snap.GC.Pause.P90Ms,
		"pause.p99":        snap.GC.Pause.P99Ms,
		"pause.max":        snap.GC.Pause.MaxMs,
		"latency.p50":      snap.Scheduler.Latency.P50,
		"latency.p90":      snap.Scheduler.Latency.P90,
		"latency.p99":      snap.Scheduler.Latency.P99,
		"cpu.percent":      snap.Process.CPUPercent,
		"cpu.usersec":      snap.Process.CPUSecondsUser,
		"cpu.syssec":       snap.Process.CPUSecondsSys,
		"locks.globalwait": snap.Locks.GlobalWaitSec,
	} {
		assert.False(t, math.IsNaN(v), name+" is NaN")
		assert.False(t, math.IsInf(v, 0), name+" is Inf")
		assert.GreaterOrEqual(t, v, 0.0, name)
	}
}

func TestRuntimeSourceUptimeGrows(t *testing.T) {
	src := newRuntimeSource()
	var first metrics.Snapshot
	src.Collect(t.Context(), &first)
	time.Sleep(5 * time.Millisecond)
	var second metrics.Snapshot
	src.Collect(t.Context(), &second)
	require.Greater(t, second.Runtime.UptimeSec, first.Runtime.UptimeSec)
}

func TestRuntimeSourceGCPercentiles(t *testing.T) {
	for range 10 {
		runtime.GC()
	}
	src := newRuntimeSource()
	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)

	require.Positive(t, snap.GC.PauseTotalMs)
	assert.LessOrEqual(t, snap.GC.Pause.P50Ms, snap.GC.Pause.P90Ms)
	assert.LessOrEqual(t, snap.GC.Pause.P90Ms, snap.GC.Pause.P99Ms)
	assert.LessOrEqual(t, snap.GC.Pause.P99Ms, snap.GC.Pause.MaxMs)
	for _, v := range []float64{snap.GC.Pause.P50Ms, snap.GC.Pause.P90Ms, snap.GC.Pause.P99Ms, snap.GC.Pause.MaxMs} {
		require.False(t, math.IsNaN(v))
		require.False(t, math.IsInf(v, 0))
	}
}

func TestRuntimeSourceSchedulerLatency(t *testing.T) {
	var wg sync.WaitGroup
	stop := make(chan struct{})
	for range 4 {
		wg.Go(func() {
			for {
				select {
				case <-stop:
					return
				default:
					runtime.Gosched()
				}
			}
		})
	}
	time.Sleep(50 * time.Millisecond)
	close(stop)
	wg.Wait()

	src := newRuntimeSource()
	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)
	require.Positive(t, snap.Scheduler.Latency.P99)
}

func TestRuntimeSourceGoroutineStates(t *testing.T) {
	release := make(chan struct{})
	ready := make(chan struct{}, 10)
	var wg sync.WaitGroup
	for range 10 {
		wg.Go(func() {
			ready <- struct{}{}
			<-release
		})
	}
	for range 10 {
		<-ready
	}
	time.Sleep(20 * time.Millisecond)

	src := newRuntimeSource()
	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)
	close(release)
	wg.Wait()

	require.GreaterOrEqual(t, snap.Runtime.GoroutineStates.Waiting, int64(10))
}

func TestRuntimeSourceMemoryClassesSum(t *testing.T) {
	src := newRuntimeSource()
	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)

	heap := snap.Runtime.Heap
	accounted := heap.Inuse + heap.Idle + heap.Released + snap.Runtime.Stack.Sys + snap.Runtime.Metadata.Sys
	require.Positive(t, heap.Sys)
	require.InDelta(t, float64(heap.Sys), float64(accounted), 0.10*float64(heap.Sys))
}

func TestRuntimeSourceNoAllocationSteadyState(t *testing.T) {
	src := newRuntimeSource()
	ctx := t.Context()
	var warm metrics.Snapshot
	src.Collect(ctx, &warm)

	allocs := testing.AllocsPerRun(1000, func() {
		var snap metrics.Snapshot
		src.Collect(ctx, &snap)
	})
	require.Less(t, allocs, 5.0, "steady-state tick must not allocate")
}

func TestRuntimeSourceSkipsUnavailableMetrics(t *testing.T) {
	src := newRuntimeSource()
	var snap metrics.Snapshot
	require.NotPanics(t, func() { src.Collect(t.Context(), &snap) })
}

func TestRuntimeSourceSkipsBadKind(t *testing.T) {
	src := &RuntimeSource{
		start: time.Now(),
		samples: []runtime_metrics.Sample{
			{Name: "/this/metric/does/not/exist"},
			{Name: "/gc/cycles/total:gc-cycles"},
		},
	}
	var snap metrics.Snapshot
	require.NotPanics(t, func() { src.Collect(t.Context(), &snap) })
	require.GreaterOrEqual(t, snap.GC.Cycles, int64(0))
}

func TestRuntimeSourceFirstTickWithoutGC(t *testing.T) {
	src := newRuntimeSource()
	var snap metrics.Snapshot
	require.NotPanics(t, func() { src.Collect(t.Context(), &snap) })
	require.False(t, math.IsNaN(snap.GC.PauseTotalMs))
	require.GreaterOrEqual(t, snap.GC.PauseTotalMs, 0.0)
}

func TestRuntimeSourceBucketCountsMatchBounds(t *testing.T) {
	src := newRuntimeSource()
	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)

	for name, h := range map[string]metrics.HistogramSnapshot{"pauses": src.pauses, "latencies": src.latencies} {
		require.NotEmpty(t, h.Bounds, name)
		require.Equal(t, len(h.Bounds)+1, len(h.Counts), name)
		var total uint64
		for _, c := range h.Counts {
			total += c
		}
		require.Equal(t, total, h.Total, name)
	}
}

func TestRuntimeSourceConcurrentSnapshot(t *testing.T) {
	src := newRuntimeSource()
	ctx := t.Context()
	churnStop := make(chan struct{})
	var churn sync.WaitGroup
	churn.Go(func() {
		for {
			select {
			case <-churnStop:
				return
			default:
				runtime.Gosched()
			}
		}
	})

	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 50 {
				var snap metrics.Snapshot
				src.Collect(ctx, &snap)
			}
		})
	}
	wg.Wait()
	close(churnStop)
	churn.Wait()
}
