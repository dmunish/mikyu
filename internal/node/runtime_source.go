package node

import (
	"context"
	"runtime"
	"runtime/debug"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

// RuntimeSource fills the runtime and process views of a snapshot.
type RuntimeSource struct {
	start time.Time
}

// NewRuntimeSource records the process start time for uptime.
func NewRuntimeSource() *RuntimeSource { return &RuntimeSource{start: time.Now()} }

// Collect populates dst.Runtime and dst.Process.
func (s *RuntimeSource) Collect(_ context.Context, dst *metrics.Snapshot) {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	dst.Runtime.Goroutines = runtime.NumGoroutine()
	dst.Runtime.GOMAXPROCS = runtime.GOMAXPROCS(0)
	dst.Runtime.Heap = metrics.HeapView{
		Alloc:      int64(m.HeapAlloc),
		Sys:        int64(m.HeapSys),
		Inuse:      int64(m.HeapInuse),
		Idle:       int64(m.HeapIdle),
		Released:   int64(m.HeapReleased),
		Objects:    int64(m.HeapObjects),
		TotalAlloc: int64(m.TotalAlloc),
	}
	dst.Runtime.Stack = metrics.MemoryClassView{Inuse: int64(m.StackInuse), Sys: int64(m.StackSys)}
	dst.Runtime.Metadata = metrics.MemoryClassView{Inuse: int64(m.MSpanInuse + m.MCacheInuse), Sys: int64(m.MSpanSys + m.MCacheSys)}
	dst.Runtime.UptimeSec = time.Since(s.start).Seconds()
	dst.GC.Cycles = int64(m.NumGC)
	dst.GC.PauseTotalMs = float64(m.PauseTotalNs) / 1e6
	dst.GC.NextGCBytes = int64(m.NextGC)
	dst.GC.MemLimitBytes = debug.SetMemoryLimit(-1)
	dst.Process.CPUPercent = readProcessCPU()
	dst.Process.RSSBytes = readProcessRSS()
}

var (
	cpuMu     sync.Mutex
	cpuSample = struct {
		usedNs int64
		wallNs int64
		valid  bool
	}{}
)

// cpuPercent converts a cumulative process CPU-time reading (nanoseconds) into
// a utilization percent against the process's own wall-clock delta. The first
// sample only establishes the baseline and reports zero.
func cpuPercent(usedNs int64) float64 {
	now := time.Now().UnixNano()
	cpuMu.Lock()
	defer cpuMu.Unlock()
	if !cpuSample.valid {
		cpuSample.usedNs = usedNs
		cpuSample.wallNs = now
		cpuSample.valid = true
		return 0
	}
	dCPU := usedNs - cpuSample.usedNs
	dWall := now - cpuSample.wallNs
	cpuSample.usedNs = usedNs
	cpuSample.wallNs = now
	if dWall <= 0 {
		return 0
	}
	return 100 * float64(dCPU) / float64(dWall)
}
