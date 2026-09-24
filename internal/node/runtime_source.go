package node

import (
	"context"
	"math"
	"runtime"
	runtime_metrics "runtime/metrics"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
)

// runtimeMetricNames is the set of runtime/metrics samples the collector reads.
// Names missing from the running build are filtered out at construction.
var runtimeMetricNames = []string{
	"/sched/goroutines:goroutines",
	"/sched/goroutines/running:goroutines",
	"/sched/goroutines/runnable:goroutines",
	"/sched/goroutines/waiting:goroutines",
	"/sched/latencies:seconds",
	"/gc/cycles/total:gc-cycles",
	"/gc/pauses:seconds",
	"/gc/heap/goal:bytes",
	"/gc/gomemlimit:bytes",
	"/gc/heap/allocs:bytes",
	"/gc/heap/frees:bytes",
	"/gc/heap/objects:objects",
	"/memory/classes/total:bytes",
	"/memory/classes/heap/objects:bytes",
	"/memory/classes/heap/unused:bytes",
	"/memory/classes/heap/free:bytes",
	"/memory/classes/heap/released:bytes",
	"/memory/classes/heap/stacks:bytes",
	"/memory/classes/os-stacks:bytes",
	"/memory/classes/metadata/mcache/inuse:bytes",
	"/memory/classes/metadata/mcache/free:bytes",
	"/memory/classes/metadata/mspan/inuse:bytes",
	"/memory/classes/metadata/mspan/free:bytes",
	"/memory/classes/metadata/other:bytes",
	"/memory/classes/other:bytes",
	"/memory/classes/profiling/buckets:bytes",
	"/sync/mutex/wait/total:seconds",
}

// RuntimeSource fills the runtime, gc, scheduler, and process views of a snapshot.
type RuntimeSource struct {
	mu        sync.Mutex
	start     time.Time
	samples   []runtime_metrics.Sample
	pauses    metrics.HistogramSnapshot
	latencies metrics.HistogramSnapshot
}

// newRuntimeSource records the process start time and filters the metric list.
func newRuntimeSource() *RuntimeSource {
	have := make(map[string]bool)
	for _, d := range runtime_metrics.All() {
		have[d.Name] = true
	}
	names := make([]string, 0, len(runtimeMetricNames))
	for _, name := range runtimeMetricNames {
		if have[name] {
			names = append(names, name)
		}
	}
	samples := make([]runtime_metrics.Sample, len(names))
	for i, name := range names {
		samples[i].Name = name
	}
	return &RuntimeSource{start: time.Now(), samples: samples}
}

// Collect populates the runtime, gc, scheduler, and process views.
func (s *RuntimeSource) Collect(_ context.Context, dst *metrics.Snapshot) {
	s.mu.Lock()
	defer s.mu.Unlock()
	runtime_metrics.Read(s.samples)
	dst.Build = metrics.BuildView{
		SpecVersion: spec.CurrentSpecVersion,
		GoVersion:   runtime.Version(),
		OS:          runtime.GOOS,
		Arch:        runtime.GOARCH,
	}
	dst.Runtime.Goroutines = runtime.NumGoroutine()
	dst.Runtime.GOMAXPROCS = runtime.GOMAXPROCS(0)
	dst.Runtime.UptimeSec = time.Since(s.start).Seconds()
	s.read(dst)
	dst.GC.Pause = metrics.PauseView{
		P50Ms: s.pauses.Percentile(0.50) * 1000,
		P90Ms: s.pauses.Percentile(0.90) * 1000,
		P99Ms: s.pauses.Percentile(0.99) * 1000,
		MaxMs: s.pauses.Max() * 1000,
	}
	dst.Scheduler.Latency = metrics.LatencyView{
		P50: s.latencies.Percentile(0.50),
		P90: s.latencies.Percentile(0.90),
		P99: s.latencies.Percentile(0.99),
	}
	readProcess(&dst.Process)
}

// read folds each runtime/metrics sample into the destination's views.
func (s *RuntimeSource) read(dst *metrics.Snapshot) {
	var heapUnused, osStacks, mcacheInuse, mcacheFree, mspanInuse, mspanFree, metadataOther int64
	for _, sample := range s.samples {
		switch sample.Name {
		case "/sched/goroutines:goroutines":
			dst.Runtime.Goroutines = int(sampleUint(sample))
		case "/sched/goroutines/running:goroutines":
			dst.Runtime.GoroutineStates.Running = int64(sampleUint(sample))
		case "/sched/goroutines/runnable:goroutines":
			dst.Runtime.GoroutineStates.Runnable = int64(sampleUint(sample))
		case "/sched/goroutines/waiting:goroutines":
			dst.Runtime.GoroutineStates.Waiting = int64(sampleUint(sample))
		case "/gc/cycles/total:gc-cycles":
			dst.GC.Cycles = int64(sampleUint(sample))
		case "/gc/heap/goal:bytes":
			dst.GC.NextGCBytes = int64(sampleUint(sample))
		case "/gc/gomemlimit:bytes":
			dst.GC.MemLimitBytes = int64(sampleUint(sample))
		case "/gc/heap/allocs:bytes":
			dst.Runtime.Heap.TotalAlloc = int64(sampleUint(sample))
		case "/gc/heap/frees:bytes":
			dst.Runtime.Heap.TotalFree = int64(sampleUint(sample))
		case "/gc/heap/objects:objects":
			dst.Runtime.Heap.Objects = int64(sampleUint(sample))
		case "/memory/classes/total:bytes":
			dst.Runtime.Heap.Sys = int64(sampleUint(sample))
		case "/memory/classes/heap/objects:bytes":
			dst.Runtime.Heap.Alloc = int64(sampleUint(sample))
		case "/memory/classes/heap/free:bytes":
			dst.Runtime.Heap.Idle = int64(sampleUint(sample))
		case "/memory/classes/heap/released:bytes":
			dst.Runtime.Heap.Released = int64(sampleUint(sample))
		case "/memory/classes/heap/unused:bytes":
			heapUnused = int64(sampleUint(sample))
		case "/memory/classes/heap/stacks:bytes":
			dst.Runtime.Stack.Inuse = int64(sampleUint(sample))
		case "/memory/classes/os-stacks:bytes":
			osStacks = int64(sampleUint(sample))
		case "/memory/classes/metadata/mcache/inuse:bytes":
			mcacheInuse = int64(sampleUint(sample))
		case "/memory/classes/metadata/mcache/free:bytes":
			mcacheFree = int64(sampleUint(sample))
		case "/memory/classes/metadata/mspan/inuse:bytes":
			mspanInuse = int64(sampleUint(sample))
		case "/memory/classes/metadata/mspan/free:bytes":
			mspanFree = int64(sampleUint(sample))
		case "/memory/classes/metadata/other:bytes":
			metadataOther = int64(sampleUint(sample))
		case "/memory/classes/other:bytes", "/memory/classes/profiling/buckets:bytes":
			metadataOther += int64(sampleUint(sample))
		case "/sync/mutex/wait/total:seconds":
			dst.Locks.GlobalWaitSec = sampleFloat(sample)
		case "/gc/pauses:seconds":
			s.readHistogram(sample, &s.pauses)
		case "/sched/latencies:seconds":
			s.readHistogram(sample, &s.latencies)
		}
	}
	dst.Runtime.Heap.Inuse = dst.Runtime.Heap.Alloc + heapUnused
	dst.Runtime.Stack.Sys = dst.Runtime.Stack.Inuse + osStacks
	dst.Runtime.Metadata.Inuse = mcacheInuse + mspanInuse
	dst.Runtime.Metadata.Sys = mcacheInuse + mcacheFree + mspanInuse + mspanFree + metadataOther
	dst.GC.PauseTotalMs = s.pauses.Sum * 1000
}

// readHistogram copies a runtime histogram's layout once and its counts each tick.
func (s *RuntimeSource) readHistogram(sample runtime_metrics.Sample, dst *metrics.HistogramSnapshot) {
	if sample.Value.Kind() != runtime_metrics.KindFloat64Histogram {
		return
	}
	h := sample.Value.Float64Histogram()
	if h == nil || len(h.Counts) == 0 || len(h.Buckets) < 2 {
		return
	}
	if dst.Bounds == nil {
		dst.Bounds = make([]float64, 0, len(h.Buckets)-2)
		for _, b := range h.Buckets[1 : len(h.Buckets)-1] {
			dst.Bounds = append(dst.Bounds, b)
		}
		dst.Counts = make([]uint64, len(h.Counts))
	}
	n := min(len(dst.Counts), len(h.Counts))
	copy(dst.Counts, h.Counts[:n])
	for i := n; i < len(dst.Counts); i++ {
		dst.Counts[i] = 0
	}
	dst.Sum = histogramSum(h)
	dst.Total = 0
	for _, count := range dst.Counts {
		dst.Total += count
	}
}

// sampleUint returns a sample's unsigned value, or zero for a non-integer kind.
func sampleUint(s runtime_metrics.Sample) uint64 {
	if s.Value.Kind() != runtime_metrics.KindUint64 {
		return 0
	}
	return s.Value.Uint64()
}

// sampleFloat returns a sample's float value, or zero for a non-float kind.
func sampleFloat(s runtime_metrics.Sample) float64 {
	if s.Value.Kind() != runtime_metrics.KindFloat64 {
		return 0
	}
	return s.Value.Float64()
}

// histogramSum approximates the total of a runtime histogram from its bucket midpoints.
func histogramSum(h *runtime_metrics.Float64Histogram) float64 {
	var sum float64
	for i, count := range h.Counts {
		if count == 0 || i+1 >= len(h.Buckets) {
			continue
		}
		lo, hi := h.Buckets[i], h.Buckets[i+1]
		switch {
		case math.IsInf(lo, -1) && hi <= 0:
			// Time histograms never record negative durations.
		case math.IsInf(lo, -1):
			sum += float64(count) * hi / 2
		case math.IsInf(hi, 1):
			sum += float64(count) * lo
		default:
			sum += float64(count) * (lo + hi) / 2
		}
	}
	return sum
}

// readProcess fills the process view from the platform's process readers.
func readProcess(dst *metrics.ProcessView) {
	dst.RSSBytes = readProcessRSS()
	dst.VSZBytes = readProcessVSZ()
	dst.Threads = readProcessThreads()
	dst.OpenFDs = readProcessFDs()
	user := readProcessUserSeconds()
	sys := readProcessSysSeconds()
	dst.CPUSecondsUser = user
	dst.CPUSecondsSys = sys
	dst.CPUPercent = cpuPercent(int64((user + sys) * float64(time.Second)))
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
