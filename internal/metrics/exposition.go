package metrics

import (
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"
)

// CollectOptions controls which series a scrape of a SnapshotCollector includes.
type CollectOptions struct {
	PerQueue   bool
	TopNQueues int
	Histograms bool
}

// SnapshotSource supplies the snapshot a scrape renders.
type SnapshotSource func() (Snapshot, error)

// SnapshotCollector renders one Snapshot per scrape as Prometheus metrics.
type SnapshotCollector struct {
	src  SnapshotSource
	opts CollectOptions
}

// NewSnapshotCollector builds a collector that calls src once per scrape.
func NewSnapshotCollector(src SnapshotSource, opts CollectOptions) *SnapshotCollector {
	return &SnapshotCollector{src: src, opts: opts}
}

// Describe sends nothing: the collector's families derive from the snapshot.
func (c *SnapshotCollector) Describe(chan<- *prometheus.Desc) {}

// Collect renders the current snapshot.
func (c *SnapshotCollector) Collect(ch chan<- prometheus.Metric) {
	snap, err := c.src()
	if err != nil {
		return
	}
	e := exposition{ch: ch}
	e.build(snap)
	e.runtime(snap)
	e.process(snap)
	e.gc(snap)
	e.scheduler(snap)
	e.locks(snap)
	e.storage(snap)
	e.totals(snap)
	if c.opts.PerQueue {
		e.queues(snap, c.opts)
	}
}

// exposition emits one snapshot's series through a metric channel.
type exposition struct {
	ch chan<- prometheus.Metric
}

// series emits one plain gauge or counter sample.
func (e exposition) series(name, help string, typ dto.MetricType, value float64, labels ...[2]string) {
	m := &dto.Metric{Label: labelPairs(labels)}
	switch typ {
	case dto.MetricType_COUNTER:
		m.Counter = &dto.Counter{Value: new(coerce(value))}
	case dto.MetricType_GAUGE:
		m.Gauge = &dto.Gauge{Value: new(coerce(value))}
	default:
		panic("exposition: unsupported series type " + typ.String())
	}
	e.send(name, help, m)
}

// histogram emits one cumulative histogram sample.
func (e exposition) histogram(name, help string, h HistogramView, labels ...[2]string) {
	m := &dto.Metric{Label: labelPairs(labels)}
	hist := &dto.Histogram{SampleSum: new(coerce(h.Sum))}
	var cumulative uint64
	for i, bound := range h.Bounds {
		if i < len(h.Counts) {
			cumulative += h.Counts[i]
		}
		hist.Bucket = append(hist.Bucket, &dto.Bucket{
			UpperBound:      new(bound),
			CumulativeCount: new(cumulative),
		})
	}
	var overflow uint64
	if len(h.Counts) > len(h.Bounds) {
		overflow = h.Counts[len(h.Bounds)]
	}
	if cumulative+overflow > h.Total {
		h.Total = cumulative + overflow
	}
	hist.SampleCount = new(h.Total)
	m.Histogram = hist
	e.send(name, help, m)
}

// quantiles emits one summary sample carrying precomputed quantiles. expfmt
// always renders _sum and _count, so callers pass real values whenever the
// snapshot carries them; a summary without count data renders zeros.
func (e exposition) quantiles(name, help string, count uint64, sum float64, qs [][2]float64, labels ...[2]string) {
	m := &dto.Metric{Label: labelPairs(labels)}
	m.Summary = &dto.Summary{
		SampleCount: new(count),
		SampleSum:   new(coerce(sum)),
	}
	for _, q := range qs {
		m.Summary.Quantile = append(m.Summary.Quantile, &dto.Quantile{
			Quantile: new(q[0]),
			Value:    new(coerce(q[1])),
		})
	}
	e.send(name, help, m)
}

// send wraps one dto metric for the registry.
func (e exposition) send(name, help string, m *dto.Metric) {
	names := make([]string, len(m.Label))
	for i, lp := range m.Label {
		names[i] = lp.GetName()
	}
	e.ch <- snapshotMetric{desc: prometheus.NewDesc(name, help, names, nil), metric: m}
}

// snapshotMetric adapts a hand-built dto.Metric to the prometheus.Metric interface.
type snapshotMetric struct {
	desc   *prometheus.Desc
	metric *dto.Metric
}

func (m snapshotMetric) Desc() *prometheus.Desc { return m.desc }

func (m snapshotMetric) Write(dst *dto.Metric) error {
	dst.Label = m.metric.Label
	dst.Counter = m.metric.Counter
	dst.Gauge = m.metric.Gauge
	dst.Histogram = m.metric.Histogram
	dst.Summary = m.metric.Summary
	dst.TimestampMs = m.metric.TimestampMs
	return nil
}

// labelPairs renders label pairs sorted by name, as the registry's consistency check expects.
func labelPairs(labels [][2]string) []*dto.LabelPair {
	pairs := make([]*dto.LabelPair, len(labels))
	for i, kv := range labels {
		pairs[i] = &dto.LabelPair{Name: new(kv[0]), Value: new(kv[1])}
	}
	slices.SortFunc(pairs, func(a, b *dto.LabelPair) int {
		return strings.Compare(a.GetName(), b.GetName())
	})
	return pairs
}

// coerce maps non-finite readings to zero, matching the exposition format's ban on them.
func coerce(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}

//go:fix inline
func floatPtr(v float64) *float64 { return new(v) }

//go:fix inline
func uint64Ptr(v uint64) *uint64 { return new(v) }

//go:fix inline
func stringPtr(v string) *string { return new(v) }

// build emits build and identity metadata.
func (e exposition) build(s Snapshot) {
	e.series("mikyu_build_info", "Build and platform identity of this node.", dto.MetricType_GAUGE, 1,
		[2]string{"spec_version", strconv.FormatUint(uint64(s.Build.SpecVersion), 10)},
		[2]string{"go_version", s.Build.GoVersion},
		[2]string{"os", s.Build.OS},
		[2]string{"arch", s.Build.Arch},
	)
}

// runtime emits goroutine counts and Go memory classes.
func (e exposition) runtime(s Snapshot) {
	e.series("mikyu_runtime_goroutines", "Goroutines in the process.", dto.MetricType_GAUGE, float64(s.Runtime.Goroutines))
	e.series("mikyu_runtime_goroutines_running", "Goroutines currently running.", dto.MetricType_GAUGE, float64(s.Runtime.GoroutineStates.Running))
	e.series("mikyu_runtime_goroutines_runnable", "Goroutines runnable but not running.", dto.MetricType_GAUGE, float64(s.Runtime.GoroutineStates.Runnable))
	e.series("mikyu_runtime_goroutines_waiting", "Goroutines blocked or idle.", dto.MetricType_GAUGE, float64(s.Runtime.GoroutineStates.Waiting))
	e.series("mikyu_runtime_gomaxprocs", "GOMAXPROCS as seen by the runtime.", dto.MetricType_GAUGE, float64(s.Runtime.GOMAXPROCS))
	e.series("mikyu_runtime_uptime_seconds", "Seconds since this node opened.", dto.MetricType_GAUGE, s.Runtime.UptimeSec)
	e.series("mikyu_runtime_heap_alloc_bytes", "Bytes of live heap objects.", dto.MetricType_GAUGE, float64(s.Runtime.Heap.Alloc))
	e.series("mikyu_runtime_heap_sys_bytes", "Bytes of heap obtained from the OS.", dto.MetricType_GAUGE, float64(s.Runtime.Heap.Sys))
	e.series("mikyu_runtime_heap_inuse_bytes", "Bytes in in-use heap spans.", dto.MetricType_GAUGE, float64(s.Runtime.Heap.Inuse))
	e.series("mikyu_runtime_heap_idle_bytes", "Bytes in idle heap spans.", dto.MetricType_GAUGE, float64(s.Runtime.Heap.Idle))
	e.series("mikyu_runtime_heap_released_bytes", "Bytes of heap returned to the OS.", dto.MetricType_GAUGE, float64(s.Runtime.Heap.Released))
	e.series("mikyu_runtime_heap_objects", "Live heap objects.", dto.MetricType_GAUGE, float64(s.Runtime.Heap.Objects))
	e.series("mikyu_runtime_heap_total_alloc_bytes_total", "Bytes of heap objects allocated since start.", dto.MetricType_COUNTER, float64(s.Runtime.Heap.TotalAlloc))
	e.series("mikyu_runtime_heap_total_free_bytes_total", "Bytes of heap objects freed since start.", dto.MetricType_COUNTER, float64(s.Runtime.Heap.TotalFree))
	e.series("mikyu_runtime_stack_inuse_bytes", "Bytes in in-use goroutine stacks.", dto.MetricType_GAUGE, float64(s.Runtime.Stack.Inuse))
	e.series("mikyu_runtime_stack_sys_bytes", "Bytes of stack obtained from the OS.", dto.MetricType_GAUGE, float64(s.Runtime.Stack.Sys))
	e.series("mikyu_runtime_metadata_inuse_bytes", "Bytes of allocator metadata in use.", dto.MetricType_GAUGE, float64(s.Runtime.Metadata.Inuse))
	e.series("mikyu_runtime_metadata_sys_bytes", "Bytes of allocator metadata from the OS.", dto.MetricType_GAUGE, float64(s.Runtime.Metadata.Sys))
}

// process emits process-level resource readings.
func (e exposition) process(s Snapshot) {
	e.series("mikyu_process_cpu_percent", "Process CPU utilization percent, 0-100.", dto.MetricType_GAUGE, s.Process.CPUPercent)
	e.series("mikyu_process_cpu_seconds_total", "CPU time consumed by the process.", dto.MetricType_COUNTER, s.Process.CPUSecondsUser, [2]string{"state", "user"})
	e.series("mikyu_process_cpu_seconds_total", "CPU time consumed by the process.", dto.MetricType_COUNTER, s.Process.CPUSecondsSys, [2]string{"state", "sys"})
	e.series("mikyu_process_rss_bytes", "Resident set size in bytes.", dto.MetricType_GAUGE, float64(s.Process.RSSBytes))
	e.series("mikyu_process_vsz_bytes", "Virtual size in bytes.", dto.MetricType_GAUGE, float64(s.Process.VSZBytes))
	e.series("mikyu_process_threads", "OS threads in the process.", dto.MetricType_GAUGE, float64(s.Process.Threads))
	e.series("mikyu_process_open_fds", "Open file descriptors.", dto.MetricType_GAUGE, float64(s.Process.OpenFDs))
	if s.Runtime.UptimeSec > 0 && !s.Timestamp.IsZero() {
		start := float64(s.Timestamp.Add(-time.Duration(s.Runtime.UptimeSec * float64(time.Second))).Unix())
		if start > 0 {
			e.series("mikyu_process_start_time_seconds", "Unix time this process started.", dto.MetricType_GAUGE, start)
		}
	}
}

// gc emits collector pacing and stop-the-world pause distribution.
func (e exposition) gc(s Snapshot) {
	e.series("mikyu_gc_cycles_total", "Completed garbage collection cycles.", dto.MetricType_COUNTER, float64(s.GC.Cycles))
	e.series("mikyu_gc_pause_total_seconds_total", "Cumulative stop-the-world pause time.", dto.MetricType_COUNTER, millis(s.GC.PauseTotalMs))
	e.quantiles("mikyu_gc_pause_seconds", "Stop-the-world pause distribution in seconds.", 0, 0, [][2]float64{
		{0.5, millis(s.GC.Pause.P50Ms)},
		{0.9, millis(s.GC.Pause.P90Ms)},
		{0.99, millis(s.GC.Pause.P99Ms)},
	})
	e.series("mikyu_gc_pause_seconds_max", "Worst stop-the-world pause observed.", dto.MetricType_GAUGE, millis(s.GC.Pause.MaxMs))
	e.series("mikyu_gc_next_bytes", "Heap goal for the next collection.", dto.MetricType_GAUGE, float64(s.GC.NextGCBytes))
	e.series("mikyu_gc_mem_limit_bytes", "GOMEMLIMIT in bytes, 0 when unlimited.", dto.MetricType_GAUGE, float64(s.GC.MemLimitBytes))
}

// scheduler emits goroutine scheduling latency distribution.
func (e exposition) scheduler(s Snapshot) {
	e.quantiles("mikyu_scheduler_latency_seconds", "Time goroutines spend runnable before running.", 0, 0, [][2]float64{
		{0.5, s.Scheduler.Latency.P50},
		{0.9, s.Scheduler.Latency.P90},
		{0.99, s.Scheduler.Latency.P99},
	})
}

// locks emits aggregate and per-observer lock contention.
func (e exposition) locks(s Snapshot) {
	e.series("mikyu_lock_global_wait_seconds_total", "Seconds the runtime spent waiting on all mutexes.", dto.MetricType_COUNTER, s.Locks.GlobalWaitSec)
	if len(s.Locks.Observers) == 0 {
		return
	}
	for _, v := range s.Locks.Observers {
		lock := [2]string{"lock", v.Name}
		e.series("mikyu_lock_waits_total", "Contended lock acquisitions.", dto.MetricType_COUNTER, float64(v.Waits), lock)
		e.series("mikyu_lock_wait_seconds_total", "Seconds spent waiting to acquire observed locks.", dto.MetricType_COUNTER, millis(v.TotalWaitMs), lock)
		e.series("mikyu_lock_wait_seconds_max", "Longest single wait on an observed lock.", dto.MetricType_GAUGE, millis(v.MaxWaitMs), lock)
		e.quantiles("mikyu_lock_wait_seconds", "Wait-time distribution per observed lock.",
			uint64(v.Waits), millis(v.TotalWaitMs), [][2]float64{
				{0.5, millis(v.P50WaitMs)},
				{0.99, millis(v.P99WaitMs)},
			}, lock)
	}
}

// storage emits node-wide storage aggregates and the data directory.
func (e exposition) storage(s Snapshot) {
	e.series("mikyu_storage_info", "Storage root of this node.", dto.MetricType_GAUGE, 1, [2]string{"data_dir", s.Storage.DataDir})
	e.series("mikyu_storage_queues", "Queues in this data directory.", dto.MetricType_GAUGE, float64(s.Storage.Queues))
	e.series("mikyu_storage_segments", "Segments across all queues.", dto.MetricType_GAUGE, float64(s.Storage.Segments))
	e.series("mikyu_storage_size_bytes", "On-disk bytes across all queues.", dto.MetricType_GAUGE, float64(s.Storage.SizeBytes))
	e.series("mikyu_storage_cache_bytes", "Bytes held in read caches.", dto.MetricType_GAUGE, float64(s.Storage.CacheBytes))
	e.series("mikyu_storage_cache_limit_bytes", "Byte budget for all read caches.", dto.MetricType_GAUGE, float64(s.Storage.CacheLimit))
	e.series("mikyu_storage_cache_hits_total", "Read cache hits.", dto.MetricType_COUNTER, float64(s.Storage.CacheHits))
	e.series("mikyu_storage_cache_misses_total", "Read cache misses.", dto.MetricType_COUNTER, float64(s.Storage.CacheMisses))
	e.series("mikyu_storage_committed_bytes_total", "Bytes acknowledged to producers.", dto.MetricType_COUNTER, float64(s.Storage.CommittedBytes))
	e.series("mikyu_storage_flushed_bytes_total", "Bytes durable on disk.", dto.MetricType_COUNTER, float64(s.Storage.FlushedBytes))
	e.series("mikyu_storage_pending_bytes", "Bytes acknowledged but not yet durable.", dto.MetricType_GAUGE, float64(s.Storage.PendingBytes))
	e.series("mikyu_storage_offset_store_bytes", "Bytes of consumer-group offset store.", dto.MetricType_GAUGE, float64(s.Storage.OffsetStoreBytes))
}

// totals emits the node-wide rollup.
func (e exposition) totals(s Snapshot) {
	e.series("mikyu_totals_queues", "Queues in the snapshot.", dto.MetricType_GAUGE, float64(s.Totals.Queues))
	e.series("mikyu_totals_messages", "Messages across queues.", dto.MetricType_GAUGE, float64(s.Totals.Messages))
	e.series("mikyu_totals_bytes", "On-disk bytes across queues.", dto.MetricType_GAUGE, float64(s.Totals.Bytes))
}

// queues emits the per-queue families.
func (e exposition) queues(s Snapshot, opts CollectOptions) {
	queues := selectQueues(s.Queues, opts.TopNQueues)
	if len(queues) == 0 {
		return
	}
	e.queueInfo(queues)
	e.queueGauges(queues)
	e.queueCounters(queues)
	e.queueFaults(queues)
	e.queueRates(queues)
	e.queueViews(queues)
	e.queueGroups(queues)
	if opts.Histograms {
		e.queueHistograms(queues)
	}
}

// queueInfo emits one info series per real queue, config collapsed into labels.
func (e exposition) queueInfo(queues []QueueView) {
	for _, q := range queues {
		if q.Kind == internalKind {
			continue // a non-queue scope carries no configuration
		}
		e.series("mikyu_queue_info", "Effective configuration of the queue.", dto.MetricType_GAUGE, 1, infoPairs(q)...)
	}
}

// queueGauges emits one family per gauge key, one sample per queue holding it.
func (e exposition) queueGauges(queues []QueueView) {
	for _, key := range sortedKeys(queues, func(q QueueView) map[string]int64 { return q.Gauges }) {
		for _, q := range queues {
			if v, ok := q.Gauges[key]; ok {
				e.series(gaugeName(key), helpOr(gaugeHelp, key, "Queue gauge."), dto.MetricType_GAUGE, float64(v), [2]string{"queue", q.Name})
			}
		}
	}
}

// queueCounters emits one family per counter key, one sample per queue holding it.
func (e exposition) queueCounters(queues []QueueView) {
	for _, key := range sortedKeys(queues, func(q QueueView) map[string]int64 { return q.Counters }) {
		if key == faultsCounter {
			continue // emitted by queueFaults, which suppresses zero values
		}
		for _, q := range queues {
			if v, ok := q.Counters[key]; ok {
				e.series(counterName(key), counterHelpFor(key), dto.MetricType_COUNTER, float64(v), [2]string{"queue", q.Name})
			}
		}
	}
}

// queueRates emits the collector's per-second derivatives.
func (e exposition) queueRates(queues []QueueView) {
	for _, key := range sortedRateKeys() {
		for _, q := range queues {
			e.series("mikyu_queue_"+key, rateHelp[key], dto.MetricType_GAUGE, rateOf(q.Rates, key), [2]string{"queue", q.Name})
		}
	}
}

// queueFaults emits the install-fault counter, reporting only queues that faulted.
func (e exposition) queueFaults(queues []QueueView) {
	positive := false
	for _, q := range queues {
		if q.Counters[faultsCounter] > 0 {
			positive = true
			break
		}
	}
	if !positive {
		return
	}
	for _, q := range queues {
		if n := q.Counters[faultsCounter]; n > 0 {
			e.series(faultsMetric, faultsHelp, dto.MetricType_COUNTER, float64(n), [2]string{"queue", q.Name})
		}
	}
}

// queueViews emits the typed per-queue views; an all-zero view contributes nothing.
func (e exposition) queueViews(queues []QueueView) {
	gaugeVals := make(map[string]map[string]float64)
	counterVals := make(map[string]map[string]float64)
	put := func(dst map[string]map[string]float64, name, queue string, v float64) {
		if dst[name] == nil {
			dst[name] = make(map[string]float64, len(queues))
		}
		dst[name][queue] = v
	}
	for _, q := range queues {
		for _, f := range queueViewFields(q) {
			dst := gaugeVals
			if f.counter {
				dst = counterVals
			}
			put(dst, f.name, q.Name, f.value)
		}
	}
	for _, field := range slices.Sorted(maps.Keys(gaugeVals)) {
		e.viewFamily("mikyu_queue_"+field, viewHelp(field), dto.MetricType_GAUGE, gaugeVals[field], queues)
	}
	for _, field := range slices.Sorted(maps.Keys(counterVals)) {
		e.viewFamily("mikyu_queue_"+field, viewHelp(field), dto.MetricType_COUNTER, counterVals[field], queues)
	}
}

// viewFamily emits one typed-view family, one sample per queue holding it.
func (e exposition) viewFamily(name, help string, typ dto.MetricType, values map[string]float64, queues []QueueView) {
	for _, q := range queues {
		if v, ok := values[q.Name]; ok {
			e.series(name, help, typ, v, [2]string{"queue", q.Name})
		}
	}
}

// queueGroups emits consumer-group positions; a group is one extra label dimension.
func (e exposition) queueGroups(queues []QueueView) {
	if !slices.ContainsFunc(queues, func(q QueueView) bool { return len(q.Groups) > 0 }) {
		return
	}
	for _, q := range queues {
		for _, g := range q.Groups {
			labels := [][2]string{{"queue", q.Name}, {"group", g.Name}}
			e.series("mikyu_queue_group_committed", "Last offset a group committed.", dto.MetricType_GAUGE, float64(g.Committed), labels...)
			e.series("mikyu_queue_group_high_water", "Highest offset a group has read.", dto.MetricType_GAUGE, float64(g.HighWater), labels...)
			e.series("mikyu_queue_group_lag", "Messages available but not yet consumed by a group.", dto.MetricType_GAUGE, float64(g.Lag), labels...)
		}
	}
}

// queueHistograms emits histogram and quantile-summary families per queue.
func (e exposition) queueHistograms(queues []QueueView) {
	for _, key := range sortedHistogramKeys(queues) {
		name := histogramName(key)
		help := helpOr(histogramHelp, key, "Queue latency distribution.")
		for _, q := range queues {
			if h, ok := q.Histograms[key]; ok {
				e.histogram(name, help, h, [2]string{"queue", q.Name})
			}
		}
		// The summary lives in its own family: a quantile sample cannot share a
		// name with a histogram family.
		summary := quantileName(key)
		for _, q := range queues {
			if h, ok := q.Histograms[key]; ok {
				e.quantiles(summary, help, h.Total, h.Sum, [][2]float64{
					{0.5, h.P50},
					{0.9, h.P90},
					{0.99, h.P99},
					{math.Inf(1), h.Max},
				}, [2]string{"queue", q.Name})
			}
		}
	}
}
