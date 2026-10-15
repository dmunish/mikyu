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

	"github.com/dmunish/mikyu/internal/spec"
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

// millis converts a millisecond reading to seconds.
func millis(ms float64) float64 { return ms / 1e3 }

// viewField is one fixed-shape per-queue series.
type viewField struct {
	name    string
	value   float64
	counter bool
}

// queueViewFields returns the series a queue's typed views contribute; a zero view adds none.
func queueViewFields(q QueueView) []viewField {
	var out []viewField
	if v := q.Producers; v != (ProducerView{}) {
		out = append(out,
			viewField{name: "producers_in_flight", value: float64(v.InFlight)},
			viewField{name: "producers_blocked_on_wal", value: float64(v.BlockedOnWAL)},
			viewField{name: "producers_block_events_total", value: float64(v.BlockEvents), counter: true},
		)
	}
	if v := q.Consumers; v != (ConsumerView{}) {
		out = append(out,
			viewField{name: "consumers_in_flight", value: float64(v.InFlight)},
			viewField{name: "consumers_parked_fetch", value: float64(v.ParkedFetch)},
			viewField{name: "consumers_parked_dequeue", value: float64(v.ParkedDequeue)},
			viewField{name: "consumers_fetch_park_events_total", value: float64(v.FetchParkEvents), counter: true},
			viewField{name: "consumers_dequeue_park_events_total", value: float64(v.DequeueParkEvents), counter: true},
		)
	}
	if v := q.Backpressure; v != (BackpressureView{}) {
		out = append(out,
			viewField{name: "backpressure_wal_channel_fill", value: v.WALChannelFill},
			viewField{name: "backpressure_wal_byte_fill", value: v.WALByteFill},
			viewField{name: "backpressure_queue_full_events_total", value: float64(v.QueueFullEvents), counter: true},
			viewField{name: "backpressure_producer_blocks_total", value: float64(v.ProducerBlocks), counter: true},
			viewField{name: "backpressure_consumer_parks_total", value: float64(v.ConsumerParks), counter: true},
			viewField{name: "backpressure_retry_budget_drops_total", value: float64(v.RetryBudgetDrops), counter: true},
		)
	}
	if v := q.Tasks; v != (TaskView{}) {
		out = append(out,
			viewField{name: "tasks_active", value: float64(v.Active)},
			viewField{name: "tasks_in_flight", value: float64(v.InFlight)},
			viewField{name: "tasks_scheduled", value: float64(v.Scheduled)},
			viewField{name: "tasks_dead_letters", value: float64(v.DeadLetters)},
			viewField{name: "tasks_uncheckpointed", value: float64(v.Uncheckpointed)},
			viewField{name: "tasks_schedule_heap", value: float64(v.ScheduleHeap)},
			viewField{name: "tasks_checkpoint_lsn", value: float64(v.CheckpointLSN)},
			viewField{name: "tasks_dirty", value: float64(v.Dirty)},
			viewField{name: "tasks_checkpoint_lag", value: float64(v.CheckpointLag)},
		)
	}
	return out
}

// infoPairs returns a queue's identity and effective configuration as label pairs.
func infoPairs(q QueueView) [][2]string {
	s := spec.Resolve(q.Effective)
	dials := map[string]string{
		"compaction":          s.Storage.Compaction.String(),
		"durability":          s.Storage.Durability.String(),
		"encryption":          s.Storage.Encryption.String(),
		"lease":               s.Task.Lease.String(),
		"max_attempts":        formatUint32(derefUint32(s.Task.MaxAttempts)),
		"max_cache":           formatInt64(derefInt64(s.Storage.MaxCache)),
		"max_dead_letters":    formatUint32(derefUint32(s.Task.MaxDeadLetters)),
		"max_flush_bytes":     formatInt64(derefInt64(s.Flush.MaxBytes)),
		"max_flush_wait":      s.Flush.MaxWait.String(),
		"max_in_flight":       formatUint32(derefUint32(s.Task.MaxInFlight)),
		"max_payload":         formatInt64(derefInt64(s.Storage.MaxPayload)),
		"max_retention_age":   s.Retention.MaxAge.String(),
		"max_retention_bytes": formatInt64(derefInt64(s.Retention.MaxBytes)),
		"max_segment":         formatInt64(derefInt64(s.Storage.MaxSegment)),
		"max_segment_age":     s.Storage.MaxSegmentAge.String(),
	}
	pairs := make([][2]string, 0, len(dials)+2)
	pairs = append(pairs, [2]string{"queue", q.Name}, [2]string{"kind", q.Kind})
	for _, k := range slices.Sorted(maps.Keys(dials)) {
		pairs = append(pairs, [2]string{k, dials[k]})
	}
	return pairs
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

func formatUint32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

func formatInt64(v int64) string { return strconv.FormatInt(v, 10) }

func derefUint32(p *uint32) uint32 {
	if p == nil {
		return 0
	}
	return *p
}

func selectQueues(queues []QueueView, topN int) []QueueView {
	out := slices.Clone(queues)
	slices.SortFunc(out, func(a, b QueueView) int {
		if d := cmpInt64(b.Gauges["size_bytes"], a.Gauges["size_bytes"]); d != 0 {
			return d
		}
		if d := cmpInt64(b.Gauges["messages"], a.Gauges["messages"]); d != 0 {
			return d
		}
		return strings.Compare(a.Name, b.Name)
	})
	if topN > 0 && len(out) > topN {
		out = out[:topN]
	}
	return out
}

func cmpInt64(a, b int64) int {
	switch {
	case a > b:
		return 1
	case a < b:
		return -1
	default:
		return 0
	}
}

// sortedKeys returns the union of the selected map's keys across queues.
func sortedKeys(queues []QueueView, pick func(QueueView) map[string]int64) []string {
	set := make(map[string]struct{})
	for _, q := range queues {
		for key := range pick(q) {
			set[key] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

func sortedHistogramKeys(queues []QueueView) []string {
	set := make(map[string]struct{})
	for _, q := range queues {
		for key := range q.Histograms {
			set[key] = struct{}{}
		}
	}
	return slices.Sorted(maps.Keys(set))
}

func sortedRateKeys() []string {
	return slices.Sorted(maps.Keys(rateHelp))
}

func rateOf(r Rates, key string) float64 {
	switch key {
	case "appends_per_second":
		return r.AppendsPerSec
	case "reads_per_second":
		return r.ReadsPerSec
	case "enqueues_per_second":
		return r.EnqueuesPerSec
	case "dequeues_per_second":
		return r.DequeuesPerSec
	case "bytes_in_per_second":
		return r.BytesInPerSec
	case "bytes_out_per_second":
		return r.BytesOutPerSec
	}
	return 0
}

func gaugeName(key string) string {
	if name, ok := gaugeNames[key]; ok {
		return name
	}
	return "mikyu_queue_" + sanitize(key)
}

func counterName(key string) string {
	if name, ok := counterNames[key]; ok {
		return name
	}
	name := "mikyu_queue_" + sanitize(key)
	if !strings.HasSuffix(name, "_total") {
		name += "_total"
	}
	return name
}

func histogramName(key string) string {
	if name, ok := histogramNames[key]; ok {
		return name
	}
	return "mikyu_queue_" + sanitize(key) + "_seconds"
}

// quantileName is the summary family carrying a histogram's precomputed quantiles.
func quantileName(key string) string {
	return strings.Replace(histogramName(key), "_seconds", "_quantiles_seconds", 1)
}

func sanitize(name string) string {
	return strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
			return r
		case r >= 'A' && r <= 'Z':
			return r + ('a' - 'A')
		default:
			return '_'
		}
	}, name)
}

func helpOr(help map[string]string, key, fallback string) string {
	if text, ok := help[key]; ok {
		return text
	}
	return fallback
}

// counterHelpFor documents a counter, deriving a line for the server op family.
func counterHelpFor(key string) string {
	if text, ok := counterHelp[key]; ok {
		return text
	}
	if op, found := strings.CutPrefix(key, "op_"); found {
		return "Server " + strings.ReplaceAll(op, "_", " ") + " operations."
	}
	return "Queue counter."
}

// The install-fault counter is emitted on its own so a healthy queue stays silent.
const (
	internalKind  = "internal"
	faultsCounter = "queue_faults"
	faultsMetric  = "mikyu_queue_faults_total"
	faultsHelp    = "Queues whose driver failed to install."
)

// viewHelpPrefix maps a typed view series prefix to its help text.
var viewHelpPrefix = map[string]string{
	"producers_":    "Producer-side pressure on the queue.",
	"consumers_":    "Consumer-side pressure on the queue.",
	"backpressure_": "Backpressure signals for the queue.",
	"tasks_":        "Task occupancy and checkpoint health for the queue.",
}

// viewHelp returns the help text for a typed view series.
func viewHelp(name string) string {
	for prefix, text := range viewHelpPrefix {
		if strings.HasPrefix(name, prefix) {
			return text
		}
	}
	return "Queue series."
}

var gaugeNames = map[string]string{
	"messages":              "mikyu_queue_messages",
	"segments":              "mikyu_queue_segments",
	"size_bytes":            "mikyu_queue_size_bytes",
	"last_offset":           "mikyu_queue_last_offset",
	"min_offset":            "mikyu_queue_min_offset",
	"key_index":             "mikyu_queue_key_index",
	"wal_pending":           "mikyu_queue_wal_pending",
	"wal_blocked_producers": "mikyu_queue_wal_blocked_producers",
	"in_flight_append":      "mikyu_queue_in_flight_appends",
	"in_flight_read":        "mikyu_queue_in_flight_reads",
	"parked_fetch":          "mikyu_queue_parked_fetch",
	"in_flight_enqueue":     "mikyu_queue_in_flight_enqueue",
	"parked_dequeue":        "mikyu_queue_parked_dequeue",
	"connections":           "mikyu_queue_connections",
}

var gaugeHelp = map[string]string{
	"messages":              "Messages stored in the queue.",
	"segments":              "Segments in the queue.",
	"size_bytes":            "On-disk bytes used by the queue.",
	"last_offset":           "Next offset to be assigned.",
	"min_offset":            "Lowest readable offset.",
	"key_index":             "Live entries in the compaction key index.",
	"wal_pending":           "WAL entries written but not yet flushed.",
	"wal_blocked_producers": "Producers currently blocked on the WAL.",
	"in_flight_append":      "Appends in flight.",
	"in_flight_read":        "Reads in flight.",
	"parked_fetch":          "Fetch calls parked waiting for data.",
	"in_flight_enqueue":     "Enqueues in flight.",
	"parked_dequeue":        "Dequeues parked waiting for a task.",
	"connections":           "Open server connections.",
}

var counterNames = map[string]string{
	"appends":              "mikyu_queue_appends_total",
	"reads":                "mikyu_queue_reads_total",
	"bytes_in":             "mikyu_queue_bytes_in_total",
	"bytes_out":            "mikyu_queue_bytes_out_total",
	"segment_rolls":        "mikyu_queue_segment_rolls_total",
	"group_commits":        "mikyu_queue_group_commits_total",
	"cache_hits":           "mikyu_queue_cache_hits_total",
	"cache_misses":         "mikyu_queue_cache_misses_total",
	"wal_flushes":          "mikyu_queue_wal_flushes_total",
	"wal_block_events":     "mikyu_queue_wal_block_events_total",
	"fetch_park_events":    "mikyu_queue_fetch_park_events_total",
	"enqueues":             "mikyu_queue_enqueues_total",
	"dequeues":             "mikyu_queue_dequeues_total",
	"acks":                 "mikyu_queue_acks_total",
	"nacks":                "mikyu_queue_nacks_total",
	"extends":              "mikyu_queue_extends_total",
	"dlq_acks":             "mikyu_queue_dlq_acks_total",
	"dlq_overflow":         "mikyu_queue_dlq_overflow_total",
	"task_stalls":          "mikyu_queue_task_stalls_total",
	"dequeue_park_events":  "mikyu_queue_dequeue_park_events_total",
	"queue_full_events":    "mikyu_queue_queue_full_events_total",
	"retry_budget_drops":   "mikyu_queue_retry_budget_drops_total",
	"connections_rejected": "mikyu_queue_op_connections_rejected_total",
	"op_create_queue":      "mikyu_queue_op_creates_total",
	"op_delete_queue":      "mikyu_queue_op_deletes_total",
	"op_list_queues":       "mikyu_queue_op_lists_total",
	"op_update_queue":      "mikyu_queue_op_updates_total",
	"op_describe_queue":    "mikyu_queue_op_describes_total",
	"op_truncate":          "mikyu_queue_op_truncates_total",
	"op_metrics":           "mikyu_queue_op_metrics_total",
	"op_append":            "mikyu_queue_op_appends_total",
	"op_append_batch":      "mikyu_queue_op_append_batches_total",
	"op_read":              "mikyu_queue_op_reads_total",
	"op_read_batch":        "mikyu_queue_op_read_batches_total",
	"op_last_offset":       "mikyu_queue_op_last_offsets_total",
	"op_min_offset":        "mikyu_queue_op_min_offsets_total",
	"op_commit":            "mikyu_queue_op_commits_total",
	"op_committed":         "mikyu_queue_op_committed_total",
	"op_fetch":             "mikyu_queue_op_fetches_total",
	"op_enqueue":           "mikyu_queue_op_enqueues_total",
	"op_dequeue":           "mikyu_queue_op_dequeues_total",
	"op_ack":               "mikyu_queue_op_acks_total",
	"op_nack":              "mikyu_queue_op_nacks_total",
	"op_extend":            "mikyu_queue_op_extends_total",
	"op_dead_letters":      "mikyu_queue_op_dead_letters_total",
	"op_dead_letter_ack":   "mikyu_queue_op_dead_letter_acks_total",
}

var counterHelp = map[string]string{
	"appends":              "Log appends.",
	"reads":                "Log reads.",
	"bytes_in":             "Bytes appended.",
	"bytes_out":            "Bytes read.",
	"segment_rolls":        "Segment rolls.",
	"group_commits":        "Consumer group commits.",
	"cache_hits":           "Read cache hits.",
	"cache_misses":         "Read cache misses.",
	"wal_flushes":          "WAL flushes.",
	"wal_block_events":     "Producers blocked on a full WAL.",
	"fetch_park_events":    "Fetch calls that parked for data.",
	"enqueues":             "Tasks enqueued.",
	"dequeues":             "Tasks dequeued.",
	"acks":                 "Task acknowledgements.",
	"nacks":                "Task negative acknowledgements.",
	"extends":              "Task lease extensions.",
	"dlq_acks":             "Dead-letter acknowledgements.",
	"dlq_overflow":         "Dead letters dropped at the DLQ bound.",
	"task_stalls":          "Leases reclaimed after expiry.",
	"dequeue_park_events":  "Dequeues that parked waiting for a task.",
	"queue_full_events":    "Enqueues refused at the in-flight bound.",
	"retry_budget_drops":   "Tasks dead-lettered by the attempt budget.",
	"connections_rejected": "Connections refused at the limit.",
}

var rateHelp = map[string]string{
	"appends_per_second":   "Appends per second.",
	"reads_per_second":     "Reads per second.",
	"enqueues_per_second":  "Tasks enqueued per second.",
	"dequeues_per_second":  "Tasks dequeued per second.",
	"bytes_in_per_second":  "Bytes appended per second.",
	"bytes_out_per_second": "Bytes read per second.",
}

var histogramNames = map[string]string{
	"append_duration":    "mikyu_queue_append_duration_seconds",
	"read_duration":      "mikyu_queue_read_duration_seconds",
	"wal_flush_duration": "mikyu_queue_wal_flush_duration_seconds",
	"enqueue_duration":   "mikyu_queue_enqueue_duration_seconds",
	"dequeue_wait":       "mikyu_queue_dequeue_wait_seconds",
}

var histogramHelp = map[string]string{
	"append_duration":    "Append latency in seconds.",
	"read_duration":      "Read latency in seconds.",
	"wal_flush_duration": "WAL flush latency in seconds.",
	"enqueue_duration":   "Enqueue latency in seconds.",
	"dequeue_wait":       "Dequeue wait in seconds.",
}
