package metrics

import (
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/dmunish/mikyu/internal/spec"
)

// EncodeOptions controls which series a Prometheus rendering includes.
type EncodeOptions struct {
	PerQueue   bool
	TopNQueues int
	Histograms bool
}

// Encode writes s in the Prometheus text exposition format 0.0.4.
func Encode(w io.Writer, s Snapshot, opts EncodeOptions) error {
	e := &encoder{w: w}
	e.build(s)
	e.runtime(s)
	e.process(s)
	e.gc(s)
	e.scheduler(s)
	e.locks(s)
	e.storage(s)
	e.totals(s)
	if opts.PerQueue {
		e.perQueue(s, opts)
	}
	return e.err
}

type encoder struct {
	w   io.Writer
	err error
}

func (e *encoder) line(format string, args ...any) {
	if e.err != nil {
		return
	}
	_, e.err = fmt.Fprintf(e.w, format, args...)
}

// metric emits an unlabeled series with its help and type headers.
func (e *encoder) metric(name, help, typ string, value float64) {
	e.line("# HELP %s %s\n", name, help)
	e.line("# TYPE %s %s\n", name, typ)
	e.line("%s %s\n", name, formatFloat(value))
}

// sample emits one series carrying the given label pairs.
func (e *encoder) sample(name string, labels []string, value float64) {
	e.line("%s{%s} %s\n", name, strings.Join(labels, ","), formatFloat(value))
}

// label renders one label pair with an escaped value.
func label(key, value string) string { return key + "=" + strconv.Quote(value) }

// millis converts a millisecond reading to seconds.
func millis(ms float64) float64 { return ms / 1e3 }

// build emits build and identity metadata.
func (e *encoder) build(s Snapshot) {
	e.line("# HELP mikyu_build_info Build and platform identity of this node.\n")
	e.line("# TYPE mikyu_build_info gauge\n")
	e.sample("mikyu_build_info", []string{
		label("spec_version", formatUint32(uint32(s.Build.SpecVersion))),
		label("go_version", s.Build.GoVersion),
		label("os", s.Build.OS),
		label("arch", s.Build.Arch),
	}, 1)
}

// runtime emits goroutine counts and Go memory classes.
func (e *encoder) runtime(s Snapshot) {
	e.metric("mikyu_runtime_goroutines", "Goroutines in the process.", "gauge", float64(s.Runtime.Goroutines))
	e.metric("mikyu_runtime_goroutines_running", "Goroutines currently running.", "gauge", float64(s.Runtime.GoroutineStates.Running))
	e.metric("mikyu_runtime_goroutines_runnable", "Goroutines runnable but not running.", "gauge", float64(s.Runtime.GoroutineStates.Runnable))
	e.metric("mikyu_runtime_goroutines_waiting", "Goroutines blocked or idle.", "gauge", float64(s.Runtime.GoroutineStates.Waiting))
	e.metric("mikyu_runtime_gomaxprocs", "GOMAXPROCS as seen by the runtime.", "gauge", float64(s.Runtime.GOMAXPROCS))
	e.metric("mikyu_runtime_uptime_seconds", "Seconds since this node opened.", "gauge", s.Runtime.UptimeSec)
	e.metric("mikyu_runtime_heap_alloc_bytes", "Bytes of live heap objects.", "gauge", float64(s.Runtime.Heap.Alloc))
	e.metric("mikyu_runtime_heap_sys_bytes", "Bytes of heap obtained from the OS.", "gauge", float64(s.Runtime.Heap.Sys))
	e.metric("mikyu_runtime_heap_inuse_bytes", "Bytes in in-use heap spans.", "gauge", float64(s.Runtime.Heap.Inuse))
	e.metric("mikyu_runtime_heap_idle_bytes", "Bytes in idle heap spans.", "gauge", float64(s.Runtime.Heap.Idle))
	e.metric("mikyu_runtime_heap_released_bytes", "Bytes of heap returned to the OS.", "gauge", float64(s.Runtime.Heap.Released))
	e.metric("mikyu_runtime_heap_objects", "Live heap objects.", "gauge", float64(s.Runtime.Heap.Objects))
	e.metric("mikyu_runtime_heap_total_alloc_bytes_total", "Bytes of heap objects allocated since start.", "counter", float64(s.Runtime.Heap.TotalAlloc))
	e.metric("mikyu_runtime_heap_total_free_bytes_total", "Bytes of heap objects freed since start.", "counter", float64(s.Runtime.Heap.TotalFree))
	e.metric("mikyu_runtime_stack_inuse_bytes", "Bytes in in-use goroutine stacks.", "gauge", float64(s.Runtime.Stack.Inuse))
	e.metric("mikyu_runtime_stack_sys_bytes", "Bytes of stack obtained from the OS.", "gauge", float64(s.Runtime.Stack.Sys))
	e.metric("mikyu_runtime_metadata_inuse_bytes", "Bytes of allocator metadata in use.", "gauge", float64(s.Runtime.Metadata.Inuse))
	e.metric("mikyu_runtime_metadata_sys_bytes", "Bytes of allocator metadata from the OS.", "gauge", float64(s.Runtime.Metadata.Sys))
}

// process emits process-level resource readings.
func (e *encoder) process(s Snapshot) {
	e.metric("mikyu_process_cpu_percent", "Process CPU utilization percent, 0-100.", "gauge", s.Process.CPUPercent)
	e.line("# HELP mikyu_process_cpu_seconds_total CPU time consumed by the process.\n")
	e.line("# TYPE mikyu_process_cpu_seconds_total counter\n")
	e.sample("mikyu_process_cpu_seconds_total", []string{label("state", "user")}, s.Process.CPUSecondsUser)
	e.sample("mikyu_process_cpu_seconds_total", []string{label("state", "sys")}, s.Process.CPUSecondsSys)
	e.metric("mikyu_process_rss_bytes", "Resident set size in bytes.", "gauge", float64(s.Process.RSSBytes))
	e.metric("mikyu_process_vsz_bytes", "Virtual size in bytes.", "gauge", float64(s.Process.VSZBytes))
	e.metric("mikyu_process_threads", "OS threads in the process.", "gauge", float64(s.Process.Threads))
	e.metric("mikyu_process_open_fds", "Open file descriptors.", "gauge", float64(s.Process.OpenFDs))
	if s.Runtime.UptimeSec > 0 && !s.Timestamp.IsZero() {
		start := float64(s.Timestamp.Add(-time.Duration(s.Runtime.UptimeSec*float64(time.Second))).Unix())
		if start > 0 {
			e.metric("mikyu_process_start_time_seconds", "Unix time this process started.", "gauge", start)
		}
	}
}

// gc emits collector pacing and stop-the-world pause distribution.
func (e *encoder) gc(s Snapshot) {
	e.metric("mikyu_gc_cycles_total", "Completed garbage collection cycles.", "counter", float64(s.GC.Cycles))
	e.metric("mikyu_gc_pause_total_seconds_total", "Cumulative stop-the-world pause time.", "counter", millis(s.GC.PauseTotalMs))
	e.line("# HELP mikyu_gc_pause_seconds Stop-the-world pause distribution in seconds.\n")
	e.line("# TYPE mikyu_gc_pause_seconds summary\n")
	for _, q := range []struct {
		label string
		value float64
	}{
		{"0.5", millis(s.GC.Pause.P50Ms)},
		{"0.9", millis(s.GC.Pause.P90Ms)},
		{"0.99", millis(s.GC.Pause.P99Ms)},
	} {
		e.sample("mikyu_gc_pause_seconds", []string{label("quantile", q.label)}, q.value)
	}
	e.metric("mikyu_gc_pause_seconds_max", "Worst stop-the-world pause observed.", "gauge", millis(s.GC.Pause.MaxMs))
	e.metric("mikyu_gc_next_bytes", "Heap goal for the next collection.", "gauge", float64(s.GC.NextGCBytes))
	e.metric("mikyu_gc_mem_limit_bytes", "GOMEMLIMIT in bytes, 0 when unlimited.", "gauge", float64(s.GC.MemLimitBytes))
}

// scheduler emits goroutine scheduling latency distribution.
func (e *encoder) scheduler(s Snapshot) {
	e.line("# HELP mikyu_scheduler_latency_seconds Time goroutines spend runnable before running.\n")
	e.line("# TYPE mikyu_scheduler_latency_seconds summary\n")
	for _, q := range []struct {
		label string
		value float64
	}{
		{"0.5", s.Scheduler.Latency.P50},
		{"0.9", s.Scheduler.Latency.P90},
		{"0.99", s.Scheduler.Latency.P99},
	} {
		e.sample("mikyu_scheduler_latency_seconds", []string{label("quantile", q.label)}, q.value)
	}
}

// locks emits aggregate and per-observer lock contention.
func (e *encoder) locks(s Snapshot) {
	e.metric("mikyu_lock_global_wait_seconds_total", "Seconds the runtime spent waiting on all mutexes.", "counter", s.Locks.GlobalWaitSec)
	if len(s.Locks.Observers) == 0 {
		return
	}
	e.line("# HELP mikyu_lock_waits_total Contended lock acquisitions.\n")
	e.line("# TYPE mikyu_lock_waits_total counter\n")
	e.line("# HELP mikyu_lock_wait_seconds_total Seconds spent waiting to acquire observed locks.\n")
	e.line("# TYPE mikyu_lock_wait_seconds_total counter\n")
	e.line("# HELP mikyu_lock_wait_seconds_max Longest single wait on an observed lock.\n")
	e.line("# TYPE mikyu_lock_wait_seconds_max gauge\n")
	e.line("# HELP mikyu_lock_wait_seconds Wait-time distribution per observed lock.\n")
	e.line("# TYPE mikyu_lock_wait_seconds summary\n")
	for _, v := range s.Locks.Observers {
		lock := label("lock", v.Name)
		e.sample("mikyu_lock_waits_total", []string{lock}, float64(v.Waits))
		e.sample("mikyu_lock_wait_seconds_total", []string{lock}, millis(v.TotalWaitMs))
		e.sample("mikyu_lock_wait_seconds_max", []string{lock}, millis(v.MaxWaitMs))
		e.sample("mikyu_lock_wait_seconds", []string{lock, label("quantile", "0.5")}, millis(v.P50WaitMs))
		e.sample("mikyu_lock_wait_seconds", []string{lock, label("quantile", "0.99")}, millis(v.P99WaitMs))
	}
}

// storage emits node-wide storage aggregates and the data directory.
func (e *encoder) storage(s Snapshot) {
	e.line("# HELP mikyu_storage_info Storage root of this node.\n")
	e.line("# TYPE mikyu_storage_info gauge\n")
	e.sample("mikyu_storage_info", []string{label("data_dir", s.Storage.DataDir)}, 1)
	e.metric("mikyu_storage_queues", "Queues in this data directory.", "gauge", float64(s.Storage.Queues))
	e.metric("mikyu_storage_segments", "Segments across all queues.", "gauge", float64(s.Storage.Segments))
	e.metric("mikyu_storage_size_bytes", "On-disk bytes across all queues.", "gauge", float64(s.Storage.SizeBytes))
	e.metric("mikyu_storage_cache_bytes", "Bytes held in read caches.", "gauge", float64(s.Storage.CacheBytes))
	e.metric("mikyu_storage_cache_limit_bytes", "Byte budget for all read caches.", "gauge", float64(s.Storage.CacheLimit))
	e.metric("mikyu_storage_cache_hits_total", "Read cache hits.", "counter", float64(s.Storage.CacheHits))
	e.metric("mikyu_storage_cache_misses_total", "Read cache misses.", "counter", float64(s.Storage.CacheMisses))
	e.metric("mikyu_storage_committed_bytes_total", "Bytes acknowledged to producers.", "counter", float64(s.Storage.CommittedBytes))
	e.metric("mikyu_storage_flushed_bytes_total", "Bytes durable on disk.", "counter", float64(s.Storage.FlushedBytes))
	e.metric("mikyu_storage_pending_bytes", "Bytes acknowledged but not yet durable.", "gauge", float64(s.Storage.PendingBytes))
	e.metric("mikyu_storage_offset_store_bytes", "Bytes of consumer-group offset store.", "gauge", float64(s.Storage.OffsetStoreBytes))
}

// totals emits the node-wide rollup.
func (e *encoder) totals(s Snapshot) {
	e.metric("mikyu_totals_queues", "Queues in the snapshot.", "gauge", float64(s.Totals.Queues))
	e.metric("mikyu_totals_messages", "Messages across queues.", "gauge", float64(s.Totals.Messages))
	e.metric("mikyu_totals_bytes", "On-disk bytes across queues.", "gauge", float64(s.Totals.Bytes))
}

func (e *encoder) perQueue(s Snapshot, opts EncodeOptions) {
	queues := selectQueues(s.Queues, opts.TopNQueues)
	if len(queues) == 0 {
		return
	}
	e.queueInfo(queues)
	e.gauges(queues)
	e.counters(queues)
	e.faults(queues)
	e.rates(queues)
	e.views(queues)
	e.groups(queues)
	if opts.Histograms {
		e.histograms(queues)
	}
}

// queueInfo emits one info series per real queue, config collapsed into labels.
func (e *encoder) queueInfo(queues []QueueView) {
	e.line("# HELP mikyu_queue_info Effective configuration of the queue.\n")
	e.line("# TYPE mikyu_queue_info gauge\n")
	for _, q := range queues {
		if q.Kind == internalKind {
			continue // a non-queue scope carries no configuration
		}
		e.sample("mikyu_queue_info", infoLabels(q), 1)
	}
}

func (e *encoder) gauges(queues []QueueView) {
	for _, key := range sortedKeys(queues, func(q QueueView) map[string]int64 { return q.Gauges }) {
		name := gaugeName(key)
		e.line("# HELP %s %s\n", name, helpOr(gaugeHelp, key, "Queue gauge."))
		e.line("# TYPE %s gauge\n", name)
		for _, q := range queues {
			if v, ok := q.Gauges[key]; ok {
				e.sample(name, []string{label("queue", q.Name)}, float64(v))
			}
		}
	}
}

func (e *encoder) counters(queues []QueueView) {
	for _, key := range sortedKeys(queues, func(q QueueView) map[string]int64 { return q.Counters }) {
		if key == faultsCounter {
			continue // emitted by faults, which suppresses zero values
		}
		name := counterName(key)
		e.line("# HELP %s %s\n", name, counterHelpFor(key))
		e.line("# TYPE %s counter\n", name)
		for _, q := range queues {
			if v, ok := q.Counters[key]; ok {
				e.sample(name, []string{label("queue", q.Name)}, float64(v))
			}
		}
	}
}

// rates emits the collector's per-second derivatives.
func (e *encoder) rates(queues []QueueView) {
	for _, key := range sortedRateKeys() {
		name := "mikyu_queue_" + key
		e.line("# HELP %s %s\n", name, rateHelp[key])
		e.line("# TYPE %s gauge\n", name)
		for _, q := range queues {
			e.sample(name, []string{label("queue", q.Name)}, rateOf(q.Rates, key))
		}
	}
}

// faults emits the install-fault counter, reporting only queues that faulted.
func (e *encoder) faults(queues []QueueView) {
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
	e.line("# HELP %s %s\n", faultsMetric, faultsHelp)
	e.line("# TYPE %s counter\n", faultsMetric)
	for _, q := range queues {
		if n := q.Counters[faultsCounter]; n > 0 {
			e.sample(faultsMetric, []string{label("queue", q.Name)}, float64(n))
		}
	}
}

// views emits the typed per-queue views; an all-zero view contributes nothing.
func (e *encoder) views(queues []QueueView) {
	gauges := make(map[string]map[string]float64)
	counters := make(map[string]map[string]float64)
	put := func(dst map[string]map[string]float64, name, queue string, v float64) {
		if dst[name] == nil {
			dst[name] = make(map[string]float64, len(queues))
		}
		dst[name][queue] = v
	}
	for _, q := range queues {
		for _, f := range queueViewFields(q) {
			dst := gauges
			if f.counter {
				dst = counters
			}
			put(dst, f.name, q.Name, f.value)
		}
	}
	for _, field := range slices.Sorted(maps.Keys(gauges)) {
		name := "mikyu_queue_" + field
		e.line("# HELP %s %s\n", name, viewHelp(field))
		e.line("# TYPE %s gauge\n", name)
		for _, q := range queues {
			if v, ok := gauges[field][q.Name]; ok {
				e.sample(name, []string{label("queue", q.Name)}, v)
			}
		}
	}
	for _, field := range slices.Sorted(maps.Keys(counters)) {
		name := "mikyu_queue_" + field
		e.line("# HELP %s %s\n", name, viewHelp(field))
		e.line("# TYPE %s counter\n", name)
		for _, q := range queues {
			if v, ok := counters[field][q.Name]; ok {
				e.sample(name, []string{label("queue", q.Name)}, v)
			}
		}
	}
}

// groups emits consumer-group positions; a group is one extra label dimension.
func (e *encoder) groups(queues []QueueView) {
	if !slices.ContainsFunc(queues, func(q QueueView) bool { return len(q.Groups) > 0 }) {
		return
	}
	e.line("# HELP mikyu_queue_group_committed Last offset a group committed.\n")
	e.line("# TYPE mikyu_queue_group_committed gauge\n")
	e.line("# HELP mikyu_queue_group_high_water Highest offset a group has read.\n")
	e.line("# TYPE mikyu_queue_group_high_water gauge\n")
	e.line("# HELP mikyu_queue_group_lag Messages available but not yet consumed by a group.\n")
	e.line("# TYPE mikyu_queue_group_lag gauge\n")
	for _, q := range queues {
		for _, g := range q.Groups {
			labels := []string{label("queue", q.Name), label("group", g.Name)}
			e.sample("mikyu_queue_group_committed", labels, float64(g.Committed))
			e.sample("mikyu_queue_group_high_water", labels, float64(g.HighWater))
			e.sample("mikyu_queue_group_lag", labels, float64(g.Lag))
		}
	}
}

func (e *encoder) histograms(queues []QueueView) {
	for _, key := range sortedHistogramKeys(queues) {
		name := histogramName(key)
		e.line("# HELP %s %s\n", name, helpOr(histogramHelp, key, "Queue latency distribution."))
		e.line("# TYPE %s histogram\n", name)
		for _, q := range queues {
			if h, ok := q.Histograms[key]; ok {
				e.histogramSamples(name, q.Name, h)
			}
		}
		// The summary lives in its own family: a quantile sample cannot share a
		// name with a histogram family.
		summary := quantileName(key)
		e.line("# HELP %s %s\n", summary, helpOr(histogramHelp, key, "Queue latency distribution."))
		e.line("# TYPE %s summary\n", summary)
		for _, q := range queues {
			if h, ok := q.Histograms[key]; ok {
				e.quantileSamples(summary, q.Name, h)
			}
		}
	}
}

func (e *encoder) histogramSamples(name, queue string, h HistogramView) {
	cumulative := uint64(0)
	for i, bound := range h.Bounds {
		if i < len(h.Counts) {
			cumulative += h.Counts[i]
		}
		e.sample(name+"_bucket", []string{label("queue", queue), label("le", formatBound(bound))}, float64(cumulative))
	}
	if len(h.Counts) > len(h.Bounds) {
		cumulative += h.Counts[len(h.Bounds)]
	}
	e.sample(name+"_bucket", []string{label("queue", queue), label("le", "+Inf")}, float64(cumulative))
	e.sample(name+"_sum", []string{label("queue", queue)}, h.Sum)
	e.sample(name+"_count", []string{label("queue", queue)}, float64(h.Total))
}

// quantileSamples emits a histogram's precomputed quantiles as a summary family.
func (e *encoder) quantileSamples(name, queue string, h HistogramView) {
	for _, q := range []struct {
		label string
		value float64
	}{
		{"0.5", h.P50},
		{"0.9", h.P90},
		{"0.99", h.P99},
		{"+Inf", h.Max},
	} {
		e.sample(name, []string{label("queue", queue), label("quantile", q.label)}, q.value)
	}
}

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

// infoLabels renders a queue's identity and effective configuration as labels.
func infoLabels(q QueueView) []string {
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
	labels := make([]string, 0, len(dials)+2)
	labels = append(labels, label("queue", q.Name), label("kind", q.Kind))
	for _, k := range slices.Sorted(maps.Keys(dials)) {
		labels = append(labels, label(k, dials[k]))
	}
	return labels
}

func derefInt64(p *int64) int64 {
	if p == nil {
		return 0
	}
	return *p
}

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

// formatFloat renders a sample value, mapping non-finite values to zero.
func formatFloat(v float64) string {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return "0"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

// formatBound renders a bucket bound, keeping the canonical +Inf overflow label.
func formatBound(v float64) string {
	if math.IsInf(v, 1) {
		return "+Inf"
	}
	return strconv.FormatFloat(v, 'g', -1, 64)
}

func formatUint32(v uint32) string { return strconv.FormatUint(uint64(v), 10) }

func formatInt64(v int64) string { return strconv.FormatInt(v, 10) }

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
