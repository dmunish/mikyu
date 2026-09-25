package metrics

import (
	"fmt"
	"io"
	"maps"
	"math"
	"slices"
	"strconv"
	"strings"

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
	e.global(s)
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

func (e *encoder) metric(name, help, typ string, value float64) {
	e.line("# HELP %s %s\n", name, help)
	e.line("# TYPE %s %s\n", name, typ)
	e.line("%s %s\n", name, formatFloat(value))
}

func (e *encoder) sample(name, queue, extra string, value float64) {
	labels := fmt.Sprintf("queue=%q", queue)
	if extra != "" {
		labels += "," + extra
	}
	e.line("%s{%s} %s\n", name, labels, formatFloat(value))
}

func (e *encoder) global(s Snapshot) {
	e.metric("mikyu_goroutines", "Number of goroutines.", "gauge", float64(s.Runtime.Goroutines))
	e.metric("mikyu_heap_alloc_bytes", "Bytes of allocated heap objects.", "gauge", float64(s.Runtime.Heap.Alloc))
	e.metric("mikyu_heap_inuse_bytes", "Bytes in in-use heap spans.", "gauge", float64(s.Runtime.Heap.Inuse))
	e.metric("mikyu_gc_count_total", "Completed garbage collection cycles.", "counter", float64(s.GC.Cycles))
	e.metric("mikyu_uptime_seconds", "Process uptime in seconds.", "gauge", s.Runtime.UptimeSec)
	e.metric("mikyu_process_cpu_percent", "Process CPU utilization percent.", "gauge", s.Process.CPUPercent)
	e.metric("mikyu_process_rss_bytes", "Resident set size in bytes.", "gauge", float64(s.Process.RSSBytes))
	e.metric("mikyu_queues", "Number of queues.", "gauge", float64(s.Totals.Queues))
	e.metric("mikyu_messages", "Total messages across queues.", "gauge", float64(s.Totals.Messages))
	e.metric("mikyu_bytes", "Total on-disk bytes across queues.", "gauge", float64(s.Totals.Bytes))
}

func (e *encoder) perQueue(s Snapshot, opts EncodeOptions) {
	queues := selectQueues(s.Queues, opts.TopNQueues)
	if len(queues) == 0 {
		return
	}

	e.line("# HELP mikyu_queue_info Static configuration of the queue.\n")
	e.line("# TYPE mikyu_queue_info gauge\n")
	for _, q := range queues {
		if q.Kind == internalKind {
			continue // a non-queue scope carries no configuration
		}
		e.line("mikyu_queue_info{%s} 1\n", strings.Join(infoLabels(q), ","))
	}

	for _, key := range sortedCounterKeys(queues, func(q QueueView) map[string]int64 { return q.Gauges }) {
		name := gaugeName(key)
		e.line("# HELP %s %s\n", name, helpOr(gaugeHelp, key, name))
		e.line("# TYPE %s gauge\n", name)
		for _, q := range queues {
			if v, ok := q.Gauges[key]; ok {
				e.sample(name, q.Name, "", float64(v))
			}
		}
	}

	for _, key := range sortedCounterKeys(queues, func(q QueueView) map[string]int64 { return q.Counters }) {
		if key == faultsCounter {
			continue // emitted by faults, which suppresses zero values
		}
		name := counterName(key)
		e.line("# HELP %s %s\n", name, helpOr(counterHelp, key, name))
		e.line("# TYPE %s counter\n", name)
		for _, q := range queues {
			if v, ok := q.Counters[key]; ok {
				e.sample(name, q.Name, "", float64(v))
			}
		}
	}

	e.faults(queues)
	e.queueViews(queues)

	if !opts.Histograms {
		return
	}
	for _, key := range sortedHistogramKeys(queues) {
		name := histogramName(key)
		e.line("# HELP %s %s\n", name, helpOr(histogramHelp, key, name))
		e.line("# TYPE %s histogram\n", name)
		for _, q := range queues {
			if h, ok := q.Histograms[key]; ok {
				e.histogramSamples(name, q.Name, h)
			}
		}
		quantiles := quantileName(key)
		e.line("# HELP %s %s\n", quantiles, helpOr(histogramHelp, key, quantiles))
		e.line("# TYPE %s summary\n", quantiles)
		for _, q := range queues {
			if h, ok := q.Histograms[key]; ok {
				e.quantileSamples(quantiles, q.Name, h)
			}
		}
	}
}

// viewField is one fixed-shape per-queue series.
type viewField struct {
	field string
	value float64
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
			e.sample(faultsMetric, q.Name, "", float64(n))
		}
	}
}

// queueViews emits the typed per-queue views as gauges, skipping an all-zero view.
func (e *encoder) queueViews(queues []QueueView) {
	series := make(map[string]map[string]float64)
	for _, q := range queues {
		for _, f := range queueViewFields(q) {
			if series[f.field] == nil {
				series[f.field] = make(map[string]float64, len(queues))
			}
			series[f.field][q.Name] = f.value
		}
	}
	for _, field := range slices.Sorted(maps.Keys(series)) {
		name := "mikyu_queue_" + field
		e.line("# HELP %s %s\n", name, viewHelp(field))
		e.line("# TYPE %s gauge\n", name)
		for _, q := range queues {
			if v, ok := series[field][q.Name]; ok {
				e.sample(name, q.Name, "", v)
			}
		}
	}
}

// queueViewFields returns the series a queue's typed views contribute; a zero view adds none.
func queueViewFields(q QueueView) []viewField {
	var out []viewField
	if v := q.Producers; v != (ProducerView{}) {
		out = append(out,
			viewField{"producers_in_flight", float64(v.InFlight)},
			viewField{"producers_blocked_on_wal", float64(v.BlockedOnWAL)},
			viewField{"producers_block_events", float64(v.BlockEvents)},
		)
	}
	if v := q.Consumers; v != (ConsumerView{}) {
		out = append(out,
			viewField{"consumers_in_flight", float64(v.InFlight)},
			viewField{"consumers_parked_fetch", float64(v.ParkedFetch)},
			viewField{"consumers_parked_dequeue", float64(v.ParkedDequeue)},
			viewField{"consumers_fetch_park_events", float64(v.FetchParkEvents)},
			viewField{"consumers_dequeue_park_events", float64(v.DequeueParkEvents)},
		)
	}
	if v := q.Backpressure; v != (BackpressureView{}) {
		out = append(out,
			viewField{"backpressure_wal_channel_fill", v.WALChannelFill},
			viewField{"backpressure_wal_byte_fill", v.WALByteFill},
			viewField{"backpressure_queue_full_events", float64(v.QueueFullEvents)},
			viewField{"backpressure_producer_blocks", float64(v.ProducerBlocks)},
			viewField{"backpressure_consumer_parks", float64(v.ConsumerParks)},
			viewField{"backpressure_retry_budget_drops", float64(v.RetryBudgetDrops)},
		)
	}
	if v := q.Tasks; v != (TaskView{}) {
		out = append(out,
			viewField{"tasks_active", float64(v.Active)},
			viewField{"tasks_in_flight", float64(v.InFlight)},
			viewField{"tasks_scheduled", float64(v.Scheduled)},
			viewField{"tasks_dead_letters", float64(v.DeadLetters)},
			viewField{"tasks_uncheckpointed", float64(v.Uncheckpointed)},
			viewField{"tasks_schedule_heap", float64(v.ScheduleHeap)},
			viewField{"tasks_checkpoint_lsn", float64(v.CheckpointLSN)},
			viewField{"tasks_dirty", float64(v.Dirty)},
			viewField{"tasks_checkpoint_lag", float64(v.CheckpointLag)},
		)
	}
	return out
}

// infoLabels renders a queue's identity and effective configuration as labels.
func infoLabels(q QueueView) []string {
	s := spec.Resolve(q.Effective)
	dials := map[string]string{
		"durability":       s.Storage.Durability.String(),
		"encryption":       s.Storage.Encryption.String(),
		"compaction":       s.Storage.Compaction.String(),
		"max_segment":      formatInt64(derefInt64(s.Storage.MaxSegment)),
		"max_segment_age":  s.Storage.MaxSegmentAge.String(),
		"max_payload":      formatInt64(derefInt64(s.Storage.MaxPayload)),
		"max_cache":        formatInt64(derefInt64(s.Storage.MaxCache)),
		"retention_age":    s.Retention.MaxAge.String(),
		"retention_bytes":  formatInt64(derefInt64(s.Retention.MaxBytes)),
		"lease":            s.Task.Lease.String(),
		"max_attempts":     formatUint32(derefUint32(s.Task.MaxAttempts)),
		"max_in_flight":    formatUint32(derefUint32(s.Task.MaxInFlight)),
		"max_dead_letters": formatUint32(derefUint32(s.Task.MaxDeadLetters)),
	}
	keys := slices.Sorted(maps.Keys(dials))
	labels := make([]string, 0, len(keys)+2)
	labels = append(labels, "queue="+strconv.Quote(q.Name), "kind="+strconv.Quote(q.Kind))
	for _, k := range keys {
		labels = append(labels, k+"="+strconv.Quote(dials[k]))
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

func (e *encoder) histogramSamples(name, queue string, h HistogramView) {
	cumulative := uint64(0)
	for i, bound := range h.Bounds {
		if i < len(h.Counts) {
			cumulative += h.Counts[i]
		}
		e.sample(name+"_bucket", queue, "le="+strconv.Quote(formatBound(bound)), float64(cumulative))
	}
	if len(h.Counts) > len(h.Bounds) {
		cumulative += h.Counts[len(h.Bounds)]
	}
	e.sample(name+"_bucket", queue, `le="+Inf"`, float64(cumulative))
	e.sample(name+"_sum", queue, "", h.Sum)
	e.sample(name+"_count", queue, "", float64(h.Total))
}

// quantileSamples emits the precomputed quantiles of a histogram as a summary family.
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
		e.sample(name, queue, "quantile="+strconv.Quote(q.label), q.value)
	}
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

func sortedCounterKeys(queues []QueueView, pick func(QueueView) map[string]int64) []string {
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

// viewHelpPrefix maps a typed view series prefix to its help text.
var viewHelpPrefix = map[string]string{
	"producers_":    "Producer-side pressure on the queue.",
	"consumers_":    "Consumer-side pressure on the queue.",
	"backpressure_": "Backpressure signals for the queue.",
	"tasks_":        "Task occupancy and checkpoint health for the queue.",
}

// viewHelp returns the help text for a typed view series.
func viewHelp(field string) string {
	for prefix, text := range viewHelpPrefix {
		if strings.HasPrefix(field, prefix) {
			return text
		}
	}
	return "Queue series."
}

func gaugeName(key string) string {
	if name, ok := gaugeNames[key]; ok {
		return name
	}
	return "mikyu_" + sanitize(key)
}

func counterName(key string) string {
	if name, ok := counterNames[key]; ok {
		return name
	}
	name := "mikyu_" + sanitize(key)
	if !strings.HasSuffix(name, "_total") {
		name += "_total"
	}
	return name
}

func histogramName(key string) string {
	if name, ok := histogramNames[key]; ok {
		return name
	}
	return "mikyu_" + sanitize(key) + "_seconds"
}

// quantileName is the summary family carrying a histogram's precomputed quantiles.
func quantileName(key string) string {
	return strings.TrimSuffix(histogramName(key), "_seconds") + "_quantiles_seconds"
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

var gaugeNames = map[string]string{
	"messages":    "mikyu_queue_messages",
	"size_bytes":  "mikyu_queue_bytes",
	"segments":    "mikyu_queue_segments",
	"last_offset": "mikyu_queue_last_offset",
	"min_offset":  "mikyu_queue_min_offset",
}

var gaugeHelp = map[string]string{
	"messages":    "Messages stored in the queue.",
	"size_bytes":  "On-disk bytes used by the queue.",
	"segments":    "Number of segments in the queue.",
	"last_offset": "Next offset to be assigned.",
	"min_offset":  "Lowest readable offset.",
}

var counterNames = map[string]string{
	"appends":       "mikyu_append_total",
	"reads":         "mikyu_read_total",
	"bytes_in":      "mikyu_bytes_in_total",
	"bytes_out":     "mikyu_bytes_out_total",
	"enqueues":      "mikyu_enqueue_total",
	"dequeues":      "mikyu_dequeue_total",
	"group_commits": "mikyu_group_commit_total",
	"wal_flushes":   "mikyu_wal_flush_total",
	"segment_rolls": "mikyu_segment_roll_total",
	"dlq_acks":      "mikyu_dlq_ack_total",
	"connections":   "mikyu_connection_total",
	"acks":          "mikyu_ack_total",
	"nacks":         "mikyu_nack_total",
	"extends":       "mikyu_extend_total",
}

var counterHelp = map[string]string{
	"appends":       "Log appends.",
	"reads":         "Log reads.",
	"bytes_in":      "Bytes appended.",
	"bytes_out":     "Bytes read.",
	"enqueues":      "Tasks enqueued.",
	"dequeues":      "Tasks dequeued.",
	"group_commits": "Consumer group commits.",
	"wal_flushes":   "WAL flushes.",
	"segment_rolls": "Segment rolls.",
	"dlq_acks":      "Dead-letter acknowledgements.",
	"connections":   "Accepted connections.",
	"acks":          "Task acknowledgements.",
	"nacks":         "Task negative acknowledgements.",
	"extends":       "Task lease extensions.",
}

// The install-fault counter is emitted on its own so a healthy queue stays silent.
const (
	internalKind  = "internal"
	faultsCounter = "queue_faults"
	faultsMetric  = "mikyu_queue_faults_total"
	faultsHelp    = "Queues whose driver failed to install."
)

var histogramNames = map[string]string{
	"append_duration":    "mikyu_append_duration_seconds",
	"read_duration":      "mikyu_read_duration_seconds",
	"wal_flush_duration": "mikyu_wal_flush_duration_seconds",
	"enqueue_duration":   "mikyu_enqueue_duration_seconds",
	"dequeue_wait":       "mikyu_dequeue_wait_seconds",
	"op_duration":        "mikyu_op_duration_seconds",
}

var histogramHelp = map[string]string{
	"append_duration":    "Append latency in seconds.",
	"read_duration":      "Read latency in seconds.",
	"wal_flush_duration": "WAL flush latency in seconds.",
	"enqueue_duration":   "Enqueue latency in seconds.",
	"dequeue_wait":       "Dequeue wait in seconds.",
	"op_duration":        "Server op latency in seconds.",
}
