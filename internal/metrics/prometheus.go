package metrics

import (
	"fmt"
	"io"
	"maps"
	"slices"
	"strconv"
	"strings"
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
		name := counterName(key)
		e.line("# HELP %s %s\n", name, helpOr(counterHelp, key, name))
		e.line("# TYPE %s counter\n", name)
		for _, q := range queues {
			if v, ok := q.Counters[key]; ok {
				e.sample(name, q.Name, "", float64(v))
			}
		}
	}

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
	}
}

func (e *encoder) histogramSamples(name, queue string, h HistogramView) {
	cumulative := uint64(0)
	for i, bound := range h.Bounds {
		if i < len(h.Counts) {
			cumulative += h.Counts[i]
		}
		e.sample(name+"_bucket", queue, "le="+strconv.Quote(formatFloat(bound)), float64(cumulative))
	}
	if len(h.Counts) > len(h.Bounds) {
		cumulative += h.Counts[len(h.Bounds)]
	}
	e.sample(name+"_bucket", queue, `le="+Inf"`, float64(cumulative))
	e.sample(name+"_sum", queue, "", h.Sum)
	e.sample(name+"_count", queue, "", float64(h.Total))
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

func formatFloat(v float64) string {
	return strconv.FormatFloat(v, 'g', -1, 64)
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
