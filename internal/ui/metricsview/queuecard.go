package metricsview

import (
	"math"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// Gauge lanes inside cards are fixed-width so a column of them lines up; the
// number lives in its own right-aligned lane next to the bar.
const gaugeLaneWidth = 10

// queueCard renders one queue's detail card for its kind: a title edge carrying
// the state glyph and health word, then the kind-driven sections.
func queueCard(q metrics.QueueView, t style.Theme, f human.Formatter, width int) string {
	health := metrics.QueueHealth(q)
	title := stateCell(t, health) + " " + q.Name
	right := t.Paint(t.Label).Render(q.Kind+" · ") +
		t.Paint(HealthColor(t, health)).Render(health.String())
	inner := width - 4
	var body string
	if q.IsTask() {
		body = taskBody(q, t, f, inner)
	} else {
		body = logBody(q, t, f, inner)
	}
	return t.TitledBox(title, right, body, width, t.Accent)
}

// logBody renders a log queue's sections: throughput, latency, I/O concurrency,
// backpressure, consumer groups, and the demoted counters.
func logBody(q metrics.QueueView, t style.Theme, f human.Formatter, inner int) string {
	parts := []string{
		throughputSection(q, t, f, inner),
		latencySection(t, f, logLatencies(q)),
		ioSection(q, t, f, inner),
		backpressureSection(q, t, f, inner),
		groupsSection(q, t, f),
		countersLine(t, logCounterCells(q, f)),
	}
	return strings.Join(nonEmpty(parts), "\n")
}

// taskBody renders a task queue's sections: throughput, latency, leases, dead
// letters, checkpoint health, policy, and the demoted counters.
func taskBody(q metrics.QueueView, t style.Theme, f human.Formatter, inner int) string {
	parts := []string{
		throughputSection(q, t, f, inner),
		latencySection(t, f, taskLatencies(q)),
		leasesSection(q, t, f, inner),
		deadLettersSection(q, t, f, inner),
		kv(t, "checkpoint", checkpointRows(q, f), false, inner),
		kv(t, "policy", policyRows(q, f), true, inner),
		countersLine(t, taskCounterCells(q, f)),
	}
	return strings.Join(nonEmpty(parts), "\n")
}

// throughputSection renders the rates section: per-second rates and byte flow.
func throughputSection(q metrics.QueueView, t style.Theme, f human.Formatter, inner int) string {
	var rows [][2]string
	if q.IsTask() {
		rows = [][2]string{
			{"enqueues/s", rate(q.Rates.EnqueuesPerSec, f)},
			{"dequeues/s", rate(q.Rates.DequeuesPerSec, f)},
		}
	} else {
		rows = [][2]string{
			{"appends/s", rate(q.Rates.AppendsPerSec, f)},
			{"reads/s", rate(q.Rates.ReadsPerSec, f)},
			{"bytes in", f.Rate(q.Rates.BytesInPerSec)},
			{"bytes out", f.Rate(q.Rates.BytesOutPerSec)},
		}
	}
	return kv(t, "throughput", rows, true, inner)
}

// latencyPair names one histogram and the label its latency line carries.
type latencyPair struct {
	label string
	view  metrics.HistogramView
}

// logLatencies collects the log queue's latency histograms in render order.
func logLatencies(q metrics.QueueView) []latencyPair {
	return namedLatencies(q, "append_duration", "read_duration", "append", "read")
}

// taskLatencies collects the task queue's latency histograms in render order.
func taskLatencies(q metrics.QueueView) []latencyPair {
	return namedLatencies(q, "enqueue_duration", "dequeue_wait", "enqueue", "dequeue wait")
}

// namedLatencies pairs the named histograms with their labels, dropping the
// ones with no observations (invariant I8: empty sections don't render).
func namedLatencies(q metrics.QueueView, first, second, firstLabel, secondLabel string) []latencyPair {
	var out []latencyPair
	for _, pair := range []struct{ name, label string }{
		{first, firstLabel},
		{second, secondLabel},
	} {
		if h, ok := q.Histograms[pair.name]; ok && h.Total > 0 {
			out = append(out, latencyPair{label: pair.label, view: h})
		}
	}
	return out
}

// latencySection renders one line per observed histogram: dim labels, bright
// numbers, percentiles only (invariant I12: buckets stay in json).
func latencySection(t style.Theme, f human.Formatter, pairs []latencyPair) string {
	if len(pairs) == 0 {
		return ""
	}
	lines := []string{t.SectionTitle("latency")}
	for _, pair := range pairs {
		lines = append(lines, latencyLine(pair, t, f))
	}
	return strings.Join(lines, "\n")
}

// latencyLine renders one histogram's p50/p90/p99/max as labeled spans.
func latencyLine(pair latencyPair, t style.Theme, f human.Formatter) string {
	dim := t.Paint(t.Label)
	span := func(stat string, seconds float64) string {
		return dim.Render(stat+" ") + f.Duration(secondsDuration(seconds))
	}
	return dim.Render(pair.label+"  ") +
		span("p50", pair.view.P50) + "  " +
		span("p90", pair.view.P90) + "  " +
		span("p99", pair.view.P99) + "  " +
		span("max", pair.view.Max)
}

// ioSection renders the producers/consumers concurrency section, six pairs in
// two columns: producers on the left, consumers on the right.
func ioSection(q metrics.QueueView, t style.Theme, f human.Formatter, inner int) string {
	rows := [][2]string{
		{"in flight", f.Count(q.Producers.InFlight)},
		{"blocked on wal", f.Count(q.Producers.BlockedOnWAL)},
		{"block events", f.Count(q.Producers.BlockEvents)},
		{"in flight (cons)", f.Count(q.Consumers.InFlight)},
		{"parked fetch", f.Count(q.Consumers.ParkedFetch)},
		{"park events", f.Count(q.Consumers.FetchParkEvents)},
	}
	return kv(t, "producers / consumers", rows, true, inner)
}

// backpressureSection renders the two backpressure lanes: the fill gauges,
// then the event counters as a key-value pair block.
func backpressureSection(q metrics.QueueView, t style.Theme, f human.Formatter, inner int) string {
	labelWidth := lipgloss.Width("wal channel fill")
	gaugeRow := func(label string, frac float64) string {
		frac = unit(frac)
		return t.Paint(t.Label).Width(labelWidth).Render(label) + "  " +
			t.Gauge(frac, gaugeLaneWidth, false) + "  " + f.Percent(frac)
	}
	lines := []string{
		t.SectionTitle("backpressure"),
		gaugeRow("wal channel fill", q.Backpressure.WALChannelFill),
		gaugeRow("wal byte fill", q.Backpressure.WALByteFill),
		kv(t, "", [][2]string{
			{"queue full", f.Count(q.Backpressure.QueueFullEvents)},
			{"producer blocks", f.Count(q.Backpressure.ProducerBlocks)},
			{"consumer parks", f.Count(q.Backpressure.ConsumerParks)},
			{"retry drops", f.Count(q.Backpressure.RetryBudgetDrops)},
		}, true, inner),
	}
	return strings.Join(nonEmpty(lines), "\n")
}

// groupsSection renders the consumer-group table, the lag cell colored by the
// lag health thresholds.
func groupsSection(q metrics.QueueView, t style.Theme, f human.Formatter) string {
	if len(q.Groups) == 0 {
		return ""
	}
	rows := make([][]string, 0, len(q.Groups))
	for _, g := range q.Groups {
		rows = append(rows, []string{
			g.Name,
			f.Count(int64(g.Committed)),
			f.Count(int64(g.HighWater)),
			t.Paint(HealthColor(t, metrics.GroupHealth(g))).Render(f.Count(int64(g.Lag))),
		})
	}
	headers := []string{"NAME", "COMMITTED", "HIGH WATER", "LAG"}
	numeric := map[int]bool{1: true, 2: true, 3: true}
	return strings.Join(nonEmpty([]string{
		t.SectionTitle("consumer groups"), t.DataTable(headers, rows, numeric),
	}), "\n")
}

// leasesSection renders the task occupancy: live counts plus the scheduler heap.
func leasesSection(q metrics.QueueView, t style.Theme, f human.Formatter, inner int) string {
	rows := [][2]string{
		{"active", f.Count(q.Tasks.Active)},
		{"in flight", f.Count(q.Tasks.InFlight)},
		{"scheduled", f.Count(q.Tasks.Scheduled)},
		{"schedule heap", f.Count(q.Tasks.ScheduleHeap)},
	}
	return kv(t, "leases", rows, true, inner)
}

// deadLettersSection renders the dead-letter count against its capacity, the
// bar calm when the queue is otherwise healthy: dead letters are by design.
func deadLettersSection(q metrics.QueueView, t style.Theme, f human.Formatter, width int) string {
	if q.Tasks.DeadLetters == 0 && q.Tasks.Uncheckpointed == 0 {
		return ""
	}
	calm := metrics.QueueHealth(q) == metrics.HealthOK
	labelWidth := lipgloss.Width("dead letters")
	head := t.SectionTitle("dead letters")
	if bound := q.MaxDeadLetters(); bound > 0 {
		frac := unit(float64(q.Tasks.DeadLetters) / float64(bound))
		note := f.Count(q.Tasks.DeadLetters) + " / " + f.Count(bound)
		if q.Tasks.Uncheckpointed > 0 {
			note += " · " + f.Count(q.Tasks.Uncheckpointed) + " uncheckpointed"
		}
		return head + "\n" + gaugeLine("dead letters", frac, calm, note, t, f, labelWidth, width)
	}
	return head + "\n" + t.Paint(t.Label).Width(labelWidth).Render("dead letters") +
		"  " + f.Count(q.Tasks.DeadLetters)
}

// gaugeLine renders one label + bar + percentage + note row in fixed lanes,
// truncating the note to whatever width the row has left.
func gaugeLine(label string, frac float64, ok bool, note string, t style.Theme, f human.Formatter, labelWidth, width int) string {
	frac = unit(frac)
	pct := f.Percent(frac)
	line := t.Paint(t.Label).Width(labelWidth).Render(label) + "  " +
		t.Gauge(frac, gaugeLaneWidth, ok) + "  " + pct
	if note != "" {
		room := width - labelWidth - 2 - (gaugeLaneWidth + 1) - 2 - lipgloss.Width(pct) - 2
		if room > 0 {
			line += "  " + t.Paint(t.Label).Render(ansi.Truncate(note, room, "…"))
		}
	}
	return line
}

// checkpointRows reads the task queue's checkpoint health for its section.
func checkpointRows(q metrics.QueueView, f human.Formatter) [][2]string {
	return [][2]string{
		{"checkpoint lsn", f.Count(q.Tasks.CheckpointLSN)},
		{"dirty records", f.Count(q.Tasks.Dirty)},
		{"checkpoint lag", f.Count(q.Tasks.CheckpointLag)},
	}
}

// policyRows reads the task queue's resolved bounds for the policy section.
func policyRows(q metrics.QueueView, f human.Formatter) [][2]string {
	return [][2]string{
		{"lease", f.Duration(q.Lease())},
		{"max attempts", f.Count(q.MaxAttempts())},
		{"max in flight", f.Count(q.MaxInFlight())},
		{"max dead letters", f.Count(q.MaxDeadLetters())},
	}
}

// countersLine renders the demoted totals and event counters as one dim line.
func countersLine(t style.Theme, cells []string) string {
	if len(cells) == 0 {
		return ""
	}
	return t.Paint(t.Label).Render(strings.Join(cells, " · "))
}

// logCounterCells picks the log queue's counters that have no section of their own.
func logCounterCells(q metrics.QueueView, f human.Formatter) []string {
	return counterCells(q, f, [][2]string{
		{"total appends", "appends"}, {"total reads", "reads"},
		{"cache hits", "cache_hits"}, {"cache misses", "cache_misses"},
		{"segment rolls", "segment_rolls"}, {"group commits", "group_commits"},
	})
}

// taskCounterCells picks the task queue's counters that have no section of their own.
func taskCounterCells(q metrics.QueueView, f human.Formatter) []string {
	return counterCells(q, f, [][2]string{
		{"total enqueues", "enqueues"}, {"total dequeues", "dequeues"},
		{"acks", "acks"}, {"nacks", "nacks"}, {"extends", "extends"},
		{"dlq acks", "dlq_acks"}, {"stalls", "task_stalls"},
		{"queue full", "queue_full_events"}, {"retry drops", "retry_budget_drops"},
	})
}

// counterCells renders the named counters that have fired at least once.
func counterCells(q metrics.QueueView, f human.Formatter, picks [][2]string) []string {
	var cells []string
	for _, pick := range picks {
		if n := q.Counters[pick[1]]; n > 0 {
			cells = append(cells, pick[0]+" "+f.Count(n))
		}
	}
	return cells
}

// unit clamps a ratio into [0,1] and maps broken readings to an end of the range.
func unit(frac float64) float64 {
	switch {
	case math.IsNaN(frac):
		return 0
	case math.IsInf(frac, 1):
		return 1
	case math.IsInf(frac, -1):
		return 0
	default:
		return min(max(frac, 0), 1)
	}
}

// secondsDuration converts a histogram reading in seconds to a duration.
func secondsDuration(seconds float64) time.Duration {
	if math.IsNaN(seconds) || math.IsInf(seconds, 0) {
		return 0
	}
	return time.Duration(seconds * float64(time.Second))
}

// finiteOrZero maps a non-finite float to zero so a broken gauge prints as 0.
func finiteOrZero(v float64) float64 {
	if math.IsNaN(v) || math.IsInf(v, 0) {
		return 0
	}
	return v
}
