// Package metricsview renders metrics snapshots for the terminal.
package metricsview

import (
	"fmt"
	"io"
	"math"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// queueKind selects which of the node's two queue tables to render.
type queueKind int

const (
	kindLog queueKind = iota
	kindTask
)

// Column headers are stable literal strings: scripts grep them, so changing one
// is a visible break rather than a quiet reformat.
var (
	logQueueHeaders  = []string{"NAME", "MESSAGES", "SIZE", "APPENDS/s", "READS/s", "LAG", "STATE"}
	taskQueueHeaders = []string{"NAME", "ACTIVE", "IN-FLIGHT", "SCHEDULED", "DEAD", "ENQ/s", "DEQ/s", "STATE"}
)

// Numeric columns per invariant I1: these hold quantities, so they align right.
// The name and state columns are words and glyphs, and stay left.
var (
	logQueueNumeric  = map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}
	taskQueueNumeric = map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}
)

// Summary writes the node summary: header, stat band, and a card per queue kind that exists, skipping the rest (invariant I8).
func Summary(w io.Writer, snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) error {
	if snap.Disabled {
		_, err := lipgloss.Fprintln(w, t.Paint(t.Danger).
			Render(style.HealthGlyph(int(metrics.HealthCritical)))+" mikyu · metrics are disabled on this node")
		return err
	}
	parts := []string{
		header(snap, t, f, width),
		statBand(snap, t, f),
		queueCard(snap, t, f, width, kindLog),
		queueCard(snap, t, f, width, kindTask),
		footer(t),
	}
	_, err := lipgloss.Fprintln(w, strings.Join(nonEmpty(parts), "\n"))
	return err
}

// header renders the one gradient surface on the screen: node identity and age.
func header(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	build := snap.Build
	identity := fmt.Sprintf("%s · %s/%s · spec v%d · up %s",
		build.GoVersion, build.OS, build.Arch, build.SpecVersion,
		f.Duration(time.Duration(snap.Runtime.UptimeSec*float64(time.Second))))
	origin := fmt.Sprintf("%s · %s · %s",
		orDash(snap.Storage.DataDir), f.Count(snap.Storage.Segments)+" segments",
		snap.Timestamp.UTC().Format("2006-01-02 15:04:05Z"))
	return t.GradientHeader(
		stateCell(t, metrics.NodeHealth(snap))+" mikyu", identity, origin, width)
}

// statBand renders the four headline numbers for the whole node.
func statBand(snap metrics.Snapshot, t style.Theme, f human.Formatter) string {
	ingest := 0.0
	for _, q := range snap.Queues {
		ingest += q.Rates.BytesInPerSec
	}
	flow := style.Stat{Value: f.Rate(ingest), Label: "throughput in"}
	if ingest > 0 && !f.Raw {
		flow.Value, flow.Color = "▲ "+flow.Value, t.Ok
	}
	return t.StatBand([]style.Stat{
		{Value: f.Count(int64(snap.Totals.Queues)), Label: "queues"},
		{Value: f.Count(int64(snap.Totals.Messages)), Label: "messages"},
		{Value: f.Bytes(snap.Totals.Bytes), Label: "on disk"},
		flow,
	})
}

// queueCard renders one queue kind's table, or a teaching empty state (I8).
func queueCard(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int, kind queueKind) string {
	queues := queuesOfKind(snap, kind)
	if len(queues) == 0 {
		return emptyState(t, kind)
	}
	rows := make([][]string, 0, len(queues))
	for _, q := range queues {
		rows = append(rows, queueRow(q, t, f, kind))
	}
	return t.TitledBox(kind.title(), t.DataTable(kind.headers(), rows, kind.numeric()), width, t.Accent)
}

// queueRow renders one queue as a table row, colored only by its state.
func queueRow(q metrics.QueueView, t style.Theme, f human.Formatter, kind queueKind) []string {
	state := stateCell(t, metrics.QueueHealth(q))
	if kind == kindTask {
		return []string{
			q.Name,
			f.Count(q.Tasks.Active),
			f.Count(q.Tasks.InFlight),
			f.Count(q.Tasks.Scheduled),
			f.Count(q.Tasks.DeadLetters),
			rate(q.Rates.EnqueuesPerSec, f),
			rate(q.Rates.DequeuesPerSec, f),
			state,
		}
	}
	return []string{
		q.Name,
		f.Count(q.Messages()),
		f.Bytes(q.SizeBytes()),
		rate(q.Rates.AppendsPerSec, f),
		rate(q.Rates.ReadsPerSec, f),
		lagCell(q, f),
		state,
	}
}

// lagCell shows consumer lag as a ratio for a reader and as a count for a pipe.
func lagCell(q metrics.QueueView, f human.Formatter) string {
	if f.Raw {
		return f.Count(int64(q.MaxLag()))
	}
	return f.Percent(q.LagRatio())
}

// rate renders a per-second rate, rounded rather than truncated.
func rate(perSec float64, f human.Formatter) string {
	return f.Count(int64(math.Round(perSec)))
}

// emptyState teaches the command that would fill the gap.
func emptyState(t style.Theme, kind queueKind) string {
	return t.Paint(t.Label).Render(fmt.Sprintf(
		"no %s queues yet — create one with: mikyu queue create <name> --kind %s", kind, kind))
}

// footer names the next moves, and only on a terminal.
func footer(t style.Theme) string {
	return t.Footer([2]string{"--queue <name>", "for detail"}, [2]string{"--detail", "for everything"})
}

// title names the card for one queue kind.
func (k queueKind) title() string {
	if k == kindTask {
		return "TASK QUEUES"
	}
	return "LOG QUEUES"
}

// headers returns the column contract for one queue kind.
func (k queueKind) headers() []string {
	if k == kindTask {
		return taskQueueHeaders
	}
	return logQueueHeaders
}

// numeric returns the right-aligned columns for one queue kind.
func (k queueKind) numeric() map[int]bool {
	if k == kindTask {
		return taskQueueNumeric
	}
	return logQueueNumeric
}

// String names the kind the way a user spells it on the command line.
func (k queueKind) String() string {
	if k == kindTask {
		return "task"
	}
	return "log"
}

// matches reports whether a queue belongs to this kind.
func (k queueKind) matches(q metrics.QueueView) bool {
	if k == kindTask {
		return q.IsTask()
	}
	return q.IsLog()
}

// queuesOfKind returns the queues of one kind in name order.
func queuesOfKind(snap metrics.Snapshot, kind queueKind) []metrics.QueueView {
	var out []metrics.QueueView
	for q := range snap.IterQueues() {
		if kind.matches(q) {
			out = append(out, q)
		}
	}
	return out
}

// orDash stands in for a value the node did not report.
func orDash(s string) string {
	if s == "" {
		return "-"
	}
	return s
}

// nonEmpty drops blank parts so the render has no blank seams.
func nonEmpty(parts []string) []string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			kept = append(kept, part)
		}
	}
	return kept
}
