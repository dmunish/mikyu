package metricsview

import (
	"errors"
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

// ErrUnknownQueue reports a --queue selection that matched no queue.
var ErrUnknownQueue = errors.New("unknown queue")

// unknownQueueError names the queue a selection missed.
type unknownQueueError struct{ name string }

func (e unknownQueueError) Error() string { return fmt.Sprintf("no queue named %q", e.name) }
func (e unknownQueueError) Unwrap() error { return ErrUnknownQueue }

// Lock column headers are stable literal strings: scripts grep them.
var lockHeaders = []string{"NAME", "WAITS", "TOTAL WAIT", "MAX WAIT", "P50", "P99"}

// Lock quantities align right; the name stays left (invariant I1).
var lockNumeric = map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}

// Detail writes the header, one card per selected queue, the node grid, contention, and the specs.
func Detail(w io.Writer, snap metrics.Snapshot, selected []string, t style.Theme, f human.Formatter, width int) error {
	if snap.Disabled {
		_, err := lipgloss.Fprintln(w, disabledNote(t))
		return err
	}
	queues, err := selectQueues(snap, selected)
	if err != nil {
		if werr := printUnknownQueue(w, err, t); werr != nil {
			return werr
		}
		return err
	}
	parts := []string{header(snap, t, f, width)}
	for _, q := range queues {
		parts = append(parts, queueCard(q, t, f, width))
	}
	parts = append(parts, nodeGrid(snap, t, f, width))
	if locks := locksSection(snap, t, f); locks != "" {
		parts = append(parts, locks)
	}
	if session := sessionSpec(snap, t, f, width); session != "" {
		parts = append(parts, session)
	}
	for _, q := range queues {
		parts = append(parts, queueSpecCard(q, snap.Session, t, f, width))
	}
	parts = append(parts, detailFooter(t))
	_, err = lipgloss.Fprintln(w, strings.Join(nonEmpty(parts), "\n"))
	return err
}

// selectQueues resolves the --queue names, or every queue when none is given.
// A named selection keeps the caller's order and fails on the first miss.
func selectQueues(snap metrics.Snapshot, selected []string) ([]metrics.QueueView, error) {
	if len(selected) == 0 {
		var out []metrics.QueueView
		for q := range snap.ResolveQueues() {
			if q.IsInternal() {
				continue
			}
			out = append(out, q)
		}
		return out, nil
	}
	seen := make(map[string]bool, len(selected))
	out := make([]metrics.QueueView, 0, len(selected))
	for _, name := range selected {
		if seen[name] {
			continue
		}
		q, ok := snap.ResolveQueue(name)
		if !ok {
			return nil, unknownQueueError{name: name}
		}
		seen[name] = true
		out = append(out, q)
	}
	return out, nil
}

// printUnknownQueue teaches the command that would have shown the queue.
func printUnknownQueue(w io.Writer, err error, t style.Theme) error {
	var uq unknownQueueError
	if !errors.As(err, &uq) {
		return nil
	}
	_, werr := lipgloss.Fprintln(w, t.Paint(t.Label).Render(
		fmt.Sprintf("%s — mikyu metrics lists what exists", uq.Error())))
	return werr
}

// nodeGrid renders the node's own sections: runtime beside process, then gc
// beside storage, stacking the pairs on a narrow terminal.
func nodeGrid(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	top := joinPair(runtimeBlock(snap, t, f, width), processBlock(snap, t, f, width), width)
	bottom := joinPair(gcBlock(snap, t, f, width), storageBlock(snap, t, f, width), width)
	return joinNonEmptyLines(top, bottom)
}

// runtimeBlock reports the Go runtime's gauges and memory classes.
func runtimeBlock(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	r := snap.Runtime
	rows := [][2]string{
		{"goroutines", f.Count(int64(r.Goroutines))},
		{"gomaxprocs", f.Count(int64(r.GOMAXPROCS))},
		{"heap in-use", f.Bytes(r.Heap.Inuse)},
		{"heap sys", f.Bytes(r.Heap.Sys)},
		{"heap idle", f.Bytes(r.Heap.Idle)},
		{"heap released", f.Bytes(r.Heap.Released)},
		{"objects", f.Count(r.Heap.Objects)},
		{"stack", f.Bytes(r.Stack.Inuse)},
		{"metadata", f.Bytes(r.Metadata.Inuse)},
	}
	block := kv(t, "runtime", rows, false, width)
	if block == "" {
		return ""
	}
	return block + "\n" + goroutineStates(r.GoroutineStates, t)
}

// goroutineStates spells the scheduler states out in words.
func goroutineStates(s metrics.GoroutineStates, t style.Theme) string {
	return t.Paint(t.Label).Render(fmt.Sprintf("%d running · %d runnable · %d waiting",
		s.Running, s.Runnable, s.Waiting))
}

// processBlock reports process-level resource gauges.
func processBlock(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	p := snap.Process
	rows := [][2]string{
		{"cpu", f.Percent(unit(p.CPUPercent / 100))},
		{"rss", f.Bytes(p.RSSBytes)},
		{"vsz", f.Bytes(p.VSZBytes)},
		{"threads", f.Count(p.Threads)},
		{"open fds", f.Count(p.OpenFDs)},
	}
	block := kv(t, "process", rows, false, width)
	if block == "" {
		return ""
	}
	if f.Raw {
		return block + "\n" + t.Paint(t.Label).Render(fmt.Sprintf("%.1f · %.1f",
			finiteOrZero(p.CPUSecondsUser), finiteOrZero(p.CPUSecondsSys)))
	}
	return block + "\n" + t.Paint(t.Label).Render(fmt.Sprintf("%.1f u · %.1f s",
		finiteOrZero(p.CPUSecondsUser), finiteOrZero(p.CPUSecondsSys)))
}

// gcBlock reports collector cycles, pauses, and allocation flow.
func gcBlock(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	g := snap.GC
	pause := func(ms float64) string { return f.Duration(msDuration(ms)) }
	rows := [][2]string{
		{"cycles", f.Count(g.Cycles)},
		{"pause p50", pause(g.Pause.P50Ms)},
		{"pause p90", pause(g.Pause.P90Ms)},
		{"pause p99", pause(g.Pause.P99Ms)},
		{"next gc", f.Bytes(g.NextGCBytes)},
		{"mem limit", memLimit(g.MemLimitBytes, f)},
		{"total alloc", f.Bytes(snap.Runtime.Heap.TotalAlloc)},
		{"total free", f.Bytes(snap.Runtime.Heap.TotalFree)},
	}
	block := kv(t, "gc", rows, false, width)
	if block == "" {
		return ""
	}
	if line := schedulerLine(snap.Scheduler.Latency, t, f); line != "" {
		return block + "\n" + line
	}
	return block
}

// memLimit renders the soft memory limit, or "none" when unbounded or not yet normalized.
func memLimit(limit int64, f human.Formatter) string {
	if limit <= 0 {
		return "none"
	}
	return f.Bytes(limit)
}

// schedulerLine renders the scheduler latency percentiles as one dim line.
func schedulerLine(lat metrics.LatencyView, t style.Theme, f human.Formatter) string {
	if lat.P50 == 0 && lat.P90 == 0 && lat.P99 == 0 {
		return ""
	}
	span := func(stat string, seconds float64) string {
		return t.Paint(t.Label).Render(stat+" ") + f.Duration(secondsDuration(seconds))
	}
	return span("sched p50", lat.P50) + "  " +
		span("p90", lat.P90) + "  " +
		span("p99", lat.P99)
}

// storageBlock reports aggregate on-disk gauges plus the cache and WAL lanes.
func storageBlock(snap metrics.Snapshot, t style.Theme, f human.Formatter, width int) string {
	s := snap.Storage
	rows := [][2]string{
		{"dir", orDash(s.DataDir)},
		{"queues", f.Count(int64(s.Queues))},
		{"segments", f.Count(s.Segments)},
		{"size", f.Bytes(s.SizeBytes)},
		{"offset store", f.Bytes(s.OffsetStoreBytes)},
	}
	labelWidth, _ := kvWidths(rows)
	for _, label := range []string{"cache", "wal pending"} {
		labelWidth = max(labelWidth, len(label))
	}
	lines := []string{kv(t, "storage", rows, false, width)}
	if line := cacheLane(s, t, f, labelWidth, width); line != "" {
		lines = append(lines, line)
	}
	if line := walLane(s, t, f, labelWidth, width); line != "" {
		lines = append(lines, line)
	}
	return strings.Join(nonEmpty(lines), "\n")
}

// cacheLane renders the ring-cache fill with its hit rate, calm and Ok-colored
// when the cache is legitimately hot.
func cacheLane(s metrics.StorageView, t style.Theme, f human.Formatter, labelWidth, width int) string {
	frac := 0.0
	if s.CacheLimit > 0 {
		frac = float64(s.CacheBytes) / float64(s.CacheLimit)
	}
	traffic := s.CacheHits + s.CacheMisses
	hitRate := 0.0
	note := f.Bytes(s.CacheBytes) + " / " + f.Bytes(s.CacheLimit)
	if traffic > 0 {
		hitRate = float64(s.CacheHits) / float64(traffic)
		note += " · " + f.Percent(hitRate) + " hit rate"
	}
	return gaugeLine("cache", frac, traffic > 0 && hitRate >= 0.9, note, t, f, labelWidth, width)
}

// walLane renders the WAL's unflushed share of the committed bytes.
func walLane(s metrics.StorageView, t style.Theme, f human.Formatter, labelWidth, width int) string {
	frac, note := 0.0, "no wal traffic"
	if s.CommittedBytes > 0 {
		frac = float64(s.PendingBytes) / float64(s.CommittedBytes)
		note = f.Bytes(s.PendingBytes) + " pending of " + f.Bytes(s.CommittedBytes)
	}
	return gaugeLine("wal pending", frac, false, note, t, f, labelWidth, width)
}

// locksSection renders contention, and only when there is any (invariant I8).
func locksSection(snap metrics.Snapshot, t style.Theme, f human.Formatter) string {
	var rows [][]string
	for _, lock := range snap.Locks.Observers {
		if lock.Waits <= 0 {
			continue
		}
		rows = append(rows, []string{
			lock.Name,
			f.Count(lock.Waits),
			f.Duration(msDuration(lock.TotalWaitMs)),
			f.Duration(msDuration(lock.MaxWaitMs)),
			f.Duration(msDuration(lock.P50WaitMs)),
			f.Duration(msDuration(lock.P99WaitMs)),
		})
	}
	if snap.Locks.GlobalWaitSec <= 0 && len(rows) == 0 {
		return ""
	}
	lines := []string{t.SectionTitle("locks")}
	if snap.Locks.GlobalWaitSec > 0 {
		lines = append(lines, t.Paint(t.Label).Render(
			"global mutex wait "+f.Duration(secondsDuration(snap.Locks.GlobalWaitSec))))
	}
	if len(rows) > 0 {
		lines = append(lines, t.DataTable(lockHeaders, rows, lockNumeric))
	}
	return strings.Join(lines, "\n")
}

// detailFooter names the next moves, and only on a terminal.
func detailFooter(t style.Theme) string {
	return t.Footer(
		[2]string{"--queue <name>", "focuses one queue"},
		[2]string{"--json", "for tools"})
}

// disabledNote is the single line a node with metrics off renders.
func disabledNote(t style.Theme) string {
	return t.Paint(t.Danger).Render(
		style.HealthGlyph(int(metrics.HealthCritical)) + " mikyu · metrics are disabled on this node")
}

// joinPair places two blocks side by side on a wide terminal, stacked otherwise.
func joinPair(left, right string, width int) string {
	switch {
	case left == "":
		return right
	case right == "":
		return left
	case width < 100:
		return left + "\n" + right
	default:
		return lipgloss.JoinHorizontal(lipgloss.Top, left, "    ", right)
	}
}

// joinNonEmptyLines joins the non-empty parts with newlines.
func joinNonEmptyLines(parts ...string) string {
	return strings.Join(nonEmpty(parts), "\n")
}

// msDuration converts a millisecond reading to a duration.
func msDuration(ms float64) time.Duration {
	if math.IsNaN(ms) || math.IsInf(ms, 0) {
		return 0
	}
	return time.Duration(ms * float64(time.Millisecond))
}
