package metricsview

import (
	"bytes"
	"strings"
	"testing"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// renderDetail renders Detail colorless and returns the text.
func renderDetail(t *testing.T, snap metrics.Snapshot, selected []string) string {
	t.Helper()
	out, err := detailResult(t, snap, selected, human.Formatter{})
	require.NoError(t, err)
	return out
}

// detailResult renders Detail colorless and hands back both outputs, for the
// paths where Detail writes a note and then fails.
func detailResult(t *testing.T, snap metrics.Snapshot, selected []string, f human.Formatter) (string, error) {
	t.Helper()
	var buf bytes.Buffer
	err := Detail(&buf, snap, selected, style.PlainTheme(), f, testWidth)
	return buf.String(), err
}

// richDetail renders Detail with color forced on, returning the raw escapes.
func richDetail(t *testing.T, snap metrics.Snapshot, selected []string) string {
	t.Helper()
	forceColor(t)
	var buf bytes.Buffer
	require.NoError(t, Detail(&buf, snap, selected, style.NewTheme(true), human.Formatter{}, testWidth))
	return buf.String()
}

// TestDetailGolden pins the whole plain render of the fixture: header, one card
// per queue, the node grid, and the footer. A layout change is a diff.
func TestDetailGolden(t *testing.T) {
	golden(t, "detail_full.txt", []byte(renderDetail(t, fixtureSnapshot(), nil)))
}

// TestDetailSingleQueueGoldens pin one card of each kind, selected by name.
func TestDetailSingleQueueGoldens(t *testing.T) {
	golden(t, "detail_events.txt", []byte(renderDetail(t, fixtureSnapshot(), []string{"events"})))
	golden(t, "detail_jobs.txt", []byte(renderDetail(t, fixtureSnapshot(), []string{"jobs"})))
}

// TestDetailQueueFilter: a named selection renders only the named queues, in
// the order given, once each.
func TestDetailQueueFilter(t *testing.T) {
	out := renderDetail(t, fixtureSnapshot(), []string{"jobs", "events", "jobs"})
	jobs, events := strings.Index(out, "jobs · task"), strings.Index(out, "events · log")
	require.Greater(t, jobs, -1)
	require.Greater(t, events, -1)
	assert.Less(t, jobs, events, "the selection order is preserved")
	assert.Equal(t, 1, strings.Count(out, "jobs · task"), "duplicates collapse")
	assert.NotContains(t, out, "audit · log", "unselected queues stay out")
	assert.Equal(t, 2, strings.Count(out, "EFFECTIVE SPEC — "),
		"one effective-spec card per selected queue")
}

// TestDetailSkipsInternalScopes: a non-queue instrument scope is not a queue card.
func TestDetailSkipsInternalScopes(t *testing.T) {
	snap := fixtureSnapshot()
	snap.Queues = append(snap.Queues, metrics.QueueView{Name: "server", Kind: "internal"})
	out := renderDetail(t, snap, nil)
	assert.NotContains(t, out, "server · internal", "the server scope is not a queue")
	assert.Contains(t, out, "events · log", "real queues still render")
}

// TestDetailUnknownQueue: a missed name prints the teaching state and fails
// with ErrUnknownQueue for the command layer to map to its exit code.
func TestDetailUnknownQueue(t *testing.T) {
	out, err := detailResult(t, fixtureSnapshot(), []string{"nope"}, human.Formatter{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownQueue)
	assert.Equal(t, "no queue named \"nope\" — mikyu metrics lists what exists\n", out)

	_, err = detailResult(t, fixtureSnapshot(), []string{"events", "nope"}, human.Formatter{})
	require.Error(t, err)
	assert.ErrorIs(t, err, ErrUnknownQueue)
}

// TestDetailSpecDiffGolden pins the layer-diff rendering (invariant I11).
func TestDetailSpecDiffGolden(t *testing.T) {
	golden(t, "detail_specdiff.txt", []byte(renderDetail(t, specDiffSnapshot(), []string{"events", "jobs"})))
}

// TestDetailSpecDiff is invariant I11: silence when the queue sets nothing, a
// faint session tag on agreement, an override note on disagreement.
func TestDetailSpecDiff(t *testing.T) {
	snap := specDiffSnapshot()
	out := renderDetail(t, snap, []string{"events", "jobs"})

	assert.Contains(t, out, "16.0 MiB  ← override (session: 32.0 MiB)")
	assert.Contains(t, out, "30s  · session")
	// The queue left durability unset, so its row carries no marker at all.
	assert.Regexp(t, `(?m)^durability\s+sync$`, out)
	// The task queue renders no compaction row; the log queue's card does.
	taskSection := out[strings.Index(out, "EFFECTIVE SPEC — jobs"):]
	assert.NotContains(t, taskSection, "compaction", "a task queue renders no compaction row")
	assert.Contains(t, out, "compaction", "a log queue renders the compaction row")
}

// TestDetailTaskCardSections: a task card speaks leases and policy, and never
// borrows the log card's producer, consumer, or backpressure sections.
func TestDetailTaskCardSections(t *testing.T) {
	out := renderDetail(t, fixtureSnapshot(), []string{"jobs"})
	for _, section := range []string{"THROUGHPUT", "LEASES", "DEAD LETTERS", "CHECKPOINT", "POLICY"} {
		assert.Contains(t, out, section)
	}
	assert.NotContains(t, out, "PRODUCERS / CONSUMERS")
	assert.NotContains(t, out, "BACKPRESSURE")
	assert.NotContains(t, out, "CONSUMER GROUPS")
	assert.Contains(t, out, "2 / 10,000", "the dead-letter gauge shows count against capacity")

	dead := renderDetail(t, fixtureSnapshot(), []string{"billing"})
	assert.Contains(t, dead, "17 / 10,000", "the dead-letter gauge shows count against capacity")
}

// TestDetailLatencyOnlyWhenObserved is invariant I8 applied to histograms.
func TestDetailLatencyOnlyWhenObserved(t *testing.T) {
	assert.NotContains(t, renderDetail(t, fixtureSnapshot(), nil), "LATENCY")
	observed := fixtureSnapshot()
	q := observed.Queues[0]
	q.Histograms = map[string]metrics.HistogramView{
		"append_duration": {P50: 0.00012, P90: 0.00034, P99: 0.0012, Max: 0.002, Total: 9},
	}
	observed.Queues[0] = q
	out := renderDetail(t, observed, []string{"events"})
	assert.Contains(t, out, "LATENCY")
	assert.Contains(t, out, "append  p50 120µs  p90 340µs  p99 1.2ms  max 2ms")
}

// TestDetailLocksSectionHiddenWhenQuiet: contention sections don't render when
// nothing ever waited (invariant I8).
func TestDetailLocksSectionHiddenWhenQuiet(t *testing.T) {
	assert.NotContains(t, renderDetail(t, fixtureSnapshot(), nil), "LOCKS")
}

// TestDetailLocksSection pins the lock table's column contract and its values.
func TestDetailLocksSection(t *testing.T) {
	snap := fixtureSnapshot()
	snap.Locks = metrics.LocksView{
		GlobalWaitSec: 0.42,
		Observers: []metrics.LockView{
			{Name: "node.mu", Waits: 42, TotalWaitMs: 33.6, MaxWaitMs: 0.8, P50WaitMs: 0.4, P99WaitMs: 0.8},
			{Name: "cold.lock", Waits: 0},
		},
	}
	out := renderDetail(t, snap, nil)
	assert.Contains(t, out, "LOCKS")
	assert.Contains(t, out, "global mutex wait 420ms")
	assert.Contains(t, out, " NAME     WAITS  TOTAL WAIT  MAX WAIT  P50    P99 ")
	assert.Contains(t, out, " node.mu     42      33.6ms     800µs  400µs  800µs")
	assert.NotContains(t, out, "cold.lock", "a lock that never waited renders no row")
}

// TestDetailNodeGridJoinsAndStacks: wide terminals put runtime beside process
// and gc beside storage; a 78-column terminal stacks the pairs.
func TestDetailNodeGridJoinsAndStacks(t *testing.T) {
	out := renderDetail(t, fixtureSnapshot(), nil)
	assert.NotContains(t, out, "RUNTIME PROCESS", "stacked at 78 columns")

	var buf bytes.Buffer
	require.NoError(t, Detail(&buf, fixtureSnapshot(), nil, style.PlainTheme(), human.Formatter{}, 120))
	wide := buf.String()
	for _, line := range strings.Split(wide, "\n") {
		if strings.Contains(line, "RUNTIME") {
			assert.Contains(t, line, "PROCESS", "the first pair shares a line")
		}
		if strings.Contains(line, "GC") && !strings.Contains(line, "GCView") {
			assert.Contains(t, line, "STORAGE", "the second pair shares a line")
		}
	}
	assert.NotContains(t, wide, "RUNTIME\n", "a wide terminal never stacks the pairs")
}

// TestDetailGroupLagColoredByThreshold: the lag cell takes the health color of
// its group's lag ratio, and stripping the color reproduces the plain cell.
func TestDetailGroupLagColoredByThreshold(t *testing.T) {
	raw := richDetail(t, fixtureSnapshot(), []string{"events"})
	row := colorOfLine(raw, "54,567")
	require.NotEmpty(t, row, "the lag cell must be present")
	assert.Contains(t, row, "\x1b[", "a colored cell carries its escape")
	assert.Contains(t, ansi.Strip(row), "54,567")
}

// TestDetailRawModeIsGreppable pins I3 for a piped detail view.
func TestDetailRawModeIsGreppable(t *testing.T) {
	out, err := detailResult(t, fixtureSnapshot(), nil, human.Formatter{Raw: true})
	require.NoError(t, err)
	assert.NotContains(t, out, "\x1b")
	assert.NotContains(t, out, "MiB")
	assert.NotContains(t, out, " %")
	assert.Contains(t, out, "4296015872", "byte counts are bare integers")
}

// TestDetailDeterministic: the same snapshot renders byte-identically twice.
func TestDetailDeterministic(t *testing.T) {
	first := renderDetail(t, fixtureSnapshot(), nil)
	second := renderDetail(t, fixtureSnapshot(), nil)
	assert.Equal(t, first, second)
}

// TestDetailDisabled says so plainly instead of showing zeroes.
func TestDetailDisabled(t *testing.T) {
	snap := fixtureSnapshot()
	snap.Disabled = true
	out := renderDetail(t, snap, nil)
	assert.Equal(t, "○ mikyu · metrics are disabled on this node\n", out)
	assert.NotContains(t, out, "RUNTIME")
}

// TestDetailNarrowWidth: 60 columns is the narrow floor the layout guarantees;
// below that it must still render without panicking or overflowing.
func TestDetailNarrowWidth(t *testing.T) {
	for _, width := range []int{60, 78, 120} {
		var buf bytes.Buffer
		require.NoError(t, Detail(&buf, fixtureSnapshot(), nil, style.NewTheme(true), human.Formatter{}, width))
		for i, line := range strings.Split(ansi.Strip(buf.String()), "\n") {
			assert.LessOrEqual(t, lipgloss.Width(line), width, "width %d line %d overflows: %q", width, i, line)
		}
	}
	for _, width := range []int{20, 30, 40, 50} {
		var buf bytes.Buffer
		assert.NotPanics(t, func() {
			_ = Detail(&buf, fixtureSnapshot(), nil, style.NewTheme(true), human.Formatter{}, width)
		}, "width %d must render", width)
	}
}

// TestDetailPropagatesWriterError keeps a broken pipe from looking like success.
func TestDetailPropagatesWriterError(t *testing.T) {
	err := Detail(failWriter{}, fixtureSnapshot(), nil, style.PlainTheme(), human.Formatter{}, testWidth)
	require.Error(t, err)
	assert.ErrorIs(t, err, errClosed)
}

// specDiffSnapshot builds a node whose queues disagree with the session in
// every way invariant I11 distinguishes.
func specDiffSnapshot() metrics.Snapshot {
	syncDur := spec.Durability(spec.DurabilitySync)
	cache16, cache32 := int64(16<<20), int64(32<<20)
	lease := spec.Duration(30 * time.Second)
	session := spec.Spec{
		Storage: spec.StorageSpec{Durability: &syncDur, MaxCache: &cache32},
		Task:    spec.TaskSpec{Lease: &lease},
	}
	events := logQueueView(func(q *metrics.QueueView) {
		// durability unset (no marker); max-cache overrides the session.
		q.Spec = spec.Spec{Storage: spec.StorageSpec{MaxCache: &cache16}}
	})
	jobs := taskQueueView(func(q *metrics.QueueView) {
		q.Spec = spec.Spec{Task: spec.TaskSpec{Lease: &lease}}
	})
	snap := fixtureSnapshot()
	snap.Session = session
	snap.Queues = []metrics.QueueView{events, jobs}
	return snap
}
