package metricsview

import (
	"bytes"
	"errors"
	"slices"
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

const testWidth = 78

// logQueueView builds a log queue with readings, a group, and no pressure.
func logQueueView(mutate ...func(*metrics.QueueView)) metrics.QueueView {
	q := metrics.QueueView{
		Name: "events",
		Kind: "log",
		Gauges: map[string]int64{
			"messages": 1_234_567, "segments": 7, "size_bytes": 4096 << 20,
			"last_offset": 1_234_567, "min_offset": 0,
		},
		Rates:  metrics.Rates{AppendsPerSec: 42.4, ReadsPerSec: 12, BytesInPerSec: 1_572_864},
		Groups: []metrics.GroupView{{Name: "workers", Committed: 1_180_000, HighWater: 1_234_567, Lag: 54_567}},
	}
	return mutateQueue(q, mutate...)
}

// taskQueueView builds a task queue with occupancy and a stored policy spec; the effective spec is derived.
func taskQueueView(mutate ...func(*metrics.QueueView)) metrics.QueueView {
	maxInFlight, maxDead := uint32(1000), uint32(10_000)
	q := metrics.QueueView{
		Name: "jobs",
		Kind: "task",
		Gauges: map[string]int64{
			"messages": 5000, "segments": 2, "size_bytes": 8192,
		},
		Rates: metrics.Rates{EnqueuesPerSec: 7, DequeuesPerSec: 6.6},
		Tasks: metrics.TaskView{Active: 120, InFlight: 30, Scheduled: 90, DeadLetters: 2},
		Spec:  spec.Spec{Task: spec.TaskSpec{MaxInFlight: &maxInFlight, MaxDeadLetters: &maxDead}},
	}
	return mutateQueue(q, mutate...)
}

// mutateQueue applies the fixture tweaks in order and returns the queue.
func mutateQueue(q metrics.QueueView, mutate ...func(*metrics.QueueView)) metrics.QueueView {
	for _, m := range mutate {
		m(&q)
	}
	return q
}

// hot is a task queue under pressure: dead letters, a full-queue event, and an
// in-flight count at its bound.
func hot() metrics.QueueView {
	return taskQueueView(func(q *metrics.QueueView) {
		q.Name = "billing"
		q.Gauges = map[string]int64{"messages": 91, "segments": 1, "size_bytes": 65536}
		q.Rates = metrics.Rates{EnqueuesPerSec: 120, DequeuesPerSec: 118}
		q.Tasks = metrics.TaskView{Active: 940, InFlight: 960, DeadLetters: 17}
		q.Backpressure.QueueFullEvents = 3
	})
}

// stale is a log queue whose only consumer is far behind.
func stale() metrics.QueueView {
	return logQueueView(func(q *metrics.QueueView) {
		q.Name = "audit"
		q.Gauges = map[string]int64{"messages": 4, "segments": 1, "size_bytes": 1024}
		q.Rates = metrics.Rates{}
		q.Groups = []metrics.GroupView{{Name: "slow", HighWater: 100, Lag: 80}}
	})
}

// fixtureSnapshot is a full node: both kinds, mixed health, real readings.
func fixtureSnapshot() metrics.Snapshot {
	return metrics.Snapshot{
		Timestamp: time.Date(2026, 9, 26, 12, 0, 0, 0, time.UTC),
		Build:     metrics.BuildView{SpecVersion: 1, GoVersion: "go1.27", OS: "linux", Arch: "amd64"},
		Runtime:   metrics.RuntimeView{UptimeSec: 4980},
		Storage: metrics.StorageView{
			DataDir: "./data", Queues: 4, Segments: 11, SizeBytes: 4097 << 20, CacheLimit: 32 << 20,
		},
		Queues: []metrics.QueueView{logQueueView(), stale(), taskQueueView(), hot()},
		Totals: metrics.TotalsView{Queues: 4, Messages: 1_239_571, Bytes: 4097 << 20},
	}
}

// render runs Summary into a string using the given theme and formatter.
func render(t *testing.T, snap metrics.Snapshot, theme style.Theme, f human.Formatter) string {
	t.Helper()
	var buf bytes.Buffer
	require.NoError(t, Summary(&buf, snap, theme, f, testWidth))
	return buf.String()
}

// plainLines renders the fixture colorless and splits it into lines.
func plainLines(t *testing.T, snap metrics.Snapshot) []string {
	return strings.Split(render(t, snap, style.PlainTheme(), human.Formatter{}), "\n")
}

// forceColor pins the process to a 24-bit terminal for one test. Lipgloss
// resolves the profile from os.Environ on every write and strips color when it
// believes the output is not a terminal, so asserting on the colored render
// means telling colorprofile what a real terminal reports. Phase 6 pins its
// goldens the same way.
func forceColor(t *testing.T) {
	t.Helper()
	t.Setenv("TTY_FORCE", "1")
	t.Setenv("CLICOLOR_FORCE", "1")
	t.Setenv("COLORTERM", "truecolor")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("NO_COLOR", "")
}

// richRender renders with color forced on and returns the unstripped output.
func richRender(t *testing.T, snap metrics.Snapshot, f human.Formatter) string {
	t.Helper()
	forceColor(t)
	var buf bytes.Buffer
	require.NoError(t, Summary(&buf, snap, style.NewTheme(true), f, testWidth))
	return buf.String()
}

// colorOfLine returns the unstripped line carrying a substring, or "".
func colorOfLine(raw, substr string) string {
	for _, line := range strings.Split(raw, "\n") {
		if strings.Contains(ansi.Strip(line), substr) {
			return line
		}
	}
	return ""
}

// TestSummaryColumnContract is the invariant I5 test: a header change breaks it.
func TestSummaryColumnContract(t *testing.T) {
	assert.Equal(t, []string{"NAME", "MESSAGES", "SIZE", "APPENDS/s", "READS/s", "LAG", "STATE"}, logQueueHeaders)
	assert.Equal(t, []string{"NAME", "ACTIVE", "IN-FLIGHT", "SCHEDULED", "DEAD", "ENQ/s", "DEQ/s", "STATE"}, taskQueueHeaders)
	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true}, logQueueNumeric)
	assert.Equal(t, map[int]bool{1: true, 2: true, 3: true, 4: true, 5: true, 6: true}, taskQueueNumeric)
	assert.False(t, logQueueNumeric[0], "NAME is a word, not a quantity")
	assert.False(t, logQueueNumeric[6], "STATE is a glyph, not a quantity")

	out := render(t, fixtureSnapshot(), style.PlainTheme(), human.Formatter{})
	for _, header := range slices.Concat(logQueueHeaders, taskQueueHeaders) {
		assert.Contains(t, out, header, "every declared header must be rendered")
	}
}

// TestSummaryGolden pins the whole plain render, so any layout change is a diff.
func TestSummaryGolden(t *testing.T) {
	assert.Equal(t, []string{
		"○ mikyu",
		"go1.27 · linux/amd64 · spec v1 · up 1h 23m",
		"./data · 11 segments · 2026-09-26 12:00:00Z",
		"   4      1.2M    4.0 GiB   ▲ 1.5 MiB/s ",
		"QUEUES  MESSAGES  ON DISK  THROUGHPUT IN",
		"LOG QUEUES",
		" NAME    MESSAGES  SIZE     APPENDS/s  READS/s  LAG     STATE ",
		" audit          4  1.0 KiB          0        0  80.0 %  ○     ",
		" events      1.2M  4.0 GiB         42       12   4.4 %  ●     ",
		"TASK QUEUES",
		" NAME     ACTIVE  IN-FLIGHT  SCHEDULED  DEAD  ENQ/s  DEQ/s  STATE ",
		" billing     940        960          0    17    120    118  ○     ",
		" jobs        120         30         90     2      7      7  ◐     ",
		"",
	}, plainLines(t, fixtureSnapshot()))
}

// TestSummaryStateGlyphsFollowHealth ties each STATE cell to its queue's health.
func TestSummaryStateGlyphsFollowHealth(t *testing.T) {
	out := render(t, fixtureSnapshot(), style.PlainTheme(), human.Formatter{})
	assert.Contains(t, out, "80.0 %  ○", "80% lag is critical")
	assert.Contains(t, out, "4.4 %  ●", "4% lag on a quiet queue is healthy")
	assert.Contains(t, out, "17    120    118  ○", "dead letters plus a saturated bound is critical")
	assert.Contains(t, out, "2      7      7  ◐", "dead letters alone is degraded")
	assert.Contains(t, out, "○ mikyu", "the node takes its worst queue's state")
}

// TestSummaryRichCards: the header card is the only gradient surface, and the
// queue cards are the only ones with a title set into the edge (invariant I7).
func TestSummaryRichCards(t *testing.T) {
	out := ansi.Strip(richRender(t, fixtureSnapshot(), human.Formatter{}))
	assert.Equal(t, 3, strings.Count(out, "╭"), "one header card plus one card per queue kind")
	assert.Equal(t, 1, strings.Count(out, "╭─ LOG QUEUES "), "edge titles belong to the queue cards")
	assert.Equal(t, 1, strings.Count(out, "╭─ TASK QUEUES "))
	assert.Equal(t, 0, strings.Count(out, "╭─ ○ mikyu"), "the header card keeps its title inside")
	assert.Contains(t, out, "│  ○ mikyu", "the node's state glyph leads the header")
}

// TestSummaryRichColorsState: the rich row carries escapes, and stripping them
// reproduces the plain row exactly. Which spans are colored is asserted in
// health_test.go, where it can be checked one glyph at a time.
func TestSummaryRichColorsState(t *testing.T) {
	raw := richRender(t, fixtureSnapshot(), human.Formatter{})
	require.Contains(t, raw, "\x1b[", "a truecolor profile keeps the escapes")

	row := colorOfLine(raw, "4.4 %")
	require.NotEmpty(t, row, "the healthy row must be present")
	assert.Contains(t, row, "\x1b[")
	assert.Equal(t, "│  events      1.2M  4.0 GiB         42       12   4.4 %  ●                  │",
		ansi.Strip(row), "the rich row is the plain row inside a card")
}

// TestSummaryEmptyStates pins invariant I8: no card, a teaching line instead.
func TestSummaryEmptyStates(t *testing.T) {
	snap := fixtureSnapshot()
	snap.Queues = []metrics.QueueView{taskQueueView(), hot()}
	out := render(t, snap, style.PlainTheme(), human.Formatter{})
	assert.NotContains(t, out, "LOG QUEUES", "a kind with no queues renders no card header")
	assert.Contains(t, out, "no log queues yet — create one with: mikyu queue create <name> --kind log")
	assert.Contains(t, out, "TASK QUEUES", "the other kind still renders")

	empty := render(t, metrics.Snapshot{}, style.PlainTheme(), human.Formatter{})
	assert.Contains(t, empty, "no log queues yet")
	assert.Contains(t, empty, "no task queues yet")
	assert.NotContains(t, empty, "LOG QUEUES")
	assert.NotContains(t, empty, "TASK QUEUES")
	assert.Contains(t, empty, "- · 0 segments", "an unreported data dir shows a dash")
}

// TestSummaryRawModeIsGreppable pins I3 for a pipe: raw numbers, no decoration.
func TestSummaryRawModeIsGreppable(t *testing.T) {
	out := render(t, fixtureSnapshot(), style.PlainTheme(), human.Formatter{Raw: true})
	assert.NotContains(t, out, "\x1b", "a pipe never gets color")
	assert.NotContains(t, out, " %", "percentages carry no unit in raw mode")
	assert.NotContains(t, out, "MiB", "byte counts are bare integers")
	assert.NotContains(t, out, "▲", "the flow glyph is decoration and stays out of a pipe")
	assert.Contains(t, out, "4294967296", "4 GiB as a plain integer")
	assert.Contains(t, out, "54567", "lag is the absolute count, not a ratio")
	assert.Contains(t, out, "up 4980", "uptime is seconds, not 1h 23m")
}

// TestSummaryDisabled says so plainly instead of showing zeroes.
func TestSummaryDisabled(t *testing.T) {
	snap := fixtureSnapshot()
	snap.Disabled = true
	out := render(t, snap, style.PlainTheme(), human.Formatter{})
	assert.Equal(t, "○ mikyu · metrics are disabled on this node\n", out)
	assert.NotContains(t, out, "LOG QUEUES")
	assert.NotContains(t, out, "QUEUES  MESSAGES")
}

// TestSummaryNarrowWidth: a 60-column terminal is the narrow floor the layout
// guarantees, and narrower than that it must still render without panicking.
func TestSummaryNarrowWidth(t *testing.T) {
	for _, width := range []int{60, 78, 120} {
		var buf bytes.Buffer
		require.NoError(t, Summary(&buf, fixtureSnapshot(), style.NewTheme(true), human.Formatter{}, width))
		for i, line := range strings.Split(ansi.Strip(buf.String()), "\n") {
			assert.LessOrEqual(t, lipgloss.Width(line), width, "width %d line %d overflows", width, i)
		}
	}
	for _, width := range []int{20, 30, 40, 50} {
		var buf bytes.Buffer
		assert.NotPanics(t, func() {
			_ = Summary(&buf, fixtureSnapshot(), style.NewTheme(true), human.Formatter{}, width)
		}, "width %d must render", width)
	}
}

// TestSummaryPropagatesWriterError keeps a broken pipe from looking like success.
func TestSummaryPropagatesWriterError(t *testing.T) {
	err := Summary(failWriter{}, fixtureSnapshot(), style.PlainTheme(), human.Formatter{}, testWidth)
	require.Error(t, err)
	assert.ErrorIs(t, err, errClosed)
}

// failWriter rejects every write, standing in for a closed pipe.
type failWriter struct{}

func (failWriter) Write([]byte) (int, error) { return 0, errClosed }

// errClosed is the write failure a broken pipe reports.
var errClosed = errors.New("closed")
