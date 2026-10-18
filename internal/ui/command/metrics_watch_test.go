package command

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"os"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/assert"
	"golang.org/x/term"

	"github.com/stretchr/testify/require"
)

// scriptedSource serves one canned result per call, then repeats the last.
type scriptedSource struct {
	mu      sync.Mutex
	results []scriptedResult
	calls   int
}

type scriptedResult struct {
	snap mikyu.Metrics
	err  error
}

// Metrics returns the next scripted result.
func (s *scriptedSource) Metrics(context.Context) (mikyu.Metrics, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	i := min(s.calls, len(s.results)-1)
	s.calls++
	return s.results[i].snap, s.results[i].err
}

// callCount reports how many ticks have been served.
func (s *scriptedSource) callCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

// fakeTicker lets a test step a watch loop one frame at a time.
type fakeTicker struct{ ch chan time.Time }

func (f *fakeTicker) C() <-chan time.Time { return f.ch }

func (f *fakeTicker) Stop() {}

// testFrame is a snapshot with a distinct, non-zero timestamp.
func testFrame(n int) mikyu.Metrics {
	return mikyu.Metrics{
		Timestamp: time.Date(2026, 9, 27, 12, 0, n, 0, time.UTC),
		Build:     metrics.BuildView{SpecVersion: 1, GoVersion: "go1.27", OS: "linux", Arch: "amd64"},
		Queues:    []metrics.QueueView{{Name: "events", Kind: "log"}},
		Totals:    metrics.TotalsView{Queues: 1},
	}
}

// testHarness drives a watcher over scripted results and a fake clock.
type testHarness struct {
	watcher *watcher
	tick    *fakeTicker
	out     *syncBuffer
	src     *scriptedSource
	done    chan struct{}
	cancel  context.CancelFunc
	err     error
}

// newTestHarness starts a watcher in a goroutine and returns its controls.
func newTestHarness(t *testing.T, mode watchMode, opts MetricsOptions, results ...scriptedResult) *testHarness {
	t.Helper()
	h := &testHarness{
		tick: &fakeTicker{ch: make(chan time.Time)},
		out:  &syncBuffer{},
		done: make(chan struct{}),
	}
	h.src = &scriptedSource{results: results}
	h.watcher = &watcher{
		src:  h.src,
		w:    h.out,
		opts: opts,
		present: func() presentation {
			return presentationFor(h.out, opts.Width)
		},
		mode: mode,
		clock: watchClock{
			interval:  time.Second,
			now:       time.Now,
			newTicker: func(time.Duration) ticker { return h.tick },
		},
	}
	ctx, cancel := context.WithCancel(t.Context())
	h.cancel = cancel
	go func() {
		h.err = h.watcher.run(ctx)
		close(h.done)
	}()
	// The loop paints one frame before its first tick; wait for it so tests that
	// inspect the first frame cannot race the goroutine.
	h.settle(t, 1)
	require.Eventually(t, func() bool { return h.out.Len() > 0 },
		5*time.Second, time.Millisecond, "the first frame must be written")
	t.Cleanup(func() {
		cancel()
		<-h.done
	})
	return h
}

// settle waits until the loop has consumed a tick, so the next step cannot race ahead.
func (h *testHarness) settle(t *testing.T, want int) {
	t.Helper()
	require.Eventually(t, func() bool { return h.src.callCount() >= want },
		5*time.Second, time.Millisecond, "the loop must consume the tick")
}

// step advances the loop by one frame and waits until that frame is written.
func (h *testHarness) step(t *testing.T) {
	t.Helper()
	want := h.src.callCount() + 1
	before := h.out.Len()
	h.tick.ch <- time.Time{}
	h.settle(t, want)
	require.Eventually(t, func() bool { return h.out.Len() > before },
		5*time.Second, time.Millisecond, "the frame must be written")
}

// stepAbort advances the loop one tick that is expected to end the watch without
// writing, so it only waits for the tick to be consumed.
func (h *testHarness) stepAbort(t *testing.T) {
	t.Helper()
	want := h.src.callCount() + 1
	h.tick.ch <- time.Time{}
	h.settle(t, want)
}

// awaitExit cancels the watch and returns its error.
func (h *testHarness) awaitExit(t *testing.T) error {
	t.Helper()
	h.cancel()
	select {
	case <-h.done:
		return h.err
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not exit")
		return nil
	}
}

// TestWatchRepaintsInPlace: on a terminal each frame after the first erases the
// previous one in place, moving up exactly the lines it printed.
func TestWatchRepaintsInPlace(t *testing.T) {
	h := newTestHarness(t, watchRich, MetricsOptions{Detail: true},
		scriptedResult{snap: testFrame(1)}, scriptedResult{snap: testFrame(2)})

	// The loop paints one frame before any tick, so the harness has it already.
	assert.Empty(t, repaintRe.FindString(h.out.String()), "the first frame has nothing to erase")
	firstLines := strings.Count(h.out.String(), "\n")

	h.step(t)
	all := repaintRe.FindAllString(h.out.String(), -1)
	require.Len(t, all, 1, "exactly one repaint per subsequent frame")
	assert.Equal(t, firstLines, watcherEraseLines(t, all[0]),
		"the repaint must move up exactly the previous frame's printed lines")
	assert.NotContains(t, h.out.String(), "watched", "the summary only appears when the watch ends")
}

// consoleWriter is a real console file: it is the one destination this test host
// offers that lipgloss resolves as a color terminal, which is what makes the
// watch color contract observable at all.
type consoleWriter struct {
	*os.File
	buf bytes.Buffer
}

func (c *consoleWriter) Write(p []byte) (int, error) { return c.buf.Write(p) }

func (c *consoleWriter) String() string { return c.buf.String() }

// newConsoleWriter opens the console output device, skipping the test where none exists.
func newConsoleWriter(t *testing.T) *consoleWriter {
	t.Helper()
	for _, name := range []string{"CONOUT$", "/dev/tty"} {
		f, err := os.OpenFile(name, os.O_RDWR, 0)
		if err != nil {
			continue
		}
		if !term.IsTerminal(int(f.Fd())) {
			_ = f.Close()
			continue
		}
		t.Cleanup(func() { _ = f.Close() })
		return &consoleWriter{File: f}
	}
	t.Skip("no console device available")
	return nil
}

// TestWatchKeepsColorOnAConsole is the regression test for the unstyled-watch
// bug. lipgloss strips every escape when the writer it is handed is not a
// terminal file, so a frame rendered anywhere other than the destination arrives
// colorless. On a real console the watched frame must carry the same escapes as
// the one-shot render, differing only by the repaint sequence.
func TestWatchKeepsColorOnAConsole(t *testing.T) {
	snap := testFrame(1)

	oneShot := newConsoleWriter(t)
	require.NoError(t, renderMetrics(oneShot, snap, MetricsOptions{}))
	require.NotEmpty(t, escapesIn(oneShot.String()),
		"the one-shot render on a console must be colored")

	watched := newConsoleWriter(t)
	require.True(t, isTerminalWriter(watched), "the console must be taken for a terminal")
	v := &watcher{
		src:     &scriptedSource{results: []scriptedResult{{snap: snap}, {snap: snap}}},
		w:       watched,
		opts:    MetricsOptions{},
		present: func() presentation { return presentationFor(watched, 0) },
		mode:    watchModeFor(watched, false),
		clock: watchClock{interval: time.Second, now: time.Now,
			newTicker: func(time.Duration) ticker { return &fakeTicker{ch: make(chan time.Time)} }},
	}
	require.Equal(t, watchRich, v.mode, "a console repaints in place")

	require.NoError(t, v.paint(snap))
	assert.Equal(t, oneShot.String(), watched.String(),
		"the first watched frame is byte-identical to the one-shot render")

	require.NoError(t, v.paint(snap))
	out := watched.String()
	assert.Equal(t, 1, len(repaintRe.FindAllString(out, -1)),
		"only the repaint separates the frames")
	assert.Equal(t, oneShot.String()+repaintRe.FindString(out)+oneShot.String(), out,
		"the second frame is the same bytes, preceded by the erase")
}

// only the cursor control that repaints it.
func TestWatchFrameMatchesOneShot(t *testing.T) {
	snap := testFrame(1)
	for _, tt := range []struct {
		name string
		opts MetricsOptions
		mode watchMode
	}{
		{"summary", MetricsOptions{}, watchPlain},
		{"detail", MetricsOptions{Detail: true}, watchPlain},
		{"queue", MetricsOptions{Queues: []string{"events"}}, watchPlain},
	} {
		t.Run(tt.name, func(t *testing.T) {
			var oneShot syncBuffer
			require.NoError(t, renderMetrics(&oneShot, snap, tt.opts))

			// The harness has already painted the first frame; no tick needed.
			h := newTestHarness(t, tt.mode, tt.opts, scriptedResult{snap: snap})
			assert.Equal(t, oneShot.String(), h.out.String(),
				"a watched frame must match the one-shot output exactly")
		})
	}
}

// TestWatchAppendsWithoutEscapesWhenPiped: a pipe gets plain frames and no
// cursor control, so a log file stays greppable.
func TestWatchAppendsWithoutEscapesWhenPiped(t *testing.T) {
	h := newTestHarness(t, watchPlain, MetricsOptions{},
		scriptedResult{snap: testFrame(1)}, scriptedResult{snap: testFrame(2)},
		scriptedResult{snap: testFrame(3)})

	h.step(t)
	h.step(t)
	out := h.out.String()
	assert.NotContains(t, out, "\x1b", "plain mode never emits escapes")
	for _, n := range []int{1, 2, 3} {
		assert.Contains(t, out, time.Date(2026, 9, 27, 12, 0, n, 0, time.UTC).Format("15:04:05Z"),
			"frame %d is appended whole", n)
	}
}

// TestWatchJSONIsNewlineDelimited: json mode emits NDJSON, no banner, no prose.
func TestWatchJSONIsNewlineDelimited(t *testing.T) {
	h := newTestHarness(t, watchJSON, MetricsOptions{JSON: true},
		scriptedResult{snap: testFrame(1)}, scriptedResult{snap: testFrame(2)})

	h.step(t)
	h.step(t)
	require.NoError(t, h.awaitExit(t))

	out := h.out.String()
	assert.NotContains(t, out, "\x1b", "json is plumbing, never decorated")
	assert.NotContains(t, out, "watched for", "no prose summary in json mode")

	lines := strings.Split(strings.TrimSpace(out), "\n")
	require.Len(t, lines, 3, "one object per tick, plus the first frame")
	for i, line := range lines {
		var snap mikyu.Metrics
		require.NoError(t, json.Unmarshal([]byte(line), &snap), "line %d is one whole snapshot", i)
	}
}

// TestWatchStaleFrameKeepsLastGood: a failed tick notes itself under the frame and recovers.
func TestWatchStaleFrameKeepsLastGood(t *testing.T) {
	boom := errors.New("dial failed: connection refused")
	h := newTestHarness(t, watchRich, MetricsOptions{},
		scriptedResult{snap: testFrame(1)},
		scriptedResult{err: boom},
		scriptedResult{snap: testFrame(3)})

	h.step(t)
	afterFirst := h.out.String()
	h.step(t)

	note := staleRe.FindString(h.out.String())
	assert.Contains(t, note, "2026-09-27T12:00:01Z", "the note names the last good snapshot")
	assert.Contains(t, note, "connection refused", "the note carries the cause")
	assert.Contains(t, h.out.String(), afterFirst, "the last good frame is still on screen")

	h.step(t)
	assert.NotEmpty(t, repaintRe.FindString(h.out.String()),
		"the recovered frame repaints, erasing the stale note on screen")
}

// awaitAbort waits for the loop to exit on its own, without cancelling it.
func (h *testHarness) awaitAbort(t *testing.T) error {
	t.Helper()
	select {
	case <-h.done:
		return h.err
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not abort")
		return nil
	}
}

// TestWatchAbortsAfterThreeFailures: a watch that cannot reach the node gives up with the error.
func TestWatchAbortsAfterThreeFailures(t *testing.T) {
	boom := errors.New("connection refused")
	h := newTestHarness(t, watchPlain, MetricsOptions{},
		scriptedResult{err: boom}, scriptedResult{err: boom}, scriptedResult{err: boom})

	// The eager first paint is failure 1 (noted); two more ticks reach the limit,
	// and the third aborts without writing.
	h.stepAbort(t)
	h.stepAbort(t)
	err := h.awaitAbort(t)
	require.Error(t, err)
	assert.ErrorIs(t, err, boom, "the unrecoverable cause is returned")
	assert.Equal(t, 2, strings.Count(h.out.String(), "last snapshot"),
		"the third failure aborts instead of noting")
}

// TestWatchCancellationExitsZero: Ctrl-C ends the watch successfully with a summary line.
func TestWatchCancellationExitsZero(t *testing.T) {
	h := newTestHarness(t, watchRich, MetricsOptions{},
		scriptedResult{snap: testFrame(1)}, scriptedResult{snap: testFrame(2)})

	h.step(t)
	h.step(t)
	require.NoError(t, h.awaitExit(t), "a cancelled watch exits 0 (I10)")

	out := h.out.String()
	assert.Contains(t, out, "watched for ", "the watch reports how long it ran")
	assert.Contains(t, out, "· 3 snapshots", "and how many frames it painted, counting the first")
	assert.Contains(t, out, "\x1b[0J\n", "rich mode restores the cursor before the summary")
}

// TestWatchIntervalEnv: the environment override parses, and a bad value is a usage error.
func TestWatchIntervalEnv(t *testing.T) {
	t.Setenv(watchEnv, "")
	d, err := watchInterval()
	require.NoError(t, err)
	assert.Equal(t, defaultWatchInterval, d, "unset falls back to the default")

	t.Setenv(watchEnv, "500ms")
	d, err = watchInterval()
	require.NoError(t, err)
	assert.Equal(t, 500*time.Millisecond, d)

	t.Setenv(watchEnv, "nope")
	_, err = watchInterval()
	require.Error(t, err)
	assert.ErrorIs(t, err, daemon.ErrUsage, "a malformed interval exits 2")

	t.Setenv(watchEnv, "0s")
	_, err = watchInterval()
	require.ErrorIs(t, err, daemon.ErrUsage, "a non-positive interval exits 2")
}

// TestWatchModeFor classifies the destination once, so the loop has no per-tick environment reads.
func TestWatchModeFor(t *testing.T) {
	assert.Equal(t, watchJSON, watchModeFor(&bytes.Buffer{}, true), "json wins over the writer")
	assert.Equal(t, watchPlain, watchModeFor(&bytes.Buffer{}, false), "a pipe appends")
	f, err := createTempFile(t)
	require.NoError(t, err)
	assert.Equal(t, watchPlain, watchModeFor(f, false), "a file appends")
}

// TestWatchPropagatesWriteErrors keeps a broken pipe from looking like a healthy watch.
func TestWatchPropagatesWriteErrors(t *testing.T) {
	w := brokenWriter{}
	v := &watcher{
		src:  &scriptedSource{results: []scriptedResult{{snap: testFrame(1)}, {snap: testFrame(1)}}},
		w:    w,
		opts: MetricsOptions{},
		present: func() presentation {
			return presentationFor(w, 0)
		},
		mode: watchPlain,
		clock: watchClock{interval: time.Second, now: time.Now,
			newTicker: func(time.Duration) ticker { return &fakeTicker{ch: make(chan time.Time)} }},
	}
	err := v.run(t.Context())
	require.Error(t, err)
	assert.ErrorIs(t, err, errWatchClosed)
}

var (
	// repaintRe matches the erase sequence emitted before a repainted frame.
	repaintRe = regexp.MustCompile(`\x1b\[(\d+)A\x1b\[0J`)
	// staleRe matches the note left under a frame whose tick failed.
	staleRe = regexp.MustCompile(`last snapshot [^\n]+`)
	// escRe matches any ANSI escape sequence.
	escRe = regexp.MustCompile("\x1b\\[[0-9;]*[A-Za-z]")
)

// escapesIn counts the ANSI escape sequences in s.
func escapesIn(s string) int { return len(escRe.FindAllString(s, -1)) }

// forceColor pins the process to a 24-bit terminal so lipgloss keeps its escapes.
func forceColor(t *testing.T) {
	t.Helper()
	t.Setenv("TTY_FORCE", "1")
	t.Setenv("CLICOLOR_FORCE", "1")
	t.Setenv("COLORTERM", "truecolor")
	t.Setenv("TERM", "xterm-256color")
	t.Setenv("NO_COLOR", "")
}

// watcherEraseLines extracts the up-distance from one repaint sequence.
func watcherEraseLines(t *testing.T, seq string) int {
	t.Helper()
	m := repaintRe.FindStringSubmatch(seq)
	require.Len(t, m, 2, "sequence %q", seq)
	n := 0
	for _, c := range m[1] {
		n = n*10 + int(c-'0')
	}
	return n
}

// brokenWriter rejects every write, standing in for a closed pipe.
type brokenWriter struct{}

func (brokenWriter) Write([]byte) (int, error) { return 0, errWatchClosed }

// errWatchClosed is the write failure a broken pipe reports.
var errWatchClosed = errors.New("closed")

// createTempFile returns an empty file for writer-classification tests.
func createTempFile(t *testing.T) (io.Writer, error) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "watch")
	t.Cleanup(func() { _ = f.Close() })
	return f, err
}
