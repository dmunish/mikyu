package command

import (
	"bytes"
	"io"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/require"
)

// screen models a terminal's cursor, wrapping, and ED 0, so a repaint can be
// replayed and its residue inspected the way a user sees it.
type screen struct {
	rows  [][]rune
	row   int
	col   int
	width int // 0 means unbounded
}

func newScreen(rows, width int) *screen {
	s := &screen{rows: make([][]rune, rows), width: width}
	for i := range s.rows {
		s.rows[i] = []rune{}
	}
	return s
}

// write replays a byte stream, honouring the cursor-up and erase-below the watch emits.
func (s *screen) write(b []byte) {
	for str := string(b); str != ""; {
		if seq, rest, ok := cutCSI(str); ok {
			s.applyCSI(seq)
			str = rest
			continue
		}
		switch r := []rune(str)[0]; r {
		case '\n':
			s.row++
			s.col = 0
			s.clamp()
		case '\r':
			s.col = 0
		default:
			s.put(r)
		}
		str = string([]rune(str)[1:])
	}
}

// clamp scrolls the screen when the cursor has moved past the last row.
func (s *screen) clamp() {
	for s.row >= len(s.rows) {
		s.rows = s.rows[1:]
		s.rows = append(s.rows, []rune{})
		s.row = len(s.rows) - 1
	}
}

// cutCSI splits a leading escape sequence off the stream.
func cutCSI(s string) (seq, rest string, ok bool) {
	if !strings.HasPrefix(s, "\x1b[") {
		return "", s, false
	}
	for i, r := range s[2:] {
		if r >= '@' && r <= '~' {
			return s[:2+i+1], s[2+i+1:], true
		}
	}
	return "", s, false
}

// applyCSI moves the cursor up and erases below, the two controls a repaint uses.
func (s *screen) applyCSI(seq string) {
	body := seq[2 : len(seq)-1]
	verb := seq[len(seq)-1]
	if body == "" {
		body = "1"
	}
	n, err := strconv.Atoi(body)
	if err != nil {
		return
	}
	switch verb {
	case 'A':
		s.row = max(s.row-n, 0)
	case 'H':
		s.row, s.col = 0, 0
	case 'J':
		// ED 2 erases the whole screen and homes the cursor; ED 0 erases from
		// the cursor down. A parameter of 0 is written bare.
		if body == "2" {
			for i := range s.rows {
				s.rows[i] = []rune{}
			}
			s.row, s.col = 0, 0
			return
		}
		s.eraseBelow()
	}
}

func (s *screen) put(r rune) {
	// A real terminal wraps at its right edge, adding a visual row that the
	// frame's own newline count knows nothing about.
	if s.width > 0 && s.col >= s.width {
		s.row++
		s.col = 0
	}
	// A real terminal scrolls: writing past the last row pushes the screen up.
	for s.row >= len(s.rows) {
		s.rows = s.rows[1:]
		s.rows = append(s.rows, []rune{})
		s.row = len(s.rows) - 1
	}
	for len(s.rows[s.row]) <= s.col {
		s.rows[s.row] = append(s.rows[s.row], ' ')
	}
	s.rows[s.row][s.col] = r
	s.col++
}

// eraseBelow implements ED 0: clear from the cursor to the end of the screen.
func (s *screen) eraseBelow() {
	if s.row >= len(s.rows) {
		return
	}
	s.rows[s.row] = s.rows[s.row][:min(s.col, len(s.rows[s.row]))]
	for i := s.row + 1; i < len(s.rows); i++ {
		s.rows[i] = []rune{}
	}
}

// text renders the screen, dropping trailing blank rows.
func (s *screen) text() string {
	out := make([]string, 0, len(s.rows))
	for _, r := range s.rows {
		out = append(out, strings.TrimRight(string(r), " "))
	}
	for len(out) > 0 && out[len(out)-1] == "" {
		out = out[:len(out)-1]
	}
	return strings.Join(out, "\n")
}

// filledRows counts the non-blank rows on the screen.
func (s *screen) filledRows() int {
	n := 0
	for l := range strings.SplitSeq(s.text(), "\n") {
		if strings.TrimSpace(l) != "" {
			n++
		}
	}
	return n
}

// mirrorWriter sends every byte to a real console and to the screen model, so a
// test can drive the production writer while watching what a terminal would show.
type mirrorWriter struct {
	to  *consoleWriter
	scr *screen
}

func (m *mirrorWriter) Write(p []byte) (int, error) {
	m.scr.write(p)
	return m.to.Write(p)
}

func (m *mirrorWriter) Fd() uintptr { return m.to.Fd() }

// queues builds a snapshot with n log queues, as a live node would report.
func queues(n int, msgs uint64) metrics.Snapshot {
	s := metrics.Snapshot{
		Timestamp: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC),
		Runtime:   metrics.RuntimeView{UptimeSec: 3, Goroutines: 42},
		Storage:   metrics.StorageView{DataDir: "C:/data/mikyu", Segments: 3, SizeBytes: 4096},
		Totals:    metrics.TotalsView{Queues: n, Messages: msgs, Bytes: 4096},
	}
	for i := range n {
		s.Queues = append(s.Queues, metrics.QueueView{
			Name:     "queue-" + string(rune('a'+i)),
			Kind:     "log",
			Counters: map[string]int64{"appends": 1, "reads": 1},
			Gauges:   map[string]int64{"messages": int64(msgs)},
		})
	}
	return s
}

// newScreenWatcher wires a watcher to a real console and a screen model of the
// given width, so the production write path is what the assertions observe.
func newScreenWatcher(t *testing.T, opts MetricsOptions, winWidth int) (*watcher, *screen) {
	return screenWatcher(t, opts, winWidth, 400)
}

// screenWatcher is newScreenWatcher with an explicit model size, so a frame
// taller than the screen scrolls the way a real terminal scrolls.
func screenWatcher(t *testing.T, opts MetricsOptions, winWidth, rows int) (*watcher, *screen) {
	t.Helper()
	con := newConsoleWriter(t)
	scr := newScreen(rows, winWidth)
	w := &mirrorWriter{to: con, scr: scr}
	v := &watcher{
		w:       w,
		opts:    opts,
		present: func() presentation { return presentationFor(w, opts.Width) },
		viewport: func() (int, int) {
			return winWidth, rows
		},
		mode: watchRich,
	}
	return v, scr
}

// TestWatchRepaintCoversAFrameWiderThanTheWindow is the flicker regression test.
// A frame rendered wider than the window wraps, so it occupies more screen rows
// than it has newlines. Erasing the newline count moves the cursor up less than
// the frame is tall, so each tick repaints over only part of the previous frame
// and the frames pile up: the view flickers. The cursor must stay put instead.
func TestWatchRepaintCoversAFrameWiderThanTheWindow(t *testing.T) {
	v, scr := newScreenWatcher(t, MetricsOptions{Width: 200}, 20)
	var rows []int
	for i := range 5 {
		require.NoError(t, v.paint(queues(1, uint64(100*(i+1)))))
		rows = append(rows, scr.filledRows())
	}
	t.Logf("rows per tick: %v", rows)
	require.Equal(t, rows[0], rows[len(rows)-1],
		"the screen must hold exactly one frame, however many ticks have passed")
	for i := 1; i < len(rows); i++ {
		require.LessOrEqual(t, rows[i], rows[0],
			"tick %d left rows from an earlier frame behind (rows %v)", i, rows)
	}
}

// TestWatchRepaintFollowsAChangingFrameHeight: a queue appearing or disappearing
// changes the frame's height. Each tick must repaint over everything the
// previous frame put on screen, so the rows that survive are the current frame's
// and nothing older.
func TestWatchRepaintFollowsAChangingFrameHeight(t *testing.T) {
	v, scr := newScreenWatcher(t, MetricsOptions{Width: 100}, 0)
	for i, n := range []int{1, 2, 1, 3, 1, 2, 1, 4, 1} {
		require.NoError(t, v.paint(queues(n, uint64(100*(i+1)))))
		rows := scr.filledRows()
		t.Logf("tick %d queues=%d region=%d rows=%d", i, n, v.region, rows)
		require.Equal(t, v.region, rows,
			"tick %d: the screen must hold exactly the current frame", i)
	}
}

// TestWatchRedrawsAFrameTallerThanTheWindow: a --detail frame is far taller than
// the summary, and taller than a normal terminal. Repainting it in place cannot
// work: writing it scrolls the screen, so a cursor-up by its own height lands
// clamped at the top and the head of the previous frame survives, which reads as
// a fresh set printed every tick. A frame that does not fit is redrawn whole.
func TestWatchRedrawsAFrameTallerThanTheWindow(t *testing.T) {
	v, scr := screenWatcher(t, MetricsOptions{Width: 80, Detail: true}, 80, 24)
	height, err := v.measure(queues(1, 100), v.present())
	require.NoError(t, err)
	require.Greater(t, height, 24, "this test needs a frame taller than the window")
	for i := range 4 {
		require.NoError(t, v.paint(queues(1, uint64(100*(i+1)))))
	}
	t.Logf("frame=%d rows, window=24, filled=%d cursorRow=%d", height, scr.filledRows(), scr.row)
	// The frame is redrawn whole, so the window ends holding only the current
	// frame: the cursor sits at the bottom, not part-way up where a cursor-up
	// sized for a 78-row frame would have clamped.
	require.Equal(t, 23, scr.row,
		"a frame taller than the window must be redrawn to the bottom of the window")
}

// TestWatchClearsTheScreenOnlyForATallFrame pins the boundary: a summary frame is
// repainted in place, while a --detail frame on a short terminal is redrawn.
func TestWatchClearsTheScreenOnlyForATallFrame(t *testing.T) {
	clear := func(t *testing.T, opts MetricsOptions, rows int) string {
		t.Helper()
		con := newConsoleWriter(t)
		var log bytes.Buffer
		w := &tallyWriter{sink: con, log: &log}
		v := &watcher{
			w:        w,
			opts:     opts,
			present:  func() presentation { return presentationFor(w, 80) },
			viewport: func() (int, int) { return 80, rows },
			mode:     watchRich,
		}
		require.NoError(t, v.paint(queues(1, 100)))
		require.NoError(t, v.paint(queues(1, 200)))
		return log.String()
	}
	require.Contains(t, clear(t, MetricsOptions{Width: 80, Detail: true}, 24), "\x1b[2J",
		"a frame taller than the window must be redrawn")
	require.NotContains(t, clear(t, MetricsOptions{Width: 80}, 60), "\x1b[2J",
		"a frame that fits must repaint in place and never clear the screen")
}

// tallyWriter mirrors writes into a log so escape sequences can be asserted.
type tallyWriter struct {
	sink io.Writer
	log  *bytes.Buffer
}

func (t *tallyWriter) Write(p []byte) (int, error) {
	t.log.Write(p)
	return t.sink.Write(p)
}

func (t *tallyWriter) Fd() uintptr {
	if f, ok := t.sink.(fdWriter); ok {
		return f.Fd()
	}
	return 0
}
