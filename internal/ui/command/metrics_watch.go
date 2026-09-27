package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"syscall"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
)

// watchEnv overrides the watch period; there is no flag for it by design.
const watchEnv = "MIKYU_WATCH_INTERVAL"

// defaultWatchInterval is the period between frames when the environment does not override it.
const defaultWatchInterval = 2 * time.Second

// maxWatchFailures ends a watch that has lost the node for this many ticks in a row.
const maxWatchFailures = 3

// metricsSource is the slice of mikyu.Client a watch needs, so tests can script snapshots.
type metricsSource interface {
	Metrics(ctx context.Context) (mikyu.Metrics, error)
}

// watchMode is how a watch writes frames: one JSON object per line, a repainted region, or a bannered append.
type watchMode int

const (
	watchJSON watchMode = iota
	watchRich
	watchPlain
)

// watchModeFor classifies the destination and output format once per watch.
func watchModeFor(w io.Writer, jsonOutput bool) watchMode {
	switch {
	case jsonOutput:
		return watchJSON
	case isTerminalWriter(w):
		return watchRich
	default:
		return watchPlain
	}
}

// ticker is the part of time.Ticker a watch loop needs, so tests can drive frames.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

// timeTicker adapts *time.Ticker to the ticker interface.
type timeTicker struct{ *time.Ticker }

func (t timeTicker) C() <-chan time.Time { return t.Ticker.C }

// watchClock carries the timing seams of a watch loop.
type watchClock struct {
	interval  time.Duration
	now       func() time.Time
	newTicker func(time.Duration) ticker
}

// watchMetrics streams snapshots until cancelled, repainting in place on a terminal.
func watchMetrics(ctx context.Context, src metricsSource, w io.Writer, opts MetricsOptions) error {
	interval, err := watchInterval()
	if err != nil {
		return err
	}
	return (&watcher{
		src:  src,
		w:    w,
		opts: opts,
		pres: presentationFor(w, opts.Width),
		mode: watchModeFor(w, opts.JSON),
		clock: watchClock{
			interval:  interval,
			now:       time.Now,
			newTicker: func(d time.Duration) ticker { return timeTicker{time.NewTicker(d)} },
		},
	}).run(ctx)
}

// watchInterval resolves the period from the environment, rejecting a malformed or non-positive value.
func watchInterval() (time.Duration, error) {
	raw := os.Getenv(watchEnv)
	if raw == "" {
		return defaultWatchInterval, nil
	}
	d, err := time.ParseDuration(raw)
	if err != nil {
		return 0, fmt.Errorf("%w: %s=%q is not a duration: %v", daemon.ErrUsage, watchEnv, raw, err)
	}
	if d <= 0 {
		return 0, fmt.Errorf("%w: %s must be positive, got %q", daemon.ErrUsage, watchEnv, raw)
	}
	return d, nil
}

// watcher runs one metrics --watch session.
type watcher struct {
	src   metricsSource
	w     io.Writer
	opts  MetricsOptions
	pres  presentation
	mode  watchMode
	clock watchClock

	frames   int
	failures int
	last     time.Time
	region   int // lines the next repaint must erase
}

// run paints a frame per tick until the context is cancelled, the node is lost, or a write fails.
func (v *watcher) run(ctx context.Context) error {
	ctx, stop := signal.NotifyContext(ctx, os.Interrupt, syscall.SIGTERM)
	defer stop()

	timeout := max(v.clock.interval/2, 100*time.Millisecond)
	tick := v.clock.newTicker(v.clock.interval)
	defer tick.Stop()
	start := v.clock.now()

	for {
		fetch, cancel := context.WithTimeout(ctx, timeout)
		snap, err := v.src.Metrics(fetch)
		cancel()
		switch {
		case err == nil:
			if err := v.paint(snap); err != nil {
				return err
			}
			v.frames, v.failures, v.last = v.frames+1, 0, snap.Timestamp
		case ctx.Err() != nil:
			return v.finish(start)
		default:
			v.failures++
			if v.failures >= maxWatchFailures {
				return err
			}
			if err := v.stale(err); err != nil {
				return err
			}
		}

		select {
		case <-ctx.Done():
			return v.finish(start)
		case <-tick.C():
		}
	}
}

// paint renders one snapshot straight to the destination, counting the lines it wrote for the next repaint.
func (v *watcher) paint(snap mikyu.Metrics) error {
	// A watched json stream is newline-delimited, so each object must be one line.
	indent := "  "
	if v.mode == watchJSON {
		indent = ""
	}
	if v.mode == watchRich && v.region > 0 {
		// Cursor movement is terminal control, not styled output, so it bypasses lipgloss.
		if _, err := fmt.Fprintf(v.w, "\x1b[%dA\x1b[0J", v.region); err != nil {
			return err
		}
	}
	// The frame is written directly, so it keeps the destination's color and scale
	// exactly as the one-shot path renders it. The wrapper only tallies newlines:
	// the repaint distance is the newline count, not lipgloss.Height, which counts
	// the trailing newline this frame does not print.
	tally := &lineCounter{w: v.w}
	if err := renderFrame(tally, snap, v.opts, v.pres, indent); err != nil {
		return err
	}
	v.region = tally.lines
	return nil
}

// lineCounter forwards to a writer while counting the newlines written.
type lineCounter struct {
	w     io.Writer
	lines int
}

// Write forwards p and tallies its newlines.
func (c *lineCounter) Write(p []byte) (int, error) {
	c.lines += bytes.Count(p, []byte("\n"))
	return c.w.Write(p)
}

// stale notes a failed tick under the last good frame without disturbing it.
func (v *watcher) stale(err error) error {
	line := fmt.Sprintf("last snapshot %s · %v", v.last.UTC().Format(time.RFC3339), err)
	dim := v.pres.theme.Paint(v.pres.theme.Label)
	if _, err := lipgloss.Fprintln(v.w, dim.Render(line)); err != nil {
		return err
	}
	v.region++
	return nil
}

// finish closes the watch: a cancelled watch reports success (invariant I10).
func (v *watcher) finish(start time.Time) error {
	if v.mode == watchJSON {
		return nil
	}
	if v.mode == watchRich {
		if _, err := fmt.Fprint(v.w, "\x1b[0J\n"); err != nil {
			return err
		}
	}
	summary := fmt.Sprintf("watched for %s · %d snapshots",
		v.pres.formatter.Duration(v.clock.now().Sub(start)), v.frames)
	dim := v.pres.theme.Paint(v.pres.theme.Label)
	_, err := lipgloss.Fprintln(v.w, dim.Render(summary))
	return err
}
