package command

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
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
	theme, _, _ := metricsPresentation(w, opts.Width)
	return (&watcher{
		src:   src,
		w:     w,
		opts:  opts,
		theme: theme,
		mode:  watchModeFor(w, opts.JSON),
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
	theme style.Theme
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

// paint renders one snapshot into a buffer, then emits it the way the mode dictates.
func (v *watcher) paint(snap mikyu.Metrics) error {
	var buf bytes.Buffer
	// A watched json stream is newline-delimited, so each object must be one line.
	indent := "  "
	if v.mode == watchJSON {
		indent = ""
	}
	if err := renderMetricsIndent(&buf, snap, v.opts, indent); err != nil {
		return err
	}
	frame := buf.String()
	if v.mode != watchRich {
		return v.append(frame, snap)
	}
	// The repaint distance is the newline count, not lipgloss.Height: a frame
	// ends in a newline, and Height counts that trailing empty line, which would
	// erase the line below the frame too.
	if v.region > 0 {
		// Cursor movement is terminal control, not styled output, so it bypasses lipgloss.
		if _, err := fmt.Fprintf(v.w, "\x1b[%dA\x1b[0J", v.region); err != nil {
			return err
		}
	}
	if _, err := lipgloss.Fprint(v.w, frame); err != nil {
		return err
	}
	v.region = strings.Count(frame, "\n")
	return nil
}

// append writes a frame below the previous one, headed by a banner in text mode.
func (v *watcher) append(frame string, snap mikyu.Metrics) error {
	if v.mode == watchPlain {
		if _, err := lipgloss.Fprintln(v.w, v.banner(snap)); err != nil {
			return err
		}
	}
	_, err := lipgloss.Fprint(v.w, frame)
	return err
}

// banner heads each appended frame with its timestamp and shape, the vmstat discipline.
func (v *watcher) banner(snap mikyu.Metrics) string {
	return fmt.Sprintf("── %s · detail=%t · queues=%d ──",
		snap.Timestamp.UTC().Format(time.RFC3339), v.opts.Detail || len(v.opts.Queues) > 0, queueCount(snap))
}

// stale notes a failed tick under the last good frame without disturbing it.
func (v *watcher) stale(err error) error {
	line := fmt.Sprintf("last snapshot %s · %v", v.last.UTC().Format(time.RFC3339), err)
	if _, err := lipgloss.Fprintln(v.w, v.theme.Paint(v.theme.Label).Render(line)); err != nil {
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
		human.Formatter{}.Duration(v.clock.now().Sub(start)), v.frames)
	_, err := lipgloss.Fprintln(v.w, v.theme.Paint(v.theme.Label).Render(summary))
	return err
}

// queueCount counts the renderable queues, leaving out non-queue instrument scopes.
func queueCount(snap mikyu.Metrics) int {
	n := 0
	for q := range snap.ResolveQueues() {
		if !q.IsInternal() {
			n++
		}
	}
	return n
}
