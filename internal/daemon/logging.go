package daemon

import (
	"context"
	"log/slog"
	"os"

	charmlog "charm.land/log/v2"
	"github.com/dmunish/mikyu/internal/logtap"
	"github.com/dmunish/mikyu/internal/spec"
)

// setupLogging makes charm.land/log the presentation for every slog call in
// the process: the daemon's lifecycle lines and the warnings internal
// packages already emit. Two sinks share one rendering decision — the
// daemon's own terminal, styled only when stderr is a TTY, and the node's
// log tap, always plain so wire followers receive clean lines. Everything
// else stays the library default: stderr, timestamped, text, no caller.
func setupLogging(sess spec.Session, tap *logtap.Tap) {
	level := charmlog.InfoLevel
	if l, err := charmlog.ParseLevel(sess.LogLevel); err == nil && sess.LogLevel != "" {
		level = l
	}

	terminal := charmlog.NewWithOptions(os.Stderr, charmlog.Options{ReportTimestamp: true})
	terminal.SetLevel(level)
	sinks := []slog.Handler{terminal}

	if tap != nil {
		ring := charmlog.NewWithOptions(writerFunc(tap.Write), charmlog.Options{ReportTimestamp: true})
		ring.SetLevel(level)
		sinks = append(sinks, ring)
	}
	slog.SetDefault(slog.New(&fanoutHandler{sinks: sinks}))
}

// writerFunc adapts a write function to io.Writer.
type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }

// fanoutHandler delivers one record to every sink; the sinks are charm
// loggers, so each renders independently — styled for the terminal, plain
// for the ring.
type fanoutHandler struct {
	sinks []slog.Handler
}

func (f *fanoutHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return f.sinks[0].Enabled(ctx, level)
}

func (f *fanoutHandler) Handle(ctx context.Context, rec slog.Record) error {
	for _, s := range f.sinks {
		_ = s.Handle(ctx, rec) // one failing sink must not silence the others
	}
	return nil
}

func (f *fanoutHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	sinks := make([]slog.Handler, len(f.sinks))
	for i, s := range f.sinks {
		sinks[i] = s.WithAttrs(attrs)
	}
	return &fanoutHandler{sinks: sinks}
}

func (f *fanoutHandler) WithGroup(name string) slog.Handler {
	sinks := make([]slog.Handler, len(f.sinks))
	for i, s := range f.sinks {
		sinks[i] = s.WithGroup(name)
	}
	return &fanoutHandler{sinks: sinks}
}
