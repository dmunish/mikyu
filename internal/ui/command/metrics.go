package command

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"

	"golang.org/x/term"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/metricsview"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// MetricsOptions carries the resolved flags for one metrics invocation.
type MetricsOptions struct {
	Detail bool
	Queues []string
	Watch  bool
	JSON   bool
	Width  int
}

// MetricsFlagSet binds the metrics flags to opts: --detail, --queue, --watch, --output.
func MetricsFlagSet(opts *MetricsOptions) *flag.FlagSet {
	fs := NewFlagSet("metrics")
	fs.BoolVar(&opts.Detail, "detail", false, "render the node grid and one card per queue")
	fs.Func("queue", "focus the text view on a queue; repeatable", func(name string) error {
		if name == "" {
			return errors.New("queue name must not be empty")
		}
		opts.Queues = append(opts.Queues, name)
		return nil
	})
	fs.BoolVar(&opts.Watch, "watch", false, "refresh the view until interrupted")
	fs.Func("output", "json emits the snapshot verbatim; text renders it for people", func(value string) error {
		switch value {
		case "text":
			opts.JSON = false
		case "json":
			opts.JSON = true
		default:
			return fmt.Errorf("unknown output %q: want text or json", value)
		}
		return nil
	})
	return fs
}

// defaultMetricsWidth is the fallback render width when no terminal is present.
const defaultMetricsWidth = 80

// RunMetrics writes a telemetry snapshot to w: a colored view on a terminal, raw text in a pipe, json on request.
func RunMetrics(ctx context.Context, c mikyu.Client, w io.Writer, opts MetricsOptions) error {
	if opts.Watch {
		return watchMetrics(ctx, c, w, opts)
	}
	snap, err := c.Metrics(ctx)
	if err != nil {
		return err
	}
	return renderMetrics(w, snap, opts)
}

// presentation is the resolved output style for one destination writer.
type presentation struct {
	theme     style.Theme
	formatter human.Formatter
	width     int
}

// renderMetrics writes one snapshot in the selected output mode.
func renderMetrics(w io.Writer, snap mikyu.Metrics, opts MetricsOptions) error {
	return renderFrame(w, snap, opts, presentationFor(w, opts.Width), "  ")
}

// renderFrame writes one snapshot with an explicit presentation and json indent, so a watch and a one-shot share the exact same bytes.
func renderFrame(w io.Writer, snap mikyu.Metrics, opts MetricsOptions, p presentation, indent string) error {
	if opts.JSON {
		if err := requireQueues(snap, opts.Queues); err != nil {
			return err
		}
		enc := json.NewEncoder(w)
		enc.SetIndent("", indent)
		return enc.Encode(snap)
	}
	if opts.Detail || len(opts.Queues) > 0 {
		return metricsview.Detail(w, snap, opts.Queues, p.theme, p.formatter, p.width)
	}
	return metricsview.Summary(w, snap, p.theme, p.formatter, p.width)
}

// presentationFor picks the theme, formatter, and width for the destination writer.
func presentationFor(w io.Writer, width int) presentation {
	theme, formatter, resolved := metricsPresentation(w, width)
	return presentation{theme: theme, formatter: formatter, width: resolved}
}

// requireQueues fails on a --queue name no queue matches, so a typo exits 1 even in json mode.
func requireQueues(snap mikyu.Metrics, names []string) error {
	for _, name := range names {
		if _, ok := snap.Queue(name); !ok {
			return fmt.Errorf("%w: %s", metricsview.ErrUnknownQueue, name)
		}
	}
	return nil
}

// metricsPresentation picks the theme, formatter, and width for the destination writer.
func metricsPresentation(w io.Writer, width int) (style.Theme, human.Formatter, int) {
	if isTerminalWriter(w) {
		if width <= 0 {
			width = resolveWidth(0)
		}
		return style.NewTheme(true), human.Formatter{}, width
	}
	if width <= 0 {
		width = defaultMetricsWidth
	}
	return style.PlainTheme(), human.Formatter{Raw: true}, width
}

// isTerminalWriter reports whether w is a terminal that can draw box-drawing and color.
func isTerminalWriter(w io.Writer) bool {
	f, ok := w.(*os.File)
	return ok && term.IsTerminal(int(f.Fd())) && os.Getenv("TERM") != "dumb"
}
