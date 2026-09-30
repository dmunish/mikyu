package benchview

import (
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// ErrUnknownMetric reports a metric the series does not carry.
var ErrUnknownMetric = errors.New("unknown bench metric")

// chartWidth is the number of columns one bar spans.
const chartWidth = 60

// plotMetrics maps each supported metric to its reading in a series row.
var plotMetrics = map[string]func(result.SeriesRow) float64{
	"throughput":  func(row result.SeriesRow) float64 { return row.Client.Throughput },
	"p50":         func(row result.SeriesRow) float64 { return float64(row.Client.P50NS) },
	"p90":         func(row result.SeriesRow) float64 { return float64(row.Client.P90NS) },
	"p99":         func(row result.SeriesRow) float64 { return float64(row.Client.P99NS) },
	"queue_depth": func(row result.SeriesRow) float64 { return float64(row.System.QueueDepth) },
	"size_bytes":  func(row result.SeriesRow) float64 { return float64(row.System.SizeBytes) },
	"rss_bytes":   func(row result.SeriesRow) float64 { return float64(row.Host.RSSBytes) },
	"cpu_percent": func(row result.SeriesRow) float64 { return row.Host.CPUPercent },
}

// Plot writes one ASCII chart per requested metric.
func Plot(w io.Writer, dir string, metrics []string, theme style.Theme) error {
	rows, err := result.ReadSeries(dir)
	if err != nil {
		return err
	}
	if len(metrics) == 0 {
		metrics = []string{"throughput"}
	}
	for _, name := range metrics {
		read, ok := plotMetrics[name]
		if !ok {
			return fmt.Errorf("%w: %q; supported: %s", ErrUnknownMetric, name, strings.Join(plotMetricNames(), ", "))
		}
		fmt.Fprint(w, chart(theme, name, rows, read))
	}
	return nil
}

// plotMetricNames lists the supported metrics in a stable order.
func plotMetricNames() []string {
	return []string{"throughput", "p50", "p90", "p99", "queue_depth", "size_bytes", "rss_bytes", "cpu_percent"}
}

// chart renders one metric's per-second bars scaled to its peak.
func chart(t style.Theme, name string, rows []result.SeriesRow, read func(result.SeriesRow) float64) string {
	peak := 0.0
	for _, row := range rows {
		peak = max(peak, read(row))
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s  peak %s\n", t.Paint(t.Accent).Render(name), formatValue(peak))
	for _, row := range rows {
		filled := 0
		if peak > 0 {
			filled = int(read(row)/peak*chartWidth + 0.5)
		}
		fmt.Fprintf(&b, "%7.1fs |%s%s\n", row.T,
			strings.Repeat("=", filled), strings.Repeat(" ", chartWidth-filled))
	}
	return b.String()
}

// formatValue renders a chart reading without decoration.
func formatValue(v float64) string {
	return strconv.FormatFloat(v, 'f', -1, 64)
}
