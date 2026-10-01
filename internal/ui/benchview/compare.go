package benchview

import (
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// compareCapColumns is where a diff starts warning about width.
const compareCapColumns = 6

// Compare writes a side-by-side configuration and metrics diff across runs of one mode.
func Compare(w io.Writer, dirs []string, theme style.Theme, fm human.Formatter) error {
	if len(dirs) == 0 {
		return fmt.Errorf("%w: compare needs at least one run", result.ErrCompareMismatch)
	}
	results := make([]result.Result, len(dirs))
	for i, dir := range dirs {
		r, err := result.ReadResult(dir)
		if err != nil {
			return err
		}
		results[i] = r
	}
	if err := sameMode(results); err != nil {
		return err
	}

	rows := append(configRows(results, fm), metricsRows(results, fm)...)
	parts := []string{
		compareHeader(theme, results),
		compareTable(theme, results, rows),
		renderNotes(theme, compareNotes(results, rows)),
	}
	_, err := lipgloss.Fprintln(w, trimTrailing(strings.Join(nonEmpty(parts), "\n")))
	return err
}

// compareRow is one line of the diff across the compared runs.
type compareRow struct {
	label   string
	values  []string
	differs bool
}

// sameMode rejects runs that do not share a mode.
func sameMode(results []result.Result) error {
	mode := results[0].Mode
	for _, r := range results[1:] {
		if r.Mode != mode {
			return result.ErrCompareMismatch
		}
	}
	return nil
}

// compareHeader names the mode and the run count.
func compareHeader(t style.Theme, results []result.Result) string {
	return t.Paint(t.Accent).Render(fmt.Sprintf("bench compare · %d runs · mode %s", len(results), results[0].Mode))
}

// compareTable lays the rows out one column per run, marking rows that differ.
func compareTable(t style.Theme, results []result.Result, rows []compareRow) string {
	headers := make([]string, len(results)+1)
	headers[0] = "FIELD"
	for i, r := range results {
		headers[i+1] = r.RunID
	}
	tableRows := make([][]string, 0, len(rows))
	for _, row := range rows {
		paint, marker := t.Paint(t.Label), "  "
		if row.differs {
			paint, marker = t.Paint(t.Text), "* "
		}
		cells := make([]string, len(row.values)+1)
		cells[0] = paint.Render(marker + row.label)
		for i, v := range row.values {
			cells[i+1] = paint.Render(v)
		}
		tableRows = append(tableRows, cells)
	}
	return t.DataTable(headers, tableRows, nil)
}

// configRows diffs the queue and workload configuration across runs.
func configRows(results []result.Result, fm human.Formatter) []compareRow {
	dimensions := []struct {
		label string
		value func(result.Result) string
	}{
		{"access", func(r result.Result) string { return r.Access }},
		{"kind", func(r result.Result) string { return r.Workload.Kind }},
		{"durability", func(r result.Result) string { return string(*effective(r).Storage.Durability) }},
		{"max segment", func(r result.Result) string { return fm.Bytes(*effective(r).Storage.MaxSegment) }},
		{"max cache", func(r result.Result) string { return fm.Bytes(*effective(r).Storage.MaxCache) }},
		{"msg size", func(r result.Result) string { return fm.Bytes(int64(r.Workload.MsgSize)) }},
		{"workers", func(r result.Result) string { return fm.Count(int64(r.Workload.Workers)) }},
		{"batch", func(r result.Result) string { return strconv.Itoa(r.Workload.Batch) }},
		{"warmup", func(r result.Result) string { return r.Workload.Warmup }},
	}
	rows := make([]compareRow, 0, len(dimensions))
	for _, d := range dimensions {
		values := valuesOf(results, d.value)
		rows = append(rows, compareRow{label: d.label, values: values, differs: differs(values)})
	}
	return rows
}

// metricsRows diffs the measured outcome across runs.
func metricsRows(results []result.Result, fm human.Formatter) []compareRow {
	dimensions := []struct {
		label string
		value func(result.Result) string
	}{
		{"throughput", metricString(func(m *result.MetricsView) string { return fm.Count(int64(m.ThroughputMsgs)) + " msg/s" })},
		{"bytes/s", metricString(func(m *result.MetricsView) string { return fm.Rate(m.ThroughputBytes) })},
		{"p50", metricString(func(m *result.MetricsView) string { return fm.Duration(time.Duration(m.LatencyUncorrected.P50)) })},
		{"p90", metricString(func(m *result.MetricsView) string { return fm.Duration(time.Duration(m.LatencyUncorrected.P90)) })},
		{"p99", metricString(func(m *result.MetricsView) string { return fm.Duration(time.Duration(m.LatencyUncorrected.P99)) })},
		{"max", metricString(func(m *result.MetricsView) string { return fm.Duration(time.Duration(m.LatencyUncorrected.Max)) })},
		{"messages", metricString(func(m *result.MetricsView) string { return fm.Count(m.Messages) })},
		{"bound", metricString(func(m *result.MetricsView) string { return m.BoundFired })},
		{"verified", metricString(func(m *result.MetricsView) string { return yesNo(m.Verified) })},
	}
	rows := make([]compareRow, 0, len(dimensions))
	for _, d := range dimensions {
		values := valuesOf(results, d.value)
		rows = append(rows, compareRow{label: d.label, values: values, differs: differs(values)})
	}
	return rows
}

// metricString reads one metric from a run, or a dash when the run has no metrics.
func metricString(fn func(*result.MetricsView) string) func(result.Result) string {
	return func(r result.Result) string {
		if r.Metrics == nil {
			return "-"
		}
		return fn(r.Metrics)
	}
}

// effective resolves a run's sparse spec over the built-ins.
func effective(r result.Result) spec.Spec {
	return r.Spec.Effective(spec.Spec{}, spec.Builtin())
}

// valuesOf collects one dimension from every run.
func valuesOf(results []result.Result, value func(result.Result) string) []string {
	values := make([]string, len(results))
	for i, r := range results {
		values[i] = value(r)
	}
	return values
}

// differs reports whether any value deviates from the first.
func differs(values []string) bool {
	for _, v := range values[1:] {
		if v != values[0] {
			return true
		}
	}
	return false
}

// compareNotes explains the markers and groups runs by access.
func compareNotes(results []result.Result, rows []compareRow) []string {
	var notes []string
	for _, row := range rows {
		if row.differs {
			notes = append(notes, "* "+row.label+" differs")
		}
	}
	if access := accessSummary(results); access != "" {
		notes = append(notes, access)
	}
	if len(results) > compareCapColumns {
		notes = append(notes, fmt.Sprintf("note: %d columns may overflow your terminal", len(results)))
	}
	return notes
}

// accessSummary groups runs by how they reached the server.
func accessSummary(results []result.Result) string {
	groups := map[string][]string{}
	var order []string
	for _, r := range results {
		if _, ok := groups[r.Access]; !ok {
			order = append(order, r.Access)
		}
		groups[r.Access] = append(groups[r.Access], r.RunID)
	}
	if len(order) <= 1 {
		return ""
	}
	parts := make([]string, 0, len(order))
	for _, access := range order {
		verb := "use"
		if len(groups[access]) == 1 {
			verb = "uses"
		}
		parts = append(parts, strings.Join(groups[access], ", ")+" "+verb+" "+access)
	}
	return strings.Join(parts, "; ")
}

// renderNotes writes the diff notes dimly.
func renderNotes(t style.Theme, notes []string) string {
	if len(notes) == 0 {
		return ""
	}
	return t.Paint(t.Label).Render(strings.Join(notes, "\n"))
}
