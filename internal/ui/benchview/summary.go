// Package benchview renders benchmark envelopes for the terminal.
package benchview

import (
	"fmt"
	"io"
	"strings"
	"time"

	"charm.land/lipgloss/v2"
	"github.com/dmunish/mikyu/internal/bench/result"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/human"
	"github.com/dmunish/mikyu/internal/ui/style"
)

// summaryWidth is the card width the header lays out to.
const summaryWidth = 60

// Summary writes one run's header, configuration, and metrics blocks.
func Summary(w io.Writer, r result.Result, dir string, theme style.Theme, fm human.Formatter) error {
	parts := []string{
		header(theme, r),
		incomplete(theme, r),
		configBlock(theme, fm, r),
		metricsBlock(theme, fm, r),
		written(theme, dir),
	}
	_, err := lipgloss.Fprintln(w, trimTrailing(strings.Join(nonEmpty(parts), "\n")))
	return err
}

// header is the run's identity: its id, mode, and access.
func header(t style.Theme, r result.Result) string {
	right := strings.Join(nonEmpty([]string{r.Mode, r.Access}), " · ")
	return t.TitledBox(r.RunID, right, "", summaryWidth, t.Accent)
}

// incomplete warns that the run stopped before its bound.
func incomplete(t style.Theme, r result.Result) string {
	if r.Complete {
		return ""
	}
	reason := r.Aborted
	if reason == "" {
		reason = "the run was killed externally"
	}
	return t.Paint(t.Warn).Render("⚠ incomplete — " + reason)
}

// configBlock pairs the effective queue spec with the workload.
func configBlock(t style.Theme, fm human.Formatter, r result.Result) string {
	rows := [][2]string{
		{"spec", specLine(fm, r)},
		{"workload", workloadLine(fm, r.Workload)},
	}
	return kvBlock(t, 9, rows)
}

// metricsBlock renders throughput as a stat band above the latency and outcome rows.
func metricsBlock(t style.Theme, fm human.Formatter, r result.Result) string {
	if r.Metrics == nil {
		return ""
	}
	m := r.Metrics
	rows := [][2]string{
		{"latency", latencyLine(fm, m)},
		{"verified", yesNo(m.Verified)},
		{"bound", m.BoundFired},
		{"cache", r.Environment.CacheState},
	}
	if m.Recovery != nil {
		rows = append(rows, [2]string{"recovery", recoveryLine(fm, m.Recovery)})
	}
	return strings.Join(nonEmpty([]string{throughputBand(t, fm, m), kvBlock(t, 9, rows)}), "\n")
}

// throughputBand shows the message and byte rates side by side.
func throughputBand(t style.Theme, fm human.Formatter, m *result.MetricsView) string {
	return t.StatBand([]style.Stat{
		{Value: fm.Count(int64(m.ThroughputMsgs)), Label: "msgs/s", Color: t.Ok},
		{Value: fm.Rate(m.ThroughputBytes), Label: "bytes/s"},
	})
}

// specLine summarizes the effective queue spec in one line.
func specLine(fm human.Formatter, r result.Result) string {
	eff := r.Spec.Effective(spec.Spec{}, spec.Builtin())
	parts := []string{
		r.Workload.Kind,
		string(*eff.Storage.Durability),
		fm.Bytes(*eff.Storage.MaxSegment) + " segment",
		fm.Bytes(*eff.Storage.MaxCache) + " cache",
	}
	return strings.Join(nonEmpty(parts), " · ")
}

// workloadLine summarizes the workload in one line.
func workloadLine(fm human.Formatter, w result.WorkloadView) string {
	amount := "-"
	switch {
	case w.Bytes > 0:
		amount = fm.Bytes(w.Bytes)
	case w.Duration != "":
		amount = w.Duration
	case w.CrashAfter > 0:
		amount = fm.Bytes(w.CrashAfter) + " crash"
	}
	parts := []string{
		fmt.Sprintf("%s × %s", fm.Bytes(int64(w.MsgSize)), amount),
		fmt.Sprintf("%s workers", fm.Count(int64(w.Workers))),
		fmt.Sprintf("batch=%d", w.Batch),
	}
	if w.Keys > 0 {
		parts = append(parts, fmt.Sprintf("keys=%d", w.Keys))
	}
	if w.Preload > 0 {
		parts = append(parts, "preload "+fm.Bytes(w.Preload))
	}
	if w.Rate > 0 {
		parts = append(parts, fm.Count(int64(w.Rate))+" msg/s")
	}
	return strings.Join(parts, " · ")
}

// latencyLine renders the uncorrected latency percentiles.
func latencyLine(fm human.Formatter, m *result.MetricsView) string {
	h := m.LatencyUncorrected
	return fmt.Sprintf("p50 %s · p90 %s · p99 %s · max %s",
		fm.Duration(time.Duration(h.P50)), fm.Duration(time.Duration(h.P90)),
		fm.Duration(time.Duration(h.P99)), fm.Duration(time.Duration(h.Max)))
}

// recoveryLine renders the three crash-recovery milestones.
func recoveryLine(fm human.Formatter, rec *result.RecoveryMetrics) string {
	return fmt.Sprintf("open %s · first read %s · full %s",
		fm.Duration(time.Duration(rec.TimeToOpenNS)),
		fm.Duration(time.Duration(rec.TimeToFirstReadNS)),
		fm.Duration(time.Duration(rec.TimeToFullRecoveryNS)))
}

// written names where the output lives.
func written(t style.Theme, dir string) string {
	if dir == "" {
		return ""
	}
	return t.Paint(t.Label).Render("written  " + dir)
}

// kvBlock renders each key-value row at its own width so values stay left-aligned.
func kvBlock(t style.Theme, labelWidth int, rows [][2]string) string {
	lines := make([]string, len(rows))
	for i, row := range rows {
		lines[i] = t.KVSection("", [][2]string{row}, labelWidth, lipgloss.Width(row[1]), false)
	}
	return strings.Join(lines, "\n")
}

// trimTrailing drops trailing spaces from every line so a render carries no invisible padding.
func trimTrailing(s string) string {
	lines := strings.Split(s, "\n")
	for i, line := range lines {
		lines[i] = strings.TrimRight(line, " ")
	}
	return strings.Join(lines, "\n")
}

// yesNo renders a boolean for people.
func yesNo(b bool) string {
	if b {
		return "yes"
	}
	return "no"
}

// nonEmpty drops blank parts so a section never renders a hole.
func nonEmpty(parts []string) []string {
	kept := make([]string, 0, len(parts))
	for _, part := range parts {
		if strings.TrimSpace(part) != "" {
			kept = append(kept, part)
		}
	}
	return kept
}
