package command

import (
	"bytes"
	"context"
	"encoding/json"
	"os"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/ui/metricsview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metricsClient opens an embedded node holding one log queue with one record.
func metricsClient(t *testing.T) mikyu.Client {
	t.Helper()
	c, err := mikyu.Open(t.TempDir(), mikyu.WithMetricsEnabled(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	ctx := t.Context()
	_, err = c.CreateLogQueue(ctx, "events")
	require.NoError(t, err)
	q, err := c.LogQueue("events")
	require.NoError(t, err)
	_, err = q.Append(ctx, []byte("hello"))
	require.NoError(t, err)
	return c
}

// fixedMetricsSnapshot is a hand-built snapshot for deterministic render tests.
func fixedMetricsSnapshot() mikyu.Metrics {
	maxSegment := int64(64 << 20)
	return mikyu.Metrics{
		Timestamp: time.Date(2026, 9, 27, 12, 0, 0, 0, time.UTC),
		Build:     metrics.BuildView{SpecVersion: 5, GoVersion: "go1.27", OS: "linux", Arch: "amd64"},
		Runtime:   metrics.RuntimeView{UptimeSec: 60},
		Storage:   metrics.StorageView{DataDir: "./data", Queues: 1, Segments: 1, SizeBytes: 1024},
		Queues: []metrics.QueueView{{
			Name:      "events",
			Kind:      "log",
			Gauges:    map[string]int64{"messages": 3, "segments": 1, "size_bytes": 1024},
			Rates:     metrics.Rates{AppendsPerSec: 2},
			Effective: spec.Spec{Storage: spec.StorageSpec{MaxSegment: &maxSegment}},
		}},
		Totals: metrics.TotalsView{Queues: 1, Messages: 3, Bytes: 1024},
	}
}

// TestRenderMetricsJSONIsVerbatim pins invariant I4: json is the snapshot, no wrapper.
func TestRenderMetricsJSONIsVerbatim(t *testing.T) {
	snap := fixedMetricsSnapshot()
	var buf bytes.Buffer
	require.NoError(t, renderMetrics(&buf, snap, MetricsOptions{JSON: true}))

	var got mikyu.Metrics
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.Equal(t, snap, got)
}

// TestRenderMetricsTextIsDeterministic pins that two renders of one snapshot agree byte for byte.
func TestRenderMetricsTextIsDeterministic(t *testing.T) {
	snap := fixedMetricsSnapshot()
	render := func(opts MetricsOptions) string {
		var buf bytes.Buffer
		require.NoError(t, renderMetrics(&buf, snap, opts))
		return buf.String()
	}

	summary := render(MetricsOptions{})
	assert.Contains(t, summary, "LOG QUEUES")
	assert.Contains(t, summary, "events")
	assert.NotContains(t, summary, "\x1b")
	assert.Equal(t, summary, render(MetricsOptions{}))

	detail := render(MetricsOptions{Detail: true})
	assert.Contains(t, detail, "RUNTIME")
	assert.Contains(t, detail, "PROCESS")
	assert.Equal(t, detail, render(MetricsOptions{Detail: true}))
}

// TestMetricsJSONRoundTrip fetches a real snapshot and reads it back.
func TestMetricsJSONRoundTrip(t *testing.T) {
	c := metricsClient(t)
	var buf bytes.Buffer
	require.NoError(t, RunMetrics(t.Context(), c, &buf, MetricsOptions{JSON: true}))

	var got mikyu.Metrics
	require.NoError(t, json.Unmarshal(buf.Bytes(), &got))
	assert.GreaterOrEqual(t, got.Totals.Queues, 1)
	_, ok := got.Queue("events")
	assert.True(t, ok)
}

// TestMetricsTextInAPipeIsPlain pins invariant I3 for the CLI's buffer writer.
func TestMetricsTextInAPipeIsPlain(t *testing.T) {
	c := metricsClient(t)
	var buf bytes.Buffer
	require.NoError(t, RunMetrics(t.Context(), c, &buf, MetricsOptions{}))
	out := buf.String()
	assert.NotContains(t, out, "\x1b", "a pipe never gets color")
	assert.Contains(t, out, "LOG QUEUES")
	assert.Contains(t, out, "events")
	assert.NotContains(t, out, "MiB", "raw mode uses bare byte counts")
}

// TestMetricsDetailAndQueueSelection exercise the two detail triggers.
func TestMetricsDetailAndQueueSelection(t *testing.T) {
	c := metricsClient(t)

	var detail bytes.Buffer
	require.NoError(t, RunMetrics(t.Context(), c, &detail, MetricsOptions{Detail: true}))
	assert.Contains(t, detail.String(), "RUNTIME")

	var selected bytes.Buffer
	require.NoError(t, RunMetrics(t.Context(), c, &selected, MetricsOptions{Queues: []string{"events"}}))
	assert.Contains(t, selected.String(), "THROUGHPUT")
}

// TestMetricsUnknownQueue: a --queue typo teaches and fails in both output modes.
func TestMetricsUnknownQueue(t *testing.T) {
	c := metricsClient(t)

	var buf bytes.Buffer
	err := RunMetrics(t.Context(), c, &buf, MetricsOptions{Queues: []string{"nope"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, metricsview.ErrUnknownQueue)
	assert.Contains(t, buf.String(), `no queue named "nope"`)

	buf.Reset()
	err = RunMetrics(t.Context(), c, &buf, MetricsOptions{JSON: true, Queues: []string{"nope"}})
	require.Error(t, err)
	assert.ErrorIs(t, err, metricsview.ErrUnknownQueue)
	assert.Empty(t, buf.String(), "json mode writes no porcelain prose")
}

// TestMetricsFlagSet covers the flag surface, including the output enum.
func TestMetricsFlagSet(t *testing.T) {
	var opts MetricsOptions
	rest, err := ParseAnywhere(MetricsFlagSet(&opts),
		[]string{"--detail", "--queue", "a", "--queue=b", "--watch", "--output", "json"})
	require.NoError(t, err)
	assert.Empty(t, rest)
	assert.True(t, opts.Detail)
	assert.Equal(t, []string{"a", "b"}, opts.Queues)
	assert.True(t, opts.Watch)
	assert.True(t, opts.JSON)

	_, err = ParseAnywhere(MetricsFlagSet(&MetricsOptions{}), []string{"--output", "yaml"})
	require.Error(t, err)
	assert.Contains(t, err.Error(), "text or json")

	_, err = ParseAnywhere(MetricsFlagSet(&MetricsOptions{}), []string{"--queue="})
	require.Error(t, err, "an empty queue name is rejected")
}

// TestMetricsPresentationPlainInAPipe pins the writer-driven theme and width choice.
func TestMetricsPresentationPlainInAPipe(t *testing.T) {
	theme, formatter, width := metricsPresentation(&bytes.Buffer{}, 0)
	assert.True(t, theme.Plain)
	assert.True(t, formatter.Raw)
	assert.Equal(t, defaultMetricsWidth, width)

	_, _, explicit := metricsPresentation(&bytes.Buffer{}, 42)
	assert.Equal(t, 42, explicit, "an explicit width wins")
}

// TestIsTerminalWriterFalseForFiles: a regular file is not a terminal.
func TestIsTerminalWriterFalseForFiles(t *testing.T) {
	f, err := os.CreateTemp(t.TempDir(), "out")
	require.NoError(t, err)
	defer f.Close()
	assert.False(t, isTerminalWriter(f))
	assert.False(t, isTerminalWriter(&bytes.Buffer{}))
}

// TestWatchStopsOnCancelledContext pins invariant I10: a cancelled watch exits clean.
func TestWatchStopsOnCancelledContext(t *testing.T) {
	old := metricsWatchInterval
	metricsWatchInterval = 5 * time.Millisecond
	t.Cleanup(func() { metricsWatchInterval = old })

	c := metricsClient(t)
	ctx, cancel := context.WithCancel(t.Context())
	var buf bytes.Buffer
	done := make(chan error, 1)
	go func() { done <- RunMetrics(ctx, c, &buf, MetricsOptions{Watch: true, JSON: true}) }()

	time.Sleep(60 * time.Millisecond)
	cancel()
	select {
	case err := <-done:
		require.NoError(t, err, "a cancelled watch exits 0")
	case <-time.After(5 * time.Second):
		t.Fatal("watch did not stop after cancellation")
	}
	assert.NotEmpty(t, buf.String(), "frames were rendered before cancellation")
}

// TestMetricsVerbDocumentsPlumbing pins the user-visible porcelain/plumbing rule.
func TestMetricsVerbDocumentsPlumbing(t *testing.T) {
	v, ok := LookupVerb("metrics")
	require.True(t, ok)
	for _, name := range []string{"detail", "queue", "watch", "output"} {
		assert.True(t, hasExtraFlag(v, name), "metrics must document --%s", name)
	}
	assert.False(t, hasExtraFlag(v, "json"), "the old --json flag is gone")

	var output string
	for _, f := range v.ExtraFlags {
		if f.Name == "output" {
			output = f.Doc
		}
	}
	assert.Contains(t, output, "verbatim")
	assert.Contains(t, output, "may change between releases")
}

// hasExtraFlag reports whether a verb documents a presentation flag.
func hasExtraFlag(v Verb, name string) bool {
	for _, f := range v.ExtraFlags {
		if f.Name == name {
			return true
		}
	}
	return false
}
