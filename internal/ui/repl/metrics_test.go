package repl

import (
	"encoding/json"
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// metricsSession opens an embedded node holding one log queue.
func metricsSession(t *testing.T) *replSession {
	t.Helper()
	client, err := mikyu.Open(t.TempDir(), mikyu.WithMetricsEnabled(true))
	require.NoError(t, err)
	t.Cleanup(func() { _ = client.Close() })

	ctx := t.Context()
	_, err = client.CreateLogQueue(ctx, "events")
	require.NoError(t, err)
	return &replSession{client: client, ctx: ctx}
}

// TestREPLMetricsJSON parses the REPL's json output back into a snapshot.
func TestREPLMetricsJSON(t *testing.T) {
	s := metricsSession(t)
	out := captureStdout(func() { s.handleMetrics([]string{"--json"}) })

	var got mikyu.Metrics
	require.NoError(t, json.Unmarshal([]byte(out), &got))
	_, ok := got.Queue("events")
	assert.True(t, ok)
}

// TestREPLMetricsTextAndDetail share the CLI render path.
func TestREPLMetricsTextAndDetail(t *testing.T) {
	s := metricsSession(t)

	out := captureStdout(func() { s.handleMetrics(nil) })
	assert.NotContains(t, out, "\x1b")
	assert.Contains(t, out, "LOG QUEUES")
	assert.Contains(t, out, "events")

	detail := captureStdout(func() { s.handleMetrics([]string{"--detail"}) })
	assert.Contains(t, detail, "RUNTIME")
	assert.Contains(t, detail, "PROCESS")
}

// TestREPLMetricsUnknownQueue reports the teaching state through the session's fail path.
func TestREPLMetricsUnknownQueue(t *testing.T) {
	s := metricsSession(t)
	out := captureStdout(func() { s.handleMetrics([]string{"--queue", "nope"}) })
	assert.Contains(t, out, `no queue named "nope"`)
}

// TestREPLMetricsUsageErrors cover a bad enum value and a stray positional.
func TestREPLMetricsUsageErrors(t *testing.T) {
	s := metricsSession(t)
	assert.Contains(t, captureStdout(func() {
		s.handleMetrics([]string{"--output", "json"})
	}), "Usage:")
	assert.Contains(t, captureStdout(func() {
		s.handleMetrics([]string{"positional"})
	}), "Usage:")
}
