package cli

import (
	"encoding/json"
	"testing"

	"github.com/dmunish/mikyu"
	"github.com/dmunish/mikyu/internal/daemon"
	"github.com/dmunish/mikyu/internal/ui/metricsview"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// TestMetricsJSON emits the snapshot verbatim.
func TestMetricsJSON(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "log", "events")
	app.run(t, "append", "events", "hello")

	var got mikyu.Metrics
	require.NoError(t, json.Unmarshal([]byte(app.run(t, "metrics", "--json")), &got))
	_, ok := got.Queue("events")
	assert.True(t, ok)
}

// TestMetricsTextInABufferIsPlain: the test app's stdout is not a terminal.
func TestMetricsTextInABufferIsPlain(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "log", "events")

	out := app.run(t, "metrics")
	assert.NotContains(t, out, "\x1b")
	assert.Contains(t, out, "LOG QUEUES")
	assert.Contains(t, out, "events")
}

// TestMetricsDetailAndQueue: --detail renders the node grid; --queue focuses one card.
func TestMetricsDetailAndQueue(t *testing.T) {
	app := newTestApp(t)
	app.run(t, "create", "--type", "log", "events")

	assert.Contains(t, app.run(t, "metrics", "--detail"), "RUNTIME")
	assert.Contains(t, app.run(t, "metrics", "--queue", "events"), "THROUGHPUT")
}

// TestMetricsUnknownQueueIsARegularError: a missed --queue exits 1, not 2.
func TestMetricsUnknownQueueIsARegularError(t *testing.T) {
	app := newTestApp(t)
	err := app.runErr(t, "metrics", "--queue", "nope")
	require.Error(t, err)
	assert.ErrorIs(t, err, metricsview.ErrUnknownQueue)
	assert.Contains(t, app.out.String(), `no queue named "nope"`)
}

// TestMetricsRemovedOutputFlagIsUsage: the old --output flag is gone and exits 2.
func TestMetricsRemovedOutputFlagIsUsage(t *testing.T) {
	app := newTestApp(t)
	err := app.runErr(t, "metrics", "--output", "json")
	require.ErrorIs(t, err, daemon.ErrUsage)
}

// TestMetricsRejectsPositionalArgs keeps the usage surface tight.
func TestMetricsRejectsPositionalArgs(t *testing.T) {
	app := newTestApp(t)
	err := app.runErr(t, "metrics", "extra")
	require.ErrorIs(t, err, daemon.ErrUsage)
}
