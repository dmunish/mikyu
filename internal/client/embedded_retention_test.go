package client

import (
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/require"
)

// A queue has exactly one driver; retention truncation is capped by that
// driver's floor and never coordinated across two drivers over one stream.
func TestRetentionHonorsSingleDriver(t *testing.T) {
	dir := t.TempDir()
	client, err := NewEmbedded(dir)
	require.NoError(t, err)
	defer func() { _ = client.Close() }()
	ctx := t.Context()
	const name = "logs"
	mustCreateLog(t, client, name, WithSegmentSize(128), WithMaxMessageBytes(128))
storage, _, err := client.node.Manager().Get(name)
	require.NoError(t, err)
	for range 24 {
		_, err := storage.Append(ctx, []byte("log-payload"))
		require.NoError(t, err)
	}
	logQ, err := client.LogQueue(name)
	require.NoError(t, err)
	logQ.Group("consumer").Mark(8)

	_, err = client.Truncate(ctx, name, ^uint64(0))
	require.NoError(t, err)

	var snap metrics.Snapshot
	client.node.Manager().Collect(ctx, &snap)
	qv, ok := snap.Queue(name)
	require.True(t, ok)
	minOffset := uint64(qv.Gauges[engine.GaugeMinOffset])
	// Truncation happens at segment boundaries, so MinOffset is <= 8 and > 0.
	require.LessOrEqual(t, minOffset, uint64(8))
	require.Greater(t, minOffset, uint64(0))
}
