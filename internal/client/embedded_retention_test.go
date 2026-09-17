package client

import (
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/require"
)

func TestEmbeddedRetentionCoordinatesLogAndTaskDrivers(t *testing.T) {
	dir := t.TempDir()
	client, err := NewEmbedded(dir)
	require.NoError(t, err)
	ctx := t.Context()
	name := "shared"
	require.NoError(t, client.CreateQueue(ctx, name, WithSegmentSize(128), WithMaxMessageBytes(128)))
	storage, err := client.node.Manager().Get(name)
	require.NoError(t, err)
	// Register the task driver first and put an active task at offset 0.
	// Subsequent log records are deliberately ahead of that task.
	taskQ, err := client.TaskQueue(name)
	require.NoError(t, err)
	_, err = taskQ.Enqueue(ctx, []byte("task"))
	require.NoError(t, err)
	for range 24 {
		_, err := storage.Append(ctx, []byte("log-payload"))
		require.NoError(t, err)
	}
	logQ, err := client.LogQueue(name)
	require.NoError(t, err)
	group := logQ.Group("consumer")
	group.Mark(18)
	// The first task is still active at its original offset. Even though the
	// log consumer would permit a much larger floor, the coordinator must keep
	// the shared storage at or before the task driver's safe floor.
	_, err = client.Truncate(ctx, name, ^uint64(0))
	require.NoError(t, err)
	var snap metrics.Snapshot
	client.node.Manager().Collect(ctx, &snap)
	qv, ok := snap.Queue(name)
	require.True(t, ok)
	require.Equal(t, uint64(0), uint64(qv.Gauges[engine.GaugeMinOffset]))
	// After the task is acknowledged and the checkpoint is written, the log
	// consumer remains the constraining driver for a deliberately smaller floor.
	leased, err := taskQ.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, taskQ.Ack(ctx, leased))

	// Properly simulate a restart by closing the client and reopening it.
	// This ensures the retention coordinator is cleanly reset.
	require.NoError(t, client.Close())

	client2, err := NewEmbedded(dir)
	require.NoError(t, err)
	defer client2.Close()

	// The log driver only considers in-memory consumer group high-watermarks.
	// Re-register the group and restore its mark on the new client.
	logQ2, err := client2.LogQueue(name)
	require.NoError(t, err)
	group2 := logQ2.Group("consumer")
	group2.Mark(18)

	_, err = client2.Truncate(ctx, name, 18)
	require.NoError(t, err)
	var snap2 metrics.Snapshot
	client2.node.Manager().Collect(ctx, &snap2)
	qv2, ok := snap2.Queue(name)
	require.True(t, ok)
	minOffset := uint64(qv2.Gauges[engine.GaugeMinOffset])
	// Truncation happens at segment boundaries, so MinOffset will be <= 18 and > 0.
	require.LessOrEqual(t, minOffset, uint64(18))
	require.Greater(t, minOffset, uint64(0))
}
