package client

import (
	"context"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestEmbeddedRetentionCoordinatesLogAndTaskDrivers(t *testing.T) {
	dir := t.TempDir()
	client, err := NewEmbedded(dir)
	require.NoError(t, err)
	ctx := context.Background()
	name := "shared"
	require.NoError(t, client.CreateQueue(ctx, name, WithMaxSegmentBytes(128)))
	storage, err := client.mgr.GetQueue(name)
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
	_, err = client.TruncateQueue(ctx, name, ^uint64(0))
	require.NoError(t, err)
	stats := client.mgr.Stats()
	qs := stats.Queues[name]
	require.Equal(t, uint64(0), qs.MinOffset)
	// After the task is acknowledged and the checkpoint is written, the log
	// consumer remains the constraining driver for a deliberately smaller floor.
	leased, err := taskQ.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, taskQ.Ack(ctx, leased))

	// Properly simulate a restart by closing the client and reopening it.
	// This ensures the retention coordinator is cleanly reset.
	require.NoError(t, taskQ.Close())
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

	_, err = client2.TruncateQueue(ctx, name, 18)
	require.NoError(t, err)
	qs = client2.mgr.Stats().Queues[name]
	// Truncation happens at segment boundaries, so MinOffset will be <= 18 and > 0.
	require.LessOrEqual(t, qs.MinOffset, uint64(18))
	require.Greater(t, qs.MinOffset, uint64(0))
}
