package logqueue_test

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestLogQueue_TruncateFloor_ConsumerGroups(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilitySync,
		MaxSegment:    256,
		MaxSegmentAge: time.Hour,
		MaxPayload:    64,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  10 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("retention-group-test")
	require.NoError(t, err)

	lq := logqueue.New("retention-group-test", storage)
	ctx := t.Context()

	// Produce 20 messages to cross segment boundaries
	for range 20 {
		_, err := lq.Append(ctx, []byte("retention-payload-1234567890"))
		require.NoError(t, err)
	}

	// No consumer groups yet: nothing constrains the floor.
	floor, constrained, err := lq.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.False(t, constrained)
	assert.Zero(t, floor)

	// Register two consumer groups
	g1 := lq.Group("group-1")
	g2 := lq.Group("group-2")

	g1.Mark(15) // Highwater = 16
	g2.Mark(8)  // Highwater = 9

	// Truncate floor must be constrained by the slowest consumer group (g2 = 9)
	floor, constrained, err = lq.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, uint64(9), floor)

	// Advance slowest consumer group
	g2.Mark(18) // Highwater = 19
	floor, constrained, err = lq.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, uint64(16), floor) // Now constrained by g1 (16)
}

func TestLogQueue_TruncateExecution(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilitySync,
		MaxSegment:    256,
		MaxSegmentAge: time.Hour,
		MaxPayload:    64,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  10 * time.Millisecond,
	}, nil)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("truncate-exec-test")
	require.NoError(t, err)

	lq := logqueue.New("truncate-exec-test", storage)
	ctx := t.Context()

	for range 20 {
		_, err := lq.Append(ctx, []byte("retention-payload-1234567890"))
		require.NoError(t, err)
	}

	g := lq.Group("fast-consumer")
	g.Mark(12) // Highwater = 13

	// Pass 0 to compute retention floor automatically from active consumer groups
	reclaimed, err := lq.Truncate(ctx, 0)
	require.NoError(t, err)
	assert.Greater(t, reclaimed, int64(0))

	// Reading truncated offset 0 must fail
	_, err = lq.Read(ctx, 0)
	assert.ErrorIs(t, err, engine.ErrOffsetTruncated)
}
