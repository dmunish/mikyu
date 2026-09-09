package retention

import (
	"context"
	"testing"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/stretchr/testify/require"
)

type floorDriver struct{ floor uint64 }

func (d floorDriver) TruncateFloor(context.Context) (uint64, error) { return d.floor, nil }

func TestCoordinatorUsesMinimumDriverFloorAndClampsManualRequests(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{MaxSegmentBytes: 128})
	require.NoError(t, err)
	defer mgr.Close()
	storage, err := mgr.CreateQueue("coordinated")
	require.NoError(t, err)
	ctx := context.Background()
	for range 16 {
		_, err = storage.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}

	c := NewCoordinator(0)
	a := floorDriver{floor: 8}
	b := floorDriver{floor: 4}
	c.RegisterDriver("coordinated", storage, a)
	c.RegisterDriver("coordinated", storage, b)

	// Requesting beyond the safe floor must be clamped to 4.
	_, err = c.ReconcileQueueAt(ctx, "coordinated", 100)
	require.NoError(t, err)
	// Truncation happens at segment boundaries, so MinOffset will be <= 4 and > 0.
	require.LessOrEqual(t, storage.MinOffset(), uint64(4))
	require.Greater(t, storage.MinOffset(), uint64(0))

	// Unregistering the blocking driver permits the next safe floor to advance.
	c.UnregisterDriver("coordinated", b)
	_, err = c.ReconcileQueue(ctx, "coordinated")
	require.NoError(t, err)
	// Safe floor is now 8. MinOffset will advance further.
	require.LessOrEqual(t, storage.MinOffset(), uint64(8))
	require.Greater(t, storage.MinOffset(), uint64(3))
}
