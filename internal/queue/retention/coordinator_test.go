package retention

import (
	"context"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/enginetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type floorDriver struct {
	floor       uint64
	constrained bool
}

func (d floorDriver) TruncateFloor(context.Context) (uint64, bool, error) {
	return d.floor, d.constrained, nil
}

// Driver floors cap manual requests but never drive truncation on their own:
// with no policy set, ReconcileQueue deletes nothing.
func TestCoordinatorDriverFloorsCapRequests(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{SegmentSize: 128}, nil)
	require.NoError(t, err)
	defer mgr.Close()
	storage, err := mgr.Create("coordinated")
	require.NoError(t, err)
	ctx := t.Context()
	for range 16 {
		_, err = storage.Append(ctx, []byte("payload"))
		require.NoError(t, err)
	}

	c := NewCoordinator(0)
	c.RegisterTarget(&Target{Name: "coordinated", Storage: storage, Driver: floorDriver{floor: 4, constrained: true}})

	// Requesting beyond the safe floor must be clamped to 4.
	_, err = c.ReconcileAt(ctx, "coordinated", 100)
	require.NoError(t, err)
	// Truncation happens at segment boundaries, so MinOffset will be <= 4 and > 0.
	require.LessOrEqual(t, storage.MinOffset(), uint64(4))
	require.Greater(t, storage.MinOffset(), uint64(0))

	// A looser driver permits the safe floor to advance.
	c.RegisterTarget(&Target{Name: "coordinated", Storage: storage, Driver: floorDriver{floor: 8, constrained: true}})
	_, err = c.ReconcileAt(ctx, "coordinated", 100)
	require.NoError(t, err)
	// Safe floor is now 8. MinOffset will advance further.
	require.LessOrEqual(t, storage.MinOffset(), uint64(8))
	require.Greater(t, storage.MinOffset(), uint64(3))

	// With no driver and no policy, nothing is deleted: driver floors cap
	// deletability, they never mandate it.
	c.RegisterTarget(&Target{Name: "coordinated", Storage: storage})
	before := storage.MinOffset()
	_, err = c.ReconcileQueue(ctx, "coordinated")
	require.NoError(t, err)
	assert.Equal(t, before, storage.MinOffset())
}

// SetPolicy reports a shrink when a dimension that was unlimited becomes
// bounded, or a bound decreases. Loosening is never a shrink.
func TestSetPolicyReportsShrink(t *testing.T) {
	c := NewCoordinator(0)

	assert.False(t, c.SetPolicy("q", Policy{}), "first policy: nothing to shrink from")
	assert.True(t, c.SetPolicy("q", Policy{Age: time.Hour}), "unlimited age -> bounded")
	assert.False(t, c.SetPolicy("q", Policy{Age: 2 * time.Hour}), "looser age")
	assert.True(t, c.SetPolicy("q", Policy{Age: time.Minute}), "tighter age")
	assert.False(t, c.SetPolicy("q", Policy{}), "back to unlimited")
	assert.True(t, c.SetPolicy("q", Policy{Bytes: 1 << 20}), "unlimited bytes -> bounded")
	assert.True(t, c.SetPolicy("q", Policy{Bytes: 512 << 10}), "smaller byte bound")
	assert.False(t, c.SetPolicy("unknown", Policy{}), "unknown queue")
}

// A retention policy that would drop everything is still capped by a lagging
// consumer group: truncation stops at the group's floor.
func TestRetentionCappedByLaggingGroup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		storage := enginetest.NewMemStorage("lagging", 1<<20, 64)
		const total = 10
		for range total {
			_, err := storage.Append(ctx, []byte("payload"))
			require.NoError(t, err)
		}
		require.GreaterOrEqual(t, len(storage.Segments()), 3)

		c := NewCoordinator(0)
		// A consumer group that has only consumed through offset 2.
		c.RegisterTarget(&Target{Name: "lagging", Storage: storage, Driver: floorDriver{floor: 3, constrained: true}})

		// Age past the policy so it would drop everything...
		synctest.Sleep(2 * time.Hour)
		c.SetPolicy("lagging", Policy{Age: time.Hour})

		reclaimed, err := c.ReconcileQueue(ctx, "lagging")
		require.NoError(t, err)
		require.Greater(t, reclaimed, int64(0))

		// ...but truncation stops at the lagging group's floor.
		assert.Equal(t, uint64(3), storage.MinOffset())
		assert.Equal(t, uint64(total), storage.LastOffset())
	})
}

// Readers below the policy-truncated minimum get ErrOffsetTruncated; the
// active segment always survives.
func TestOffsetTruncatedAfterPolicyTruncation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx := t.Context()
		// 130B cap with 43B records: 3 per segment, so 6 records span one
		// closed segment [0,3) and an active one [3,6).
		storage := enginetest.NewMemStorage("truncated", 1<<20, 130)
		for range 6 {
			_, err := storage.Append(ctx, []byte("payload"))
			require.NoError(t, err)
		}
		require.Equal(t, 2, len(storage.Segments()))
		c := NewCoordinator(0)
		// No drivers on this queue: the policy floor applies uncapped.
		c.RegisterTarget(&Target{Name: "truncated", Storage: storage})
		synctest.Sleep(2 * time.Hour)
		c.SetPolicy("truncated", Policy{Age: time.Hour})

		reclaimed, err := c.ReconcileQueue(ctx, "truncated")
		require.NoError(t, err)
		require.Greater(t, reclaimed, int64(0))
		// The active segment [3,6) survives.
		assert.Equal(t, uint64(3), storage.MinOffset())

		_, err = storage.Read(ctx, 2)
		assert.ErrorIs(t, err, engine.ErrOffsetTruncated)
		_, err = storage.Read(ctx, 3)
		require.NoError(t, err)
	})
}
