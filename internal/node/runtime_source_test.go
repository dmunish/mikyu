package node

import (
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/require"
)

func TestRuntimeSourceShape(t *testing.T) {
	src := NewRuntimeSource()
	time.Sleep(time.Millisecond)

	var snap metrics.Snapshot
	src.Collect(t.Context(), &snap)

	require.Greater(t, snap.Runtime.Goroutines, 0)
	require.Greater(t, snap.Runtime.Heap.Alloc, int64(0))
	require.Greater(t, snap.Runtime.Heap.Inuse, int64(0))
	require.Greater(t, snap.Runtime.UptimeSec, 0.0)
	require.GreaterOrEqual(t, snap.GC.Cycles, int64(0))
}
