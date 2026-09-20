package engine

import (
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerCreateGetDelete(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir, Config{}, nil)
	require.NoError(t, err)
	defer mgr.Close()

	q, err := mgr.Create("orders")
	require.NoError(t, err)
	assert.NotNil(t, q)

	_, err = mgr.Create("orders")
	assert.Error(t, err)

	q2, err := mgr.Get("orders")
	require.NoError(t, err)
	assert.Equal(t, "orders", q2.Name())

	queues := mgr.List()
	assert.Equal(t, []string{"orders"}, queues)

	err = mgr.Delete("orders")
	require.NoError(t, err)
	_, err = mgr.Get("orders")
	assert.Error(t, err)
}

func TestManagerRejectsEscape(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir, Config{}, nil)
	require.NoError(t, err)
	defer mgr.Close()

	_, err = mgr.Create("../escape", Config{})
	require.Error(t, err)

	// A queue can only be deleted when it exists, so escaping removal names
	// are rejected as not-found before ever reaching the filesystem.
	require.Error(t, mgr.Delete("../../outside"))
}

func TestManagerCollectsQueues(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := OpenManager(dir, Config{}, nil)
	defer mgr.Close()

	_, _ = mgr.Create("q1")
	_, _ = mgr.Create("q2")
	var snap metrics.Snapshot
	mgr.Collect(t.Context(), &snap)
	require.Len(t, snap.Queues, 2)
	names := []string{snap.Queues[0].Name, snap.Queues[1].Name}
	assert.ElementsMatch(t, []string{"q1", "q2"}, names)
}

func TestManagerRestoresQueuesOnOpen(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := OpenManager(dir, Config{}, nil)
	_, _ = mgr.Create("existing")
	_ = mgr.Close()

	mgr2, err := OpenManager(dir, Config{}, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	_, err = mgr2.Get("existing")
	require.NoError(t, err)
}

func TestManagerCloseCleansUp(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := OpenManager(dir, Config{}, nil)
	_, _ = mgr.Create("q")
	err := mgr.Close()
	assert.NoError(t, err)
}

func TestManagerCollectConcurrent(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir, Config{BatchSize: 10, MaxInterval: time.Millisecond}, nil)
	require.NoError(t, err)
	defer mgr.Close()

	q, err := mgr.Create("concurrent-stats")
	require.NoError(t, err)

	ctx := t.Context()
	var wg sync.WaitGroup
	const workers = 8
	const ops = 50

	// Concurrent producers & readers
	for i := range workers {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for range ops {
				off, err := q.Append(ctx, []byte("data"))
				assert.NoError(t, err)
				_, _ = q.Read(ctx, off)
			}
		}(i)
	}

	// Concurrent snapshot readers
	for range 4 {
		wg.Go(func() {
			for range ops {
				var snap metrics.Snapshot
				mgr.Collect(ctx, &snap)
				assert.GreaterOrEqual(t, len(snap.Queues), 1)
			}
		})
	}

	wg.Wait()
}
