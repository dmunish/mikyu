package engine

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestManagerCreateGetDelete(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir, Config{})
	require.NoError(t, err)
	defer mgr.Close()

	q, err := mgr.CreateQueue("orders")
	require.NoError(t, err)
	assert.NotNil(t, q)

	_, err = mgr.CreateQueue("orders")
	assert.Error(t, err)

	q2, err := mgr.GetQueue("orders")
	require.NoError(t, err)
	assert.Equal(t, "orders", q2.Name())

	queues := mgr.ListQueues()
	assert.Equal(t, []string{"orders"}, queues)

	err = mgr.DeleteQueue("orders")
	require.NoError(t, err)
	_, err = mgr.GetQueue("orders")
	assert.Error(t, err)
}

func TestManagerStats(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := OpenManager(dir, Config{})
	defer mgr.Close()

	_, _ = mgr.CreateQueue("q1")
	_, _ = mgr.CreateQueue("q2")
	stats := mgr.Stats()
	assert.Equal(t, 2, stats.ActiveQueues)
	assert.Contains(t, stats.Queues, "q1")
	assert.Contains(t, stats.Queues, "q2")
}

func TestManagerRestoresQueuesOnOpen(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := OpenManager(dir, Config{})
	_, _ = mgr.CreateQueue("existing")
	_ = mgr.Close()

	mgr2, err := OpenManager(dir, Config{})
	require.NoError(t, err)
	defer mgr2.Close()
	_, err = mgr2.GetQueue("existing")
	require.NoError(t, err)
}

func TestManagerCloseCleansUp(t *testing.T) {
	dir := t.TempDir()
	mgr, _ := OpenManager(dir, Config{})
	_, _ = mgr.CreateQueue("q")
	err := mgr.Close()
	assert.NoError(t, err)
}

func TestManagerStatsConcurrent(t *testing.T) {
	dir := t.TempDir()
	mgr, err := OpenManager(dir, Config{BatchSize: 10, MaxInterval: time.Millisecond})
	require.NoError(t, err)
	defer mgr.Close()

	q, err := mgr.CreateQueue("concurrent-stats")
	require.NoError(t, err)

	ctx := context.Background()
	var wg sync.WaitGroup
	const workers = 8
	const ops = 50

	// Concurrent producers & readers
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				off, err := q.Append(ctx, []byte("data"))
				assert.NoError(t, err)
				_, _ = q.Read(ctx, off)
			}
		}(i)
	}

	// Concurrent stats readers
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < ops; j++ {
				st := mgr.Stats()
				assert.GreaterOrEqual(t, st.ActiveQueues, 1)
			}
		}()
	}

	wg.Wait()
}
