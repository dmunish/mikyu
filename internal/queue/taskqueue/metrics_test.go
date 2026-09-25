package taskqueue

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newScopedQueue opens a task driver over a temp engine with a live scope.
func newScopedQueue(t *testing.T, reg *metrics.Registry, d Defaults) *queue {
	t.Helper()
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilityAsync,
		MaxSegment:    64 << 20,
		MaxSegmentAge: time.Hour,
		MaxPayload:    1 << 20,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  time.Millisecond,
	}, nil, reg)
	require.NoError(t, err)
	t.Cleanup(func() { _ = mgr.Close() })

	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	queueIface, err := New("tasks", storage, d, nil, reg.Scope("tasks"))
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() { _ = q.Close() })
	return q
}

func TestTaskQueueCounters(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{Histograms: true})
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilityAsync,
		MaxSegment:    64 << 20,
		MaxSegmentAge: time.Hour,
		MaxPayload:    1 << 20,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  time.Millisecond,
	}, nil, reg)
	require.NoError(t, err)
	defer mgr.Close()

	storage, err := mgr.Create("tasks")
	require.NoError(t, err)

	queueIface, err := New("tasks", storage, Defaults{}, func(ctx context.Context, source string) (ArchiveAppender, error) { return nil, nil }, reg.Scope("tasks"))
	require.NoError(t, err)
	q := queueIface.(*queue)
	defer q.Close()

	ctx := t.Context()
	_, err = q.Enqueue(ctx, []byte("a"))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, []byte("b"))
	require.NoError(t, err)

	task, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Ack(ctx, task))

	scope := reg.Scope("tasks")
	assert.EqualValues(t, 2, scope.Counter("enqueues").Load())
	assert.EqualValues(t, 1, scope.Counter("dequeues").Load())
	assert.EqualValues(t, 1, scope.Counter("acks").Load())
	assert.EqualValues(t, 2, scope.Histogram("enqueue_duration", nil).Snapshot().Total)
	assert.EqualValues(t, 1, scope.Histogram("dequeue_wait", nil).Snapshot().Total)
}

func TestTaskSnapshotAtRest(t *testing.T) {
	q := newScopedQueue(t, metrics.NewRegistry(metrics.Config{}), Defaults{})
	ctx := t.Context()
	for range 5 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	view := q.TaskSnapshot()
	assert.EqualValues(t, 5, view.Active)
	assert.Zero(t, view.InFlight)
	assert.EqualValues(t, 5, view.Scheduled)
	assert.Zero(t, view.DeadLetters)
}

func TestTaskSnapshotInFlight(t *testing.T) {
	q := newScopedQueue(t, metrics.NewRegistry(metrics.Config{}), Defaults{})
	ctx := t.Context()
	for range 5 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	for range 3 {
		_, err := q.Dequeue(ctx)
		require.NoError(t, err)
	}
	view := q.TaskSnapshot()
	assert.EqualValues(t, 5, view.Active)
	assert.EqualValues(t, 3, view.InFlight)
	assert.EqualValues(t, 2, view.Scheduled)
}

func TestTaskSnapshotDLQ(t *testing.T) {
	q := newScopedQueue(t, metrics.NewRegistry(metrics.Config{}), Defaults{MaxAttempts: 1})
	ctx := t.Context()
	_, err := q.Enqueue(ctx, []byte("doomed"))
	require.NoError(t, err)
	task, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Nack(ctx, task))

	dead, err := q.DeadLetters(ctx)
	require.NoError(t, err)
	view := q.TaskSnapshot()
	assert.EqualValues(t, len(dead), view.DeadLetters)
	assert.EqualValues(t, 1, view.DeadLetters)
}

func TestTaskSnapshotCheckpointLag(t *testing.T) {
	q := newScopedQueue(t, metrics.NewRegistry(metrics.Config{}), Defaults{})
	ctx := t.Context()
	for range 3 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	q.mu.Lock()
	err := q.checkpointLocked(context.Background())
	q.mu.Unlock()
	require.NoError(t, err)
	assert.Zero(t, q.TaskSnapshot().CheckpointLag)

	_, err = q.Enqueue(ctx, []byte("t"))
	require.NoError(t, err)
	assert.Positive(t, q.TaskSnapshot().CheckpointLag)
}

func TestDequeueParkEvents(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{})
	scope := reg.Scope("tasks")

	ctx, cancel := context.WithTimeout(t.Context(), 500*time.Millisecond)
	defer cancel()
	_, err := q.Dequeue(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.EqualValues(t, 1, scope.Counter("dequeue_park_events").Load())
	assert.Zero(t, scope.Gauge("parked_dequeue").Load())
}

func TestQueueFullEvent(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{MaxInFlight: 1})
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("first"))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, []byte("second"))
	require.ErrorIs(t, err, ErrQueueFull)
	assert.EqualValues(t, 1, reg.Scope("tasks").Counter("queue_full_events").Load())
}

func TestRetryBudgetDrop(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{MaxAttempts: 1})
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("doomed"))
	require.NoError(t, err)
	task, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Nack(ctx, task))
	assert.EqualValues(t, 1, reg.Scope("tasks").Counter("retry_budget_drops").Load())
}

func TestTaskSnapshotClosed(t *testing.T) {
	q := newScopedQueue(t, metrics.NewRegistry(metrics.Config{}), Defaults{})
	require.NoError(t, q.Close())
	require.NotPanics(t, func() {
		assert.Equal(t, metrics.TaskView{}, q.TaskSnapshot())
	})
}

func TestTaskSnapshotConcurrentWithDequeue(t *testing.T) {
	q := newScopedQueue(t, metrics.NewRegistry(metrics.Config{}), Defaults{Lease: time.Minute})
	ctx := t.Context()
	const n = 32
	for range n {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}

	var wg sync.WaitGroup
	wg.Add(n)
	for range n {
		go func() {
			defer wg.Done()
			dctx, cancel := context.WithTimeout(ctx, 2*time.Second)
			defer cancel()
			_, _ = q.Dequeue(dctx)
		}()
	}
	for range 100 {
		_ = q.TaskSnapshot()
	}
	wg.Wait()
}
