package taskqueue

import (
	"context"
	"math/rand/v2"
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
	return newScopedQueueArchive(t, reg, d, nil)
}

// newScopedQueueArchive opens a task driver with a live scope and an optional archive writer.
func newScopedQueueArchive(t *testing.T, reg *metrics.Registry, d Defaults, archive ArchiveLogFor) *queue {
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
	queueIface, err := New("tasks", storage, d, archive, reg.Scope("tasks"))
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// publishedTaskView reads the occupancy gauges a driver publishes.
func publishedTaskView(scope *metrics.Scope) metrics.TaskView {
	return metrics.TaskView{
		Active:         scope.Gauge("active").Load(),
		InFlight:       scope.Gauge("in_flight").Load(),
		Scheduled:      scope.Gauge("scheduled").Load(),
		DeadLetters:    scope.Gauge("dead_letters").Load(),
		Uncheckpointed: scope.Gauge("uncheckpointed").Load(),
		ScheduleHeap:   scope.Gauge("schedule_heap").Load(),
		CheckpointLSN:  scope.Gauge("checkpoint_lsn").Load(),
		Dirty:          scope.Gauge("dirty").Load(),
		CheckpointLag:  scope.Gauge("checkpoint_lag").Load(),
	}
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

func TestTaskGaugesAtRest(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{})
	ctx := t.Context()
	for range 5 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	view := publishedTaskView(reg.Scope("tasks"))
	assert.EqualValues(t, 5, view.Active)
	assert.Zero(t, view.InFlight)
	assert.EqualValues(t, 5, view.Scheduled)
	assert.Zero(t, view.DeadLetters)
}

func TestTaskGaugesInFlight(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{})
	ctx := t.Context()
	for range 5 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	for range 3 {
		_, err := q.Dequeue(ctx)
		require.NoError(t, err)
	}
	view := publishedTaskView(reg.Scope("tasks"))
	assert.EqualValues(t, 5, view.Active)
	assert.EqualValues(t, 3, view.InFlight)
	assert.EqualValues(t, 2, view.Scheduled)
}

func TestTaskGaugesDeadLetters(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{MaxAttempts: 1})
	ctx := t.Context()
	_, err := q.Enqueue(ctx, []byte("doomed"))
	require.NoError(t, err)
	task, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Nack(ctx, task))

	dead, err := q.DeadLetters(ctx)
	require.NoError(t, err)
	view := publishedTaskView(reg.Scope("tasks"))
	assert.Len(t, dead, 1)
	assert.EqualValues(t, len(dead), view.DeadLetters)
	assert.EqualValues(t, len(dead), view.Uncheckpointed)
}

func TestTaskGaugesCheckpointLag(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{})
	ctx := t.Context()
	for range 3 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	q.mu.Lock()
	err := q.checkpointLocked(ctx)
	q.mu.Unlock()
	require.NoError(t, err)
	scope := reg.Scope("tasks")
	assert.Zero(t, publishedTaskView(scope).CheckpointLag)
	assert.Positive(t, scope.Gauge("checkpoint_lsn").Load())
	assert.Zero(t, scope.Gauge("dirty").Load())

	_, err = q.Enqueue(ctx, []byte("t"))
	require.NoError(t, err)
	assert.Positive(t, publishedTaskView(scope).CheckpointLag)
}

func TestTaskGaugesClosed(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{})
	_, err := q.Enqueue(t.Context(), []byte("t"))
	require.NoError(t, err)
	require.NoError(t, q.Close())
	assert.Equal(t, metrics.TaskView{}, publishedTaskView(reg.Scope("tasks")))
}

func TestTaskGaugeInFlightCount(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{Lease: time.Minute})
	ctx := t.Context()
	for range 4 {
		_, err := q.Enqueue(ctx, []byte("t"))
		require.NoError(t, err)
	}
	delivered := make([]Task, 0, 4)
	for range 3 {
		task, err := q.Dequeue(ctx)
		require.NoError(t, err)
		delivered = append(delivered, task)
	}
	require.NoError(t, q.Ack(ctx, delivered[0]))
	require.NoError(t, q.Nack(ctx, delivered[1], WithRetryAfter(0)))

	q.mu.Lock()
	inFlight := 0
	for _, state := range q.active {
		if state.inFlight {
			inFlight++
		}
	}
	require.Equal(t, inFlight, int(q.inFlightCount))
	q.mu.Unlock()

	view := publishedTaskView(reg.Scope("tasks"))
	assert.EqualValues(t, 3, view.Active)
	assert.EqualValues(t, 1, view.InFlight)
	assert.EqualValues(t, 2, view.Scheduled)
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

func TestTaskGaugesConcurrentWithDequeue(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	q := newScopedQueue(t, reg, Defaults{Lease: time.Minute})
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
	scope := reg.Scope("tasks")
	for range 100 {
		_ = publishedTaskView(scope)
	}
	wg.Wait()
	assert.Positive(t, publishedTaskView(scope).InFlight)
}

// TestTaskGaugeDrift runs a scripted op mix and asserts the published gauges
// still match the driver's own state.
func TestTaskGaugeDrift(t *testing.T) {
	reg := metrics.NewRegistry(metrics.Config{})
	archive := func(ctx context.Context, source string) (ArchiveAppender, error) {
		return newArchiveLogAppender(t), nil
	}
	q := newScopedQueueArchive(t, reg, Defaults{MaxAttempts: 2, Lease: time.Minute}, archive)
	ctx := t.Context()

	rng := rand.New(rand.NewPCG(1, 2))
	var live []Task
	pick := func() (Task, bool) {
		if len(live) == 0 {
			return Task{}, false
		}
		i := rng.IntN(len(live))
		task := live[i]
		live = append(live[:i], live[i+1:]...)
		return task, true
	}
	for range 500 {
		switch rng.IntN(7) {
		case 0:
			_, _ = q.Enqueue(ctx, []byte("t"))
		case 1:
			if task, err := q.TryDequeue(ctx); err == nil {
				live = append(live, task)
			}
		case 2:
			if task, ok := pick(); ok {
				_ = q.Ack(ctx, task)
			}
		case 3:
			if task, ok := pick(); ok {
				_ = q.Nack(ctx, task, WithRetryAfter(0))
			}
		case 4:
			if task, ok := pick(); ok {
				_ = q.Extend(ctx, task, time.Minute)
			}
		case 5:
			if task, ok := pick(); ok {
				_ = q.Archive(ctx, task)
			}
		case 6:
			dead, err := q.DeadLetters(ctx)
			if err == nil && len(dead) > 0 {
				_ = q.AckDeadLetter(ctx, dead[rng.IntN(len(dead))])
			}
		}
	}

	q.mu.Lock()
	inFlight := 0
	for _, state := range q.active {
		if state.inFlight {
			inFlight++
		}
	}
	require.Equal(t, inFlight, int(q.inFlightCount), "inFlightCount drifted from state.inFlight")
	want := q.viewLocked()
	q.mu.Unlock()

	assert.Equal(t, want, publishedTaskView(reg.Scope("tasks")))
}
