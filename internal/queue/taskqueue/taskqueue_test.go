package taskqueue

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/enginetest"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func newTestTaskQueue(t *testing.T) (*queue, *engine.Manager, engine.Storage) {
	t.Helper()
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		BatchSize:       8,
		MaxInterval:     time.Millisecond,
		MinInterval:     0,
		SegmentSize:     16 * 1024,
		MaxMessageBytes: 2 * 1024 * 1024,
		Fsync:           false,
	}, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	queueIface, err := New("tasks", storage, Defaults{})
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() {
		_ = q.Close()
		_ = mgr.Close()
	})
	return q, mgr, storage
}

// newMemTaskQueue builds a TaskQueue over in-memory storage for synctest-bubble tests.
func newMemTaskQueue(t *testing.T, name string) *queue {
	t.Helper()
	storage := enginetest.NewMemStorage(name, 1<<20, 1<<20)
	queueIface, err := New(name, storage, Defaults{})
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() { _ = q.Close() })
	return q
}

// mustQueue opens a task driver and returns the concrete handle for tests that
// inspect unexported state.
func mustQueue(t *testing.T, name string, storage engine.Storage) *queue {
	t.Helper()
	queueIface, err := New(name, storage, Defaults{})
	require.NoError(t, err)
	return queueIface.(*queue)
}

func dequeueWithTimeout(t *testing.T, q *queue, d time.Duration) (Task, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(t.Context(), d)
	defer cancel()
	return q.Dequeue(ctx)
}

func TestTaskQueueBasicLifecycle(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	task, err := q.Enqueue(ctx, []byte("hello"), WithMaxAttempts(3), WithVisibility(time.Second))
	require.NoError(t, err)
	assert.NotEmpty(t, task.ID)
	assert.Equal(t, uint32(0), task.Attempts)
	assert.Equal(t, uint32(3), task.MaxAttempts)
	assert.False(t, task.ScheduledAt.IsZero())
	assert.Empty(t, task.Visibility)

	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, []byte("hello"), got.Payload)
	assert.Equal(t, uint32(1), got.Attempts)
	assert.NotEmpty(t, got.DeliveryID)
	assert.WithinDuration(t, time.Now().Add(time.Second), got.Visibility, 250*time.Millisecond)

	require.NoError(t, q.Ack(ctx, got))
	_, err = dequeueWithTimeout(t, q, 50*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTaskQueueScheduleAndContextCancellation(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	at := time.Now().Add(75 * time.Millisecond)
	_, err := q.Enqueue(ctx, []byte("future"), WithScheduledAt(at))
	require.NoError(t, err)

	start := time.Now()
	_, err = dequeueWithTimeout(t, q, 20*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
	assert.Less(t, time.Since(start), 100*time.Millisecond)

	got, err := dequeueWithTimeout(t, q, 500*time.Millisecond)
	require.NoError(t, err)
	assert.Equal(t, "future", string(got.Payload))
}

func TestTaskQueueVisibilityAndStaleLeaseProtection(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "tasks")
		ctx := t.Context()

		original, err := q.Enqueue(ctx, []byte("lease"), WithVisibility(40*time.Millisecond), WithMaxAttempts(3))
		require.NoError(t, err)
		first, err := q.Dequeue(ctx)
		require.NoError(t, err)

		synctest.Sleep(60 * time.Millisecond)

		second, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, first.ID, second.ID)
		assert.Equal(t, uint32(2), second.Attempts)
		assert.Equal(t, original.ID, second.ID)

		assert.ErrorIs(t, q.Ack(ctx, first), ErrTaskStale)
		require.NoError(t, q.Ack(ctx, second))
	})
}

func TestTaskQueueFailRetryAndDeadLetter(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("retry"), WithMaxAttempts(2), WithVisibility(time.Second))
	require.NoError(t, err)
	first, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Nack(ctx, first, WithRetryAfter(0)))

	second, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), second.Attempts)
	require.NoError(t, q.Nack(ctx, second, WithRetryAfter(0)))

	_, err = dequeueWithTimeout(t, q, 40*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTaskQueueDeliveryIDAndLeaseExtension(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "tasks")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("long-job"), WithVisibility(40*time.Millisecond), WithMaxAttempts(3))
		require.NoError(t, err)
		first, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.NotEmpty(t, first.DeliveryID)

		before := first.Visibility
		require.NoError(t, q.Extend(ctx, first, 250*time.Millisecond))
		secondView := first
		q.mu.Lock()
		state := q.active[first.ID]
		require.NotNil(t, state)
		secondView = cloneTask(state.task)
		q.mu.Unlock()

		assert.Equal(t, first.DeliveryID, secondView.DeliveryID)
		assert.Equal(t, first.Attempts, secondView.Attempts)
		assert.True(t, secondView.Visibility.After(before))

		stale := first
		stale.DeliveryID = "stale-delivery"
		assert.ErrorIs(t, q.Extend(ctx, stale, time.Second), ErrTaskStale)

		deadline := secondView.Visibility
		synctest.Sleep(time.Until(deadline) + 10*time.Millisecond)
		redelivered, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, first.ID, redelivered.ID)
		assert.Equal(t, uint32(2), redelivered.Attempts)
		assert.NotEqual(t, first.DeliveryID, redelivered.DeliveryID)
		require.NoError(t, q.Ack(ctx, redelivered))
	})
}

func TestTaskQueueExpiredLeaseCannotBeAcknowledgedOrExtended(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "tasks")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("expires-late"), WithMaxAttempts(3), WithVisibility(20*time.Millisecond))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		synctest.Sleep(30 * time.Millisecond)

		assert.ErrorIs(t, q.Ack(ctx, leased), ErrTaskStale)
		assert.ErrorIs(t, q.Extend(ctx, leased, time.Second), ErrTaskStale)
	})
}

func TestTaskQueueFinalLeaseExpiryDeadLetters(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "tasks")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("expires"), WithMaxAttempts(1), WithVisibility(20*time.Millisecond))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), leased.Attempts)

		synctest.Sleep(30 * time.Millisecond)
		_, err = q.TryDequeue(ctx)
		assert.ErrorIs(t, err, ErrTaskNotAvailable)

		dls, err := q.DeadLetters(ctx)
		require.NoError(t, err)
		require.Len(t, dls, 1)
		assert.Equal(t, []byte("expires"), dls[0].Payload)
		assert.Equal(t, uint32(1), dls[0].Attempts)
		assert.Empty(t, dls[0].DeliveryID)
	})
}

func TestTaskQueueCheckpointRecovery(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256, MaxMessageBytes: 1 << 20}, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	inflightSource, err := q.Enqueue(t.Context(), []byte("inflight"), WithVisibility(50*time.Millisecond))
	require.NoError(t, err)
	pending, err := q.Enqueue(t.Context(), []byte("pending"))
	require.NoError(t, err)
	ackedSource, err := q.Enqueue(t.Context(), []byte("acked"))
	require.NoError(t, err)

	inflight, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, inflightSource.ID, inflight.ID)
	acked, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, pending.ID, acked.ID)
	require.NoError(t, q.Ack(t.Context(), acked))
	// The third task was not consumed; it remains pending after restart.

	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	checkpoint := q.checkpointOffset
	q.mu.Unlock()
	require.NotZero(t, checkpoint)
	require.NoError(t, q.Close())
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256, MaxMessageBytes: 1 << 20}, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer q2.Close()

	firstAfterRestart, err := q2.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, ackedSource.ID, firstAfterRestart.ID)
	require.NoError(t, q2.Ack(t.Context(), firstAfterRestart))
	time.Sleep(75 * time.Millisecond)
	secondAfterRestart, err := q2.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, inflight.ID, secondAfterRestart.ID)
	assert.Equal(t, uint32(2), secondAfterRestart.Attempts)
	assert.False(t, secondAfterRestart.ID == pending.ID)
}

func TestTaskQueueCheckpointPreservesActiveDeliveryLease(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256, MaxMessageBytes: 1 << 20}
	mgr, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	// A generous lease keeps the test deterministic: what is under test is
	// lease survival across restart, not expiry under package-load latency.
	_, err = q.Enqueue(t.Context(), []byte("lease"), WithMaxAttempts(3), WithVisibility(30*time.Second))
	require.NoError(t, err)
	leased, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	require.NotEmpty(t, leased.DeliveryID)

	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	q.mu.Unlock()
	require.NoError(t, q.Close())
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer q2.Close()

	assert.NoError(t, q2.Extend(t.Context(), leased, time.Second))
	assert.NoError(t, q2.Ack(t.Context(), leased))
	_, err = dequeueWithTimeout(t, q2, 30*time.Millisecond)
	assert.ErrorIs(t, err, context.DeadlineExceeded)
}

func TestTaskQueueTruncateFloorTracksActiveTasksAndCheckpoint(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	first, err := q.Enqueue(ctx, []byte("first"))
	require.NoError(t, err)
	second, err := q.Enqueue(ctx, []byte("second"))
	require.NoError(t, err)

	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(ctx))
	cp := q.checkpointOffset
	q.mu.Unlock()
	require.Equal(t, first.Offset, func() uint64 {
		if first.Offset < second.Offset {
			return first.Offset
		}
		return second.Offset
	}())

	// In-flight tasks anchor the floor at offset 0, protecting the whole log.
	floor, constrained, err := q.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, uint64(0), floor)

	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	floor, constrained, err = q.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, uint64(0), floor)
	require.NoError(t, q.Ack(ctx, got))
	floor, constrained, err = q.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, uint64(1), floor)
	got2, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Ack(ctx, got2))
	// No in-flight tasks left: the checkpoint alone constrains the floor.
	floor, constrained, err = q.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, cp, floor)
}

func TestTaskQueueConcurrentConsumersNoDuplicateDeliveryPerLease(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()
	const total = 200
	for i := range total {
		_, err := q.Enqueue(ctx, fmt.Appendf(nil, "task-%d", i), WithVisibility(2*time.Second))
		require.NoError(t, err)
	}

	seen := make(map[string]int, total)
	var mu sync.Mutex
	var wg sync.WaitGroup
	const workers = 32
	for range workers {
		wg.Go(func() {
			for {
				task, err := dequeueWithTimeout(t, q, 100*time.Millisecond)
				if errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				mu.Lock()
				seen[string(task.Payload)]++
				mu.Unlock()
				if err := q.Ack(ctx, task); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
			}
		})
	}
	wg.Wait()

	mu.Lock()
	defer mu.Unlock()
	require.Len(t, seen, total)
	for payload, count := range seen {
		assert.Equal(t, 1, count, payload)
	}
}

func TestTaskQueueAppendAndConsumeUnderConcurrentLoad(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()
	const producers = 8
	const perProducer = 100

	var wg sync.WaitGroup
	for p := range producers {
		wg.Add(1)
		go func(p int) {
			defer wg.Done()
			for i := range perProducer {
				_, err := q.Enqueue(ctx, fmt.Appendf(nil, "%d/%d", p, i))
				if err != nil {
					t.Errorf("enqueue: %v", err)
					return
				}
			}
		}(p)
	}
	wg.Wait()

	count := 0
	for {
		task, err := dequeueWithTimeout(t, q, 50*time.Millisecond)
		if errors.Is(err, context.DeadlineExceeded) {
			break
		}
		require.NoError(t, err)
		require.NoError(t, q.Ack(ctx, task))
		count++
	}
	assert.Equal(t, producers*perProducer, count)
}

func TestTaskQueueCorruptLatestCheckpointFallsBackToPriorCheckpoint(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 1024, MaxMessageBytes: 1 << 20}
	mgr, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	_, err = q.Enqueue(t.Context(), []byte("survivor"))
	require.NoError(t, err)
	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	q.mu.Unlock()
	require.NoError(t, q.Close())

	_, err = storage.Append(t.Context(), append(encodeCheckpointCommit(1, 0, 1), 0xFF))
	require.NoError(t, err)
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer q2.Close()

	got, err := q2.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "survivor", string(got.Payload))
}

func TestTaskQueueIncompleteCheckpointIsIgnored(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 1024, MaxMessageBytes: 1 << 20}
	mgr, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	_, err = q.Enqueue(t.Context(), []byte("survivor"))
	require.NoError(t, err)
	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	q.mu.Unlock()
	require.NoError(t, q.Close())

	last := storage.LastOffset()
	_, err = storage.Append(t.Context(), encodeCheckpointStart(last))
	require.NoError(t, err)
	_, err = mgr.Get("tasks")
	require.NoError(t, err)
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer q2.Close()

	got, err := q2.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, "survivor", string(got.Payload))
}

func TestTaskQueueLeaseChurnUnderConcurrency(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()
	const total = 300
	for i := range total {
		_, err := q.Enqueue(ctx, fmt.Appendf(nil, "job-%d", i), WithMaxAttempts(20), WithVisibility(time.Second))
		require.NoError(t, err)
	}

	seen := make(map[string]int, total)
	var seenMu sync.Mutex
	var wg sync.WaitGroup
	const workers = 24
	for worker := range workers {
		wg.Add(1)
		go func(worker int) {
			defer wg.Done()
			for {
				task, err := dequeueWithTimeout(t, q, 200*time.Millisecond)
				if errors.Is(err, context.DeadlineExceeded) {
					return
				}
				if err != nil {
					t.Errorf("dequeue: %v", err)
					return
				}
				seenMu.Lock()
				seen[string(task.Payload)]++
				seenMu.Unlock()
				if (worker+int(task.Attempts))%4 == 0 {
					if err := q.Nack(ctx, task, WithRetryAfter(0)); err != nil {
						t.Errorf("fail: %v", err)
						return
					}
					continue
				}
				if err := q.Ack(ctx, task); err != nil {
					t.Errorf("ack: %v", err)
					return
				}
			}
		}(worker)
	}
	wg.Wait()

	seenMu.Lock()
	defer seenMu.Unlock()
	require.Len(t, seen, total)
	for payload, deliveries := range seen {
		if deliveries < 1 || deliveries > 20 {
			t.Errorf("unexpected delivery count for %s: %d", payload, deliveries)
		}
	}
}

func TestTaskQueueLargeCheckpointChunksRecover(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256 * 1024, MaxMessageBytes: 512 * 1024}
	mgr, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	payload := make([]byte, 200*1024)
	for i := range payload {
		payload[i] = byte(i)
	}
	_, err = q.Enqueue(t.Context(), payload)
	require.NoError(t, err)
	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	q.mu.Unlock()
	require.NoError(t, q.Close())
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer q2.Close()

	got, err := q2.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, payload, got.Payload)
}

func TestTaskQueueDeadLetterPersistsAndCanBeAcked(t *testing.T) {
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256, MaxMessageBytes: 1 << 20}, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	task, err := q.Enqueue(t.Context(), []byte("poison"), WithMaxAttempts(1))
	require.NoError(t, err)
	leased, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint32(1), leased.Attempts)
	require.NoError(t, q.Nack(t.Context(), leased, WithRetryAfter(0)))

	dls, err := q.DeadLetters(t.Context())
	require.NoError(t, err)
	require.Len(t, dls, 1)
	assert.Equal(t, task.ID, dls[0].ID)
	assert.Equal(t, uint32(1), dls[0].Attempts)
	assert.Equal(t, []byte("poison"), dls[0].Payload)

	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	q.mu.Unlock()
	require.NoError(t, q.Close())
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256, MaxMessageBytes: 1 << 20}, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer q2.Close()

	dls, err = q2.DeadLetters(t.Context())
	require.NoError(t, err)
	require.Len(t, dls, 1)
	require.NoError(t, q2.AckDeadLetter(t.Context(), dls[0]))
	dls, err = q2.DeadLetters(t.Context())
	require.NoError(t, err)
	assert.Empty(t, dls)
}

func TestTaskQueueDeadLetterRetainsOriginalFloorUntilCheckpoint(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	task, err := q.Enqueue(ctx, []byte("dead"), WithMaxAttempts(1))
	require.NoError(t, err)
	leased, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Nack(ctx, leased, WithRetryAfter(0)))

	// The uncheckpointed dead letter anchors its record, even at offset 0.
	floor, constrained, err := q.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, task.Offset, floor)

	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(ctx))
	cp := q.checkpointOffset
	q.mu.Unlock()

	floor, constrained, err = q.TruncateFloor(ctx)
	require.NoError(t, err)
	assert.True(t, constrained)
	assert.Equal(t, cp, floor)
}
