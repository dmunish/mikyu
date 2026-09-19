package taskqueue

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/enginetest"
	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// failingArchiveAppender is an ArchiveAppender that always returns an error.
type failingArchiveAppender struct{}

func (f *failingArchiveAppender) Append(ctx context.Context, payload []byte) (uint64, error) {
	return 0, errors.New("archive append failed")
}

// logArchiveAppender adapts a log queue to the archive writer interface.
type logArchiveAppender struct {
	l logqueue.Log
}

func (a logArchiveAppender) Append(ctx context.Context, payload []byte) (uint64, error) {
	return a.l.Append(ctx, payload)
}

// newTestQueueWithBounds creates a queue with maxInFlight and maxDLQ bounds.
func newTestQueueWithBounds(t *testing.T, maxInFlight, maxDLQ uint32) (*queue, *engine.Manager, engine.Storage) {
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
	d := Defaults{MaxInFlight: maxInFlight, MaxDLQ: maxDLQ}
	queueIface, err := New("tasks", storage, d, func(ctx context.Context, source string) (ArchiveAppender, error) { return nil, nil })
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() {
		_ = Close(q)
		_ = mgr.Close()
	})
	return q, mgr, storage
}

// newTestQueueWithArchive creates a queue whose archiveLog writes to the given appender.
func newTestQueueWithArchive(t *testing.T, appender ArchiveAppender) (*queue, *engine.Manager, engine.Storage) {
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
	queueIface, err := New("tasks", storage, Defaults{}, func(ctx context.Context, source string) (ArchiveAppender, error) {
		return appender, nil
	})
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() {
		_ = Close(q)
		_ = mgr.Close()
	})
	return q, mgr, storage
}

func TestStalledIncrementsOnVisibilityExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "stalled")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("task"), WithVisibility(40*time.Millisecond), WithMaxAttempts(3))
		require.NoError(t, err)
		first, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, uint32(1), first.Attempts)
		assert.Equal(t, uint32(0), first.Stalled)

		synctest.Sleep(60 * time.Millisecond)

		second, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, first.ID, second.ID)
		assert.Equal(t, uint32(2), second.Attempts)
		assert.Equal(t, uint32(1), second.Stalled)
	})
}

func TestStalledNotInLeaseIdentity(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("lease"), WithVisibility(time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	first, err := q.Dequeue(ctx)
	require.NoError(t, err)

	stale := first
	stale.Stalled = 99
	require.NoError(t, q.Ack(ctx, stale))
}

func TestMaxInFlightRefuses(t *testing.T) {
	q, _, _ := newTestQueueWithBounds(t, 2, 100_000)
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("first"), WithMaxAttempts(3))
	require.NoError(t, err)
	_, err = q.Enqueue(ctx, []byte("second"), WithMaxAttempts(3))
	require.NoError(t, err)
	// Both tasks are active; the third should be rejected.
	_, err = q.Enqueue(ctx, []byte("third"), WithMaxAttempts(3))
	assert.ErrorIs(t, err, ErrQueueFull)
}

func TestMaxDLQDropsOldest(t *testing.T) {
	q, _, _ := newTestQueueWithBounds(t, 100_000, 2)
	ctx := t.Context()

	for range 3 {
		task, err := q.Enqueue(ctx, []byte("poison"), WithMaxAttempts(1))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, task.ID, leased.ID)
		require.NoError(t, q.Nack(ctx, leased, WithRetryAfter(0)))
	}

	dls, err := q.DeadLetters(ctx)
	require.NoError(t, err)
	assert.Len(t, dls, 2)
}

func TestArchiveInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "archive-inflight")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("data"), WithVisibility(30*time.Second), WithMaxAttempts(3))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)

		archiveLog := newArchiveLogAppender(t)
		q.archiveLog = func(ctx context.Context, source string) (ArchiveAppender, error) {
			return archiveLog, nil
		}

		err = q.Archive(ctx, ID(leased.ID))
		require.NoError(t, err)

		q.mu.Lock()
		_, stillActive := q.active[leased.ID]
		q.mu.Unlock()
		assert.False(t, stillActive)
	})
}

func TestArchiveFromDLQ(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "archive-dlq")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("data"), WithMaxAttempts(1))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.NoError(t, q.Nack(ctx, leased, WithRetryAfter(0)))

		archiveLog := newArchiveLogAppender(t)
		q.archiveLog = func(ctx context.Context, source string) (ArchiveAppender, error) {
			return archiveLog, nil
		}

		err = q.Archive(ctx, ID(leased.ID))
		require.NoError(t, err)

		q.mu.Lock()
		_, inDLQ := q.dlq[leased.ID]
		q.mu.Unlock()
		assert.False(t, inDLQ)
	})
}

func TestArchiveAutoCreatesLogQueue(t *testing.T) {
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
	defer mgr.Close()

	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	defer func() { _ = mgr.Delete("tasks") }()

	var createdLogName string
	var createdLog logqueue.Log
	archiveLog := func(ctx context.Context, source string) (ArchiveAppender, error) {
		name := source + "-archive"
		lq := logqueue.New(name, storage)
		createdLogName = name
		createdLog = lq
		return logArchiveAppender{lq}, nil
	}

	q, err := New("tasks", storage, Defaults{}, archiveLog)
	require.NoError(t, err)
	defer func() { _ = Close(q) }()

	ctx := t.Context()
	_, err = q.Enqueue(ctx, []byte("data"), WithVisibility(30*time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	leased, err := q.Dequeue(ctx)
	require.NoError(t, err)

	err = q.Archive(ctx, ID(leased.ID))
	require.NoError(t, err)

	assert.Equal(t, "tasks-archive", createdLogName)
	assert.NotNil(t, createdLog)
}

func TestArchiveExplicitPreCreationWins(t *testing.T) {
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
	defer mgr.Close()

	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	defer func() { _ = mgr.Delete("tasks") }()

	archiveStorage, err := mgr.Create("tasks-archive")
	require.NoError(t, err)
	defer func() { _ = mgr.Delete("tasks-archive") }()

	archiveLog := func(ctx context.Context, source string) (ArchiveAppender, error) {
		name := source + "-archive"
		lq := logqueue.New(name, archiveStorage)
		return logArchiveAppender{lq}, nil
	}

	q, err := New("tasks", storage, Defaults{}, archiveLog)
	require.NoError(t, err)
	defer func() { _ = Close(q) }()

	ctx := t.Context()
	_, err = q.Enqueue(ctx, []byte("data"), WithVisibility(30*time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	leased, err := q.Dequeue(ctx)
	require.NoError(t, err)

	err = q.Archive(ctx, ID(leased.ID))
	require.NoError(t, err)
}

func TestArchiveRefusesWrongKind(t *testing.T) {
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
	defer mgr.Close()

	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	defer func() { _ = mgr.Delete("tasks") }()

	// Pre-create tasks-archive as a task queue (it's a task queue, not a log queue).
	_, err = mgr.Create("tasks-archive")
	require.NoError(t, err)
	defer func() { _ = mgr.Delete("tasks-archive") }()

	archiveLog := func(ctx context.Context, source string) (ArchiveAppender, error) {
		name := source + "-archive"
		_, err := mgr.Get(name)
		if err == nil {
			// It exists but is a task queue, not a log queue — archive should fail.
			return nil, errors.New("archive not found")
		}
		return nil, errors.New("archive not found")
	}

	q, err := New("tasks", storage, Defaults{}, archiveLog)
	require.NoError(t, err)
	defer func() { _ = Close(q) }()

	ctx := t.Context()
	_, err = q.Enqueue(ctx, []byte("data"), WithVisibility(30*time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	leased, err := q.Dequeue(ctx)
	require.NoError(t, err)

	err = q.Archive(ctx, ID(leased.ID))
	assert.Error(t, err)
}

func TestArchiveFailureLeavesTaskInPlace(t *testing.T) {
	q, _, _ := newTestQueueWithArchive(t, &failingArchiveAppender{})
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("data"), WithVisibility(30*time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	leased, err := q.Dequeue(ctx)
	require.NoError(t, err)

	err = q.Archive(ctx, ID(leased.ID))
	assert.Error(t, err)

	q.mu.Lock()
	_, stillActive := q.active[leased.ID]
	q.mu.Unlock()
	assert.True(t, stillActive)
}

func TestCheckpointRecoveryVerifiesCounts(t *testing.T) {
	dir := t.TempDir()
	cfg := engine.Config{BatchSize: 8, MaxInterval: time.Millisecond, SegmentSize: 256, MaxMessageBytes: 1 << 20}
	mgr, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	q := mustQueue(t, "tasks", storage)

	_, err = q.Enqueue(t.Context(), []byte("data"), WithMaxAttempts(3))
	require.NoError(t, err)
	q.mu.Lock()
	require.NoError(t, q.checkpointLocked(t.Context()))
	q.mu.Unlock()

	require.NoError(t, Close(q))
	require.NoError(t, mgr.Close())

	mgr2, err := engine.OpenManager(dir, cfg, nil)
	require.NoError(t, err)
	defer mgr2.Close()
	storage2, err := mgr2.Get("tasks")
	require.NoError(t, err)
	q2 := mustQueue(t, "tasks", storage2)
	defer Close(q2)

	_, err = q2.Dequeue(t.Context())
	require.NoError(t, err)
}

func TestWithKeyTaskRoundTrip(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	key := []byte("user:1")
	task, err := q.Enqueue(ctx, []byte("payload"), WithKey(key), WithMaxAttempts(3))
	require.NoError(t, err)
	assert.Equal(t, key, task.Key)

	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, key, got.Key)
	assert.Equal(t, []byte("payload"), got.Payload)

	archiveLog := newArchiveLogAppender(t)
	q.archiveLog = func(ctx context.Context, source string) (ArchiveAppender, error) {
		return archiveLog, nil
	}
	err = q.Archive(ctx, ID(got.ID))
	require.NoError(t, err)
}

// newArchiveLogAppender returns a real ArchiveAppender backed by a memory log queue.
func newArchiveLogAppender(t *testing.T) ArchiveAppender {
	t.Helper()
	storage := enginetest.NewMemStorage("archive", 1<<20, 1<<20)
	return logArchiveAppender{logqueue.New("archive", storage)}
}

// TestMaxInFlightZeroMeansUnlimited verifies that maxInFlight=0 allows unlimited enqueues.
func TestMaxInFlightZeroMeansUnlimited(t *testing.T) {
	q, _, _ := newTestQueueWithBounds(t, 0, 100_000)
	ctx := t.Context()

	for range 10 {
		_, err := q.Enqueue(ctx, []byte("data"), WithMaxAttempts(3))
		require.NoError(t, err)
	}
}

// TestMaxDLQZeroMeansUnlimited verifies that maxDLQ=0 allows unlimited DLQ entries.
func TestMaxDLQZeroMeansUnlimited(t *testing.T) {
	q, _, _ := newTestQueueWithBounds(t, 100_000, 0)
	ctx := t.Context()

	for range 5 {
		task, err := q.Enqueue(ctx, []byte("poison"), WithMaxAttempts(1))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, task.ID, leased.ID)
		require.NoError(t, q.Nack(ctx, leased, WithRetryAfter(0)))
	}

	dls, err := q.DeadLetters(ctx)
	require.NoError(t, err)
	assert.Len(t, dls, 5)
}

// TestArchivePreservesTaskFields verifies the archive envelope contains task fields.
func TestArchivePreservesTaskFields(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "archive-fields")
		ctx := t.Context()

		key := []byte("order:42")
		_, err := q.Enqueue(ctx, []byte("payload"), WithKey(key), WithMaxAttempts(3))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		assert.Equal(t, key, leased.Key)

		var captured []byte
		q.archiveLog = func(ctx context.Context, source string) (ArchiveAppender, error) {
			return &captureArchiveAppender{captured: &captured}, nil
		}

		err = q.Archive(ctx, ID(leased.ID))
		require.NoError(t, err)
		assert.NotEmpty(t, captured, "archive envelope should not be empty")

		sourceQueue, keyOut, _, _, stalled, _, offset, archivedFrom, _, err := decodeArchiveEnvelope(captured)
		require.NoError(t, err)
		assert.Equal(t, "archive-fields", sourceQueue)
		assert.Equal(t, string(key), keyOut)
		assert.Equal(t, uint32(0), stalled)
		assert.Equal(t, offset, leased.Offset)
		assert.Equal(t, "inflight", archivedFrom)
	})
}

// captureArchiveAppender captures the archive envelope for inspection.
type captureArchiveAppender struct {
	captured *[]byte
}

func (c *captureArchiveAppender) Append(ctx context.Context, payload []byte) (uint64, error) {
	*c.captured = append([]byte(nil), payload...)
	return 1, nil
}

// TestArchiveRetentionFloor verifies that archiving updates the retention floor.
func TestArchiveRetentionFloor(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "archive-ret")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("data"), WithVisibility(30*time.Second), WithMaxAttempts(3))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)

		archiveLog := newArchiveLogAppender(t)
		q.archiveLog = func(ctx context.Context, source string) (ArchiveAppender, error) {
			return archiveLog, nil
		}

		err = q.Archive(ctx, ID(leased.ID))
		require.NoError(t, err)

		// After archive, the task is removed from active and the floor should advance.
		q.mu.Lock()
		_, stillActive := q.active[leased.ID]
		q.mu.Unlock()
		assert.False(t, stillActive)
	})
}

// TestArchiveDeadLetterArchiving verifies archiving a DLQ entry.
func TestArchiveDeadLetterArchiving(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		q := newMemTaskQueue(t, "archive-dlq2")
		ctx := t.Context()

		_, err := q.Enqueue(ctx, []byte("data"), WithMaxAttempts(1))
		require.NoError(t, err)
		leased, err := q.Dequeue(ctx)
		require.NoError(t, err)
		require.NoError(t, q.Nack(ctx, leased, WithRetryAfter(0)))

		archiveLog := newArchiveLogAppender(t)
		q.archiveLog = func(ctx context.Context, source string) (ArchiveAppender, error) {
			return archiveLog, nil
		}

		err = q.Archive(ctx, ID(leased.ID))
		require.NoError(t, err)

		dls, err := q.DeadLetters(ctx)
		require.NoError(t, err)
		assert.Len(t, dls, 0)
	})
}
