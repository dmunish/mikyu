package taskqueue

import (
	"context"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newTaskQueueWithDefaults builds a queue with explicit per-queue defaults.
func newTaskQueueWithDefaults(t *testing.T, d Defaults) *queue {
	t.Helper()
	dir := t.TempDir()
	mgr, err := engine.OpenManager(dir, engine.Config{
		Durability:    spec.DurabilityAsync,
		MaxSegment:    16 * 1024,
		MaxSegmentAge: time.Hour,
		MaxPayload:    2 * 1024 * 1024,
		MaxCache:      32 << 20,
		MaxFlushBytes: 1 << 20,
		MaxFlushWait:  time.Millisecond,
	}, nil)
	require.NoError(t, err)
	storage, err := mgr.Create("tasks")
	require.NoError(t, err)
	queueIface, err := New("tasks", storage, d, func(ctx context.Context, source string) (ArchiveAppender, error) { return nil, nil })
	require.NoError(t, err)
	q := queueIface.(*queue)
	t.Cleanup(func() {
		_ = q.Close()
		_ = mgr.Close()
	})
	return q
}

func TestQueueDefaultsApply(t *testing.T) {
	q := newTaskQueueWithDefaults(t, Defaults{Visibility: time.Minute, MaxAttempts: 3})
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("defaults"))
	require.NoError(t, err)
	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(3), got.MaxAttempts)
	left := time.Until(got.Visibility)
	assert.GreaterOrEqual(t, left, 55*time.Second, "lease should be ~1m out")
	assert.LessOrEqual(t, left, time.Minute+5*time.Second)
}

func TestPerCallOverridesQueueDefault(t *testing.T) {
	q := newTaskQueueWithDefaults(t, Defaults{Visibility: time.Minute, MaxAttempts: 3})
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("override"), WithLease(10*time.Second), WithMaxAttempts(2))
	require.NoError(t, err)
	got, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), got.MaxAttempts)
	left := time.Until(got.Visibility)
	assert.GreaterOrEqual(t, left, 5*time.Second, "lease should be ~10s out")
	assert.LessOrEqual(t, left, 15*time.Second)
}

func TestDefaultsUpdateAfterUpdateQueue(t *testing.T) {
	q := newTaskQueueWithDefaults(t, Defaults{Visibility: time.Minute, MaxAttempts: 5})
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("before"))
	require.NoError(t, err)
	before, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(5), before.MaxAttempts)

	q.SetDefaults(Defaults{Visibility: 30 * time.Second, MaxAttempts: 2})

	_, err = q.Enqueue(ctx, []byte("after"))
	require.NoError(t, err)
	after, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, uint32(2), after.MaxAttempts)
	left := time.Until(after.Visibility)
	assert.GreaterOrEqual(t, left, 25*time.Second, "lease should be ~30s out")
	assert.LessOrEqual(t, left, 35*time.Second)
}

func TestAckViaRef(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("ref"), WithLease(time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	got, err := q.Dequeue(ctx)
	require.NoError(t, err)

	stale := got
	stale.DeliveryID = "stale"
	assert.ErrorIs(t, q.Ack(ctx, stale), ErrTaskStale)

	require.NoError(t, q.Ack(ctx, ID(got.ID)))

	_, err = q.TryDequeue(ctx)
	assert.ErrorIs(t, err, ErrTaskNotAvailable)
}

func TestNackViaRef(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("nack-ref"), WithLease(time.Second), WithMaxAttempts(3))
	require.NoError(t, err)
	got, err := q.Dequeue(ctx)
	require.NoError(t, err)

	stale := got
	stale.Attempts = 99
	assert.ErrorIs(t, q.Nack(ctx, stale, WithRetryAfter(0)), ErrTaskStale)

	require.NoError(t, q.Nack(ctx, ID(got.ID), WithRetryAfter(0)))
	redelivered, err := q.Dequeue(ctx)
	require.NoError(t, err)
	assert.Equal(t, got.ID, redelivered.ID)
	assert.Equal(t, uint32(2), redelivered.Attempts)
}

func TestExtendViaRef(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	_, err := q.Enqueue(ctx, []byte("extend-ref"), WithLease(50*time.Millisecond), WithMaxAttempts(3))
	require.NoError(t, err)
	got, err := q.Dequeue(ctx)
	require.NoError(t, err)

	require.NoError(t, q.Extend(ctx, ID(got.ID), 2*time.Second))

	stale := got
	stale.DeliveryID = "stale"
	assert.ErrorIs(t, q.Extend(ctx, stale, 2*time.Second), ErrTaskStale)

	// Extend does not change the delivery, so the original Task ref still acks.
	require.NoError(t, q.Ack(ctx, got))
}

func TestDeadLetterAckViaRef(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	created, err := q.Enqueue(ctx, []byte("dl-ref"), WithLease(time.Second), WithMaxAttempts(1))
	require.NoError(t, err)
	leased, err := q.Dequeue(ctx)
	require.NoError(t, err)
	require.NoError(t, q.Nack(ctx, ID(leased.ID), WithRetryAfter(0)))

	dls, err := q.DeadLetters(ctx)
	require.NoError(t, err)
	require.Len(t, dls, 1)

	stale := dls[0]
	stale.Offset = dls[0].Offset + 1
	assert.ErrorIs(t, q.AckDeadLetter(ctx, stale), ErrDeadLetterStale)
	require.NoError(t, q.AckDeadLetter(ctx, ID(created.ID)))

	dls, err = q.DeadLetters(ctx)
	require.NoError(t, err)
	assert.Empty(t, dls)
}

func TestTryDequeueNonBlocking(t *testing.T) {
	q, _, _ := newTestTaskQueue(t)
	ctx := t.Context()

	start := time.Now()
	_, err := q.TryDequeue(ctx)
	assert.ErrorIs(t, err, ErrTaskNotAvailable)
	assert.Less(t, time.Since(start), 50*time.Millisecond)
}

func TestNoIDMethods(t *testing.T) {
	it := reflect.TypeFor[Queue]()
	for method := range it.Methods() {
		name := method.Name
		assert.False(t, strings.HasSuffix(name, "ID"), "Queue must not expose %s", name)
	}
}

func TestNoCloseOnDriver(t *testing.T) {
	it := reflect.TypeFor[Queue]()
	for method := range it.Methods() {
		assert.NotEqual(t, "Close", method.Name)
	}
}
