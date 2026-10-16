package mikyu_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOpenLogGetOrCreate(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events", mikyu.WithMaxSegment(4096))
	require.NoError(t, err)
	off, err := q.Append(t.Context(), []byte("hello"))
	require.NoError(t, err)
	assert.Equal(t, uint64(0), off)

	again, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	assert.Equal(t, q, again, "OpenLog must return the same handle for an existing queue")
}

func TestOpenTaskGetOrCreate(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	task, err := q.Enqueue(t.Context(), []byte("x"))
	require.NoError(t, err)
	assert.NotEmpty(t, task.ID)

	again, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	assert.Equal(t, q, again)
}

func TestCreateLogRejectsDuplicate(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	_, err = c.CreateLog(t.Context(), "events")
	require.NoError(t, err)
	_, err = c.CreateLog(t.Context(), "events")
	require.ErrorIs(t, err, mikyu.ErrQueueExists)

	_, err = c.CreateTask(t.Context(), "jobs")
	require.NoError(t, err)
	_, err = c.CreateTask(t.Context(), "jobs")
	require.ErrorIs(t, err, mikyu.ErrQueueExists)
}

func TestResolveNeverCreates(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Log(t.Context(), "ghost")
	require.ErrorIs(t, err, mikyu.ErrQueueNotFound)
	_, err = c.Task(t.Context(), "ghost")
	require.ErrorIs(t, err, mikyu.ErrQueueNotFound)

	names, err := c.ListQueues(t.Context())
	require.NoError(t, err)
	assert.Empty(t, names, "resolve must not create queues")
}

func TestResolveWrongKind(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	_, err = c.CreateTask(t.Context(), "jobs")
	require.NoError(t, err)
	_, err = c.Log(t.Context(), "jobs")
	require.ErrorIs(t, err, mikyu.ErrWrongKind)

	_, err = c.CreateLog(t.Context(), "events")
	require.NoError(t, err)
	_, err = c.Task(t.Context(), "events")
	require.ErrorIs(t, err, mikyu.ErrWrongKind)
}

func TestResolveRemoteParity(t *testing.T) {
	target := newTestServer(t)
	c, err := mikyu.Dial(target)
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Log(t.Context(), "ghost")
	require.ErrorIs(t, err, mikyu.ErrQueueNotFound)

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	task, err := q.Enqueue(t.Context(), []byte("x"))
	require.NoError(t, err)

	opened, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	got, err := opened.TryDequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)

	_, err = c.CreateTask(t.Context(), "jobs")
	require.ErrorIs(t, err, mikyu.ErrQueueExists)
}

func TestOpenLogRemoteCreates(t *testing.T) {
	target := newTestServer(t)
	c, err := mikyu.Dial(target)
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	off, err := q.Append(t.Context(), []byte("hello"))
	require.NoError(t, err)
	rec, err := q.Read(t.Context(), off)
	require.NoError(t, err)
	assert.Equal(t, "hello", string(rec.Payload))
}

func TestGroupOffsetRoundTrip(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	for i := range 3 {
		_, err := q.Append(t.Context(), []byte{byte(i)})
		require.NoError(t, err)
	}

	g := q.Group("workers")
	assert.Equal(t, "workers", g.Name())
	g.Mark(1)
	require.NoError(t, g.Commit(t.Context()))
	committed, err := g.Committed(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), committed)

	require.NoError(t, g.Close())

	reopened, err := c.Log(t.Context(), "events")
	require.NoError(t, err)
	committed, err = reopened.Group("workers").Committed(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(2), committed, "committed offsets survive a handle drop")
}

func TestTaskRefStrictAck(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	_, err = q.Enqueue(t.Context(), []byte("x"))
	require.NoError(t, err)

	delivered, err := q.Dequeue(t.Context())
	require.NoError(t, err)
	require.NoError(t, q.Ack(t.Context(), delivered))
}

func TestTaskRefLooseAck(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	_, err = q.Enqueue(t.Context(), []byte("x"))
	require.NoError(t, err)

	delivered, err := q.Dequeue(t.Context())
	require.NoError(t, err)

	err = q.Ack(t.Context(), mikyu.TaskID(delivered.ID))
	require.NoError(t, err, "a bare TaskID acks without the strict stale-lease check")
}

func TestDequeueBlocksUntilContextDone(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 150*time.Millisecond)
	defer cancel()
	start := time.Now()
	_, err = q.Dequeue(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, time.Since(start), 100*time.Millisecond)
}

func TestDequeueReturnsArrivingTask(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)

	ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
	defer cancel()

	done := make(chan error, 1)
	go func() {
		task, err := q.Dequeue(ctx)
		if err == nil {
			err = q.Ack(ctx, task)
		}
		done <- err
	}()

	time.Sleep(50 * time.Millisecond)
	_, err = q.Enqueue(t.Context(), []byte("x"))
	require.NoError(t, err)
	require.NoError(t, <-done)
}

func TestRecordValueConversion(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events", mikyu.WithMaxPayload(1<<10))
	require.NoError(t, err)
	ts := time.Now().Round(0)
	off, err := q.Append(t.Context(), []byte("payload"), mikyu.WithRecordKey([]byte("k")))
	require.NoError(t, err)

	rec, err := q.Read(t.Context(), off)
	require.NoError(t, err)
	assert.Equal(t, off, rec.Offset)
	assert.NotEmpty(t, rec.ID)
	assert.False(t, rec.Timestamp.IsZero())
	assert.WithinDuration(t, ts, rec.Timestamp, time.Minute)
	assert.Equal(t, []byte("k"), rec.Group)
	assert.Equal(t, []byte("payload"), rec.Payload)
}

func TestTaskValueConversion(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	_, err = q.Enqueue(t.Context(), []byte("payload"), mikyu.WithKey([]byte("k")), mikyu.WithTaskMaxAttempts(3))
	require.NoError(t, err)

	task, err := q.TryDequeue(t.Context())
	require.NoError(t, err)
	assert.NotEmpty(t, task.ID)
	assert.NotEmpty(t, task.DeliveryID)
	assert.Equal(t, []byte("payload"), task.Payload)
	assert.Equal(t, []byte("k"), task.Key)
	assert.Equal(t, uint32(1), task.Attempts)
	assert.Equal(t, uint32(3), task.MaxAttempts)
	assert.False(t, task.Lease.IsZero())
}

func TestErrorPredicates(t *testing.T) {
	cases := []struct {
		name  string
		err   error
		check func(error) bool
		want  bool
	}{
		{"queue not found", mikyu.ErrQueueNotFound, mikyu.IsNotFound, true},
		{"task not found", mikyu.ErrTaskNotFound, mikyu.IsNotFound, true},
		{"dead letter not found", mikyu.ErrDeadLetterNotFound, mikyu.IsNotFound, true},
		{"offset not found is not IsNotFound", mikyu.ErrOffsetNotFound, mikyu.IsNotFound, false},
		{"task stale", mikyu.ErrTaskStale, mikyu.IsStale, true},
		{"dead letter stale", mikyu.ErrDeadLetterStale, mikyu.IsStale, true},
		{"queue closed", mikyu.ErrQueueClosed, mikyu.IsClosed, true},
		{"task closed", mikyu.ErrTaskClosed, mikyu.IsClosed, true},
		{"queue full", mikyu.ErrQueueFull, mikyu.IsFull, true},
		{"message too large", mikyu.ErrMessageTooLarge, mikyu.IsTooLarge, true},
		{"record too large", mikyu.ErrRecordTooLarge, mikyu.IsTooLarge, true},
		{"corrupt record", mikyu.ErrCorruptRecord, mikyu.IsCorrupt, true},
		{"checkpoint corrupt", mikyu.ErrCheckpointCorrupt, mikyu.IsCorrupt, true},
		{"wrong kind matches nothing", mikyu.ErrWrongKind, mikyu.IsNotFound, false},
		{"nil matches nothing", nil, mikyu.IsNotFound, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			assert.Equal(t, tc.want, tc.check(tc.err))
		})
	}
}

func TestIsNotFoundOverQueue(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	_, err = c.Log(t.Context(), "ghost")
	require.ErrorIs(t, err, mikyu.ErrQueueNotFound)
	assert.True(t, mikyu.IsNotFound(err))
}

type item struct {
	Name string `json:"name"`
	N    int    `json:"n"`
}

func TestTypedJSONRoundTrip(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	tq := mikyu.NewTyped(q, mikyu.JSONCodec[item]())

	off, err := tq.Append(t.Context(), item{Name: "a", N: 1})
	require.NoError(t, err)

	val, err := tq.Read(t.Context(), off)
	require.NoError(t, err)
	assert.Equal(t, item{Name: "a", N: 1}, val)

	offs, err := tq.AppendBatch(t.Context(), []item{{Name: "b", N: 2}, {Name: "c", N: 3}})
	require.NoError(t, err)
	assert.Equal(t, []uint64{off + 1, off + 2}, offs)

	vals, err := tq.ReadBatch(t.Context(), off, 3)
	require.NoError(t, err)
	assert.Equal(t, []item{{Name: "a", N: 1}, {Name: "b", N: 2}, {Name: "c", N: 3}}, vals)

	batch, err := tq.Fetch(t.Context(), off, 10, 0)
	require.NoError(t, err)
	assert.Len(t, batch, 3)

	last, err := tq.LastOffset(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(3), last)

	minOff, err := tq.MinOffset(t.Context())
	require.NoError(t, err)
	assert.Equal(t, uint64(0), minOff)
}

func TestTypedStreamAndTail(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	raw, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	tq := mikyu.NewTyped(raw, mikyu.JSONCodec[item]())

	for i := range 3 {
		_, err := tq.Append(t.Context(), item{Name: "e", N: i})
		require.NoError(t, err)
	}

	var seen []item
	for val, err := range tq.Stream(t.Context(), 0) {
		require.NoError(t, err)
		seen = append(seen, val)
	}
	assert.Equal(t, []item{{Name: "e", N: 0}, {Name: "e", N: 1}, {Name: "e", N: 2}}, seen)

	tailCtx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()
	var tailed []item
	for val, err := range tq.Tail(tailCtx, 0) {
		require.NoError(t, err)
		tailed = append(tailed, val)
		if len(tailed) == 3 {
			break
		}
	}
	assert.Equal(t, seen, tailed)
}

func TestTypedStreamSurfacesDecodeError(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	raw, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	tq := mikyu.NewTyped(raw, mikyu.JSONCodec[item]())

	_, err = raw.Append(t.Context(), []byte("not json"))
	require.NoError(t, err)

	encountered := false
	for _, err := range tq.Stream(t.Context(), 0) {
		require.Error(t, err)
		encountered = true
		break
	}
	assert.True(t, encountered, "a decode failure must surface as the iteration error")
}

func TestTypedTaskFlow(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	tq := mikyu.NewTypedTask(q, mikyu.JSONCodec[item]())

	task, err := tq.Enqueue(t.Context(), item{Name: "job", N: 7})
	require.NoError(t, err)
	assert.NotEmpty(t, task.ID)

	delivered, err := tq.TryDequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, item{Name: "job", N: 7}, delivered.Value)
	assert.Equal(t, task.ID, delivered.Task.ID)

	require.NoError(t, tq.Ack(t.Context(), delivered))

	_, err = q.TryDequeue(t.Context())
	require.ErrorIs(t, err, mikyu.ErrTaskNotAvailable)
}

func TestTypedTaskDecodeError(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	tq := mikyu.NewTypedTask(q, mikyu.JSONCodec[item]())

	_, err = q.Enqueue(t.Context(), []byte("not json"))
	require.NoError(t, err)

	_, err = tq.TryDequeue(t.Context())
	require.Error(t, err)
}

func TestTypedQueueEscapeHatch(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	tq := mikyu.NewTyped(q, mikyu.JSONCodec[item]())
	assert.Equal(t, "events", tq.Queue().Name())

	jobs, err := c.OpenTask(t.Context(), "jobs")
	require.NoError(t, err)
	tq2 := mikyu.NewTypedTask(jobs, mikyu.JSONCodec[item]())
	assert.Equal(t, "jobs", tq2.Queue().Name())
}

func TestHandleStabilityAcrossAccessors(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	created, err := c.CreateLog(t.Context(), "events")
	require.NoError(t, err)

	resolved, err := c.Log(t.Context(), "events")
	require.NoError(t, err)
	opened, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)
	assert.Equal(t, created, resolved)
	assert.Equal(t, created, opened)

	var wg sync.WaitGroup
	const n = 8
	handles := make([]mikyu.LogQueue, n)
	for i := range n {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			q, err := c.Log(t.Context(), "events")
			assert.NoError(t, err)
			handles[i] = q
		}(i)
	}
	wg.Wait()
	for i := range n {
		assert.Equal(t, created, handles[i])
	}
}

func TestCapabilityInterfacesSatisfied(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	var _ mikyu.QueueAccess = c
	var _ mikyu.QueueAdmin = c
	var _ mikyu.Telemetry = c
	var _ mikyu.Client = c
}

func TestTruncateViaAdmin(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events", mikyu.WithMaxSegment(256), mikyu.WithMaxPayload(128))
	require.NoError(t, err)
	for range 8 {
		_, err := q.Append(t.Context(), []byte("payload"))
		require.NoError(t, err)
	}

	reclaimed, err := c.Truncate(t.Context(), "events", 5)
	require.NoError(t, err)
	assert.Greater(t, reclaimed, int64(0))

	_, err = q.Read(t.Context(), 0)
	require.ErrorIs(t, err, mikyu.ErrOffsetTruncated)
}

func TestContextCancellationPropagates(t *testing.T) {
	c, err := mikyu.Open(t.TempDir())
	require.NoError(t, err)
	defer c.Close()

	q, err := c.OpenLog(t.Context(), "events")
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err = q.Fetch(ctx, 0, 10, time.Second)
	require.Error(t, err)
	assert.True(t, errors.Is(err, context.Canceled))
}
