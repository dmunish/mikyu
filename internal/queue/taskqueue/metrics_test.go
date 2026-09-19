package taskqueue

import (
	"context"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestTaskQueueCounters(t *testing.T) {
	dir := t.TempDir()
	reg := metrics.NewRegistry(metrics.Config{Histograms: true})
	mgr, err := engine.OpenManager(dir, engine.Config{BatchSize: 8, MaxInterval: time.Millisecond}, nil, reg)
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
