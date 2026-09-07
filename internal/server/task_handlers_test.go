package server

import (
	"context"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/protocol"
	"github.com/dmunish/mikyu/internal/taskqueue"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func taskBody(name string, payload []byte) []byte {
	var b protocol.Buffer
	b.WriteBytes([]byte(name))
	b.WriteBytes(payload)
	b.WriteU32(3)
	b.WriteU64(uint64(int64(time.Second)))
	b.WriteU64(uint64(time.Now().UnixNano()))
	return b.Bytes()
}

func TestTaskHandlersLifecycle(t *testing.T) {
	mgr, err := engine.OpenManager(t.TempDir(), engine.Config{MaxMessageBytes: 1 << 20})
	require.NoError(t, err)
	defer mgr.Close()
	_, err = mgr.CreateQueue("tasks")
	require.NoError(t, err)
	s := New(mgr)

	op, body := s.handleTaskEnqueue(context.Background(), taskBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpTaskEnqueue, op)
	created, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	var dequeueBody protocol.Buffer
	dequeueBody.WriteBytes([]byte("tasks"))
	op, body = s.handleTaskDequeue(context.Background(), dequeueBody.Bytes())
	require.Equal(t, protocol.OpTaskDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, created.ID, leased.ID)
	assert.Equal(t, uint32(1), leased.Attempts)

	var ackBody protocol.Buffer
	ackBody.WriteBytes([]byte("tasks"))
	protocol.EncodeTask(&ackBody, leased)
	op, _ = s.handleTaskAck(context.Background(), ackBody.Bytes())
	assert.Equal(t, protocol.OpTaskAck, op)

	op, body = s.handleTaskDequeue(context.Background(), dequeueBody.Bytes())
	assert.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTaskNotAvailable, code)
}

func TestTaskHandlersStaleLeaseError(t *testing.T) {
	mgr, err := engine.OpenManager(t.TempDir(), engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()
	_, err = mgr.CreateQueue("tasks")
	require.NoError(t, err)
	s := New(mgr)

	op, body := s.handleTaskEnqueue(context.Background(), taskBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpTaskEnqueue, op)
	first, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	var b protocol.Buffer
	b.WriteBytes([]byte("tasks"))
	op, body = s.handleTaskDequeue(context.Background(), b.Bytes())
	require.Equal(t, protocol.OpTaskDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	var ack protocol.Buffer
	ack.WriteBytes([]byte("tasks"))
	// The pre-delivery task has the same ID but stale Attempts/Visibility.
	protocol.EncodeTask(&ack, first)
	op, body = s.handleTaskAck(context.Background(), ack.Bytes())
	assert.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTaskStale, code)
	assert.Equal(t, uint32(1), leased.Attempts)
}

func TestTaskQueueHandleCaching(t *testing.T) {
	mgr, err := engine.OpenManager(t.TempDir(), engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()
	_, err = mgr.CreateQueue("tasks")
	require.NoError(t, err)
	s := New(mgr)

	first, err := s.getOrCreateTaskQueue("tasks")
	require.NoError(t, err)
	second, err := s.getOrCreateTaskQueue("tasks")
	require.NoError(t, err)
	assert.Same(t, first, second)

	const n = 32
	results := make(chan *taskqueue.TaskQueue, n)
	for i := 0; i < n; i++ {
		go func() {
			q, err := s.getOrCreateTaskQueue("tasks")
			if err != nil {
				t.Errorf("getOrCreateTaskQueue: %v", err)
				return
			}
			results <- q
		}()
	}
	for i := 0; i < n; i++ {
		assert.Same(t, first, <-results)
	}
}

func TestTaskHandlersExtendLease(t *testing.T) {
	mgr, err := engine.OpenManager(t.TempDir(), engine.Config{})
	require.NoError(t, err)
	defer mgr.Close()
	_, err = mgr.CreateQueue("tasks")
	require.NoError(t, err)
	s := New(mgr)

	op, body := s.handleTaskEnqueue(context.Background(), taskBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpTaskEnqueue, op)
	_, err = protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	var dequeue protocol.Buffer
	dequeue.WriteBytes([]byte("tasks"))
	op, body = s.handleTaskDequeue(context.Background(), dequeue.Bytes())
	require.Equal(t, protocol.OpTaskDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	var ext protocol.Buffer
	ext.WriteBytes([]byte("tasks"))
	protocol.EncodeTask(&ext, leased)
	ext.WriteU64(uint64(int64(250 * time.Millisecond)))
	op, body = s.handleTaskExtend(context.Background(), ext.Bytes())
	assert.Equal(t, protocol.OpTaskExtend, op)
	assert.Empty(t, body)
}
