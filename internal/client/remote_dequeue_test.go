package client

import (
	"bufio"
	"context"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/server"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// newRemoteTaskQueue starts a live server on a loopback port and returns a remote handle for its "tasks" queue.
func newRemoteTaskQueue(tb testing.TB, opts ...Option) TaskQueue {
	tb.Helper()

	n, release, err := node.Acquire(tb.TempDir(), spec.Spec{Flush: spec.FlushSpec{MaxBytes: new(int64(1 << 20))}})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = release() })
	_, err = n.CreateTaskQueue(tb.Context(), "tasks", spec.Spec{})
	require.NoError(tb, err)

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	tb.Cleanup(srv.Stop)

	c, err := NewRemote("tcp://"+ln.Addr().String(), opts...)
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = c.Close() })

	tq, err := c.TaskQueue("tasks")
	require.NoError(tb, err)
	return tq
}

// frameWatcher returns a hook and a channel that closes the first time op is written.
func frameWatcher(op byte) (func(byte), <-chan struct{}) {
	ch := make(chan struct{})
	var once sync.Once
	return func(sent byte) {
		if sent == op {
			once.Do(func() { close(ch) })
		}
	}, ch
}

func TestRemoteDequeueBlocksUntilBudget(t *testing.T) {
	t.Parallel()
	// The server honors the client's long-poll budget; a modest budget proves the same property faster.
	const budget = 150 * time.Millisecond
	tq := newRemoteTaskQueue(t)

	ctx, cancel := context.WithTimeout(t.Context(), budget)
	defer cancel()

	start := time.Now()
	_, err := tq.Dequeue(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, elapsed, budget-20*time.Millisecond)
	assert.Less(t, elapsed, 5*time.Second)
}

func TestRemoteDequeueCancelReturnsPromptly(t *testing.T) {
	t.Parallel()
	hook, sent := frameWatcher(protocol.OpDequeue)
	tq := newRemoteTaskQueue(t, withFrameHook(hook))

	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()

	result := make(chan error, 1)
	go func() {
		_, err := tq.Dequeue(ctx)
		result <- err
	}()

	// The write happens in the calling goroutine, so this is race-free.
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue request never went out")
	}

	start := time.Now()
	cancel()
	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		assert.Less(t, time.Since(start), 2*time.Second)
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue did not return after cancel")
	}
}

func TestRemoteDequeueReturnsTask(t *testing.T) {
	t.Parallel()
	tq := newRemoteTaskQueue(t)

	task, err := tq.Enqueue(t.Context(), []byte("payload"))
	require.NoError(t, err)

	got, err := tq.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, []byte("payload"), got.Payload)
}

func TestRemoteDequeueSurfacesQueueClose(t *testing.T) {
	t.Parallel()

	n, release, err := node.Acquire(t.TempDir(), spec.Spec{Flush: spec.FlushSpec{MaxBytes: new(int64(1 << 20))}})
	require.NoError(t, err)
	t.Cleanup(func() { _ = release() })
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	t.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	t.Cleanup(srv.Stop)

	hook, sent := frameWatcher(protocol.OpDequeue)
	c, err := NewRemote("tcp://"+ln.Addr().String(), withFrameHook(hook))
	require.NoError(t, err)
	t.Cleanup(func() { _ = c.Close() })

	tq, err := c.TaskQueue("tasks")
	require.NoError(t, err)

	result := make(chan error, 1)
	go func() {
		_, err := tq.Dequeue(t.Context())
		result <- err
	}()

	// The server's handler is now parked on the queue, holding its connection slot.
	select {
	case <-sent:
	case <-time.After(2 * time.Second):
		t.Fatal("Dequeue request never went out")
	}

	// Releasing the node closes the queue and wakes the parked handler with ErrTaskClosed.
	require.NoError(t, release())

	select {
	case err := <-result:
		assert.ErrorIs(t, err, taskqueue.ErrTaskClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue did not return after node release")
	}
}

// TestRemoteNackSendsModeZero pins the inherit wire shape: Nack with no options sends mode 0 and no delay bytes.
func TestRemoteNackSendsModeZero(t *testing.T) {
	t.Parallel()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()

	type captured struct {
		mode     byte
		trailing bool
	}
	got := make(chan captured, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()

		r := bufio.NewReader(conn)
		hop, hbody, err := protocol.ReadFrame(r)
		if err != nil || hop != protocol.OpAuth {
			got <- captured{mode: 0xFF}
			return
		}
		if _, _, err := protocol.DecodeAuth(protocol.NewReader(hbody)); err != nil {
			got <- captured{mode: 0xFF}
			return
		}
		if err := protocol.WriteFrame(conn, protocol.OpAuth, nil); err != nil {
			got <- captured{mode: 0xFF}
			return
		}

		op, body, err := protocol.ReadFrame(r)
		if err != nil || op != protocol.OpNack {
			got <- captured{mode: 0xFF}
			return
		}
		pr := protocol.NewReader(body)
		if _, err := pr.ReadBytes(); err != nil {
			got <- captured{mode: 0xFF}
			return
		}
		if _, _, _, err := protocol.DecodeTaskRef(pr); err != nil {
			got <- captured{mode: 0xFF}
			return
		}
		mode, err := protocol.ReadMode(pr)
		if err != nil {
			got <- captured{mode: 0xFF}
			return
		}
		_, trailing := pr.ReadU64()
		got <- captured{mode: mode, trailing: trailing == nil}
		_ = protocol.WriteFrame(conn, protocol.OpNack, nil)
	}()

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	tq, err := c.TaskQueue("tasks")
	require.NoError(t, err)

	require.NoError(t, tq.Nack(t.Context(), taskqueue.ID("task-1")))
	select {
	case cap := <-got:
		assert.Equal(t, protocol.NackModeInherit, cap.mode)
		assert.False(t, cap.trailing, "inherit mode must not send a delay")
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw the Nack frame")
	}
}

// BenchmarkRemoteTaskDequeue measures one idle dequeue attempt with a 100 ms budget: one round trip, not a polling loop.
func BenchmarkRemoteTaskDequeue(b *testing.B) {
	tq := newRemoteTaskQueue(b)

	for b.Loop() {
		ctx, cancel := context.WithTimeout(b.Context(), 100*time.Millisecond)
		_, _ = tq.Dequeue(ctx)
		cancel()
	}
}

// BenchmarkRemoteTaskDequeueReady measures the hot path of a worker consuming an already-queued task.
func BenchmarkRemoteTaskDequeueReady(b *testing.B) {
	tq := newRemoteTaskQueue(b)

	for b.Loop() {
		b.StopTimer()
		_, err := tq.Enqueue(b.Context(), []byte("payload"))
		require.NoError(b, err)
		b.StartTimer()
		_, err = tq.Dequeue(b.Context())
		require.NoError(b, err)
	}
}
