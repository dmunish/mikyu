package client

import (
	"bufio"
	"context"
	"net"
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

// newRemoteTaskQueue starts a live server on a loopback port and returns a
// remote handle for its "tasks" queue.
func newRemoteTaskQueue(tb testing.TB) TaskQueue {
	tb.Helper()

	n, release, err := node.Acquire(tb.TempDir(), spec.Spec{
		Engine: spec.EngineSpec{MaxMessageBytes: new(int64(1 << 20))},
	})
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = release() })
	_, err = n.Manager().CreateQueue("tasks")
	require.NoError(tb, err)

	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = ln.Close() })
	go srv.Serve(ln)
	tb.Cleanup(srv.Stop)

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(tb, err)
	tb.Cleanup(func() { _ = c.Close() })

	tq, err := c.TaskQueue("tasks")
	require.NoError(tb, err)
	return tq
}

func TestRemoteDequeueBlocksUntilBudget(t *testing.T) {
	tq := newRemoteTaskQueue(t)

	// An empty queue with a 200 ms budget must block for roughly the whole
	// budget (server-side long-poll), not return after the old 10 ms pass.
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()

	start := time.Now()
	_, err := tq.Dequeue(ctx)
	elapsed := time.Since(start)

	require.ErrorIs(t, err, context.DeadlineExceeded)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(190))
	assert.Less(t, elapsed.Milliseconds(), int64(5000))
}

func TestRemoteDequeueCancelReturnsPromptly(t *testing.T) {
	tq := newRemoteTaskQueue(t)

	ctx, cancel := context.WithCancel(context.Background())
	result := make(chan error, 1)
	go func() {
		_, err := tq.Dequeue(ctx)
		result <- err
	}()

	time.Sleep(150 * time.Millisecond)
	cancel()
	start := time.Now()

	select {
	case err := <-result:
		require.ErrorIs(t, err, context.Canceled)
		// Cancellation must interrupt the socket read, not wait out the
		// 30 s long-poll window.
		assert.Less(t, time.Since(start).Milliseconds(), int64(2000))
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue did not return after cancel")
	}
}

func TestRemoteDequeueReturnsTask(t *testing.T) {
	tq := newRemoteTaskQueue(t)

	task, err := tq.Enqueue(t.Context(), []byte("payload"))
	require.NoError(t, err)

	got, err := tq.Dequeue(t.Context())
	require.NoError(t, err)
	assert.Equal(t, task.ID, got.ID)
	assert.Equal(t, []byte("payload"), got.Payload)
}

func TestRemoteDequeueSurfacesQueueClose(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{
		Engine: spec.EngineSpec{MaxMessageBytes: new(int64(1 << 20))},
	})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.Manager().CreateQueue("tasks")
	require.NoError(t, err)
	srv := server.New(n)
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	defer ln.Close()
	go srv.Serve(ln)
	defer srv.Stop()

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	tq, err := c.TaskQueue("tasks")
	require.NoError(t, err)

	// A worker parked on an infinite Dequeue must be released when the node
	// shuts the queue down.
	result := make(chan error, 1)
	go func() {
		_, err := tq.Dequeue(context.Background())
		result <- err
	}()
	time.Sleep(150 * time.Millisecond)
	require.NoError(t, c.Close())
	srv.Stop()
	// Stop only closes the listener; the node's final release is what
	// closes the queue and wakes the parked dequeue (release is idempotent,
	// so the deferred release below stays a no-op).
	_ = release()

	select {
	case err := <-result:
		assert.ErrorIs(t, err, taskqueue.ErrTaskClosed)
	case <-time.After(5 * time.Second):
		t.Fatal("Dequeue did not return after server stop")
	}
}

// BenchmarkRemoteTaskDequeue measures one idle dequeue attempt with a 100 ms
// budget. The old 10 ms polling loop spent roughly ten request round trips
// per attempt; long-poll spends exactly one.
func BenchmarkRemoteTaskDequeue(b *testing.B) {
	tq := newRemoteTaskQueue(b)

	for b.Loop() {
		ctx, cancel := context.WithTimeout(b.Context(), 100*time.Millisecond)
		_, _ = tq.Dequeue(ctx)
		cancel()
	}
}

// BenchmarkRemoteTaskDequeueReady measures the hot path of a worker consuming
// a task that is already queued: one round trip, no polling delay.
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

// TestRemoteNackSendsModeZero pins the inherit wire shape: Nack with no options
// sends mode 0 and no delay bytes, leaving the backoff to the server.
func TestRemoteNackSendsModeZero(t *testing.T) {
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
		if _, err := pr.ReadBytes(); err != nil { // queue name
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
		_, trailing := pr.ReadU64() // inherit mode leaves nothing to read
		got <- captured{mode: mode, trailing: trailing == nil}
		_ = protocol.WriteFrame(conn, protocol.OpNack, nil)
	}()

	c, err := NewRemote("tcp://" + ln.Addr().String())
	require.NoError(t, err)
	defer c.Close()
	tq, err := c.TaskQueue("tasks")
	require.NoError(t, err)

	require.NoError(t, tq.Nack(t.Context(), taskqueue.TaskID("task-1")))
	select {
	case cap := <-got:
		assert.Equal(t, protocol.NackModeInherit, cap.mode)
		assert.False(t, cap.trailing, "inherit mode must not send a delay")
	case <-time.After(2 * time.Second):
		t.Fatal("server never saw the Nack frame")
	}
}
