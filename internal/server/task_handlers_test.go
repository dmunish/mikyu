package server

import (
	"bufio"
	"net"
	"testing"
	"time"

	"github.com/dmunish/mikyu/internal/node"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/spec"
	"github.com/dmunish/mikyu/internal/transport/protocol"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// enqueueBody builds an Enqueue request:
// bytes(name) bytes(payload) u64 lease u32 maxAttempts u64 scheduledAt.
func enqueueBody(name string, payload []byte) []byte {
	var b protocol.Buffer
	b.WriteBytes([]byte(name))
	b.WriteBytes(payload)
	b.WriteU64(uint64(int64(time.Second)))
	b.WriteU32(3)
	b.WriteU64(uint64(time.Now().UnixNano()))
	return b.Bytes()
}

// dequeueBody builds a Dequeue request with a millisecond long-poll budget;
// 0 asks for a single non-blocking pass.
func dequeueBody(name string, wait time.Duration) []byte {
	var b protocol.Buffer
	protocol.EncodeTaskDequeue(&b, name, wait)
	return b.Bytes()
}

// ackBody builds an Ack request with a full-Task (mode 0) ref.
func ackBody(name string, task protocol.Task) []byte {
	var b protocol.Buffer
	b.WriteBytes([]byte(name))
	protocol.EncodeTaskRef(&b, task)
	return b.Bytes()
}

// nackBody builds a Nack request; explicit mode appends the retry delay.
func nackBody(name string, task protocol.Task, mode byte, delay time.Duration) []byte {
	var b protocol.Buffer
	b.WriteBytes([]byte(name))
	protocol.EncodeTaskRef(&b, task)
	protocol.WriteMode(&b, mode)
	if mode == protocol.NackModeExplicit {
		b.WriteU64(uint64(int64(delay)))
	}
	return b.Bytes()
}

func TestTaskHandlersLifecycle(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{Flush: spec.FlushSpec{MaxBytes: new(int64(1 << 20))}})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	op, body := s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpEnqueue, op)
	created, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	body = dequeueBody("tasks", 0)
	op, body = s.handleDequeue(t.Context(), body)
	require.Equal(t, protocol.OpDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, created.ID, leased.ID)
	assert.Equal(t, uint32(1), leased.Attempts)

	op, body = s.handleAck(t.Context(), ackBody("tasks", leased))
	require.Equal(t, protocol.OpAck, op)
	assert.Empty(t, body)

	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	assert.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTaskNotAvailable, code)
}

// TestAckModeByteStrict covers mode 0 (full Task record): a fresh lease is
// accepted, and a stale record with the same ID is rejected.
func TestAckModeByteStrict(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	op, body := s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpEnqueue, op)
	preDelivery, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	require.Equal(t, protocol.OpDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	// Stale mode-0 ack: the pre-delivery task has the same ID but stale
	// Attempts/Lease/DeliveryID. (Checked before the fresh ack removes
	// the task from the active set.)
	op, body = s.handleAck(t.Context(), ackBody("tasks", preDelivery))
	assert.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTaskStale, code)

	// Fresh mode-0 ack: OK.
	op, body = s.handleAck(t.Context(), ackBody("tasks", leased))
	require.Equal(t, protocol.OpAck, op)
	assert.Empty(t, body)
}

// TestAckModeByteTaskID covers mode 1 (bare task ID): a currently leased ID
// is accepted (skipping the strict check), an ID that is not leased is
// rejected.
func TestAckModeByteTaskID(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	// Enqueue two tasks: the first will be leased, the second will not.
	op, body := s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("one")))
	require.Equal(t, protocol.OpEnqueue, op)
	first, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	op, body = s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("two")))
	require.Equal(t, protocol.OpEnqueue, op)
	second, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	// Lease "one" so its ID is in-flight.
	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	require.Equal(t, protocol.OpDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, first.ID, leased.ID)

	idAck := func(taskID string) (byte, []byte) {
		var b protocol.Buffer
		b.WriteBytes([]byte("tasks"))
		protocol.EncodeTaskRef(&b, taskID)
		return s.handleAck(t.Context(), b.Bytes())
	}

	// Mode 1 with a valid, leased ID: OK.
	op, body = idAck(first.ID)
	assert.Equal(t, protocol.OpAck, op)

	// Mode 1 with an ID that is scheduled but never leased: NotInflight.
	op, body = idAck(second.ID)
	assert.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTaskNotInflight, code)
}

func TestTaskQueueHandleCaching(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	first, err := s.getOrCreateTaskQueue("tasks")
	require.NoError(t, err)
	second, err := s.getOrCreateTaskQueue("tasks")
	require.NoError(t, err)
	assert.Same(t, first, second)

	const workers = 32
	results := make(chan taskqueue.Queue, workers)
	for range workers {
		go func() {
			q, err := s.getOrCreateTaskQueue("tasks")
			if err != nil {
				t.Errorf("getOrCreateTaskQueue: %v", err)
				return
			}
			results <- q
		}()
	}
	for range workers {
		assert.Same(t, first, <-results)
	}
}

func TestTaskHandlersExtendLease(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	op, body := s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpEnqueue, op)
	_, err = protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	require.Equal(t, protocol.OpDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	var ext protocol.Buffer
	ext.WriteBytes([]byte("tasks"))
	protocol.EncodeTaskRef(&ext, leased)
	ext.WriteU64(uint64(int64(250 * time.Millisecond)))
	op, body = s.handleExtend(t.Context(), ext.Bytes())
	assert.Equal(t, protocol.OpExtend, op)
	assert.Empty(t, body)
}

func TestTaskDequeueLongPollBlocksUntilTimeout(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	start := time.Now()
	op, resp := s.handleDequeue(t.Context(), dequeueBody("tasks", 300*time.Millisecond))
	elapsed := time.Since(start)

	require.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(resp)
	assert.Equal(t, protocol.ErrCodeTaskNotAvailable, code)
	// The handler blocked for the requested window instead of answering
	// immediately (old non-blocking semantics).
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(290))
	assert.Less(t, elapsed.Milliseconds(), int64(5000))
}

// TestRemoteDequeueLongPoll: a 200 ms budget on an empty queue blocks for
// roughly the whole window, then reports ErrTaskNotAvailable.
func TestRemoteDequeueLongPoll(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	start := time.Now()
	op, resp := s.handleDequeue(t.Context(), dequeueBody("tasks", 200*time.Millisecond))
	elapsed := time.Since(start)

	require.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(resp)
	assert.Equal(t, protocol.ErrCodeTaskNotAvailable, code)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(180))
	assert.Less(t, elapsed.Milliseconds(), int64(5000))
}

func TestTaskDequeueLongPollWakesOnEnqueue(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	ctx := t.Context()
	go func() {
		time.Sleep(100 * time.Millisecond)
		s.handleEnqueue(ctx, enqueueBody("tasks", []byte("wake")))
	}()

	start := time.Now()
	op, resp := s.handleDequeue(t.Context(), dequeueBody("tasks", 2*time.Second))
	elapsed := time.Since(start)

	require.Equal(t, protocol.OpDequeue, op, "long-poll must wake on enqueue instead of waiting out the window")
	task, err := protocol.DecodeTask(protocol.NewReader(resp))
	require.NoError(t, err)
	assert.Equal(t, []byte("wake"), task.Payload)
	assert.GreaterOrEqual(t, elapsed.Milliseconds(), int64(90))
	assert.Less(t, elapsed.Milliseconds(), int64(1500))
}

func TestServerDequeueDisconnectCancelsHandler(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	s.listener = ln // so Stop() unblocks Serve's Accept (as in TestServerServeClosesOnStop)
	go s.Serve(ln)

	// Start a 10s long-poll, then hang up mid-poll.
	conn, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	require.NoError(t, protocol.WriteFrame(conn, protocol.OpDequeue, dequeueBody("tasks", 10*time.Second)))
	time.Sleep(150 * time.Millisecond)
	require.NoError(t, conn.Close())

	// Give the disconnect watchdog time to cancel the handler.
	time.Sleep(400 * time.Millisecond)

	// The abandoned poll must not steal this task: a fresh dequeue on a
	// second connection still has to see it. If the dead poll were still
	// waiting, it would take the task the instant it is enqueued.
	conn2, err := net.Dial("tcp", ln.Addr().String())
	require.NoError(t, err)
	defer conn2.Close()
	r2 := bufio.NewReader(conn2)

	require.NoError(t, protocol.WriteFrame(conn2, protocol.OpEnqueue, enqueueBody("tasks", []byte("mine"))))
	op, _, err := protocol.ReadFrame(r2)
	require.NoError(t, err)
	require.Equal(t, protocol.OpEnqueue, op)

	require.NoError(t, protocol.WriteFrame(conn2, protocol.OpDequeue, dequeueBody("tasks", 0)))
	op, body, err := protocol.ReadFrame(r2)
	require.NoError(t, err)
	require.Equal(t, protocol.OpDequeue, op, "task was consumed by the abandoned long-poll")
	task, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, []byte("mine"), task.Payload)
}

// TestNackInheritBackoff: mode 0 leaves the delay to the server, which doubles
// it per attempt (1s, then 2s) from the task's own Attempts.
func TestNackInheritBackoff(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	op, body := s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpEnqueue, op)
	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	require.Equal(t, protocol.OpDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	require.Equal(t, uint32(1), leased.Attempts)

	op, body = s.handleNack(t.Context(), nackBody("tasks", leased, protocol.NackModeInherit, 0))
	require.Equal(t, protocol.OpNack, op)

	// Not immediately due: the server applied the first backoff.
	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	require.Equal(t, protocol.OpError, op)
	code, _ := protocol.DecodeError(body)
	assert.Equal(t, protocol.ErrCodeTaskNotAvailable, code)

	start := time.Now()
	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 3*time.Second))
	require.Equal(t, protocol.OpDequeue, op)
	firstWait := time.Since(start)
	second, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, uint32(2), second.Attempts)
	assert.GreaterOrEqual(t, firstWait, 800*time.Millisecond, "first retry should wait about 1s")
	assert.Less(t, firstWait, 5*time.Minute)

	// The second backoff doubles, proving the delay tracks Attempts.
	op, body = s.handleNack(t.Context(), nackBody("tasks", second, protocol.NackModeInherit, 0))
	require.Equal(t, protocol.OpNack, op)
	start = time.Now()
	op, _ = s.handleDequeue(t.Context(), dequeueBody("tasks", 4*time.Second))
	require.Equal(t, protocol.OpDequeue, op)
	secondWait := time.Since(start)
	assert.GreaterOrEqual(t, secondWait, 1600*time.Millisecond, "second retry should wait about 2s")
	assert.Greater(t, secondWait, firstWait)
}

// TestNackExplicitDelay: mode 1 carries a caller-supplied delay; zero redelivers immediately.
func TestNackExplicitDelay(t *testing.T) {
	n, release, err := node.Acquire(t.TempDir(), spec.Spec{})
	require.NoError(t, err)
	defer func() { _ = release() }()
	_, err = n.CreateTaskQueue(t.Context(), "tasks", spec.Spec{})
	require.NoError(t, err)
	s := New(n)
	defer s.Stop()

	op, body := s.handleEnqueue(t.Context(), enqueueBody("tasks", []byte("hello")))
	require.Equal(t, protocol.OpEnqueue, op)
	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 0))
	require.Equal(t, protocol.OpDequeue, op)
	leased, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)

	op, body = s.handleNack(t.Context(), nackBody("tasks", leased, protocol.NackModeExplicit, 0))
	require.Equal(t, protocol.OpNack, op)

	op, body = s.handleDequeue(t.Context(), dequeueBody("tasks", 100*time.Millisecond))
	require.Equal(t, protocol.OpDequeue, op, "an explicit zero delay must make the task immediately available")
	got, err := protocol.DecodeTask(protocol.NewReader(body))
	require.NoError(t, err)
	assert.Equal(t, leased.ID, got.ID)
}


