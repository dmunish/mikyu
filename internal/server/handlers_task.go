package server

import (
	"context"
	"errors"
	"time"

	"github.com/dmunish/mikyu/internal/queue/taskqueue"
	"github.com/dmunish/mikyu/internal/transport/protocol"
)

// toWireTask converts a task driver task to the wire DTO.
func toWireTask(t taskqueue.Task) protocol.Task {
	return protocol.Task{
		ID:          t.ID,
		Payload:     t.Payload, // zero-copy: payload is not mutated after encode
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Visibility:  t.Visibility,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
	}
}

// fromWireTask converts a wire DTO to a task driver task, copying the payload
// so the driver owns its bytes.
func fromWireTask(t protocol.Task) taskqueue.Task {
	return taskqueue.NewTaskFromWire(
		t.ID, t.Payload, t.Attempts, t.MaxAttempts,
		t.Visibility, t.ScheduledAt, t.DeliveryID, t.Offset,
	)
}

// queueTaskDefaults reads the queue's stored spec and returns its task
// defaults when explicitly set. Unset (nil) values fall through to the
// driver's built-in defaults.
func (s *Server) queueTaskDefaults(ctx context.Context, name string) (visibility time.Duration, maxAttempts uint32) {
	d, err := s.node.DescribeQueue(ctx, name)
	if err != nil {
		return 0, 0
	}
	if v := d.Spec.Task.DefaultVisibility; v != nil {
		visibility = v.Duration()
	}
	if m := d.Spec.Task.DefaultMaxAttempts; m != nil {
		maxAttempts = *m
	}
	return
}

func (s *Server) handleEnqueue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	payload, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	visRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	maxAttempts, err := r.ReadU32()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	schedRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}

	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}

	// Unset wire zeros inherit the queue spec's task defaults, then the
	// driver's built-ins. Explicit wire values always win.
	var opts []taskqueue.EnqueueOption
	var defVis time.Duration
	var defMax uint32
	if visRaw == 0 || maxAttempts == 0 {
		defVis, defMax = s.queueTaskDefaults(ctx, string(name))
	}
	if visRaw > 0 {
		opts = append(opts, taskqueue.WithVisibility(time.Duration(int64(visRaw))))
	} else if defVis > 0 {
		opts = append(opts, taskqueue.WithVisibility(defVis))
	}
	if maxAttempts > 0 {
		opts = append(opts, taskqueue.WithMaxAttempts(maxAttempts))
	} else if defMax > 0 {
		opts = append(opts, taskqueue.WithMaxAttempts(defMax))
	}
	if schedRaw > 0 {
		opts = append(opts, taskqueue.WithScheduledAt(time.Unix(0, int64(schedRaw))))
	}

	t, err := q.Enqueue(ctx, payload, opts...)
	if err != nil {
		return taskErr(err)
	}
	var b protocol.Buffer
	protocol.EncodeTask(&b, toWireTask(t))
	return protocol.OpEnqueue, b.Bytes()
}

// maxDequeueWait bounds the server-side long-poll window a client may
// request, so a malicious or buggy timeout cannot pin a handler forever.
const maxDequeueWait = 5 * time.Minute

func (s *Server) handleDequeue(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, timeout, err := protocol.DecodeTaskDequeue(r)
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(name)
	if err != nil {
		return taskErr(err)
	}

	var t taskqueue.Task
	if timeout <= 0 {
		// Non-blocking single scheduling pass (matches pre-long-poll
		// semantics for zero budgets).
		t, err = q.TryDequeue(ctx)
	} else {
		if timeout > maxDequeueWait {
			timeout = maxDequeueWait
		}
		dequeueCtx, cancel := context.WithTimeout(ctx, timeout)
		defer cancel()
		t, err = q.Dequeue(dequeueCtx)
		// The poll window expired with no task due: report the sentinel so
		// long-polling clients re-arm, unless the connection itself died.
		if err != nil && ctx.Err() == nil && errors.Is(err, context.DeadlineExceeded) {
			err = taskqueue.ErrTaskNotAvailable
		}
	}
	if err != nil {
		return taskErr(err)
	}
	var b protocol.Buffer
	protocol.EncodeTask(&b, toWireTask(t))
	return protocol.OpDequeue, b.Bytes()
}

// decodeTaskRef parses the mode byte and its payload from a frame that has
// already consumed the queue name.
func decodeTaskRef(r *protocol.Reader) (taskID string, task *protocol.Task, err error) {
	id, wt, _, err := protocol.DecodeTaskRef(r)
	if err != nil {
		return "", nil, err
	}
	return id, wt, nil
}

// refFromWire converts a decoded wire ref into the driver's TaskRef.
func refFromWire(taskID string, wt *protocol.Task) taskqueue.Ref {
	if wt != nil {
		return fromWireTask(*wt)
	}
	return taskqueue.ID(taskID)
}

func (s *Server) handleAck(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	taskID, wt, err := decodeTaskRef(r)
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	ackErr := q.Ack(ctx, refFromWire(taskID, wt))
	if ackErr != nil {
		return taskErr(ackErr)
	}
	return protocol.OpAck, nil
}

func (s *Server) handleNack(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	taskID, wt, err := decodeTaskRef(r)
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	mode, err := protocol.ReadMode(r)
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	var opts []taskqueue.NackOption
	if mode == protocol.NackModeExplicit {
		retryRaw, err := r.ReadU64()
		if err != nil {
			return opErr(protocol.ErrCodeTaskInvalid, err.Error())
		}
		opts = append(opts, taskqueue.WithRetryAfter(time.Duration(int64(retryRaw))))
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	nackErr := q.Nack(ctx, refFromWire(taskID, wt), opts...)
	if nackErr != nil {
		return taskErr(nackErr)
	}
	return protocol.OpNack, nil
}

func (s *Server) handleExtend(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	taskID, wt, err := decodeTaskRef(r)
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	timeoutRaw, err := r.ReadU64()
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	timeout := time.Duration(int64(timeoutRaw))
	extendErr := q.Extend(ctx, refFromWire(taskID, wt), timeout)
	if extendErr != nil {
		return taskErr(extendErr)
	}
	return protocol.OpExtend, nil
}

func (s *Server) handleDeadLetters(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	tasks, err := q.DeadLetters(ctx)
	if err != nil {
		return taskErr(err)
	}
	wireTasks := make([]protocol.Task, len(tasks))
	for i, t := range tasks {
		wireTasks[i] = toWireTask(t)
	}
	var b protocol.Buffer
	protocol.EncodeTaskList(&b, wireTasks)
	return protocol.OpDeadLetters, b.Bytes()
}

func (s *Server) handleDeadLetterAck(ctx context.Context, body []byte) (byte, []byte) {
	r := protocol.NewReader(body)
	name, err := r.ReadBytes()
	if err != nil {
		return opErr(protocol.ErrCodeUnknown, err.Error())
	}
	taskID, wt, err := decodeTaskRef(r)
	if err != nil {
		return opErr(protocol.ErrCodeTaskInvalid, err.Error())
	}
	q, err := s.getOrCreateTaskQueue(string(name))
	if err != nil {
		return taskErr(err)
	}
	dlErr := q.AckDeadLetter(ctx, refFromWire(taskID, wt))
	if dlErr != nil {
		return taskErr(dlErr)
	}
	return protocol.OpDeadLetterAck, nil
}
