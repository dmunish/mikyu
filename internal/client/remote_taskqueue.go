package client

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

// fromWireTask converts a wire DTO to a task driver task, copying the payload.
func fromWireTask(t protocol.Task) taskqueue.Task {
	return taskqueue.NewTaskFromWire(
		t.ID, t.Payload, t.Attempts, t.MaxAttempts,
		t.Visibility, t.ScheduledAt, t.DeliveryID, t.Offset,
	)
}

// toWireRef converts a driver TaskRef to the wire type EncodeTaskRef accepts.
func toWireRef(ref taskqueue.Ref) any {
	if t, ok := ref.(taskqueue.Task); ok {
		return toWireTask(t)
	}
	if id, ok := ref.(taskqueue.ID); ok {
		return string(id)
	}
	return ""
}

func (q *remoteTaskQueue) Close() error {
	return nil
}

func (q *remoteTaskQueue) SetDefaults(d taskqueue.Defaults) {}

func (q *remoteTaskQueue) TruncateFloor(ctx context.Context) (uint64, bool, error) {
	return 0, false, nil
}

func (c *remoteClient) TaskQueue(name string) (TaskQueue, error) {
	return &remoteTaskQueue{client: c, name: name}, nil
}

type remoteTaskQueue struct {
	client *remoteClient
	name   string
}

func (q *remoteTaskQueue) TryDequeue(ctx context.Context) (taskqueue.Task, error) {
	return q.Dequeue(ctx)
}

func (q *remoteTaskQueue) Name() string { return q.name }

// Enqueue sends unset zeros where the caller set no option, letting the server resolve queue defaults and built-ins instead of pre-resolving them.
func (q *remoteTaskQueue) Enqueue(ctx context.Context, payload []byte, opts ...taskqueue.EnqueueOption) (taskqueue.Task, error) {
	visibility, maxAttempts, scheduled := taskqueue.ResolveWireEnqueueOptions(opts...)
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	b.WriteBytes(payload)
	b.WriteU64(uint64(int64(visibility))) // 0 = inherit
	b.WriteU32(maxAttempts)               // 0 = inherit
	if scheduled.IsZero() {
		b.WriteU64(0)
	} else {
		b.WriteU64(uint64(scheduled.UnixNano()))
	}
	resp, err := q.client.exchange(ctx, protocol.OpEnqueue, b.Bytes())
	if err != nil {
		return taskqueue.Task{}, err
	}
	r := protocol.NewReader(resp)
	wt, err := protocol.DecodeTask(r)
	if err != nil {
		return taskqueue.Task{}, err
	}
	return fromWireTask(wt), nil
}

const (
	// longPollTimeout caps one server-side long-poll window on idle queues.
	longPollTimeout = 30 * time.Second

	// longPollMargin keeps the client socket deadline behind the server's timeout.
	longPollMargin = 5 * time.Second
)

// Dequeue long-polls the server until a task is due or ctx ends.
func (q *remoteTaskQueue) Dequeue(ctx context.Context) (taskqueue.Task, error) {
	for {
		if err := ctx.Err(); err != nil {
			return taskqueue.Task{}, err
		}

		wait := longPollTimeout
		if deadline, ok := ctx.Deadline(); ok {
			if remaining := time.Until(deadline); remaining < wait {
				wait = remaining
			}
		}

		var b protocol.Buffer
		protocol.EncodeTaskDequeue(&b, q.name, wait)

		callCtx, cancel := context.WithTimeout(ctx, wait+longPollMargin)
		resp, err := q.client.exchange(callCtx, protocol.OpDequeue, b.Bytes())
		cancel()
		if err == nil {
			r := protocol.NewReader(resp)
			wt, derr := protocol.DecodeTask(r)
			if derr != nil {
				return taskqueue.Task{}, derr
			}
			return fromWireTask(wt), nil
		}
		if parentErr := ctx.Err(); parentErr != nil {
			return taskqueue.Task{}, parentErr
		}
		switch {
		case errors.Is(err, taskqueue.ErrTaskNotAvailable):
			// Poll window elapsed; re-arm immediately on a new window.
		case errors.Is(err, context.DeadlineExceeded):
			// The server did not answer in time; retry on a fresh connection.
		default:
			return taskqueue.Task{}, err
		}
	}
}

// Ack a task; a bare TaskID skips the server's strict lease check.
func (q *remoteTaskQueue) Ack(ctx context.Context, ref taskqueue.Ref) error {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTaskRef(&b, toWireRef(ref))
	_, err := q.client.exchange(ctx, protocol.OpAck, b.Bytes())
	return err
}

// Nack a task; with no options the server computes exponential backoff from the task's attempt count.
func (q *remoteTaskQueue) Nack(ctx context.Context, ref taskqueue.Ref, opts ...taskqueue.NackOption) error {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTaskRef(&b, toWireRef(ref))
	if len(opts) == 0 {
		protocol.WriteMode(&b, protocol.NackModeInherit)
	} else {
		var task taskqueue.Task
		if t, ok := ref.(taskqueue.Task); ok {
			task = t
		}
		protocol.WriteMode(&b, protocol.NackModeExplicit)
		b.WriteU64(uint64(int64(taskqueue.ResolveNackOptions(task, opts...))))
	}
	_, err := q.client.exchange(ctx, protocol.OpNack, b.Bytes())
	return err
}

// Extend the current delivery lease.
func (q *remoteTaskQueue) Extend(ctx context.Context, ref taskqueue.Ref, timeout time.Duration) error {
	if timeout <= 0 {
		return taskqueue.ErrInvalidTask
	}
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTaskRef(&b, toWireRef(ref))
	b.WriteU64(uint64(int64(timeout)))
	_, err := q.client.exchange(ctx, protocol.OpExtend, b.Bytes())
	return err
}

// DeadLetters returns the queue's durable dead-letter snapshot.
func (q *remoteTaskQueue) DeadLetters(ctx context.Context) ([]taskqueue.Task, error) {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	resp, err := q.client.exchange(ctx, protocol.OpDeadLetters, b.Bytes())
	if err != nil {
		return nil, err
	}
	r := protocol.NewReader(resp)
	wts, err := protocol.DecodeTaskList(r)
	if err != nil {
		return nil, err
	}
	tasks := make([]taskqueue.Task, len(wts))
	for i, wt := range wts {
		tasks[i] = fromWireTask(wt)
	}
	return tasks, nil
}

// AckDeadLetter permanently removes a dead-lettered task.
func (q *remoteTaskQueue) AckDeadLetter(ctx context.Context, ref taskqueue.Ref) error {
	var b protocol.Buffer
	b.WriteBytes([]byte(q.name))
	protocol.EncodeTaskRef(&b, toWireRef(ref))
	_, err := q.client.exchange(ctx, protocol.OpDeadLetterAck, b.Bytes())
	return err
}
