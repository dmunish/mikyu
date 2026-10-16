package mikyu

import (
	"context"
	"time"

	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

func toTask(t taskqueue.Task) Task {
	return Task{
		ID:          t.ID,
		Payload:     t.Payload,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Lease:       t.Lease,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
		Key:         t.Key,
		Stalled:     t.Stalled,
	}
}

func toTasks(ts []taskqueue.Task) []Task {
	out := make([]Task, len(ts))
	for i, t := range ts {
		out[i] = toTask(t)
	}
	return out
}

func fromTask(t Task) taskqueue.Task {
	return taskqueue.Task{
		ID:          t.ID,
		Payload:     t.Payload,
		Attempts:    t.Attempts,
		MaxAttempts: t.MaxAttempts,
		Lease:       t.Lease,
		ScheduledAt: t.ScheduledAt,
		DeliveryID:  t.DeliveryID,
		Offset:      t.Offset,
		Key:         t.Key,
		Stalled:     t.Stalled,
	}
}

func toInternalRef(r Ref) taskqueue.Ref {
	switch v := r.(type) {
	case Task:
		return fromTask(v)
	case TaskID:
		return taskqueue.ID(v)
	default:
		return nil
	}
}

type taskQueue struct {
	inner taskqueue.Queue
}

func (q *taskQueue) Name() string { return q.inner.Name() }

func (q *taskQueue) Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error) {
	t, err := q.inner.Enqueue(ctx, payload, opts...)
	if err != nil {
		return Task{}, err
	}
	return toTask(t), nil
}

func (q *taskQueue) Dequeue(ctx context.Context) (Task, error) {
	t, err := q.inner.Dequeue(ctx)
	if err != nil {
		return Task{}, err
	}
	return toTask(t), nil
}

func (q *taskQueue) TryDequeue(ctx context.Context) (Task, error) {
	t, err := q.inner.TryDequeue(ctx)
	if err != nil {
		return Task{}, err
	}
	return toTask(t), nil
}

func (q *taskQueue) Ack(ctx context.Context, ref Ref) error {
	return q.inner.Ack(ctx, toInternalRef(ref))
}

func (q *taskQueue) Nack(ctx context.Context, ref Ref, opts ...NackOption) error {
	return q.inner.Nack(ctx, toInternalRef(ref), opts...)
}

func (q *taskQueue) Extend(ctx context.Context, ref Ref, timeout time.Duration) error {
	return q.inner.Extend(ctx, toInternalRef(ref), timeout)
}

func (q *taskQueue) DeadLetters(ctx context.Context) ([]Task, error) {
	ts, err := q.inner.DeadLetters(ctx)
	if err != nil {
		return nil, err
	}
	return toTasks(ts), nil
}

func (q *taskQueue) AckDeadLetter(ctx context.Context, ref Ref) error {
	return q.inner.AckDeadLetter(ctx, toInternalRef(ref))
}

func (q *taskQueue) Archive(ctx context.Context, ref Ref) error {
	return q.inner.Archive(ctx, toInternalRef(ref))
}

var _ TaskQueue = (*taskQueue)(nil)
