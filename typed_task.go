package mikyu

import (
	"context"
	"time"
)

type Delivered[T any] struct {
	Task  Task
	Value T
}

type TypedTask[T any] struct {
	queue TaskQueue
	codec Codec[T]
}

func NewTypedTask[T any](q TaskQueue, codec Codec[T]) *TypedTask[T] {
	return &TypedTask[T]{queue: q, codec: codec}
}

func (t *TypedTask[T]) Queue() TaskQueue { return t.queue }

func (t *TypedTask[T]) Enqueue(ctx context.Context, val T, opts ...EnqueueOption) (Task, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return Task{}, err
	}
	return t.queue.Enqueue(ctx, payload, opts...)
}

func (t *TypedTask[T]) Dequeue(ctx context.Context) (Delivered[T], error) {
	return t.decode(t.queue.Dequeue(ctx))
}

func (t *TypedTask[T]) TryDequeue(ctx context.Context) (Delivered[T], error) {
	return t.decode(t.queue.TryDequeue(ctx))
}

func (t *TypedTask[T]) Ack(ctx context.Context, d Delivered[T]) error {
	return t.queue.Ack(ctx, d.Task)
}

func (t *TypedTask[T]) Nack(ctx context.Context, d Delivered[T], opts ...NackOption) error {
	return t.queue.Nack(ctx, d.Task, opts...)
}

func (t *TypedTask[T]) Extend(ctx context.Context, d Delivered[T], timeout time.Duration) error {
	return t.queue.Extend(ctx, d.Task, timeout)
}

func (t *TypedTask[T]) DeadLetters(ctx context.Context) ([]Task, error) {
	return t.queue.DeadLetters(ctx)
}

func (t *TypedTask[T]) AckDeadLetter(ctx context.Context, ref Ref) error {
	return t.queue.AckDeadLetter(ctx, ref)
}

func (t *TypedTask[T]) Archive(ctx context.Context, ref Ref) error {
	return t.queue.Archive(ctx, ref)
}

func (t *TypedTask[T]) decode(task Task, err error) (Delivered[T], error) {
	if err != nil {
		return Delivered[T]{}, err
	}
	var val T
	if derr := t.codec.Unmarshal(task.Payload, &val); derr != nil {
		return Delivered[T]{Task: task}, derr
	}
	return Delivered[T]{Task: task, Value: val}, nil
}
