package mikyu

import (
	"context"
	"time"
)

// Delivered is one task as the queue handed it to you, paired with its
// decoded value. Ack or Nack it, or Extend its lease while the work
// runs.
type Delivered[T any] struct {
	// Task is the raw task: ID, delivery identity, lease, attempts,
	// everything the queue tracks.
	Task Task

	// Value is the decoded payload.
	Value T
}

// TypedTask is a TaskQueue that speaks a Go type instead of raw bytes.
//
// Enqueue marshals, Dequeue and TryDequeue unmarshal, and the delivered
// pair travels together so acking what you decoded is one call. The
// identity errors (ErrTaskStale, ErrTaskNotInflight) behave exactly as
// on the raw queue.
//
// DeadLetters returns raw Task values, because payloads sitting in a
// dead-letter queue may predate the current codec and are worth seeing
// even when they do not decode. Queue returns the TaskQueue underneath.
type TypedTask[T any] struct {
	queue TaskQueue
	codec Codec[T]
}

// NewTypedTask wraps a TaskQueue with a codec.
func NewTypedTask[T any](q TaskQueue, codec Codec[T]) *TypedTask[T] {
	return &TypedTask[T]{queue: q, codec: codec}
}

// Queue returns the TaskQueue underneath, for when the typed surface
// is not enough.
func (t *TypedTask[T]) Queue() TaskQueue { return t.queue }

// Enqueue marshals val and adds it as a task.
func (t *TypedTask[T]) Enqueue(ctx context.Context, val T, opts ...EnqueueOption) (Task, error) {
	payload, err := t.codec.Marshal(val)
	if err != nil {
		return Task{}, err
	}
	return t.queue.Enqueue(ctx, payload, opts...)
}

// Dequeue waits until a task is due, unmarshals it, and returns the
// pair. It blocks until ctx is done.
func (t *TypedTask[T]) Dequeue(ctx context.Context) (Delivered[T], error) {
	return t.decode(t.queue.Dequeue(ctx))
}

// TryDequeue performs one non-blocking attempt and returns the pair,
// or ErrTaskNotAvailable when nothing is due.
func (t *TypedTask[T]) TryDequeue(ctx context.Context) (Delivered[T], error) {
	return t.decode(t.queue.TryDequeue(ctx))
}

// Ack confirms the work behind a delivered pair is done.
func (t *TypedTask[T]) Ack(ctx context.Context, d Delivered[T]) error {
	return t.queue.Ack(ctx, d.Task)
}

// Nack returns a delivered pair for another attempt. Without options
// the retry delay grows with the attempt count; WithRetryAfter sets it
// explicitly.
func (t *TypedTask[T]) Nack(ctx context.Context, d Delivered[T], opts ...NackOption) error {
	return t.queue.Nack(ctx, d.Task, opts...)
}

// Extend renews the lease of a delivered pair by timeout.
func (t *TypedTask[T]) Extend(ctx context.Context, d Delivered[T], timeout time.Duration) error {
	return t.queue.Extend(ctx, d.Task, timeout)
}

// DeadLetters returns the queue's dead-lettered tasks as raw Task
// values.
func (t *TypedTask[T]) DeadLetters(ctx context.Context) ([]Task, error) {
	return t.queue.DeadLetters(ctx)
}

// AckDeadLetter removes one dead-lettered task permanently.
func (t *TypedTask[T]) AckDeadLetter(ctx context.Context, ref Ref) error {
	return t.queue.AckDeadLetter(ctx, ref)
}

// Archive moves a task into the queue's durable archive log.
func (t *TypedTask[T]) Archive(ctx context.Context, ref Ref) error {
	return t.queue.Archive(ctx, ref)
}

// decode pairs a dequeued task with its value. A task whose payload
// does not decode comes back with the Task filled in and the decode
// error, so the caller can Nack it rather than lose it.
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
