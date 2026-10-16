package mikyu

import (
	"context"
	"time"
)

// TaskQueue is a work queue with at-least-once delivery.
//
// The model is simple: Enqueue adds work, Dequeue borrows it under a
// visibility lease, and Ack releases it for good. A worker that dies
// without acking loses only its lease; the task returns and is
// delivered again, with a growing delay between attempts, until its
// attempt budget runs out and it moves to the queue's durable
// dead-letter queue.
//
// Delivery identity is explicit. Every dequeue hands out a fresh
// DeliveryID, so a late worker holding a stale Task cannot ack work
// that someone else is already doing: the ack fails with ErrTaskStale.
type TaskQueue interface {
	// Name returns the queue's name, as given to OpenTask, CreateTask,
	// or Task.
	Name() string

	// Enqueue adds one task and returns it with its assigned ID.
	//
	// The queue's lease and attempt settings apply by default; per-task
	// options (WithTaskLease, WithTaskMaxAttempts, WithScheduledAt)
	// override them for this task alone.
	//
	// An enqueue can fail with ErrQueueFull when the queue's
	// max_in_flight bound is already saturated by unfinished tasks.
	Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error)

	// Dequeue waits until a task is due and leases it.
	//
	// A task is due when it has been enqueued, its scheduled time has
	// arrived, or a previous delivery's retry delay has elapsed. The
	// call blocks until one arrives or ctx is done; there is no hidden
	// timeout. When ctx ends, the error is ctx's own.
	//
	// Taking a task starts its lease and raises Attempts by one.
	Dequeue(ctx context.Context) (Task, error)

	// TryDequeue performs one non-blocking attempt. When nothing is due
	// it returns ErrTaskNotAvailable.
	TryDequeue(ctx context.Context) (Task, error)

	// Ack confirms the work behind a delivery is done. The task never
	// comes back.
	//
	// Passing the Task you dequeued runs the strict stale-lease check:
	// the ack must match the current delivery, or it fails with
	// ErrTaskStale. Passing a bare TaskID skips that check, which suits
	// admin tools that never saw the delivery, but the task must still
	// be in flight under a live lease.
	Ack(ctx context.Context, ref Ref) error

	// Nack returns a task for another attempt instead of acking it.
	//
	// Without options the retry delay grows with the attempt count:
	// one second, doubling, capped at five minutes. WithRetryAfter
	// sets the delay explicitly.
	//
	// A task whose Attempts have reached its MaxAttempts does not come
	// back from a nack. It moves to the dead-letter queue instead.
	Nack(ctx context.Context, ref Ref, opts ...NackOption) error

	// Extend renews the current delivery's lease by timeout. It moves
	// the expiry forward only, and never counts as a new attempt. Use
	// it when work is going well but needs more than one lease length.
	Extend(ctx context.Context, ref Ref, timeout time.Duration) error

	// DeadLetters returns the tasks currently sitting in the queue's
	// dead-letter queue, ordered by their original position.
	DeadLetters(ctx context.Context) ([]Task, error)

	// AckDeadLetter removes one dead-lettered task permanently, as if
	// its work had finally been accepted. Pass a Task for the strict
	// identity check, or a bare TaskID from an admin tool.
	AckDeadLetter(ctx context.Context, ref Ref) error

	// Archive moves a task, whether in flight or dead-lettered, into a
	// durable archive log named after the queue. The task leaves the
	// queue; the archive keeps its payload, key, and history for later
	// inspection through an ordinary log queue read.
	Archive(ctx context.Context, ref Ref) error
}
