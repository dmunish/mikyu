package taskqueue

import (
	"context"
	"errors"
	"time"
)

const (
	defaultVisibilityTimeout  = 30 * time.Second
	defaultMaxAttempts        = 5
	defaultRetryDelay         = time.Second
	defaultMaxRetryDelay      = 5 * time.Minute
	defaultCheckpointInterval = 5 * time.Second
	checkpointEvery           = 128
	checkpointChunkBytes      = 64 * 1024
)

var (
	ErrTaskNotAvailable   = errors.New("no task is currently available")
	ErrTaskNotFound       = errors.New("task not found")
	ErrTaskNotInflight    = errors.New("task is not currently leased")
	ErrTaskStale          = errors.New("task lease is stale")
	ErrTaskClosed         = errors.New("task queue is closed")
	ErrInvalidTask        = errors.New("invalid task record")
	ErrCheckpointCorrupt  = errors.New("task checkpoint is corrupt")
	ErrDeadLetterNotFound = errors.New("dead letter task not found")
	ErrDeadLetterStale    = errors.New("dead letter task is stale")
)

// Task represents a unit of work owned by a TaskQueue.
type Task struct {
	ID          string
	Payload     []byte
	Attempts    uint32
	MaxAttempts uint32
	Visibility  time.Time
	ScheduledAt time.Time
	DeliveryID  string
	Offset      uint64
}

// TaskRef identifies a task for Ack/Nack/Extend/AckDeadLetter.
type TaskRef interface {
	taskRef()
	taskID() string
}

// TaskID is a bare task identifier (no strict lease check).
type TaskID string

func (TaskID) taskRef() {}

func (id TaskID) taskID() string { return string(id) }

func (Task) taskRef() {}

func (t Task) taskID() string { return t.ID }

// ClientTaskQueue is the task-side client API.
type ClientTaskQueue interface {
	Name() string

	Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error)
	Dequeue(ctx context.Context) (Task, error)

	Ack(ctx context.Context, ref TaskRef) error
	Nack(ctx context.Context, ref TaskRef, opts ...NackOption) error

	// Extend renews the current delivery lease. It never creates a new delivery.
	Extend(ctx context.Context, ref TaskRef, timeout time.Duration) error

	DeadLetters(ctx context.Context) ([]Task, error)
	AckDeadLetter(ctx context.Context, ref TaskRef) error
}

// EnqueueOption configures a task at enqueue time.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	visibilityTimeout time.Duration
	maxAttempts       uint32
	scheduledAt       time.Time
}

// resolveEnqueueOptionsLocked applies opts over this queue's live defaults.
func (q *TaskQueue) resolveEnqueueOptionsLocked(opts ...EnqueueOption) (visibilityTimeout time.Duration, maxAttempts uint32, scheduledAt time.Time) {
	o := enqueueOptions{
		visibilityTimeout: q.defaultVisibility,
		maxAttempts:       q.defaultMaxAttempts,
		scheduledAt:       time.Now(),
	}
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.visibilityTimeout, o.maxAttempts, o.scheduledAt
}

// ResolveWireEnqueueOptions applies opts over unset zeros so wire clients send "inherit" for unset values.
func ResolveWireEnqueueOptions(opts ...EnqueueOption) (visibilityTimeout time.Duration, maxAttempts uint32, scheduledAt time.Time) {
	var o enqueueOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.visibilityTimeout, o.maxAttempts, o.scheduledAt
}

// WithVisibility controls how long a dequeued task stays leased.
func WithVisibility(timeout time.Duration) EnqueueOption {
	return func(o *enqueueOptions) {
		if timeout > 0 {
			o.visibilityTimeout = timeout
		}
	}
}

// WithMaxAttempts sets the maximum number of deliveries before dead-lettering.
func WithMaxAttempts(n uint32) EnqueueOption {
	return func(o *enqueueOptions) {
		if n > 0 {
			o.maxAttempts = n
		}
	}
}

// WithScheduledAt delays the first delivery until the supplied time.
func WithScheduledAt(at time.Time) EnqueueOption {
	return func(o *enqueueOptions) {
		if !at.IsZero() {
			o.scheduledAt = at
		}
	}
}

// NewTaskFromWire reconstructs a task from wire values, copying the payload so the driver owns its bytes.
func NewTaskFromWire(id string, payload []byte, attempts, maxAttempts uint32, visibility, scheduledAt time.Time, deliveryID string, offset uint64) Task {
	return Task{
		ID: id, Payload: append([]byte(nil), payload...), Attempts: attempts,
		MaxAttempts: maxAttempts, Visibility: visibility, ScheduledAt: scheduledAt, DeliveryID: deliveryID, Offset: offset,
	}
}

// NackOption configures a retry after a nacked delivery.
type NackOption func(*nackOptions)

type nackOptions struct{ retryAfter time.Duration }

func defaultNackOptions(task Task) nackOptions {
	delay := defaultRetryDelay
	if task.Attempts > 1 {
		for i := uint32(1); i < task.Attempts; i++ {
			if delay >= defaultMaxRetryDelay/2 {
				delay = defaultMaxRetryDelay
				break
			}
			delay *= 2
		}
	}
	return nackOptions{retryAfter: delay}
}

// ResolveNackOptions applies NackOption values and returns the effective retry delay.
func ResolveNackOptions(task Task, opts ...NackOption) time.Duration {
	o := defaultNackOptions(task)
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.retryAfter
}

// WithRetryAfter overrides the retry delay for a nacked task.
func WithRetryAfter(delay time.Duration) NackOption {
	return func(o *nackOptions) {
		if delay >= 0 {
			o.retryAfter = delay
		}
	}
}
