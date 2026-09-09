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
// Attempts is the number of deliveries including the current delivery.
// MaxAttempts is the maximum number of deliveries permitted before the task
// is moved to this queue's durable dead-letter queue.
// Visibility is the current lease deadline while the task is in-flight.
// DeliveryID identifies this specific delivery/lease and changes on every redelivery.
// ScheduledAt is the original enqueue schedule and remains unchanged across retries.
type Task struct {
	ID          string
	Payload     []byte
	Attempts    uint32
	MaxAttempts uint32
	Visibility  time.Time
	ScheduledAt time.Time
	DeliveryID  string
	offset      uint64
}

// OriginalOffset returns the immutable engine offset for this task.
func (t Task) OriginalOffset() uint64 { return t.offset }

// ClientTaskQueue is the task-side client API.
type ClientTaskQueue interface {
	Close() error
	Name() string

	Enqueue(ctx context.Context, payload []byte, opts ...TaskOption) (Task, error)
	Dequeue(ctx context.Context) (Task, error)

	Ack(ctx context.Context, task Task) error
	Fail(ctx context.Context, task Task, opts ...FailOption) error

	// Extend renews the current delivery lease. It never creates a new delivery.
	Extend(ctx context.Context, task Task, timeout time.Duration) error

	DeadLetters(ctx context.Context) ([]Task, error)
	AckDeadLetter(ctx context.Context, task Task) error

	// By-ID convenience APIs for stateless human/admin clients (ergonomic, but less strict)
	AckID(ctx context.Context, taskID string) error
	FailID(ctx context.Context, taskID string, opts ...FailOption) error
	ExtendID(ctx context.Context, taskID string, timeout time.Duration) error
	AckDeadLetterID(ctx context.Context, taskID string) error
}

// TaskOption configures a task at enqueue time.
type TaskOption func(*taskOptions)

type taskOptions struct {
	visibilityTimeout time.Duration
	maxAttempts       uint32
	scheduledAt       time.Time
}

func defaultTaskOptions() taskOptions {
	return taskOptions{
		visibilityTimeout: defaultVisibilityTimeout,
		maxAttempts:       defaultMaxAttempts,
		scheduledAt:       time.Now(),
	}
}

// ResolveTaskOptions applies TaskOption values and returns the effective values.
// It is shared by embedded and wire clients so option semantics stay identical.
func ResolveTaskOptions(opts ...TaskOption) (visibilityTimeout time.Duration, maxAttempts uint32, scheduledAt time.Time) {
	o := defaultTaskOptions()
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.visibilityTimeout, o.maxAttempts, o.scheduledAt
}

// WithVisibilityTimeout controls how long a dequeued task stays leased.
func WithVisibilityTimeout(timeout time.Duration) TaskOption {
	return func(o *taskOptions) {
		if timeout > 0 {
			o.visibilityTimeout = timeout
		}
	}
}

// WithMaxAttempts sets the maximum number of deliveries before dead-lettering.
func WithMaxAttempts(n uint32) TaskOption {
	return func(o *taskOptions) {
		if n > 0 {
			o.maxAttempts = n
		}
	}
}

// WithScheduleAt delays the first delivery until the supplied time.
func WithScheduleAt(at time.Time) TaskOption {
	return func(o *taskOptions) {
		if !at.IsZero() {
			o.scheduledAt = at
		}
	}
}

// NewTaskFromWire reconstructs the private engine offset carried by a remote task.
// It is kept for compatibility with older internal callers; new wire decoding uses
// NewTaskFromWireWithDeliveryID so stale acknowledgements are bound to one lease.
func NewTaskFromWire(id string, payload []byte, attempts, maxAttempts uint32, visibility, scheduledAt time.Time, offset uint64) Task {
	return NewTaskFromWireWithDeliveryID(id, payload, attempts, maxAttempts, visibility, scheduledAt, "", offset)
}

// NewTaskFromWireWithDeliveryID reconstructs a task including its delivery token.
func NewTaskFromWireWithDeliveryID(id string, payload []byte, attempts, maxAttempts uint32, visibility, scheduledAt time.Time, deliveryID string, offset uint64) Task {
	return Task{
		ID: id, Payload: append([]byte(nil), payload...), Attempts: attempts,
		MaxAttempts: maxAttempts, Visibility: visibility, ScheduledAt: scheduledAt, DeliveryID: deliveryID, offset: offset,
	}
}

// FailOption configures a retry after a failed delivery.
type FailOption func(*failOptions)

type failOptions struct{ retryAfter time.Duration }

func defaultFailOptions(task Task) failOptions {
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
	return failOptions{retryAfter: delay}
}

// ResolveFailOptions applies FailOption values and returns the effective retry delay.
func ResolveFailOptions(task Task, opts ...FailOption) time.Duration {
	o := defaultFailOptions(task)
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.retryAfter
}

// WithRetryAfter overrides the retry delay for a failed task.
func WithRetryAfter(delay time.Duration) FailOption {
	return func(o *failOptions) {
		if delay >= 0 {
			o.retryAfter = delay
		}
	}
}
