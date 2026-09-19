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
	ErrQueueFull          = errors.New("task queue is full")
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
	Key         []byte
	Stalled     uint32
}

// Ref identifies a task for Ack/Nack/Extend/AckDeadLetter.
type Ref interface {
	taskRef()
	taskID() string
}

// ID is a bare task identifier (no strict lease check).
type ID string

func (ID) taskRef() {}

func (id ID) taskID() string { return string(id) }

func (Task) taskRef() {}

func (t Task) taskID() string { return t.ID }

// Queue is the task-side client API.
type Queue interface {
	Name() string

	Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error)
	Dequeue(ctx context.Context) (Task, error)
	TryDequeue(ctx context.Context) (Task, error)

	Ack(ctx context.Context, ref Ref) error
	Nack(ctx context.Context, ref Ref, opts ...NackOption) error

	// Extend renews the current delivery lease. It never creates a new delivery.
	Extend(ctx context.Context, ref Ref, timeout time.Duration) error

	DeadLetters(ctx context.Context) ([]Task, error)
	AckDeadLetter(ctx context.Context, ref Ref) error

	// Archive moves an in-flight or DLQ task to a durable archive log.
	Archive(ctx context.Context, ref Ref) error

	SetDefaults(d Defaults)
	TruncateFloor(ctx context.Context) (uint64, bool, error)
}

// Close stops a driver's checkpoint loop and writes a final checkpoint when dirty.
func Close(q Queue) error {
	if c, ok := q.(interface{ Close() error }); ok {
		return c.Close()
	}
	return nil
}

// EnqueueOption configures a task at enqueue time.
type EnqueueOption func(*enqueueOptions)

type enqueueOptions struct {
	visibilityTimeout time.Duration
	maxAttempts       uint32
	scheduledAt       time.Time
	key               []byte
}

// resolveEnqueueOptionsLocked applies opts over this queue's live defaults.
func (q *queue) resolveEnqueueOptionsLocked(opts ...EnqueueOption) (visibilityTimeout time.Duration, maxAttempts uint32, scheduledAt time.Time, key []byte) {
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
	return o.visibilityTimeout, o.maxAttempts, o.scheduledAt, o.key
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

// WithKey associates a key with the task for compaction or archival.
func WithKey(k []byte) EnqueueOption {
	return func(o *enqueueOptions) {
		o.key = append([]byte(nil), k...)
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
func NewTaskFromWire(id string, payload []byte, attempts, maxAttempts uint32, visibility, scheduledAt time.Time, deliveryID string, offset uint64, key []byte, stalled uint32) Task {
	return Task{
		ID: id, Payload: append([]byte(nil), payload...), Attempts: attempts,
		MaxAttempts: maxAttempts, Visibility: visibility, ScheduledAt: scheduledAt, DeliveryID: deliveryID, Offset: offset,
		Key: append([]byte(nil), key...), Stalled: stalled,
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
