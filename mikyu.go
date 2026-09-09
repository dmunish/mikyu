// Package mikyu provides the public API for the Mikyu message queue.
package mikyu

import (
	"context"
	"io"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/client"
	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/logqueue"
	"github.com/dmunish/mikyu/internal/taskqueue"
)

// Re-export core engine types as type aliases.
type Message = engine.Message
type SystemStats = engine.EngineStats
type QueueStats = engine.QueueStats

// Group tracks consumer offsets for a log queue.
type Group = logqueue.Group

// Task represents a unit of work owned by a TaskQueue. MaxAttempts is the maximum
// number of deliveries. DeliveryID identifies the current delivery lease.
type Task = taskqueue.Task

type TaskOption = taskqueue.TaskOption
type FailOption = taskqueue.FailOption

func WithVisibilityTimeout(timeout time.Duration) TaskOption {
	return taskqueue.WithVisibilityTimeout(timeout)
}
func WithMaxAttempts(n uint32) TaskOption           { return taskqueue.WithMaxAttempts(n) }
func WithScheduleAt(at time.Time) TaskOption        { return taskqueue.WithScheduleAt(at) }
func WithRetryAfter(delay time.Duration) FailOption { return taskqueue.WithRetryAfter(delay) }

// LogQueue provides log-oriented operations (streaming, tailing, groups).
type LogQueue = logqueue.ClientLogQueue

// TaskQueue provides at-least-once delivery, visibility leases, retries, stale-delivery
// protection, checkpoint recovery, and a durable per-queue dead-letter queue.
type TaskQueue = client.TaskQueue

// Client is the top-level interface for interacting with a Mikyu server.
type Client interface {
	io.Closer
	CreateQueue(ctx context.Context, name string, opts ...QueueOption) error
	LogQueue(name string) (LogQueue, error)
	TaskQueue(name string) (TaskQueue, error)
	ListQueues(ctx context.Context) ([]string, error)
	DeleteQueue(ctx context.Context, name string) error
	TruncateQueue(ctx context.Context, name string, minOffset uint64) (int64, error)
	Stats(ctx context.Context) (SystemStats, error)
}

// Open creates an in-process Client.
func Open(dir string, opts ...ClientOption) (Client, error) {
	return client.NewEmbedded(dir, opts...)
}

// Dial creates a lazy remote Client for target.
func Dial(target string, opts ...ClientOption) (Client, error) {
	return client.NewRemote(target, opts...)
}

// Error variables re-exported from the engine/client layers.
var (
	ErrTaskNotAvailable   = taskqueue.ErrTaskNotAvailable
	ErrTaskNotFound       = taskqueue.ErrTaskNotFound
	ErrTaskNotInflight    = taskqueue.ErrTaskNotInflight
	ErrTaskStale          = taskqueue.ErrTaskStale
	ErrTaskClosed         = taskqueue.ErrTaskClosed
	ErrInvalidTask        = taskqueue.ErrInvalidTask
	ErrCheckpointCorrupt  = taskqueue.ErrCheckpointCorrupt
	ErrDeadLetterNotFound = taskqueue.ErrDeadLetterNotFound
	ErrDeadLetterStale    = taskqueue.ErrDeadLetterStale

	ErrQueueExists      = engine.ErrQueueExists
	ErrQueueNotFound    = engine.ErrQueueNotFound
	ErrOffsetNotFound   = engine.ErrOffsetNotFound
	ErrOffsetTruncated  = engine.ErrOffsetTruncated
	ErrMessageTooLarge  = engine.ErrMessageTooLarge
	ErrQueueClosed      = engine.ErrQueueClosed
	ErrCorruptRecord    = engine.ErrCorruptRecord
	ErrTaskNotSupported = client.ErrTaskNotSupported
)

type _ = iter.Seq2[Message, error]
