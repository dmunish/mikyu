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
)

// Re-export core engine types as type aliases.
type Message = engine.Message
type SystemStats = engine.EngineStats
type QueueStats = engine.QueueStats

// Group tracks consumer offsets for a log queue.
type Group = logqueue.Group

// Task represents a unit of work (future).
type Task struct {
	ID          string
	Payload     []byte
	Attempts    uint32
	MaxAttempts uint32
	Visibility  time.Time
	ScheduledAt time.Time
}

// LogQueue provides log-oriented operations (streaming, tailing, groups).
type LogQueue = logqueue.ClientLogQueue

// TaskQueue is a placeholder for future task semantics.
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
	ErrQueueExists      = engine.ErrQueueExists
	ErrQueueNotFound    = engine.ErrQueueNotFound
	ErrOffsetNotFound   = engine.ErrOffsetNotFound
	ErrOffsetTruncated  = engine.ErrOffsetTruncated
	ErrMessageTooLarge  = engine.ErrMessageTooLarge
	ErrCorruptRecord    = engine.ErrCorruptRecord
	ErrTaskNotSupported = client.ErrTaskNotSupported
)

type _ = iter.Seq2[Message, error]
