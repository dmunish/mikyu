package mikyu

import (
	"context"
	"io"
	"iter"
	"time"
)

// Message is the primary domain record stored in a queue log.
type Message struct {
	Offset    uint64
	ID        string
	Timestamp time.Time
	Payload   []byte
}

// SystemStats contains global metrics across all queues in an engine instance.
type SystemStats struct {
	ActiveQueues int
	TotalBytes   int64
	Queues       map[string]QueueStats
}

// QueueStats contains operational metrics for a specific queue.
type QueueStats struct {
	MessageCount uint64
	SegmentCount int
	SizeBytes    int64
	LastOffset   uint64
}

// Client represents a top-level Mikyu instance, working in both
// in-process embedded mode (Open) and remote server mode (Dial).
type Client interface {
	io.Closer

	// Queue retrieval and management
	CreateQueue(ctx context.Context, name string, opts ...QueueOption) (Queue, error)
	Queue(name string) (Queue, error)
	ListQueues(ctx context.Context) ([]string, error)
	DeleteQueue(ctx context.Context, name string) error

	// Observability
	Stats(ctx context.Context) (SystemStats, error)
}

// Queue represents operations scoped to a single log-structured queue.
type Queue interface {
	Name() string

	// Produce I/O
	Produce(ctx context.Context, payload []byte) (uint64, error)
	ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)

	// Read I/O
	Read(ctx context.Context, offset uint64) (Message, error)
	ReadBatch(ctx context.Context, startOffset uint64, count int) ([]Message, error)

	// Offsets & Iteration
	LastOffset(ctx context.Context) (uint64, error)
	Stream(ctx context.Context, startOffset uint64) iter.Seq2[Message, error]
	Tail(ctx context.Context, startOffset uint64) iter.Seq2[Message, error]

	// Consumer Groups
	Group(name string) Group
}

// Group manages consumer group high-water mark progress and commits.
type Group interface {
	io.Closer
	Name() string

	// Progress tracking
	Mark(offset uint64)
	Commit(ctx context.Context) error
	FetchOffset(ctx context.Context) (uint64, error)
}
