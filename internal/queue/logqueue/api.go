package logqueue

import (
	"context"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// Log is the queue-driver interface exposed through the public client API.
type Log interface {
	Name() string
	Append(ctx context.Context, payload []byte) (uint64, error)
	AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
	Read(ctx context.Context, offset uint64) (engine.Record, error)
	ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Record, error)
	LastOffset(ctx context.Context) (uint64, error)
	MinOffset(ctx context.Context) (uint64, error)
	Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]engine.Record, error)
	CommitOffset(ctx context.Context, group string, offset uint64) error
	Committed(ctx context.Context, group string) (uint64, error)
	Truncate(ctx context.Context, minOffset uint64) (int64, error)
	TruncateFloor(ctx context.Context) (uint64, bool, error)
	Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error]
	Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error]
	Group(name string) Group
}

var _ Log = (*queue)(nil)
