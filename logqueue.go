package mikyu

import (
	"context"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/queue/logqueue"
)

type LogQueue interface {
	Name() string
	Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error)
	AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error)
	Read(ctx context.Context, offset uint64) (Record, error)
	ReadBatch(ctx context.Context, start uint64, count int) ([]Record, error)
	Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Record, error)
	Stream(ctx context.Context, start uint64) iter.Seq2[Record, error]
	Tail(ctx context.Context, start uint64) iter.Seq2[Record, error]
	LastOffset(ctx context.Context) (uint64, error)
	MinOffset(ctx context.Context) (uint64, error)
	Group(name string) Group
}

type Group = logqueue.Group
