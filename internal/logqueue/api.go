package logqueue

import (
	"context"
	"iter"

	"github.com/dmunish/mikyu/internal/engine"
)

// ClientLogQueue is the queue-driver interface exposed through the public
// client API. The concrete LogQueue remains an implementation detail of this
// package while the interface lets upper layers avoid importing engine details.
type ClientLogQueue interface {
	Name() string
	Produce(ctx context.Context, payload []byte) (uint64, error)
	ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error)
	Read(ctx context.Context, offset uint64) (engine.Message, error)
	ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error)
	LastOffset(ctx context.Context) (uint64, error)
	MinOffset(ctx context.Context) (uint64, error)
	Truncate(ctx context.Context, minOffset uint64) (int64, error)
	Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error]
	Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error]
	Group(name string) Group
}

var _ ClientLogQueue = (*LogQueue)(nil)
