package logqueue

import (
	"context"
	"io"
	"sync"
)

// Group defines the consumer group offset tracking interface.
type Group interface {
	io.Closer
	Name() string
	Mark(offset uint64)
	Commit(ctx context.Context) error
	FetchOffset(ctx context.Context) (uint64, error)
}

type group struct {
	q         *LogQueue
	name      string
	mu        sync.Mutex
	highWater uint64
	closed    bool
}

func (g *group) Name() string {
	return g.name
}

func (g *group) Mark(offset uint64) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if offset >= g.highWater {
		g.highWater = offset + 1
	}
}

func (g *group) Commit(ctx context.Context) error {
	g.mu.Lock()
	offset := g.highWater
	g.mu.Unlock()
	return g.q.CommitOffset(ctx, g.name, offset)
}

func (g *group) FetchOffset(ctx context.Context) (uint64, error) {
	return g.q.FetchOffset(ctx, g.name)
}

func (g *group) Close() error {
	g.mu.Lock()
	if g.closed {
		g.mu.Unlock()
		return nil
	}
	g.closed = true
	offset := g.highWater
	g.mu.Unlock()
	return g.q.CommitOffset(context.Background(), g.name, offset)
}
