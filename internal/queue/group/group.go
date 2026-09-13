package group

import (
	"context"
	"sync"
)

// CommitFunc persists a group's current high-watermark.
type CommitFunc func(ctx context.Context, offset uint64) error

// Base contains the concurrency-safe high-watermark and lifecycle state shared by embedded and remote consumer groups.
type Base struct {
	Name      string
	Mu        sync.Mutex
	HighWater uint64
	Closed    bool
	CommitFn  CommitFunc
}

// Mark advances the high-watermark to one past offset. Marks are monotonic.
func (g *Base) Mark(offset uint64) {
	g.Mu.Lock()
	defer g.Mu.Unlock()
	if offset >= g.HighWater {
		g.HighWater = offset + 1
	}
}

// Commit persists a snapshot of the current high-watermark.
func (g *Base) Commit(ctx context.Context) error {
	g.Mu.Lock()
	off := g.HighWater
	g.Mu.Unlock()
	return g.CommitFn(ctx, off)
}

// Close is idempotent and commits the final high-watermark before returning.
func (g *Base) Close() error {
	g.Mu.Lock()
	if g.Closed {
		g.Mu.Unlock()
		return nil
	}
	g.Closed = true
	g.Mu.Unlock()
	return g.Commit(context.Background())
}
