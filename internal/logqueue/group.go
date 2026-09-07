package logqueue

import (
	"context"
	"io"

	"github.com/dmunish/mikyu/internal/group"
)

// Group defines the consumer group offset tracking interface.
type Group interface {
	io.Closer
	Name() string
	Mark(offset uint64)
	Commit(ctx context.Context) error
	FetchOffset(ctx context.Context) (uint64, error)
}

type queueGroup struct {
	group.Base
	q *LogQueue
}

func newQueueGroup(q *LogQueue, name string) *queueGroup {
	g := &queueGroup{q: q}
	g.Base.Name = name
	g.Base.CommitFn = func(ctx context.Context, offset uint64) error {
		return q.CommitOffset(ctx, name, offset)
	}
	return g
}

func (g *queueGroup) Name() string {
	return g.Base.Name
}

func (g *queueGroup) FetchOffset(ctx context.Context) (uint64, error) {
	return g.q.FetchOffset(ctx, g.Base.Name)
}
