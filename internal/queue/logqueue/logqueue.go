package logqueue

import (
	"context"
	"sync"

	"github.com/dmunish/mikyu/internal/engine"
)

type LogQueue struct {
	name    string
	storage engine.Storage
	mu      sync.RWMutex
	groups  map[string]*queueGroup
}

func NewLogQueue(name string, storage engine.Storage) *LogQueue {
	return &LogQueue{
		name:    name,
		storage: storage,
		groups:  make(map[string]*queueGroup),
	}
}

func (q *LogQueue) Name() string {
	return q.name
}

func (q *LogQueue) Append(ctx context.Context, payload []byte) (uint64, error) {
	return q.storage.Append(ctx, payload)
}

func (q *LogQueue) AppendBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
	return q.storage.AppendBatch(ctx, payloads)
}

func (q *LogQueue) Read(ctx context.Context, offset uint64) (engine.Message, error) {
	return q.storage.Read(ctx, offset)
}

func (q *LogQueue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Message, error) {
	return q.storage.ReadBatch(ctx, startOffset, count)
}

func (q *LogQueue) LastOffset(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return q.storage.LastOffset(), nil
}

func (q *LogQueue) MinOffset(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return q.storage.MinOffset(), nil
}

// Group returns the consumer group interface.
func (q *LogQueue) Group(name string) Group {
	q.mu.Lock()
	defer q.mu.Unlock()
	if g, ok := q.groups[name]; ok {
		return g
	}
	g := newQueueGroup(q, name)
	q.groups[name] = g
	return g
}

func (q *LogQueue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	return q.storage.CommitOffset(ctx, group, offset)
}

func (q *LogQueue) Committed(ctx context.Context, group string) (uint64, error) {
	return q.storage.Committed(ctx, group)
}

func (q *LogQueue) Close() error {
	return nil
}
