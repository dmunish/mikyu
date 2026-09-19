package logqueue

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

type queue struct {
	name    string
	storage engine.Storage
	mu      sync.RWMutex
	groups  map[string]*queueGroup
}

func New(name string, storage engine.Storage) Log {
	return &queue{
		name:    name,
		storage: storage,
		groups:  make(map[string]*queueGroup),
	}
}

// appendOptions carries the effective per-call append settings.
type appendOptions struct {
	key []byte
}

// AppendOption configures the record(s) being appended.
type AppendOption func(*appendOptions)

// WithKey associates a compaction key with the appended record(s); only the newest survives compaction.
func WithKey(k []byte) AppendOption {
	return func(o *appendOptions) { o.key = append([]byte(nil), k...) }
}

// ResolveAppendOptions folds a set of append options into their effective values.
func ResolveAppendOptions(opts ...AppendOption) (key []byte) {
	var o appendOptions
	for _, opt := range opts {
		if opt != nil {
			opt(&o)
		}
	}
	return o.key
}

func (q *queue) Name() string {
	return q.name
}

func (q *queue) Append(ctx context.Context, payload []byte, opts ...AppendOption) (uint64, error) {
	return q.storage.Append(ctx, payload, engine.WithGroup(ResolveAppendOptions(opts...)))
}

func (q *queue) AppendBatch(ctx context.Context, payloads [][]byte, opts ...AppendOption) ([]uint64, error) {
	return q.storage.AppendBatch(ctx, payloads, engine.WithGroup(ResolveAppendOptions(opts...)))
}

func (q *queue) Read(ctx context.Context, offset uint64) (engine.Record, error) {
	return q.storage.Read(ctx, offset)
}

func (q *queue) ReadBatch(ctx context.Context, startOffset uint64, count int) ([]engine.Record, error) {
	return q.storage.ReadBatch(ctx, startOffset, count)
}

func (q *queue) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]engine.Record, error) {
	return q.storage.Fetch(ctx, offset, maxCount, maxWait)
}

func (q *queue) LastOffset(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return q.storage.LastOffset(), nil
}

func (q *queue) MinOffset(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	return q.storage.MinOffset(), nil
}

// Group returns the consumer group interface.
func (q *queue) Group(name string) Group {
	q.mu.Lock()
	defer q.mu.Unlock()
	if g, ok := q.groups[name]; ok {
		return g
	}
	g := newQueueGroup(q, name)
	q.groups[name] = g
	return g
}

func (q *queue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	return q.storage.CommitOffset(ctx, group, offset)
}

func (q *queue) Committed(ctx context.Context, group string) (uint64, error) {
	return q.storage.Committed(ctx, group)
}

func (q *queue) Close() error {
	return nil
}
