package logqueue

import (
	"context"
	"errors"
	"io"
	"iter"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

var ErrNotSupported = errors.New("feature not supported")

type LogQueue struct {
	name    string
	storage engine.Storage
	policy  LogRetentionPolicy
	mu      sync.RWMutex
	groups  map[string]*group
}

func NewLogQueue(name string, storage engine.Storage) *LogQueue {
	return &LogQueue{
		name:    name,
		storage: storage,
		groups:  make(map[string]*group),
	}
}

func (q *LogQueue) Name() string {
	return q.name
}

func (q *LogQueue) Produce(ctx context.Context, payload []byte) (uint64, error) {
	return q.storage.Append(ctx, payload)
}

func (q *LogQueue) ProduceBatch(ctx context.Context, payloads [][]byte) ([]uint64, error) {
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

func (q *LogQueue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		offset := startOffset
		for {
			msg, err := q.storage.Read(ctx, offset)
			if err != nil {
				if err == engine.ErrOffsetNotFound {
					return
				}
				if !yield(engine.Message{}, err) {
					return
				}
				return
			}
			if !yield(msg, nil) {
				return
			}
			offset++
		}
	}
}

func (q *LogQueue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Message, error] {
	return func(yield func(engine.Message, error) bool) {
		offset := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(engine.Message{}, err)
				return
			}
			last := q.storage.LastOffset()
			if offset < last {
				msg, err := q.storage.Read(ctx, offset)
				if err != nil {
					// Transient read error while WAL flusher is writing payload to disk
					if errors.Is(err, engine.ErrOffsetNotFound) || errors.Is(err, engine.ErrCorruptRecord) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
						select {
						case <-ctx.Done():
							yield(engine.Message{}, ctx.Err())
							return
						case <-time.After(5 * time.Millisecond):
							continue // Retry reading current offset
						}
					}
					if !yield(engine.Message{}, err) {
						return
					}
					return
				}
				if !yield(msg, nil) {
					return
				}
				offset++
			} else {
				select {
				case <-ctx.Done():
					return
				case <-time.After(5 * time.Millisecond):
				}
			}
		}
	}
}

// Group returns the consumer group interface.
func (q *LogQueue) Group(name string) Group {
	q.mu.Lock()
	defer q.mu.Unlock()
	if g, ok := q.groups[name]; ok {
		return g
	}
	g := &group{
		q:         q,
		name:      name,
		highWater: 0,
	}
	q.groups[name] = g
	return g
}

func (q *LogQueue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	return q.storage.CommitOffset(ctx, group, offset)
}

func (q *LogQueue) FetchOffset(ctx context.Context, group string) (uint64, error) {
	return q.storage.FetchOffset(ctx, group)
}

func (q *LogQueue) Close() error {
	return nil
}
