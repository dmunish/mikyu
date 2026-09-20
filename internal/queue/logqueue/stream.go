package logqueue

import (
	"context"
	"errors"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

const (
	streamBatch = 1024
	tailWait    = 30 * time.Second
)

// Stream yields every existing record from startOffset, then returns.
func (q *queue) Stream(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error] {
	return func(yield func(engine.Record, error) bool) {
		curr := startOffset
		for {
			msgs, err := q.storage.Fetch(ctx, curr, streamBatch, 0)
			if err != nil {
				if errors.Is(err, engine.ErrNoData) {
					return
				}
				yield(engine.Record{}, err)
				return
			}
			for _, msg := range msgs {
				if !yield(msg, nil) {
					return
				}
				curr = msg.Offset + 1
			}
		}
	}
}

// Tail yields existing records then blocks for new ones until ctx ends.
func (q *queue) Tail(ctx context.Context, startOffset uint64) iter.Seq2[engine.Record, error] {
	return func(yield func(engine.Record, error) bool) {
		curr := startOffset
		for {
			if err := ctx.Err(); err != nil {
				yield(engine.Record{}, err)
				return
			}
			msgs, err := q.storage.Fetch(ctx, curr, streamBatch, tailWait)
			if err != nil {
				if errors.Is(err, engine.ErrNoData) {
					continue
				}
				yield(engine.Record{}, err)
				return
			}
			for _, msg := range msgs {
				if !yield(msg, nil) {
					return
				}
				curr = msg.Offset + 1
			}
		}
	}
}
