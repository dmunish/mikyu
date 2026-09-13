package logqueue

import (
	"context"
	"errors"
	"io"
	"iter"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

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
