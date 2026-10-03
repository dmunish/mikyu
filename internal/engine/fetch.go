package engine

import (
	"context"
	"time"
)

// Fetch returns up to maxCount messages starting at offset, blocking up to maxWait.
func (q *Queue) Fetch(ctx context.Context, offset uint64, maxCount int, maxWait time.Duration) ([]Record, error) {
	if maxCount <= 0 {
		return nil, nil
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.hasReader.Store(true)
	deadline := time.Now().Add(maxWait)
	for {
		q.mu.RLock()
		closed := q.closed
		minOff := q.minOffset
		published := q.publishedOffset
		notify := q.notify
		q.mu.RUnlock()

		if closed {
			return nil, ErrQueueClosed
		}
		if offset < minOff {
			return nil, ErrOffsetTruncated
		}
		if offset < published {
			msgs, err := q.ReadBatch(ctx, offset, maxCount)
			if err != nil {
				return nil, err
			}
			if len(msgs) > 0 {
				return msgs, nil
			}
			// The window held only compacted records; advance past it so callers see the next live record.
			if offset+uint64(maxCount) >= published {
				return nil, ErrNoData
			}
			offset += uint64(maxCount)
			continue
		}
		if maxWait <= 0 {
			return nil, ErrNoData
		}
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, ErrNoData
		}
		timer := time.NewTimer(remaining)
		select {
		case <-notify:
			timer.Stop()
			continue
		default:
		}
		q.m.fetchParkEvents.Inc()
		q.m.parkedFetch.Add(1)
		select {
		case <-ctx.Done():
			q.m.parkedFetch.Add(-1)
			timer.Stop()
			return nil, ctx.Err()
		case <-notify:
			q.m.parkedFetch.Add(-1)
			timer.Stop()
			continue
		case <-timer.C:
			q.m.parkedFetch.Add(-1)
			return nil, ErrNoData
		}
	}
}
