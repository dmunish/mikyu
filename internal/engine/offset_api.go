package engine

import "context"

// LastOffset implements Storage.LastOffset.
func (q *Queue) LastOffset() uint64 {
	q.mu.RLock()
	defer q.mu.RUnlock()
	return q.publishedOffset
}

// Name returns the queue name.
func (q *Queue) Name() string {
	return q.name
}

// CommitOffset implements Storage.CommitOffset.
func (q *Queue) CommitOffset(ctx context.Context, group string, offset uint64) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return ErrQueueClosed
	}
	if q.offsets == nil {
		return ErrQueueNotFound
	}
	if err := q.offsets.Commit(q.name, group, offset); err != nil {
		return err
	}
	q.m.groupCommits.Inc()
	return nil
}

// Committed implements Storage.Committed.
func (q *Queue) Committed(ctx context.Context, group string) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	q.mu.RLock()
	closed := q.closed
	q.mu.RUnlock()
	if closed {
		return 0, ErrQueueClosed
	}
	if q.offsets == nil {
		return 0, ErrQueueNotFound
	}
	return q.offsets.Fetch(q.name, group), nil
}
