package taskqueue

import (
	"context"
	"time"
)

// Extend renews the current delivery lease without incrementing Attempts.
func (q *TaskQueue) Extend(ctx context.Context, ref TaskRef, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		return ErrInvalidTask
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return ErrTaskClosed
	}
	if err := q.refreshLocked(ctx); err != nil {
		return err
	}
	state, err := q.resolveLeaseLocked(ref)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	if deadline.After(state.task.Visibility) {
		state.task.Visibility = deadline
		state.version++
		q.pushScheduleLocked(state)
		q.dirty++
		q.broadcastLocked()
		q.pruneScheduleLocked()
	}
	q.m.extends.Inc()
	return nil
}
