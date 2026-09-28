package taskqueue

import "context"

// Ack removes a task after successful processing.
func (q *queue) Ack(ctx context.Context, ref Ref) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	defer q.publishLocked()
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
	q.removeActiveLocked(state.task.ID)
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	q.m.acks.Inc()
	return nil
}
