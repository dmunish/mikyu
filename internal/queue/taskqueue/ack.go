package taskqueue

import "context"

// Ack removes a task after successful processing.
func (q *TaskQueue) Ack(ctx context.Context, ref TaskRef) error {
	if err := ctx.Err(); err != nil {
		return err
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
	delete(q.active, state.task.ID)
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	return nil
}
