package taskqueue

import (
	"context"
	"time"
)

// Nack re-queues a task for redelivery, or moves it to the DLQ when its attempt budget is exhausted.
func (q *queue) Nack(ctx context.Context, ref Ref, opts ...NackOption) error {
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
	delay := ResolveNackOptions(state.task, opts...)
	if state.task.Attempts >= state.task.MaxAttempts {
		if _, err := q.storage.Append(ctx, encodeDeadLetterRecord(state.task)); err != nil {
			return err
		}
		q.dlq[state.task.ID] = cloneTask(state.task)
		q.dlqUncheckpointed[state.task.ID] = struct{}{}
		delete(q.active, state.task.ID)
		q.dirty++
		q.broadcastLocked()
		q.pruneScheduleLocked()
		q.m.nacks.Inc()
		return nil
	}
	state.inFlight = false
	state.task.Visibility = time.Time{}
	state.task.DeliveryID = ""
	state.nextAt = time.Now().Add(delay)
	state.version++
	q.pushScheduleLocked(state)
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	q.m.nacks.Inc()
	return nil
}
