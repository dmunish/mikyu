package taskqueue

import (
	"context"
	"time"
)

// TryDequeue performs one non-blocking scheduling pass.
func (q *queue) TryDequeue(ctx context.Context) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return Task{}, ErrTaskClosed
	}
	if err := q.refreshLocked(ctx); err != nil {
		return Task{}, err
	}
	if state, _, ok, err := q.takeDueLocked(ctx, time.Now()); err != nil {
		return Task{}, err
	} else if ok {
		return cloneTask(state.task), nil
	}
	return Task{}, ErrTaskNotAvailable
}

func (q *queue) Dequeue(ctx context.Context) (Task, error) {
	for {
		if err := ctx.Err(); err != nil {
			return Task{}, err
		}
		q.mu.Lock()
		if q.closed || q.closing {
			q.mu.Unlock()
			return Task{}, ErrTaskClosed
		}
		if err := q.refreshLocked(ctx); err != nil {
			q.mu.Unlock()
			return Task{}, err
		}
		state, waitUntil, ok, err := q.takeDueLocked(ctx, time.Now())
		if err != nil {
			q.mu.Unlock()
			return Task{}, err
		}
		if ok {
			result := cloneTask(state.task)
			q.mu.Unlock()
			return result, nil
		}
		notify := q.notify
		q.mu.Unlock()
		if waitUntil.IsZero() {
			select {
			case <-notify:
				continue
			default:
			}
			q.m.dequeueParkEvents.Inc()
			q.m.parkedDeq.Add(1)
			select {
			case <-ctx.Done():
				q.m.parkedDeq.Add(-1)
				return Task{}, ctx.Err()
			case <-notify:
				q.m.parkedDeq.Add(-1)
			}
			continue
		}
		d := max(time.Until(waitUntil), 0)
		timer := time.NewTimer(d)
		select {
		case <-notify:
			if !timer.Stop() {
				drainTimer(timer)
			}
			continue
		default:
		}
		q.m.dequeueParkEvents.Inc()
		q.m.parkedDeq.Add(1)
		select {
		case <-ctx.Done():
			q.m.parkedDeq.Add(-1)
			if !timer.Stop() {
				drainTimer(timer)
			}
			return Task{}, ctx.Err()
		case <-notify:
			q.m.parkedDeq.Add(-1)
			if !timer.Stop() {
				drainTimer(timer)
			}
		case <-timer.C:
			q.m.parkedDeq.Add(-1)
		}
	}
}

func (q *queue) takeDueLocked(ctx context.Context, now time.Time) (*taskState, time.Time, bool, error) {
	defer q.publishLocked()
	for len(q.schedule) > 0 {
		item := q.schedule[0]
		state, ok := q.active[item.id]
		if !ok || state.version != item.version {
			schedPop(&q.schedule)
			continue
		}
		due := state.nextAt
		if state.inFlight {
			due = state.task.Lease
		}
		if due.IsZero() {
			due = now
		}
		if due.After(now) {
			return nil, due, false, nil
		}
		schedPop(&q.schedule)
		if state.inFlight && state.task.Attempts >= state.task.MaxAttempts {
			if _, err := q.storage.Append(ctx, encodeDeadLetterRecord(state.task)); err != nil {
				return nil, time.Time{}, false, err
			}
			q.dlq[state.task.ID] = cloneTask(state.task)
			q.dlqUncheckpointed[state.task.ID] = struct{}{}
			q.removeActiveLocked(state.task.ID)
			q.dirty++
			q.m.retryBudgetDrops.Inc()
			q.broadcastLocked()
			q.pruneScheduleLocked()
			continue
		}
		if state.inFlight {
			state.task.Stalled++
			q.recordStall()
		}
		deliveryID := newID()
		q.markInFlightLocked(state)
		state.task.Attempts++
		state.task.DeliveryID = deliveryID
		state.task.Lease = now.Add(state.lease)
		state.version++
		q.pushScheduleLocked(state)
		q.dirty++
		q.broadcastLocked()
		q.pruneScheduleLocked()
		q.recordDequeue(due)
		return state, time.Time{}, true, nil
	}
	return nil, time.Time{}, false, nil
}
