package taskqueue

import (
	"bytes"
	"context"
	"time"
)

func (q *queue) Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error) {
	q.m.inFlightEnq.Add(1)
	defer q.m.inFlightEnq.Add(-1)

	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	q.mu.Lock()
	if q.maxInFlight > 0 && uint32(len(q.active)) >= q.maxInFlight {
		q.mu.Unlock()
		q.m.queueFullEvents.Inc()
		return Task{}, ErrQueueFull
	}
	lease, maxAttempts, scheduledAt, key := q.resolveEnqueueOptionsLocked(opts...)
	q.mu.Unlock()
	if lease <= 0 || maxAttempts == 0 || scheduledAt.IsZero() {
		return Task{}, ErrInvalidTask
	}
	id := newID()
	q.mu.Lock()
	defer q.mu.Unlock()
	defer q.publishLocked()
	if q.closed || q.closing {
		return Task{}, ErrTaskClosed
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	start := time.Now()
	offset, err := q.storage.Append(ctx, encodeTaskRecord(id, maxAttempts, lease, scheduledAt, payload, key, 0))
	if err != nil {
		return Task{}, err
	}
	q.recordEnqueue(start)
	state := &taskState{task: Task{ID: id, Payload: bytes.Clone(payload), MaxAttempts: maxAttempts, ScheduledAt: scheduledAt, Offset: offset, Key: append([]byte(nil), key...)}, nextAt: scheduledAt, lease: lease}
	q.active[id] = state
	q.pushScheduleLocked(state)
	q.scanNext = offset + 1
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	return cloneTask(state.task), nil
}
