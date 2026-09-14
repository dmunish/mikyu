package taskqueue

import (
	"bytes"
	"context"
	"time"
)

func (q *TaskQueue) Enqueue(ctx context.Context, payload []byte, opts ...EnqueueOption) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	q.mu.Lock()
	visibilityTimeout, maxAttempts, scheduledAt := q.resolveEnqueueOptionsLocked(opts...)
	q.mu.Unlock()
	if visibilityTimeout <= 0 || maxAttempts == 0 || scheduledAt.IsZero() {
		return Task{}, ErrInvalidTask
	}
	id := newTaskID()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return Task{}, ErrTaskClosed
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	start := time.Now()
	offset, err := q.storage.Append(ctx, encodeTaskRecord(id, maxAttempts, visibilityTimeout, scheduledAt, payload))
	if err != nil {
		return Task{}, err
	}
	q.recordEnqueue(start)
	state := &taskState{task: Task{ID: id, Payload: bytes.Clone(payload), MaxAttempts: maxAttempts, ScheduledAt: scheduledAt, Offset: offset}, nextAt: scheduledAt, visibilityTimeout: visibilityTimeout}
	q.active[id] = state
	q.pushScheduleLocked(state)
	q.scanNext = offset + 1
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	return cloneTask(state.task), nil
}
