package taskqueue

import "context"

// TruncateFloor returns the highest safe Storage.TruncateBefore offset.
// A checkpoint is the recovery anchor. Active tasks older than that checkpoint
// remain anchors until acknowledged or dead-lettered.
func (q *TaskQueue) TruncateFloor(ctx context.Context) (uint64, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return 0, ErrTaskClosed
	}
	if q.checkpointOffset == 0 {
		return 0, nil
	}
	floor := q.checkpointOffset
	for _, state := range q.active {
		if state.task.offset < floor {
			floor = state.task.offset
		}
	}
	for _, task := range q.dlq {
		if task.offset < floor {
			floor = task.offset
		}
	}
	return floor, nil
}
