package taskqueue

import "context"

// TruncateFloor reports the highest offset this task queue allows Storage.TruncateBefore to reach: the checkpoint, lowered by every in-flight task and every uncheckpointed dead letter that still anchors its record.
func (q *queue) TruncateFloor(ctx context.Context) (uint64, bool, error) {
	if err := ctx.Err(); err != nil {
		return 0, false, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return 0, false, ErrTaskClosed
	}

	var (
		floor       uint64
		constrained bool
	)
	anchor := func(off uint64) {
		if !constrained {
			floor, constrained = off, true
		} else if off < floor {
			floor = off
		}
	}
	if q.checkpointOffset > 0 {
		anchor(q.checkpointOffset)
	}
	for _, state := range q.active {
		anchor(state.task.Offset)
	}
	for id, task := range q.dlq {
		if _, ok := q.dlqUncheckpointed[id]; ok {
			anchor(task.Offset)
		}
	}
	return floor, constrained, nil
}
