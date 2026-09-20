package taskqueue

import (
	"cmp"
	"context"
	"slices"
)

// DeadLetters returns a stable snapshot of this queue's durable dead-letter queue.
func (q *queue) DeadLetters(ctx context.Context) ([]Task, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return nil, ErrTaskClosed
	}
	if err := q.refreshLocked(ctx); err != nil {
		return nil, err
	}
	out := make([]Task, 0, len(q.dlq))
	for _, task := range q.dlq {
		out = append(out, cloneTask(task))
	}
	slices.SortFunc(out, func(a, b Task) int {
		return cmp.Compare(a.Offset, b.Offset)
	})
	return out, nil
}

// AckDeadLetter permanently removes a dead-lettered task after inspection.
func (q *queue) AckDeadLetter(ctx context.Context, ref Ref) error {
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
	current, ok := q.dlq[ref.taskID()]
	if !ok {
		return ErrDeadLetterNotFound
	}
	if t, strict := ref.(Task); strict && (current.Offset != t.Offset || current.Attempts != t.Attempts) {
		return ErrDeadLetterStale
	}
	if _, err := q.storage.Append(ctx, encodeDeadLetterAck(current)); err != nil {
		return err
	}
	delete(q.dlq, ref.taskID())
	delete(q.dlqUncheckpointed, ref.taskID())
	q.dirty++
	q.m.dlqAcks.Inc()
	return nil
}
