package taskqueue

import (
	"bytes"
	"time"
)

// resolveLeaseLocked resolves ref to the current in-flight lease; a Task ref
// is additionally matched on offset, attempts, and delivery ID (strict).
func (q *queue) resolveLeaseLocked(ref Ref) (*taskState, error) {
	if ref == nil {
		return nil, ErrInvalidTask
	}
	state, ok := q.active[ref.taskID()]
	if !ok {
		return nil, ErrTaskNotFound
	}
	if !state.inFlight {
		return nil, ErrTaskNotInflight
	}
	if state.task.Lease.IsZero() || !time.Now().Before(state.task.Lease) {
		return nil, ErrTaskStale
	}
	if t, strict := ref.(Task); strict {
		if state.task.Offset != t.Offset ||
			state.task.Attempts != t.Attempts ||
			state.task.DeliveryID != t.DeliveryID ||
			t.DeliveryID == "" {
			return nil, ErrTaskStale
		}
	}
	return state, nil
}

func (q *queue) broadcastLocked() {
	old := q.notify
	q.notify = make(chan struct{})
	close(old)
}

func cloneTask(t Task) Task {
	t.Payload = bytes.Clone(t.Payload)
	return t
}

func drainTimer(t *time.Timer) {
	select {
	case <-t.C:
	default:
	}
}
