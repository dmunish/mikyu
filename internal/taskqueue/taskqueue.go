package taskqueue

import (
	"bytes"
	"container/heap"
	"context"
	"sort"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// TaskQueue implements at-least-once task processing over engine.Storage.
// State transitions are serialized under mu; task writes and checkpoint writes
// share the same critical section so a checkpoint cannot overtake this driver.
type TaskQueue struct {
	name    string
	storage engine.Storage

	mu                sync.Mutex
	active            map[string]*taskState
	dlq               map[string]Task
	dlqUncheckpointed map[string]struct{}
	schedule          scheduleHeap
	sequence          uint64
	scanNext          uint64

	checkpointOffset uint64
	dirty            uint64
	notify           chan struct{}
	stopCh           chan struct{}
	wg               sync.WaitGroup
	closing          bool
	closed           bool
}

var _ ClientTaskQueue = (*TaskQueue)(nil)

// NewTaskQueue opens a task driver over an existing engine storage queue.
func NewTaskQueue(name string, storage engine.Storage) (*TaskQueue, error) {
	if storage == nil {
		return nil, engine.ErrQueueNotFound
	}
	q := &TaskQueue{
		name:              name,
		storage:           storage,
		active:            make(map[string]*taskState),
		dlq:               make(map[string]Task),
		dlqUncheckpointed: make(map[string]struct{}),
		notify:            make(chan struct{}),
		stopCh:            make(chan struct{}),
	}
	heap.Init(&q.schedule)
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, err := q.loadCheckpointLocked(context.Background()); err != nil {
		return nil, err
	}
	if q.scanNext == 0 {
		q.scanNext = storage.MinOffset()
	}
	if err := q.refreshLocked(context.Background()); err != nil {
		return nil, err
	}
	q.wg.Add(1)
	go q.checkpointLoop()
	return q, nil
}

func (q *TaskQueue) Name() string { return q.name }

func (q *TaskQueue) Enqueue(ctx context.Context, payload []byte, opts ...TaskOption) (Task, error) {
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	visibilityTimeout, maxAttempts, scheduledAt := ResolveTaskOptions(opts...)
	if visibilityTimeout <= 0 || maxAttempts == 0 || scheduledAt.IsZero() {
		return Task{}, ErrInvalidTask
	}
	id, err := newTaskID()
	if err != nil {
		return Task{}, err
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return Task{}, ErrTaskClosed
	}
	if err := ctx.Err(); err != nil {
		return Task{}, err
	}
	offset, err := q.storage.Append(ctx, encodeTaskRecord(id, maxAttempts, visibilityTimeout, scheduledAt, payload))
	if err != nil {
		return Task{}, err
	}
	state := &taskState{task: Task{ID: id, Payload: bytes.Clone(payload), MaxAttempts: maxAttempts, ScheduledAt: scheduledAt, offset: offset}, nextAt: scheduledAt, visibilityTimeout: visibilityTimeout}
	q.active[id] = state
	q.pushScheduleLocked(state)
	q.scanNext = offset + 1
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	return cloneTask(state.task), nil
}

// DequeueNow performs one non-blocking scheduling pass.
func (q *TaskQueue) DequeueNow(ctx context.Context) (Task, error) {
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

func (q *TaskQueue) Dequeue(ctx context.Context) (Task, error) {
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
			case <-ctx.Done():
				return Task{}, ctx.Err()
			case <-notify:
			}
			continue
		}
		d := time.Until(waitUntil)
		if d < 0 {
			d = 0
		}
		timer := time.NewTimer(d)
		select {
		case <-ctx.Done():
			if !timer.Stop() {
				drainTimer(timer)
			}
			return Task{}, ctx.Err()
		case <-notify:
			if !timer.Stop() {
				drainTimer(timer)
			}
		case <-timer.C:
		}
	}
}

func (q *TaskQueue) Ack(ctx context.Context, task Task) error {
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
	state, err := q.validateLeaseLocked(task)
	if err != nil {
		return err
	}
	delete(q.active, state.task.ID)
	q.dirty++
	q.broadcastLocked()
	q.pruneScheduleLocked()
	return nil
}

func (q *TaskQueue) Fail(ctx context.Context, task Task, opts ...FailOption) error {
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
	state, err := q.validateLeaseLocked(task)
	if err != nil {
		return err
	}
	delay := ResolveFailOptions(state.task, opts...)
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
	return nil
}

// Extend renews the current delivery lease without incrementing Attempts.
// The timeout is measured from the time of the call. If the requested deadline
// is earlier than the current lease, Extend is a no-op and succeeds.
func (q *TaskQueue) Extend(ctx context.Context, task Task, timeout time.Duration) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if timeout <= 0 {
		return ErrInvalidTask
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed || q.closing {
		return ErrTaskClosed
	}
	if err := q.refreshLocked(ctx); err != nil {
		return err
	}
	state, err := q.validateLeaseLocked(task)
	if err != nil {
		return err
	}
	deadline := time.Now().Add(timeout)
	if deadline.After(state.task.Visibility) {
		state.task.Visibility = deadline
		state.version++
		q.pushScheduleLocked(state)
		q.dirty++
		q.broadcastLocked()
		q.pruneScheduleLocked()
	}
	return nil
}

// DeadLetters returns a stable snapshot of this queue's durable dead-letter queue.
// Returned payloads are cloned and may be safely modified by the caller.
func (q *TaskQueue) DeadLetters(ctx context.Context) ([]Task, error) {
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
	sort.Slice(out, func(i, j int) bool { return out[i].OriginalOffset() < out[j].OriginalOffset() })
	return out, nil
}

// AckDeadLetter permanently removes a dead-lettered task after inspection/handling.
// Dead-letter tasks are queue-local and have no active delivery lease.
func (q *TaskQueue) AckDeadLetter(ctx context.Context, task Task) error {
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
	current, ok := q.dlq[task.ID]
	if !ok {
		return ErrDeadLetterNotFound
	}
	if current.OriginalOffset() != task.OriginalOffset() || current.Attempts != task.Attempts || current.MaxAttempts != task.MaxAttempts {
		return ErrDeadLetterStale
	}
	if _, err := q.storage.Append(ctx, encodeDeadLetterAck(current)); err != nil {
		return err
	}
	delete(q.dlq, task.ID)
	delete(q.dlqUncheckpointed, task.ID)
	q.dirty++
	return nil
}

func (q *TaskQueue) Close() error {
	q.mu.Lock()
	if q.closed {
		q.mu.Unlock()
		return nil
	}
	if !q.closing {
		q.closing = true
		close(q.stopCh)
	}
	q.mu.Unlock()
	q.wg.Wait()
	q.mu.Lock()
	defer q.mu.Unlock()
	if q.closed {
		return nil
	}
	var err error
	if q.dirty > 0 || q.checkpointOffset == 0 {
		err = q.checkpointLocked(context.Background())
	}
	q.closed = true
	q.broadcastLocked()
	return err
}

func (q *TaskQueue) checkpointLoop() {
	defer q.wg.Done()
	t := time.NewTicker(defaultCheckpointInterval)
	defer t.Stop()
	for {
		select {
		case <-q.stopCh:
			return
		case <-t.C:
			q.mu.Lock()
			if !q.closed && !q.closing && q.dirty > 0 {
				_ = q.checkpointLocked(context.Background())
			}
			q.mu.Unlock()
		}
	}
}

func (q *TaskQueue) takeDueLocked(ctx context.Context, now time.Time) (*taskState, time.Time, bool, error) {
	for q.schedule.Len() > 0 {
		item := q.schedule[0]
		state, ok := q.active[item.id]
		if !ok || state.version != item.version {
			heap.Pop(&q.schedule)
			continue
		}
		due := state.nextAt
		if state.inFlight {
			due = state.task.Visibility
		}
		if due.IsZero() {
			due = now
		}
		if due.After(now) {
			return nil, due, false, nil
		}
		heap.Pop(&q.schedule)
		if state.inFlight && state.task.Attempts >= state.task.MaxAttempts {
			if _, err := q.storage.Append(ctx, encodeDeadLetterRecord(state.task)); err != nil {
				return nil, time.Time{}, false, err
			}
			q.dlq[state.task.ID] = cloneTask(state.task)
			q.dlqUncheckpointed[state.task.ID] = struct{}{}
			delete(q.active, state.task.ID)
			q.dirty++
			q.broadcastLocked()
			q.pruneScheduleLocked()
			continue
		}
		deliveryID, err := newTaskID()
		if err != nil {
			return nil, time.Time{}, false, err
		}
		state.inFlight = true
		state.task.Attempts++
		state.task.DeliveryID = deliveryID
		state.task.Visibility = now.Add(state.visibilityTimeout)
		state.version++
		q.pushScheduleLocked(state)
		q.dirty++
		q.broadcastLocked()
		return state, time.Time{}, true, nil
	}
	return nil, time.Time{}, false, nil
}

func (q *TaskQueue) validateLeaseLocked(task Task) (*taskState, error) {
	state, ok := q.active[task.ID]
	if task.ID == "" || !ok {
		return nil, ErrTaskNotFound
	}
	if !state.inFlight {
		return nil, ErrTaskNotInflight
	}
	if state.task.Visibility.IsZero() || !time.Now().Before(state.task.Visibility) {
		return nil, ErrTaskStale
	}
	if state.task.offset != task.offset || state.task.Attempts != task.Attempts || state.task.DeliveryID != task.DeliveryID || task.DeliveryID == "" {
		return nil, ErrTaskStale
	}
	return state, nil
}

func (q *TaskQueue) broadcastLocked() { old := q.notify; q.notify = make(chan struct{}); close(old) }
func cloneTask(t Task) Task           { t.Payload = bytes.Clone(t.Payload); return t }
func drainTimer(t *time.Timer) {
	select {
	case <-t.C:
	default:
	}
}
