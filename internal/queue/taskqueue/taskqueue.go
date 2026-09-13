package taskqueue

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
)

// TaskQueue implements at-least-once task processing over engine.Storage.
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

	// defaultVisibility and defaultMaxAttempts fall back for optionless enqueues; SetDefaults updates them live.
	defaultVisibility  time.Duration
	defaultMaxAttempts uint32
}

// Defaults carry the queue's task-queue defaults into the driver.
type Defaults struct {
	Visibility  time.Duration
	MaxAttempts uint32
}

var _ ClientTaskQueue = (*TaskQueue)(nil)

// NewTaskQueue opens a task driver over an existing engine storage queue.
func NewTaskQueue(name string, storage engine.Storage, defaults Defaults) (*TaskQueue, error) {
	if storage == nil {
		return nil, engine.ErrQueueNotFound
	}
	visibility := defaults.Visibility
	if visibility <= 0 {
		visibility = defaultVisibilityTimeout
	}
	maxAttempts := defaults.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = defaultMaxAttempts
	}
	q := &TaskQueue{
		name:               name,
		storage:            storage,
		active:             make(map[string]*taskState),
		dlq:                make(map[string]Task),
		dlqUncheckpointed:  make(map[string]struct{}),
		notify:             make(chan struct{}),
		stopCh:             make(chan struct{}),
		defaultVisibility:  visibility,
		defaultMaxAttempts: maxAttempts,
	}
	schedInit(q.schedule)
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

// SetDefaults changes the fallback visibility and attempts for future enqueues.
func (q *TaskQueue) SetDefaults(d Defaults) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if d.Visibility > 0 {
		q.defaultVisibility = d.Visibility
	}
	if d.MaxAttempts > 0 {
		q.defaultMaxAttempts = d.MaxAttempts
	}
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
