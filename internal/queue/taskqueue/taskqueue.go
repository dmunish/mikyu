package taskqueue

import (
	"context"
	"sync"
	"time"

	"github.com/dmunish/mikyu/internal/engine"
	"github.com/dmunish/mikyu/internal/metrics"
)

// Queue implements at-least-once task processing over engine.Storage.
type queue struct {
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

	// defaultLease and defaultMaxAttempts fall back for optionless enqueues; SetDefaults updates them live.
	defaultLease  time.Duration
	defaultMaxAttempts uint32

	// Bounds: 0 means unlimited.
	maxInFlight uint32
	maxDeadLetters      uint32

	// archiveLog creates or returns the archive log queue for source.
	archiveLog ArchiveLogFor

	m *taskMetrics
}

// Defaults carry the queue's task-queue defaults into the driver.
type Defaults struct {
	Lease  time.Duration
	MaxAttempts uint32
	MaxInFlight uint32
	MaxDeadLetters      uint32
}

// ArchiveAppender is the minimal interface for writing archive envelopes.
type ArchiveAppender interface {
	Append(ctx context.Context, payload []byte) (uint64, error)
}

// ArchiveLogFor creates or returns the archive log queue for a source queue.
type ArchiveLogFor func(ctx context.Context, source string) (ArchiveAppender, error)

var _ Queue = (*queue)(nil)

// Name returns the queue's name.
func (q *queue) Name() string { return q.name }

// New opens a task driver over existing engine storage with optional metrics scope.
func New(name string, storage engine.Storage, defaults Defaults, archiveLog ArchiveLogFor, scopes ...*metrics.Scope) (Queue, error) {
	if storage == nil {
		return nil, engine.ErrQueueNotFound
	}
	var scope *metrics.Scope
	if len(scopes) > 0 {
		scope = scopes[0]
	}
	lease := defaults.Lease
	if lease <= 0 {
		lease = defaultLease
	}
	maxAttempts := defaults.MaxAttempts
	if maxAttempts == 0 {
		maxAttempts = defaultMaxAttempts
	}
	q := &queue{
		name:               name,
		storage:            storage,
		active:             make(map[string]*taskState),
		dlq:                make(map[string]Task),
		dlqUncheckpointed:  make(map[string]struct{}),
		notify:             make(chan struct{}),
		stopCh:             make(chan struct{}),
		defaultLease:  lease,
		defaultMaxAttempts: maxAttempts,
		maxInFlight:        defaults.MaxInFlight,
		maxDeadLetters:             defaults.MaxDeadLetters,
		m:                  newTaskMetrics(scope),
	}
	if archiveLog != nil {
		q.archiveLog = archiveLog
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

// SetDefaults changes the fallback lease, attempts, and bounds for future enqueues.
func (q *queue) SetDefaults(d Defaults) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if d.Lease > 0 {
		q.defaultLease = d.Lease
	}
	if d.MaxAttempts > 0 {
		q.defaultMaxAttempts = d.MaxAttempts
	}
	q.maxInFlight = d.MaxInFlight
	q.maxDeadLetters = d.MaxDeadLetters
}

func (q *queue) Close() error {
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

func (q *queue) checkpointLoop() {
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
