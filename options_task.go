package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

// WithTaskLease sets how long this task stays hidden from other
// workers after a dequeue, overriding the queue's lease setting for
// this task alone. Long work wants a generous lease, or regular
// Extend calls.
func WithTaskLease(d time.Duration) EnqueueOption { return taskqueue.WithLease(d) }

// WithTaskMaxAttempts sets the delivery budget for this task,
// overriding the queue's max_attempts setting. A task past its budget
// lands in the dead-letter queue instead of being redelivered.
func WithTaskMaxAttempts(n uint32) EnqueueOption { return taskqueue.WithMaxAttempts(n) }

// WithKey associates a key with this task. It travels with the task
// through its whole life, including into the archive.
func WithKey(k []byte) EnqueueOption { return taskqueue.WithKey(k) }

// WithScheduledAt delays this task's first delivery until at. Use it
// for work that should not start immediately; Dequeue simply will not
// hand the task out before then.
func WithScheduledAt(at time.Time) EnqueueOption { return taskqueue.WithScheduledAt(at) }

// WithRetryAfter sets exactly when a nacked task becomes deliverable
// again, overriding the default exponential backoff. A zero delay
// makes it due immediately.
func WithRetryAfter(d time.Duration) NackOption { return taskqueue.WithRetryAfter(d) }

// WithRecordKey attaches a compaction key to one appended record.
// Records that share a key are versions of one thing: when a queue
// compacts with CompactionKeys, only the newest version of each key
// survives.
func WithRecordKey(k []byte) AppendOption { return logqueue.WithKey(k) }
