package mikyu

import (
	"time"

	"github.com/dmunish/mikyu/internal/queue/logqueue"
	"github.com/dmunish/mikyu/internal/queue/taskqueue"
)

func WithTaskLease(d time.Duration) EnqueueOption { return taskqueue.WithLease(d) }

func WithTaskMaxAttempts(n uint32) EnqueueOption { return taskqueue.WithMaxAttempts(n) }

func WithKey(k []byte) EnqueueOption { return taskqueue.WithKey(k) }

func WithScheduledAt(at time.Time) EnqueueOption { return taskqueue.WithScheduledAt(at) }

func WithRetryAfter(d time.Duration) NackOption { return taskqueue.WithRetryAfter(d) }

func WithRecordKey(k []byte) AppendOption { return logqueue.WithKey(k) }
