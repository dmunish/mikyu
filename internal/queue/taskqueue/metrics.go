package taskqueue

import (
	"time"

	"github.com/dmunish/mikyu/internal/metrics"
)

// taskDurationBounds are latency buckets, in seconds, for task operations.
var taskDurationBounds = []float64{
	1e-6, 5e-6, 1e-5, 5e-5, 1e-4, 5e-4, 1e-3, 5e-3, 1e-2, 5e-2, 1e-1, 5e-1, 1,
}

// taskMetrics caches a task queue's instruments so hot paths avoid map lookups.
type taskMetrics struct {
	enqueues    *metrics.Counter
	dequeues    *metrics.Counter
	acks        *metrics.Counter
	nacks       *metrics.Counter
	extends     *metrics.Counter
	dlqAcks     *metrics.Counter
	enqueueDur  *metrics.Histogram
	dequeueWait *metrics.Histogram
}

func newTaskMetrics(scope *metrics.Scope) *taskMetrics {
	return &taskMetrics{
		enqueues:    scope.Counter("enqueues"),
		dequeues:    scope.Counter("dequeues"),
		acks:        scope.Counter("acks"),
		nacks:       scope.Counter("nacks"),
		extends:     scope.Counter("extends"),
		dlqAcks:     scope.Counter("dlq_acks"),
		enqueueDur:  scope.Histogram("enqueue_duration", taskDurationBounds),
		dequeueWait: scope.Histogram("dequeue_wait", taskDurationBounds),
	}
}

// recordEnqueue updates the enqueue counters and latency histogram.
func (q *queue) recordEnqueue(start time.Time) {
	q.m.enqueues.Inc()
	q.m.enqueueDur.Observe(time.Since(start).Seconds())
}

// recordDequeue updates the dequeue counter and the wait before delivery.
func (q *queue) recordDequeue(due time.Time) {
	q.m.dequeues.Inc()
	wait := time.Since(due).Seconds()
	if wait < 0 {
		wait = 0
	}
	q.m.dequeueWait.Observe(wait)
}
