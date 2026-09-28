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
	enqueues          *metrics.Counter
	dequeues          *metrics.Counter
	acks              *metrics.Counter
	nacks             *metrics.Counter
	extends           *metrics.Counter
	dlqAcks           *metrics.Counter
	dlqOverflow       *metrics.Counter
	taskStalls        *metrics.Counter
	dequeueParkEvents *metrics.Counter
	queueFullEvents   *metrics.Counter
	retryBudgetDrops  *metrics.Counter
	enqueueDur        *metrics.Histogram
	dequeueWait       *metrics.Histogram
	inFlightEnq       *metrics.Gauge
	parkedDeq         *metrics.Gauge
	active            *metrics.Gauge
	inFlight          *metrics.Gauge
	scheduled         *metrics.Gauge
	deadLetters       *metrics.Gauge
	uncheckpointed    *metrics.Gauge
	scheduleHeap      *metrics.Gauge
	checkpointLSN     *metrics.Gauge
	dirtyRecords      *metrics.Gauge
	checkpointLag     *metrics.Gauge
}

func newTaskMetrics(scope *metrics.Scope) *taskMetrics {
	return &taskMetrics{
		enqueues:          scope.Counter("enqueues"),
		dequeues:          scope.Counter("dequeues"),
		acks:              scope.Counter("acks"),
		nacks:             scope.Counter("nacks"),
		extends:           scope.Counter("extends"),
		dlqAcks:           scope.Counter("dlq_acks"),
		dlqOverflow:       scope.Counter("dlq_overflow"),
		taskStalls:        scope.Counter("task_stalls"),
		dequeueParkEvents: scope.Counter("dequeue_park_events"),
		queueFullEvents:   scope.Counter("queue_full_events"),
		retryBudgetDrops:  scope.Counter("retry_budget_drops"),
		enqueueDur:        scope.Histogram("enqueue_duration", taskDurationBounds),
		dequeueWait:       scope.Histogram("dequeue_wait", taskDurationBounds),
		inFlightEnq:       scope.Gauge("in_flight_enqueue"),
		parkedDeq:         scope.Gauge("parked_dequeue"),
		active:            scope.Gauge("active"),
		inFlight:          scope.Gauge("in_flight"),
		scheduled:         scope.Gauge("scheduled"),
		deadLetters:       scope.Gauge("dead_letters"),
		uncheckpointed:    scope.Gauge("uncheckpointed"),
		scheduleHeap:      scope.Gauge("schedule_heap"),
		checkpointLSN:     scope.Gauge("checkpoint_lsn"),
		dirtyRecords:      scope.Gauge("dirty"),
		checkpointLag:     scope.Gauge("checkpoint_lag"),
	}
}

// publish writes the occupancy view into the queue scope.
func (m *taskMetrics) publish(v metrics.TaskView) {
	m.active.Set(v.Active)
	m.inFlight.Set(v.InFlight)
	m.scheduled.Set(v.Scheduled)
	m.deadLetters.Set(v.DeadLetters)
	m.uncheckpointed.Set(v.Uncheckpointed)
	m.scheduleHeap.Set(v.ScheduleHeap)
	m.checkpointLSN.Set(v.CheckpointLSN)
	m.dirtyRecords.Set(v.Dirty)
	m.checkpointLag.Set(v.CheckpointLag)
}

// viewLocked returns the queue's current occupancy. Callers hold q.mu.
func (q *queue) viewLocked() metrics.TaskView {
	if q.closed {
		return metrics.TaskView{}
	}
	// checkpointCovered is only ever set to a value at or below scanNext,
	// so the lag subtraction cannot underflow.
	return metrics.TaskView{
		Active:         int64(len(q.active)),
		InFlight:       q.inFlightCount,
		Scheduled:      int64(len(q.active)) - q.inFlightCount,
		DeadLetters:    int64(len(q.dlq)),
		Uncheckpointed: int64(len(q.dlqUncheckpointed)),
		ScheduleHeap:   int64(len(q.schedule)),
		CheckpointLSN:  int64(q.checkpointOffset),
		Dirty:          int64(q.dirty),
		CheckpointLag:  int64(q.scanNext - q.checkpointCovered),
	}
}

// publishLocked mirrors driver state into the queue scope. Callers hold q.mu.
func (q *queue) publishLocked() {
	q.m.publish(q.viewLocked())
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

// recordStall counts a lease reclaimed after its lease expired.
func (q *queue) recordStall() {
	q.m.taskStalls.Inc()
}
