package taskqueue

import (
	"container/heap"
	"time"
)

type taskState struct {
	task              Task
	nextAt            time.Time
	visibilityTimeout time.Duration
	inFlight          bool
	version           uint64
}

type scheduleItem struct {
	id      string
	due     time.Time
	version uint64
	seq     uint64
}

type scheduleHeap []*scheduleItem

func (h scheduleHeap) Len() int { return len(h) }
func (h scheduleHeap) Less(i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}
func (h scheduleHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *scheduleHeap) Push(x any)   { *h = append(*h, x.(*scheduleItem)) }
func (h *scheduleHeap) Pop() any {
	old := *h
	n := len(old)
	item := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	return item
}

func (q *TaskQueue) pushScheduleLocked(state *taskState) {
	due := state.nextAt
	if state.inFlight {
		due = state.task.Visibility
	}
	if due.IsZero() {
		due = time.Now()
	}
	q.sequence++
	heap.Push(&q.schedule, &scheduleItem{id: state.task.ID, due: due, version: state.version, seq: q.sequence})
}

func (q *TaskQueue) pruneScheduleLocked() {
	max := len(q.active)*3 + 1024
	if q.schedule.Len() <= max {
		return
	}
	fresh := make(scheduleHeap, 0, len(q.active))
	for _, state := range q.active {
		due := state.nextAt
		if state.inFlight {
			due = state.task.Visibility
		}
		if due.IsZero() {
			due = time.Now()
		}
		q.sequence++
		fresh = append(fresh, &scheduleItem{id: state.task.ID, due: due, version: state.version, seq: q.sequence})
	}
	q.schedule = fresh
	heap.Init(&q.schedule)
}
