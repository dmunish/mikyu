package taskqueue

import "time"

type taskState struct {
	task              Task
	nextAt            time.Time
	lease time.Duration
	inFlight          bool
	version           uint64
}

type scheduleItem struct {
	id      string
	due     time.Time
	version uint64
	seq     uint64
}

// scheduleHeap is a binary min-heap ordered by (due, seq), mirroring
// container/heap semantics without exposing a Pop-named method.
type scheduleHeap []*scheduleItem

func schedLess(h scheduleHeap, i, j int) bool {
	if h[i].due.Equal(h[j].due) {
		return h[i].seq < h[j].seq
	}
	return h[i].due.Before(h[j].due)
}

// schedInit re-establishes the heap invariant over a freshly built slice.
func schedInit(h scheduleHeap) {
	for i := len(h)/2 - 1; i >= 0; i-- {
		schedDown(h, i, len(h))
	}
}

// schedPush inserts item and restores the heap invariant.
func schedPush(h *scheduleHeap, item *scheduleItem) {
	*h = append(*h, item)
	schedUp(*h, len(*h)-1)
}

// schedPop removes and returns the root, restoring the heap invariant.
func schedPop(h *scheduleHeap) *scheduleItem {
	old := *h
	last := len(old) - 1
	top := old[0]
	old[0] = old[last]
	old[last] = nil
	*h = old[:last]
	schedDown(*h, 0, last)
	return top
}

func schedUp(h scheduleHeap, j int) {
	for {
		i := (j - 1) / 2
		if i == j || !schedLess(h, j, i) {
			return
		}
		h[i], h[j] = h[j], h[i]
		j = i
	}
}

func schedDown(h scheduleHeap, i0, n int) {
	i := i0
	for {
		j1 := 2*i + 1
		if j1 < 0 || j1 >= n {
			return
		}
		j := j1
		if j2 := j1 + 1; j2 < n && schedLess(h, j2, j1) {
			j = j2
		}
		if !schedLess(h, j, i) {
			return
		}
		h[i], h[j] = h[j], h[i]
		i = j
	}
}

func (q *queue) pushScheduleLocked(state *taskState) {
	due := state.nextAt
	if state.inFlight {
		due = state.task.Lease
	}
	if due.IsZero() {
		due = time.Now()
	}
	q.sequence++
	schedPush(&q.schedule, &scheduleItem{id: state.task.ID, due: due, version: state.version, seq: q.sequence})
}

func (q *queue) pruneScheduleLocked() {
	max := len(q.active)*3 + 1024
	if len(q.schedule) <= max {
		return
	}
	fresh := make(scheduleHeap, 0, len(q.active))
	for _, state := range q.active {
		due := state.nextAt
		if state.inFlight {
			due = state.task.Lease
		}
		if due.IsZero() {
			due = time.Now()
		}
		q.sequence++
		fresh = append(fresh, &scheduleItem{id: state.task.ID, due: due, version: state.version, seq: q.sequence})
	}
	q.schedule = fresh
	schedInit(q.schedule)
}
